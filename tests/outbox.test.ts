import { afterAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod';
import {
  Dispatcher,
  OutboxWorker,
  defineEvent,
  listDeadLetters,
  publish,
  requeueDeadLetters,
  retryDelay,
  withTransaction,
} from '../src/index.js';
import { OrderPlaced, placeOrder, registerOrderConsumers } from '../examples/orders/domain.js';
import { createPool, eventRow, fastForward, resetDatabase, silentLogger } from './helpers.js';

const pool = createPool();
const Pinged = defineEvent('ping', 'test', z.object({ n: z.number() }));

beforeEach(async () => {
  await resetDatabase(pool);
  await pool.query(`INSERT INTO inventory (sku, available) VALUES ('BOOK-1', 10)`);
});

afterAll(async () => {
  await pool.end();
});

const newDispatcher = (options = {}) =>
  new Dispatcher(pool, { baseDelayMs: 10, maxAttempts: 3, logger: silentLogger, ...options });

describe('publishing', () => {
  it('commits the event together with the state change', async () => {
    const orderId = await placeOrder(pool, { sku: 'BOOK-1', quantity: 1, email: 'a@example.com' });
    const { rows } = await pool.query('SELECT * FROM outbox_events');
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({ event_type: 'order.placed', aggregate_id: orderId });
  });

  it('discards the event when the transaction rolls back', async () => {
    await expect(
      withTransaction(pool, async (tx) => {
        await publish(tx, Pinged, 'x', { n: 1 });
        throw new Error('business rule violated');
      }),
    ).rejects.toThrow('business rule violated');
    const { rowCount } = await pool.query('SELECT 1 FROM outbox_events');
    expect(rowCount).toBe(0);
  });

  it('rejects payloads that do not match the event schema', async () => {
    await expect(
      publish(pool, Pinged, 'x', { n: 'one' } as unknown as { n: number }),
    ).rejects.toThrow();
  });
});

describe('dispatching', () => {
  it('delivers events to every consumer and marks them processed', async () => {
    const mailer = { send: vi.fn().mockResolvedValue(undefined) };
    const dispatcher = registerOrderConsumers(newDispatcher(), mailer);
    const orderId = await placeOrder(pool, { sku: 'BOOK-1', quantity: 2, email: 'a@example.com' });

    expect(await dispatcher.drain()).toBe(1);

    const { rows: stock } = await pool.query(`SELECT available FROM inventory`);
    expect(stock[0].available).toBe(8);
    const { rows: order } = await pool.query('SELECT status FROM orders WHERE id = $1', [orderId]);
    expect(order[0].status).toBe('reserved');
    expect(mailer.send).toHaveBeenCalledWith(
      'a@example.com',
      expect.stringContaining(orderId),
      expect.any(String),
    );
    expect(await dispatcher.drain()).toBe(0);
  });

  it('retries only the consumer that failed', async () => {
    const mailer = {
      send: vi.fn().mockRejectedValueOnce(new Error('SMTP timeout')).mockResolvedValue(undefined),
    };
    const dispatcher = registerOrderConsumers(newDispatcher(), mailer);
    await placeOrder(pool, { sku: 'BOOK-1', quantity: 1, email: 'a@example.com' });

    expect(await dispatcher.dispatchNext()).toBe('retry_scheduled');
    const [event] = (await pool.query('SELECT * FROM outbox_events')).rows;
    expect(event).toMatchObject({ attempts: 1, last_error: 'confirmation-email: SMTP timeout' });
    expect(event.available_at.getTime()).toBeGreaterThan(Date.now() - 1000);

    // The retry is not due yet...
    expect(await dispatcher.dispatchNext()).toBeNull();

    await fastForward(pool);
    expect(await dispatcher.dispatchNext()).toBe('processed');

    // ...and inventory was reserved exactly once despite two deliveries.
    const { rows } = await pool.query('SELECT available FROM inventory');
    expect(rows[0].available).toBe(9);
    expect(mailer.send).toHaveBeenCalledTimes(2);
  });

  it('rolls back the writes of a failing consumer', async () => {
    const dispatcher = newDispatcher().on(Pinged, 'writer', async (_event, tx) => {
      await tx.query(`UPDATE inventory SET available = 0`);
      throw new Error('fails after writing');
    });
    await publish(pool, Pinged, 'x', { n: 1 });
    await dispatcher.dispatchNext();
    const { rows } = await pool.query('SELECT available FROM inventory');
    expect(rows[0].available).toBe(10);
  });

  it('dead-letters an event after the maximum attempts and can requeue it', async () => {
    let healthy = false;
    const dispatcher = newDispatcher({ maxAttempts: 3 }).on(Pinged, 'flaky', async () => {
      if (!healthy) throw new Error('downstream unavailable');
    });
    const id = await publish(pool, Pinged, 'x', { n: 1 });

    const outcomes = [];
    for (let i = 0; i < 3; i++) {
      await fastForward(pool);
      outcomes.push(await dispatcher.dispatchNext());
    }
    expect(outcomes).toEqual(['retry_scheduled', 'retry_scheduled', 'dead_lettered']);

    await fastForward(pool);
    expect(await dispatcher.dispatchNext()).toBeNull();
    const [deadLetter] = await listDeadLetters(pool);
    expect(deadLetter).toMatchObject({
      id,
      attempts: 3,
      lastError: 'flaky: downstream unavailable',
    });

    healthy = true;
    expect(await requeueDeadLetters(pool, [id])).toBe(1);
    expect(await dispatcher.dispatchNext()).toBe('processed');
    expect((await eventRow(pool, id)).processed_at).not.toBeNull();
  });

  it('dead-letters events whose stored payload no longer matches the schema', async () => {
    const dispatcher = newDispatcher({ maxAttempts: 1 }).on(Pinged, 'consumer', async () => {});
    const id = await publish(pool, Pinged, 'x', { n: 1 });
    await pool.query(`UPDATE outbox_events SET payload = '{"n": "bad"}' WHERE id = $1`, [id]);
    expect(await dispatcher.dispatchNext()).toBe('dead_lettered');
  });

  it('marks events without consumers as processed', async () => {
    const id = await publish(pool, Pinged, 'x', { n: 1 });
    expect(await newDispatcher().dispatchNext()).toBe('processed');
    expect((await eventRow(pool, id)).processed_at).not.toBeNull();
  });

  it('rejects duplicate consumer names for the same event', () => {
    const dispatcher = newDispatcher().on(OrderPlaced, 'inventory', async () => {});
    expect(() => dispatcher.on(OrderPlaced, 'inventory', async () => {})).toThrow();
  });
});

describe('concurrency', () => {
  it('never delivers an event to the same consumer twice across competing dispatchers', async () => {
    const deliveries = new Map<string, number>();
    const dispatchers = Array.from({ length: 6 }, () =>
      newDispatcher().on(Pinged, 'counter', async (event) => {
        deliveries.set(event.id, (deliveries.get(event.id) ?? 0) + 1);
        await new Promise((resolve) => setTimeout(resolve, Math.random() * 5));
      }),
    );

    for (let i = 0; i < 300; i++) await publish(pool, Pinged, String(i), { n: i });

    const handled = await Promise.all(dispatchers.map((d) => d.drain(1_000)));
    expect(handled.reduce((a, b) => a + b, 0)).toBe(300);
    expect(handled.filter((n) => n > 0).length).toBeGreaterThan(1);
    expect(deliveries.size).toBe(300);
    expect([...deliveries.values()].every((count) => count === 1)).toBe(true);
  });
});

describe('OutboxWorker', () => {
  it('wakes on NOTIFY and processes events published after it started', async () => {
    const seen: number[] = [];
    const dispatcher = newDispatcher().on(Pinged, 'collector', async (event) => {
      seen.push(event.payload.n);
    });
    // A long poll interval proves the wake-up comes from the notification.
    const worker = new OutboxWorker(pool, dispatcher, { pollIntervalMs: 60_000 });
    await worker.start();
    try {
      await new Promise((resolve) => setTimeout(resolve, 100));
      await publish(pool, Pinged, 'x', { n: 42 });
      await vi.waitFor(() => expect(seen).toEqual([42]), { timeout: 3_000, interval: 20 });
    } finally {
      await worker.stop();
    }
  });
});

describe('retryDelay', () => {
  it('doubles per attempt up to the cap', () => {
    const options = { baseDelayMs: 1_000, maxDelayMs: 5_000 };
    expect([1, 2, 3, 4, 5].map((n) => retryDelay(n, options))).toEqual([
      1_000, 2_000, 4_000, 5_000, 5_000,
    ]);
  });
});
