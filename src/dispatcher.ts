import pg from 'pg';
import { retryDelay } from './backoff.js';
import { EventDefinition, EventEnvelope } from './events.js';

export type Handler<TPayload> = (
  event: EventEnvelope<TPayload>,
  tx: pg.PoolClient,
) => Promise<void>;

interface Subscription {
  consumer: string;
  definition: EventDefinition;
  handler: Handler<unknown>;
}

export interface DispatcherOptions {
  /** Deliveries before an event is dead-lettered. */
  maxAttempts?: number;
  baseDelayMs?: number;
  maxDelayMs?: number;
  logger?: Pick<Console, 'warn' | 'error'>;
}

export type DispatchOutcome = 'processed' | 'retry_scheduled' | 'dead_lettered';

interface OutboxRow {
  id: string;
  aggregate_type: string;
  aggregate_id: string;
  event_type: string;
  payload: unknown;
  occurred_at: Date;
  attempts: number;
}

/**
 * Delivers outbox events to registered consumers.
 *
 * Each event is handled in its own transaction:
 * 1. the row is claimed with `FOR UPDATE SKIP LOCKED`, so concurrent dispatchers never
 *    receive the same event;
 * 2. every consumer runs inside a savepoint, receives the transaction and can write its
 *    own state through it; a consumer's writes and its `processed_events` marker are
 *    committed together, so a consumer that succeeded is never run again for that event;
 * 3. if any consumer failed, only the failed ones are retried later with exponential
 *    backoff, and the event is dead-lettered after `maxAttempts` deliveries.
 */
export class Dispatcher {
  private readonly subscriptions = new Map<string, Subscription[]>();
  private readonly maxAttempts: number;
  private readonly backoff: { baseDelayMs: number; maxDelayMs: number };
  private readonly logger: Pick<Console, 'warn' | 'error'>;

  constructor(
    private readonly pool: pg.Pool,
    options: DispatcherOptions = {},
  ) {
    this.maxAttempts = options.maxAttempts ?? 5;
    this.backoff = {
      baseDelayMs: options.baseDelayMs ?? 1_000,
      maxDelayMs: options.maxDelayMs ?? 5 * 60_000,
    };
    this.logger = options.logger ?? console;
  }

  /** Registers a consumer. Consumer names must be stable: they key idempotency records. */
  on<TPayload>(
    definition: EventDefinition<string, TPayload>,
    consumer: string,
    handler: Handler<TPayload>,
  ): this {
    const list = this.subscriptions.get(definition.type) ?? [];
    if (list.some((s) => s.consumer === consumer)) {
      throw new Error(`Consumer "${consumer}" is already subscribed to ${definition.type}`);
    }
    list.push({ consumer, definition, handler: handler as Handler<unknown> });
    this.subscriptions.set(definition.type, list);
    return this;
  }

  /** Handles up to `limit` due events and returns how many were claimed. */
  async drain(limit = 100): Promise<number> {
    let handled = 0;
    while (handled < limit && (await this.dispatchNext()) !== null) handled++;
    return handled;
  }

  /** Claims and handles one due event. Returns null when nothing is due. */
  async dispatchNext(): Promise<DispatchOutcome | null> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const { rows } = await client.query<OutboxRow>(
        `SELECT id, aggregate_type, aggregate_id, event_type, payload, occurred_at, attempts
           FROM outbox_events
          WHERE processed_at IS NULL
            AND dead_lettered_at IS NULL
            AND available_at <= now()
          ORDER BY available_at, occurred_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED`,
      );
      const row = rows[0];
      if (!row) {
        await client.query('COMMIT');
        return null;
      }

      const failures = await this.runConsumers(client, row);
      const outcome = await this.settle(client, row, failures);
      await client.query('COMMIT');
      return outcome;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async runConsumers(client: pg.PoolClient, row: OutboxRow) {
    const subscriptions = this.subscriptions.get(row.event_type) ?? [];
    const { rows: done } = await client.query<{ consumer: string }>(
      'SELECT consumer FROM processed_events WHERE event_id = $1',
      [row.id],
    );
    const completed = new Set(done.map((d) => d.consumer));
    const failures: { consumer: string; error: string }[] = [];

    for (const { consumer, definition, handler } of subscriptions) {
      if (completed.has(consumer)) continue;

      await client.query('SAVEPOINT consumer');
      try {
        const envelope: EventEnvelope = {
          id: row.id,
          type: row.event_type,
          aggregateType: row.aggregate_type,
          aggregateId: row.aggregate_id,
          payload: definition.schema.parse(row.payload),
          occurredAt: row.occurred_at,
          attempts: row.attempts,
        };
        await handler(envelope, client);
        await client.query('INSERT INTO processed_events (consumer, event_id) VALUES ($1, $2)', [
          consumer,
          row.id,
        ]);
        await client.query('RELEASE SAVEPOINT consumer');
      } catch (error) {
        await client.query('ROLLBACK TO SAVEPOINT consumer');
        const message = error instanceof Error ? error.message : String(error);
        failures.push({ consumer, error: message });
        this.logger.warn(`[outbox] ${consumer} failed on ${row.event_type} ${row.id}: ${message}`);
      }
    }
    return failures;
  }

  private async settle(
    client: pg.PoolClient,
    row: OutboxRow,
    failures: { consumer: string; error: string }[],
  ): Promise<DispatchOutcome> {
    if (failures.length === 0) {
      await client.query('UPDATE outbox_events SET processed_at = now() WHERE id = $1', [row.id]);
      return 'processed';
    }

    const attempts = row.attempts + 1;
    const lastError = failures
      .map((f) => `${f.consumer}: ${f.error}`)
      .join('; ')
      .slice(0, 2000);

    if (attempts >= this.maxAttempts) {
      await client.query(
        `UPDATE outbox_events
            SET attempts = $2, last_error = $3, dead_lettered_at = now()
          WHERE id = $1`,
        [row.id, attempts, lastError],
      );
      this.logger.error(
        `[outbox] ${row.event_type} ${row.id} dead-lettered after ${attempts} attempts`,
      );
      return 'dead_lettered';
    }

    await client.query(
      `UPDATE outbox_events
          SET attempts = $2,
              last_error = $3,
              available_at = now() + make_interval(secs => $4::double precision / 1000)
        WHERE id = $1`,
      [row.id, attempts, lastError, retryDelay(attempts, this.backoff)],
    );
    return 'retry_scheduled';
  }
}
