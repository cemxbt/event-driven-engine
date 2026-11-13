import pg from 'pg';
import { z } from 'zod';
import { Dispatcher, defineEvent, publish, withTransaction } from '../../src/index.js';

export const OrderPlaced = defineEvent(
  'order.placed',
  'order',
  z.object({
    orderId: z.string().uuid(),
    sku: z.string(),
    quantity: z.number().int().positive(),
    email: z.string().email(),
  }),
);

export interface PlaceOrderInput {
  sku: string;
  quantity: number;
  email: string;
}

/** Writes the order and its event in one transaction. */
export async function placeOrder(pool: pg.Pool, input: PlaceOrderInput): Promise<string> {
  return withTransaction(pool, async (tx) => {
    const { rows } = await tx.query<{ id: string }>(
      'INSERT INTO orders (sku, quantity, email) VALUES ($1, $2, $3) RETURNING id',
      [input.sku, input.quantity, input.email],
    );
    const orderId = rows[0]!.id;
    await publish(tx, OrderPlaced, orderId, { orderId, ...input });
    return orderId;
  });
}

/** Stand-in for an email provider: an external side effect outside the database. */
export interface Mailer {
  send(to: string, subject: string, idempotencyKey: string): Promise<void>;
}

export function registerOrderConsumers(dispatcher: Dispatcher, mailer: Mailer) {
  dispatcher.on(OrderPlaced, 'inventory', async (event, tx) => {
    const { sku, quantity, orderId } = event.payload;
    const reserved = await tx.query(
      `UPDATE inventory SET available = available - $2
        WHERE sku = $1 AND available >= $2`,
      [sku, quantity],
    );
    if (reserved.rowCount === 0) throw new Error(`Insufficient stock for ${sku}`);
    await tx.query(`UPDATE orders SET status = 'reserved' WHERE id = $1`, [orderId]);
  });

  dispatcher.on(OrderPlaced, 'confirmation-email', async (event) => {
    // The event id doubles as an idempotency key for providers that support one.
    await mailer.send(event.payload.email, `Order ${event.payload.orderId} received`, event.id);
  });

  return dispatcher;
}
