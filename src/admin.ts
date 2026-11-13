import { Queryable } from './db.js';

export interface DeadLetter {
  id: string;
  eventType: string;
  aggregateId: string;
  attempts: number;
  lastError: string | null;
  deadLetteredAt: Date;
}

export async function listDeadLetters(db: Queryable, limit = 100): Promise<DeadLetter[]> {
  const { rows } = await db.query(
    `SELECT id, event_type AS "eventType", aggregate_id AS "aggregateId", attempts,
            last_error AS "lastError", dead_lettered_at AS "deadLetteredAt"
       FROM outbox_events
      WHERE dead_lettered_at IS NOT NULL
      ORDER BY dead_lettered_at DESC
      LIMIT $1`,
    [limit],
  );
  return rows;
}

/**
 * Puts dead-lettered events back in the queue, typically after deploying a fix.
 * Consumers that already succeeded are still skipped thanks to `processed_events`.
 */
export async function requeueDeadLetters(db: Queryable, ids: string[]): Promise<number> {
  const result = await db.query(
    `UPDATE outbox_events
        SET dead_lettered_at = NULL, attempts = 0, last_error = NULL, available_at = now()
      WHERE id = ANY($1::uuid[]) AND dead_lettered_at IS NOT NULL`,
    [ids],
  );
  return result.rowCount ?? 0;
}
