import { Queryable } from './db.js';
import { EventDefinition } from './events.js';

export const OUTBOX_CHANNEL = 'outbox_events';

/**
 * Records an event in the outbox using the caller's connection. Call it inside the same
 * transaction as the state change it describes: either both are committed or neither is,
 * so an event can never be published for a change that was rolled back (or lost for one
 * that was committed).
 *
 * The NOTIFY is transactional too – listeners are woken only after the commit.
 */
export async function publish<TPayload>(
  db: Queryable,
  definition: EventDefinition<string, TPayload>,
  aggregateId: string,
  payload: TPayload,
): Promise<string> {
  const parsed = definition.schema.parse(payload);
  const { rows } = await db.query<{ id: string }>(
    `INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload)
     VALUES ($1, $2, $3, $4)
     RETURNING id`,
    [definition.aggregateType, aggregateId, definition.type, JSON.stringify(parsed)],
  );
  await db.query('SELECT pg_notify($1, $2)', [OUTBOX_CHANNEL, definition.type]);
  return rows[0]!.id;
}
