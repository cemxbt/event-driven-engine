# Event-Driven Engine

[![CI](https://github.com/cemxbt/event-driven-engine/actions/workflows/ci.yml/badge.svg)](https://github.com/cemxbt/event-driven-engine/actions/workflows/ci.yml)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![TypeScript](https://img.shields.io/badge/TypeScript-5-3178C6?logo=typescript&logoColor=white)
![License](https://img.shields.io/badge/license-MIT-blue)

A small, dependency-light implementation of the **transactional outbox** pattern on
PostgreSQL, written in TypeScript with plain SQL through `pg`. It solves the dual-write
problem — saving state *and* notifying other parts of the system without ever doing one
without the other — and delivers events with retries, idempotent consumers and
dead-lettering.

## The problem

```ts
await db.orders.insert(order);
await queue.publish('order.placed', order); // process crashes here → order exists, nobody knows
```

Writing to the database and publishing to a broker are two separate operations; a crash or
network error between them leaves the system inconsistent. With an outbox, the event is written
to a table **in the same transaction** as the state change, and a dispatcher delivers it
afterwards.

```
┌──────────────── one transaction ────────────────┐
│ INSERT INTO orders ...                           │      ┌────────────┐
│ INSERT INTO outbox_events ...  + NOTIFY          │ ───▶ │ dispatcher │ ─▶ consumers
└──────────────────────────────────────────────────┘      └────────────┘
```

## Features

- **Atomic publishing** — `publish(tx, Event, id, payload)` joins the caller's transaction;
  a rollback discards the event too
- **Typed, validated events** — each event is defined with a Zod schema; payloads are checked
  when published and again when delivered
- **Concurrent dispatchers** — events are claimed with `FOR UPDATE SKIP LOCKED`, so any number
  of workers can share the queue without delivering an event twice
- **Idempotent consumers** — each consumer runs in a savepoint and its writes commit together
  with a `processed_events` marker, so a redelivered event skips consumers that already
  succeeded
- **Retries with exponential backoff** for the consumers that failed, then **dead-lettering**
  after a configurable number of attempts, with helpers to inspect and requeue
- **Low latency without busy polling** — workers `LISTEN` for a `NOTIFY` sent on commit, with
  polling as a safety net for missed notifications and scheduled retries
- **Migrations** with an advisory lock so several instances can start at once

## Usage

```ts
import pg from 'pg';
import { z } from 'zod';
import { Dispatcher, OutboxWorker, defineEvent, publish, withTransaction } from './src';

const pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });

const OrderPlaced = defineEvent(
  'order.placed',
  'order',
  z.object({ orderId: z.string().uuid(), sku: z.string(), quantity: z.number().int() }),
);

// Write state and event atomically
await withTransaction(pool, async (tx) => {
  const { rows } = await tx.query('INSERT INTO orders (...) VALUES (...) RETURNING id', [...]);
  await publish(tx, OrderPlaced, rows[0].id, { orderId: rows[0].id, sku: 'BOOK-1', quantity: 1 });
});

// Consume
const dispatcher = new Dispatcher(pool, { maxAttempts: 5, baseDelayMs: 1_000 })
  .on(OrderPlaced, 'inventory', async (event, tx) => {
    // Writes through `tx` commit together with the "processed" marker.
    await tx.query('UPDATE inventory SET available = available - $2 WHERE sku = $1', [
      event.payload.sku,
      event.payload.quantity,
    ]);
  })
  .on(OrderPlaced, 'confirmation-email', async (event) => {
    await mailer.send(/* ... */, { idempotencyKey: event.id });
  });

const worker = new OutboxWorker(pool, dispatcher);
await worker.start();
```

## Running the example

[`examples/orders`](examples/orders) places four orders against a stock of three, with two
workers competing for events. Three orders are reserved, the fourth is retried and finally
dead-lettered, and each confirmation email is sent exactly once.

```bash
docker compose up -d
cp .env.example .env
npm install
npm run migrate
npm run example
```

## Delivery guarantees

| Effect | Guarantee |
|---|---|
| Event recorded | Exactly when the business transaction commits |
| Consumer writes through `tx` | Exactly once per consumer |
| External side effects (HTTP, email) | At least once — pass `event.id` as an idempotency key |
| Ordering | Not guaranteed across events, by design (see below) |

## Design decisions and trade-offs

- **One event per transaction.** Claiming and handling a single row keeps lock time short and
  makes failures independent. Batching would raise throughput at the cost of coarser retries.
- **Per-consumer progress.** Recording success per consumer instead of per event means one
  broken consumer does not cause the others to repeat their work on every retry.
- **No ordering guarantee.** `SKIP LOCKED` lets workers process in parallel, so two events for
  the same aggregate can be handled out of order. Consumers that need ordering should check
  versions, or the claim query can be restricted to the oldest pending event per aggregate.
- **PostgreSQL as the queue.** This avoids running a separate broker and gives transactional
  guarantees for free. At very high volume, the outbox would instead be relayed to Kafka or a
  similar system, using the same table as the source.
- **Retention.** Processed events are kept for auditing; production deployments should archive
  or delete them periodically.

## Project structure

```
src/
├── outbox.ts      publish() – atomic event recording
├── dispatcher.ts  claiming, consumer execution, retries, dead-lettering
├── worker.ts      LISTEN/NOTIFY + polling loop
├── admin.ts       inspect and requeue dead letters
├── events.ts      typed event definitions
└── migrate.ts     SQL migrations with an advisory lock
migrations/        outbox and processed_events tables
examples/orders/   order placement with inventory and email consumers
tests/             integration tests against PostgreSQL
```

## Testing

```bash
docker compose up -d
npm test
```

The test suite covers atomicity, retries of only the failed consumer, rollback of a failing
consumer's writes, dead-lettering and requeueing, schema drift, `NOTIFY` wake-ups, and six
competing dispatchers processing 300 events with every event delivered exactly once.

## License

[MIT](LICENSE)
