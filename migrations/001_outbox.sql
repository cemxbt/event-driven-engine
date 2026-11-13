CREATE TABLE IF NOT EXISTS outbox_events (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  text        NOT NULL,
    aggregate_id    text        NOT NULL,
    event_type      text        NOT NULL,
    payload         jsonb       NOT NULL,
    occurred_at     timestamptz NOT NULL DEFAULT now(),
    available_at    timestamptz NOT NULL DEFAULT now(),
    attempts        integer     NOT NULL DEFAULT 0,
    last_error      text,
    processed_at    timestamptz,
    dead_lettered_at timestamptz
);

-- Workers only ever scan pending events, so the index covers exactly that set.
CREATE INDEX IF NOT EXISTS outbox_events_pending_idx
    ON outbox_events (available_at, occurred_at)
    WHERE processed_at IS NULL AND dead_lettered_at IS NULL;

CREATE INDEX IF NOT EXISTS outbox_events_aggregate_idx
    ON outbox_events (aggregate_type, aggregate_id, occurred_at);

-- One row per (consumer, event) that has been handled successfully. This is what makes
-- consumers idempotent: a redelivered event skips consumers that already succeeded.
CREATE TABLE IF NOT EXISTS processed_events (
    consumer     text        NOT NULL,
    event_id     uuid        NOT NULL REFERENCES outbox_events (id) ON DELETE CASCADE,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
