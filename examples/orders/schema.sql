CREATE TABLE IF NOT EXISTS orders (
    id         uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    sku        text        NOT NULL,
    quantity   integer     NOT NULL CHECK (quantity > 0),
    email      text        NOT NULL,
    status     text        NOT NULL DEFAULT 'placed',
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS inventory (
    sku       text    PRIMARY KEY,
    available integer NOT NULL CHECK (available >= 0)
);
