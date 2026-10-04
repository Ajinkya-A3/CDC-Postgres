-- Same schema as the source, but empty. No foreign keys on purpose.

CREATE TABLE public.customers (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.products (
    product_id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.orders (
    order_id UUID PRIMARY KEY,
    customer_id UUID NOT NULL,
    total NUMERIC(12,2) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.order_items (
    order_id UUID NOT NULL,
    product_id UUID NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

-- Stays BIGINT on purpose: scripts/cutover.sh writes an epoch number here, not an entity id.
CREATE TABLE public.cutover_marker (
    id BIGINT PRIMARY KEY,
    note TEXT
);