-- Runs ONCE, only when the source data volume is empty.

-- Debezium user: REPLICATION lets it open a replication connection and create the slot.
CREATE USER debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
GRANT CONNECT ON DATABASE migration TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;

CREATE TABLE public.customers (
    id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.products (
    product_id BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    price NUMERIC(12,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE public.orders (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    total NUMERIC(12,2) NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Composite primary key
CREATE TABLE public.order_items (
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    price NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (order_id, product_id)
);

-- Sentinel table used by scripts/cutover.sh
CREATE TABLE public.cutover_marker (
    id BIGINT PRIMARY KEY,
    note TEXT
);

INSERT INTO public.customers (id, name, email) VALUES
    (1, 'Ajinkya', 'ajinkya@example.com'),
    (2, 'Atlas', 'atlas@example.com');

INSERT INTO public.products (product_id, name, price) VALUES
    (101, 'Laptop', 75000.00),
    (102, 'Keyboard', 2500.00);

INSERT INTO public.orders (order_id, customer_id, total, status) VALUES
    (1001, 1, 77500.00, 'PAID'),
    (1002, 2, 2500.00, 'PENDING');

INSERT INTO public.order_items (order_id, product_id, quantity, price) VALUES
    (1001, 101, 1, 75000.00),
    (1001, 102, 1, 2500.00),
    (1002, 102, 1, 2500.00);

-- Snapshot permissions (granted AFTER the tables exist)
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON SEQUENCES TO debezium;

-- FOR ALL TABLES needs superuser, which is why it is created here
-- (init scripts run as the postgres superuser) and not by Debezium.
-- publish_via_partition_root = true: if a table is ever partitioned, its changes
-- are published under the ROOT table's name. Requires PostgreSQL 13+.
CREATE PUBLICATION dbz_publication
FOR ALL TABLES
WITH (publish_via_partition_root = true);