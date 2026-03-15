-- Enable pg_stat_statements (shared_preload_libraries must be set at server start)
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Enable function call tracking so pg_stat_user_functions has data
ALTER SYSTEM SET track_functions = 'all';
SELECT pg_reload_conf();

-- Schema: two tables with indexes and constraints
CREATE TABLE customers (
    id serial PRIMARY KEY,
    name text NOT NULL,
    email text NOT NULL,
    UNIQUE(email)
);

CREATE TABLE orders (
    id serial PRIMARY KEY,
    customer_id integer NOT NULL,
    total numeric(10,2) NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_customers_name ON customers(name);

-- Seed data for pg_stats / pg_statistic (needs enough rows for ANALYZE)
INSERT INTO customers (name, email)
SELECT 'customer_' || i, 'customer_' || i || '@example.com'
FROM generate_series(1, 200) AS i;

INSERT INTO orders (customer_id, total, created_at)
SELECT
    (i % 200) + 1,
    (random() * 1000)::numeric(10,2),
    now() - (random() * interval '365 days')
FROM generate_series(1, 1000) AS i;

-- A tracked function for pg_stat_user_functions
CREATE FUNCTION public.test_func(x integer) RETURNS integer
LANGUAGE sql AS $$ SELECT x * 2; $$;

SELECT public.test_func(i) FROM generate_series(1, 10) AS i;

-- Force statistics collection
ANALYZE;

-- Touch indexes to generate I/O stats
SELECT count(*) FROM orders WHERE customer_id = 1;
SELECT count(*) FROM customers WHERE email = 'customer_1@example.com';
