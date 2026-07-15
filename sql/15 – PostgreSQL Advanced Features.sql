-- 1. UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE customers_uuid (
    customer_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(150)
);

-- 2. ENUM
CREATE TYPE order_status_enum AS ENUM ('Pending', 'Completed', 'Cancelled', 'Shipped');

CREATE TABLE orders_enum (
    order_id SERIAL PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    status order_status_enum DEFAULT 'Pending'
);

-- 3. Trigger
CREATE OR REPLACE FUNCTION update_stock_on_sale()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE products
    SET stock = stock - NEW.quantity
    WHERE product_id = NEW.product_id;
    
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_update_stock
AFTER INSERT ON sales
FOR EACH ROW
EXECUTE FUNCTION update_stock_on_sale();

-- 4. Sequence
CREATE SEQUENCE custom_order_seq
START WITH 1000
INCREMENT BY 1
MINVALUE 1000
MAXVALUE 99999
CYCLE;

CREATE TABLE orders_with_seq (
    order_id INT DEFAULT nextval('custom_order_seq') PRIMARY KEY,
    customer_id INT,
    order_date DATE
);

-- 5. Partitioned Table
CREATE TABLE orders_partitioned (
    order_id SERIAL,
    customer_id INT,
    order_date DATE NOT NULL,
    total NUMERIC
) PARTITION BY RANGE (order_date);

CREATE TABLE orders_2024 PARTITION OF orders_partitioned
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE orders_2025 PARTITION OF orders_partitioned
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

-- 6. Inheritance
CREATE TABLE employees_base (
    employee_id SERIAL PRIMARY KEY,
    employee_name VARCHAR(100),
    salary NUMERIC
);

CREATE TABLE sales_employees (
    department VARCHAR(50)
) INHERITS (employees_base);

CREATE TABLE it_employees (
    skill_set TEXT[]
) INHERITS (employees_base);

-- 7. CTID (physical location)
SELECT ctid, * FROM customers ORDER BY ctid LIMIT 10;

-- 8. VACUUM
VACUUM customers;
VACUUM FULL customers;
VACUUM ANALYZE products;

-- 9. ANALYZE
ANALYZE customers;
ANALYZE products;

-- 10. EXPLAIN
EXPLAIN SELECT * FROM customers WHERE country = 'USA';

-- 11. EXPLAIN ANALYZE
EXPLAIN ANALYZE SELECT * FROM customers WHERE country = 'USA';

-- 12. Full Text Search
CREATE INDEX idx_customer_name_gin ON customers USING GIN (
    to_tsvector('english', first_name || ' ' || last_name)
);

SELECT * FROM customers
WHERE to_tsvector('english', first_name || ' ' || last_name) @@ to_tsquery('john');

-- 13. Table and Column Comments
COMMENT ON TABLE customers IS 'Contains customer information and demographics';
COMMENT ON COLUMN customers.email IS 'Customer email address for communication';

-- 14. Role management
CREATE ROLE sales_analyst LOGIN PASSWORD 'secure_pass';
GRANT SELECT ON sales TO sales_analyst;
GRANT SELECT ON orders TO sales_analyst;

-- 15. Schema management
CREATE SCHEMA reporting;
CREATE SCHEMA staging;

-- 16. Extension Management
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS dblink;

-- 17. Foreign Data Wrapper (example)
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

CREATE SERVER remote_server
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (host 'remote_host', dbname 'remote_db', port '5432');

-- 18. Audit Trail
CREATE TABLE audit_log (
    log_id SERIAL PRIMARY KEY,
    table_name VARCHAR(50),
    action VARCHAR(20),
    record_id INT,
    changed_by TEXT,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    old_data JSONB,
    new_data JSONB
);

CREATE OR REPLACE FUNCTION audit_trigger_function()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO audit_log (table_name, action, record_id, changed_by, old_data, new_data)
    VALUES (
        TG_TABLE_NAME,
        TG_OP,
        COALESCE(OLD.customer_id, NEW.customer_id),
        current_user,
        CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN row_to_json(OLD) ELSE NULL END,
        CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN row_to_json(NEW) ELSE NULL END
    );
    RETURN NEW;
END;
$$;

-- 19. Table Foreign Data Wrapper
CREATE FOREIGN TABLE remote_customers (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50)
)
SERVER remote_server
OPTIONS (schema_name 'public', table_name 'customers');

-- 20. Parallel Query
SET max_parallel_workers_per_gather = 4;
SET parallel_tuple_cost = 0.001;
SET parallel_setup_cost = 0.001;

SELECT COUNT(*) FROM sales
WHERE total > 100
AND EXTRACT(YEAR FROM sale_id::DATE) > 2023;