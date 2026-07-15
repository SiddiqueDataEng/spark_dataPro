-- 1. Simple index
CREATE INDEX idx_customer_id ON customers(customer_id);

-- 2. Composite index
CREATE INDEX idx_orders_customer_store ON orders(customer_id, store_id);

-- 3. Unique index
CREATE UNIQUE INDEX idx_unique_email ON customers(email);

-- 4. Partial index
CREATE INDEX idx_active_orders ON orders(order_id) 
WHERE status = 'Completed';

-- 5. Expression index
CREATE INDEX idx_lower_email ON customers(LOWER(email));

-- 6. Function-based index
CREATE INDEX idx_order_date_year ON orders(EXTRACT(YEAR FROM order_date));

-- 7. Multicolumn partial index
CREATE INDEX idx_high_value_sales ON sales(order_id, total) 
WHERE total > 1000;

-- 8. Index with INCLUDE
CREATE INDEX idx_customers_name ON customers(first_name, last_name) 
INCLUDE (email, city);

-- 9. Hash index
CREATE INDEX idx_customer_email_hash ON customers USING HASH (email);

-- 10. BRIN index
CREATE INDEX idx_orders_date_brin ON orders USING BRIN (order_date);

-- 11. Concurrent index creation
CREATE INDEX CONCURRENTLY idx_sales_total ON sales(total);

-- 12. Drop index
DROP INDEX IF EXISTS idx_old_index;

-- 13. List all indexes
SELECT 
    tablename,
    indexname,
    indexdef 
FROM pg_indexes 
WHERE schemaname = 'public';

-- 14. Check index usage
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;

-- 15. Reindex
REINDEX INDEX idx_customer_id;