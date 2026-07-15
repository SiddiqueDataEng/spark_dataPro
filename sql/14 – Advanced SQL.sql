-- 1. Pivot with CASE
SELECT 
    category,
    SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2024 THEN total ELSE 0 END) AS sales_2024,
    SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2025 THEN total ELSE 0 END) AS sales_2025,
    SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2026 THEN total ELSE 0 END) AS sales_2026
FROM sales
JOIN orders USING(order_id)
JOIN products USING(product_id)
GROUP BY category;

-- 2. Pivot with crosstab (requires tablefunc)
CREATE EXTENSION IF NOT EXISTS tablefunc;

SELECT * FROM crosstab(
    'SELECT category, EXTRACT(YEAR FROM order_date) AS year, SUM(total) 
     FROM sales s
     JOIN orders o ON s.order_id = o.order_id
     JOIN products p ON s.product_id = p.product_id
     GROUP BY category, year
     ORDER BY 1,2',
    'SELECT DISTINCT EXTRACT(YEAR FROM order_date) FROM orders ORDER BY 1'
) AS ct(category TEXT, y2024 NUMERIC, y2025 NUMERIC, y2026 NUMERIC);

-- 3. Unpivot with UNION ALL
SELECT category, '2024' AS year, sales_2024 AS sales
FROM category_sales
UNION ALL
SELECT category, '2025', sales_2025
FROM category_sales
UNION ALL
SELECT category, '2026', sales_2026
FROM category_sales;

-- 4. Recursive Query (Category Tree)
WITH RECURSIVE category_tree AS (
    SELECT 
        category_id,
        category_name,
        parent_id,
        1 AS level
    FROM categories
    WHERE parent_id IS NULL
    
    UNION ALL
    
    SELECT 
        c.category_id,
        c.category_name,
        c.parent_id,
        ct.level + 1
    FROM categories c
    INNER JOIN category_tree ct ON c.parent_id = ct.category_id
)
SELECT * FROM category_tree;

-- 5. Dynamic SQL
CREATE OR REPLACE FUNCTION dynamic_search(table_name TEXT, search_column TEXT, search_value TEXT)
RETURNS SETOF RECORD
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
        SELECT * FROM %I WHERE %I = %L
    ', table_name, search_column, search_value);
END;
$$;

-- 6. JSON Operations
SELECT 
    product_id,
    product_name,
    json_build_object(
        'name', product_name,
        'category', category,
        'price', selling_price,
        'stock', stock
    ) AS product_json
FROM products;

-- 7. JSONB Operations
SELECT 
    product_id,
    product_name,
    jsonb_build_object(
        'id', product_id,
        'info', jsonb_build_object(
            'name', product_name,
            'category', category,
            'price', selling_price
        ),
        'inventory', jsonb_build_object(
            'stock', stock,
            'available', stock > 0
        )
    ) AS product_details
FROM products;

-- 8. LATERAL JOIN
SELECT 
    c.customer_id,
    c.first_name,
    recent_orders.order_id,
    recent_orders.order_date
FROM customers c
LEFT JOIN LATERAL (
    SELECT order_id, order_date
    FROM orders
    WHERE customer_id = c.customer_id
    ORDER BY order_date DESC
    LIMIT 3
) recent_orders ON true;

-- 9. Generate Series
SELECT generate_series(1, 10) AS numbers;

-- 10. Generate Series with dates
SELECT 
    generate_series(
        '2025-01-01'::DATE,
        '2025-12-31'::DATE,
        '1 day'::INTERVAL
    ) AS all_dates;

-- 11. Materialized View Refresh
REFRESH MATERIALIZED VIEW mv_daily_sales;

-- 12. Temporary Table
CREATE TEMP TABLE temp_high_value_orders AS
SELECT * FROM orders 
WHERE order_id IN (
    SELECT order_id 
    FROM sales 
    GROUP BY order_id 
    HAVING SUM(total) > 500
);

-- 13. Grouping Sets
SELECT 
    category,
    EXTRACT(YEAR FROM order_date) AS year,
    SUM(total) AS total_sales
FROM sales
JOIN orders USING(order_id)
JOIN products USING(product_id)
GROUP BY GROUPING SETS (
    (category, year),
    (category),
    (year),
    ()
);

-- 14. Array Operations
SELECT 
    product_id,
    product_name,
    ARRAY_AGG(category) OVER (PARTITION BY product_id) AS categories
FROM products;

-- 15. Array Functions
SELECT 
    product_id,
    ARRAY_LENGTH(ARRAY_AGG(category), 1) AS category_count
FROM products
GROUP BY product_id;

-- 16. JSON with Aggregation
SELECT 
    category,
    json_agg(
        json_build_object(
            'name', product_name,
            'price', selling_price,
            'stock', stock
        )
    ) AS products_json
FROM products
GROUP BY category;

-- 17. Window with Frame
SELECT 
    sale_id,
    product_id,
    total,
    SUM(total) OVER (
        ORDER BY sale_id
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total,
    AVG(total) OVER (
        ORDER BY sale_id
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS moving_avg
FROM sales;

-- 18. Conditional Aggregation
SELECT 
    category,
    COUNT(*) AS total_products,
    COUNT(CASE WHEN stock > 100 THEN 1 END) AS high_stock,
    COUNT(CASE WHEN stock BETWEEN 50 AND 100 THEN 1 END) AS medium_stock,
    COUNT(CASE WHEN stock < 50 THEN 1 END) AS low_stock
FROM products
GROUP BY category;

-- 19. Percentile with window
SELECT 
    category,
    product_name,
    selling_price,
    PERCENT_RANK() OVER (PARTITION BY category ORDER BY selling_price) AS percentile_rank,
    CUME_DIST() OVER (PARTITION BY category ORDER BY selling_price) AS cume_dist
FROM products;

-- 20. Median calculation
SELECT 
    category,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY selling_price) AS median_price
FROM products
GROUP BY category;