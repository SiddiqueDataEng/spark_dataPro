-- 1. Basic Aggregation
SELECT 
    product_id,
    SUM(total) AS total_revenue,
    COUNT(*) AS sale_count
FROM sales
GROUP BY product_id
ORDER BY total_revenue DESC;

-- 2. Join Equivalent
SELECT 
    s.sale_id,
    p.product_name,
    s.total
FROM sales s
JOIN products p ON s.product_id = p.product_id;

-- 3. Window Function Equivalent
SELECT 
    product_id,
    product_name,
    category,
    selling_price,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY selling_price DESC) AS rank
FROM products;

-- 4. Aggregation with Filter
SELECT 
    category,
    COUNT(*) AS total_products,
    SUM(CASE WHEN stock > 100 THEN 1 ELSE 0 END) AS high_stock_count
FROM products
GROUP BY category;

-- 5. Multiple Aggregations
SELECT 
    category,
    COUNT(*) AS product_count,
    AVG(selling_price) AS avg_price,
    MAX(selling_price) AS max_price,
    SUM(stock) AS total_stock
FROM products
GROUP BY category;

-- 6. Window with Running Total
SELECT 
    order_date,
    total,
    SUM(total) OVER (ORDER BY order_date) AS running_total
FROM orders o
JOIN sales s ON o.order_id = s.order_id
ORDER BY order_date;

-- 7. Distinct with Count
SELECT 
    category,
    COUNT(DISTINCT product_id) AS distinct_products,
    SUM(quantity) AS total_quantity
FROM products
JOIN sales USING(product_id)
GROUP BY category;

-- 8. Filter with Where
SELECT * FROM sales
WHERE total > 100
AND discount < 10
ORDER BY total DESC;

-- 9. With Column Alias
SELECT 
    product_id,
    total AS revenue,
    quantity AS units_sold
FROM sales
WHERE total > 50;

-- 10. Union Equivalent
SELECT product_id, product_name FROM products WHERE category = 'Electronics'
UNION
SELECT product_id, product_name FROM products WHERE category = 'Clothing';

-- 11. Aggregation with Having
SELECT 
    product_id,
    SUM(total) AS total_revenue
FROM sales
GROUP BY product_id
HAVING SUM(total) > 1000
ORDER BY total_revenue DESC;

-- 12. Date Operations
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    SUM(total) AS monthly_revenue
FROM orders o
JOIN sales s ON o.order_id = s.order_id
GROUP BY month
ORDER BY month;

-- 13. Pivot Equivalent
SELECT 
    product_id,
    SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2024 THEN total ELSE 0 END) AS sales_2024,
    SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2025 THEN total ELSE 0 END) AS sales_2025
FROM sales
JOIN orders USING(order_id)
GROUP BY product_id;

-- 14. Rank with Dense_Rank
SELECT 
    product_id,
    product_name,
    category,
    selling_price,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY selling_price DESC) AS dense_rank
FROM products;

-- 15. Lag/Lead
SELECT 
    order_id,
    order_date,
    total,
    LAG(total, 1, 0) OVER (ORDER BY order_date) AS previous_total,
    LEAD(total, 1, 0) OVER (ORDER BY order_date) AS next_total
FROM sales
JOIN orders USING(order_id);

-- 16. Percentile
SELECT 
    product_id,
    total,
    PERCENT_RANK() OVER (ORDER BY total DESC) AS percentile_rank
FROM sales;

-- 17. NTile
SELECT 
    product_id,
    total,
    NTILE(4) OVER (ORDER BY total DESC) AS quartile
FROM sales;

-- 18. Aggregate with Rollup
SELECT 
    category,
    EXTRACT(YEAR FROM order_date) AS year,
    SUM(total) AS total_revenue
FROM sales
JOIN products USING(product_id)
JOIN orders USING(order_id)
GROUP BY ROLLUP(category, year)
ORDER BY category, year;

-- 19. Join with Aggregate
SELECT 
    p.category,
    SUM(s.total) AS total_revenue,
    COUNT(DISTINCT o.order_id) AS order_count,
    AVG(s.total) AS avg_order_value
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN orders o ON s.order_id = o.order_id
GROUP BY p.category;

-- 20. Complex Window with Partition
SELECT 
    product_id,
    product_name,
    category,
    selling_price,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY selling_price DESC) AS rank_in_category,
    RANK() OVER (PARTITION BY category ORDER BY selling_price DESC) AS rank_with_gaps,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY selling_price DESC) AS dense_rank,
    NTILE(10) OVER (PARTITION BY category ORDER BY selling_price DESC) AS decile
FROM products
WHERE category IN ('Electronics', 'Clothing', 'Books');