-- 1. ROW_NUMBER
SELECT 
    product_name,
    category,
    selling_price,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY selling_price DESC) AS rank_in_category
FROM products;

-- 2. RANK
SELECT 
    product_name,
    category,
    selling_price,
    RANK() OVER (PARTITION BY category ORDER BY selling_price DESC) AS rank
FROM products;

-- 3. DENSE_RANK
SELECT 
    product_name,
    category,
    selling_price,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY selling_price DESC) AS dense_rank
FROM products;

-- 4. NTILE
SELECT 
    product_name,
    selling_price,
    NTILE(4) OVER (ORDER BY selling_price DESC) AS quartile
FROM products;

-- 5. LAG
SELECT 
    order_id,
    order_date,
    LAG(order_date, 1) OVER (ORDER BY order_date) AS previous_order_date
FROM orders;

-- 6. LEAD
SELECT 
    customer_id,
    order_date,
    LEAD(order_date, 1) OVER (PARTITION BY customer_id ORDER BY order_date) AS next_order_date
FROM orders;

-- 7. FIRST_VALUE
SELECT 
    product_name,
    category,
    selling_price,
    FIRST_VALUE(selling_price) OVER (PARTITION BY category ORDER BY selling_price DESC) AS highest_price
FROM products;

-- 8. LAST_VALUE
SELECT 
    product_name,
    category,
    selling_price,
    LAST_VALUE(selling_price) OVER (PARTITION BY category ORDER BY selling_price DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lowest_price
FROM products;

-- 9. SUM OVER (Running Total)
SELECT 
    order_id,
    order_date,
    total,
    SUM(total) OVER (ORDER BY order_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total
FROM sales
JOIN orders USING(order_id);

-- 10. AVG OVER (Moving Average)
SELECT 
    order_id,
    order_date,
    total,
    AVG(total) OVER (ORDER BY order_date ROWS BETWEEN 3 PRECEDING AND 1 FOLLOWING) AS moving_avg_5
FROM sales
JOIN orders USING(order_id);

-- 11. COUNT OVER
SELECT 
    product_id,
    total,
    COUNT(*) OVER (PARTITION BY product_id) AS sale_count_per_product
FROM sales;

-- 12. Percent Rank
SELECT 
    product_name,
    selling_price,
    PERCENT_RANK() OVER (ORDER BY selling_price DESC) AS percentile_rank
FROM products;

-- 13. CUME_DIST
SELECT 
    product_name,
    selling_price,
    CUME_DIST() OVER (ORDER BY selling_price DESC) AS cumulative_distribution
FROM products;

-- 14. Window with aggregate
SELECT 
    order_id,
    product_id,
    quantity,
    total,
    total / SUM(total) OVER (PARTITION BY order_id) AS percentage_of_order
FROM sales;

-- 15. Partitioned window with multiple functions
SELECT 
    product_name,
    category,
    selling_price,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY selling_price DESC) AS row_num,
    RANK() OVER (PARTITION BY category ORDER BY selling_price DESC) AS rank,
    DENSE_RANK() OVER (PARTITION BY category ORDER BY selling_price DESC) AS dense_rank,
    SUM(selling_price) OVER (PARTITION BY category) AS category_total,
    AVG(selling_price) OVER (PARTITION BY category) AS category_avg,
    selling_price - AVG(selling_price) OVER (PARTITION BY category) AS price_diff_from_avg
FROM products;

-- 16-60. Additional window function variations (continued in other modules)