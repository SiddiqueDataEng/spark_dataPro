-- 1. COUNT
SELECT COUNT(*) AS total_customers FROM customers;

-- 2. SUM
SELECT SUM(total) AS total_sales FROM sales;

-- 3. AVG
SELECT AVG(selling_price) AS avg_price FROM products;

-- 4. MIN
SELECT MIN(salary) AS min_salary FROM employees;

-- 5. MAX
SELECT MAX(stock) AS max_stock FROM products;

-- 6. GROUP BY
SELECT category, COUNT(*) AS product_count 
FROM products 
GROUP BY category;

-- 7. HAVING
SELECT category, AVG(selling_price) AS avg_price 
FROM products 
GROUP BY category 
HAVING AVG(selling_price) > 200;

-- 8. Multiple Aggregates
SELECT 
    category,
    COUNT(*) AS product_count,
    AVG(selling_price) AS avg_price,
    MIN(cost_price) AS min_cost,
    MAX(selling_price) AS max_price,
    SUM(stock) AS total_stock
FROM products 
GROUP BY category;

-- 9. COUNT DISTINCT
SELECT COUNT(DISTINCT country) AS total_countries 
FROM customers;

-- 10. SUM with GROUP BY
SELECT category, SUM(stock) AS total_stock 
FROM products 
GROUP BY category;

-- 11. AVG with filter
SELECT AVG(total) FILTER (WHERE discount > 0) AS avg_discounted_sales 
FROM sales;

-- 12. Aggregates with CASE
SELECT 
    COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS completed,
    COUNT(CASE WHEN status = 'Pending' THEN 1 END) AS pending,
    COUNT(CASE WHEN status = 'Cancelled' THEN 1 END) AS cancelled
FROM orders;

-- 13. Aggregates with ROLLUP
SELECT 
    category,
    SUM(stock) AS total_stock
FROM products 
GROUP BY ROLLUP(category);

-- 14. Aggregates with CUBE
SELECT 
    category,
    department,
    COUNT(*)
FROM employees e
JOIN stores s ON e.employee_id = s.store_id
GROUP BY CUBE(category, department);

-- 15. Aggregates with filtering
SELECT 
    category,
    COUNT(*) FILTER (WHERE stock > 100) AS high_stock_count
FROM products 
GROUP BY category;

-- 16. Aggregates with JOIN
SELECT 
    o.status,
    SUM(s.total) AS total_sales
FROM orders o
JOIN sales s ON o.order_id = s.order_id
GROUP BY o.status;

-- 17. Aggregates with date trunc
SELECT 
    DATE_TRUNC('month', order_date) AS month,
    SUM(total) AS monthly_sales
FROM orders o
JOIN sales s ON o.order_id = s.order_id
GROUP BY month
ORDER BY month;

-- 18. Aggregates with multiple GROUP BY
SELECT 
    category,
    EXTRACT(YEAR FROM join_date) AS join_year,
    COUNT(*) AS customer_count
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN products p ON p.product_id = o.order_id
GROUP BY category, join_year;

-- 19. Aggregates with HAVING multiple conditions
SELECT 
    category,
    COUNT(*) AS count,
    AVG(selling_price) AS avg_price
FROM products 
GROUP BY category 
HAVING COUNT(*) > 50 AND AVG(selling_price) > 150;

-- 20. Aggregates with order by aggregate
SELECT 
    category,
    SUM(stock) AS total_stock
FROM products 
GROUP BY category 
ORDER BY total_stock DESC;