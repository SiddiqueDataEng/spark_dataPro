-- 1. SELECT all customers
SELECT * FROM customers;

-- 2. SELECT specific columns
SELECT first_name, last_name, email FROM customers;

-- 3. DISTINCT countries
SELECT DISTINCT country FROM customers;

-- 4. WHERE clause
SELECT * FROM products WHERE category = 'Electronics';

-- 5. AND / OR
SELECT * FROM orders 
WHERE status = 'Completed' AND order_date > '2025-01-01';

-- 6. BETWEEN
SELECT * FROM products 
WHERE selling_price BETWEEN 100 AND 500;

-- 7. IN
SELECT * FROM customers 
WHERE country IN ('USA', 'UK', 'Canada');

-- 8. NOT IN
SELECT * FROM products 
WHERE category NOT IN ('Food', 'Sports');

-- 9. LIKE
SELECT * FROM customers 
WHERE first_name LIKE 'J%';

-- 10. ILIKE (case insensitive)
SELECT * FROM customers 
WHERE last_name ILIKE '%son%';

-- 11. ORDER BY
SELECT * FROM products 
ORDER BY selling_price DESC;

-- 12. LIMIT
SELECT * FROM orders 
ORDER BY order_date DESC 
LIMIT 10;

-- 13. OFFSET
SELECT * FROM customers 
ORDER BY customer_id 
OFFSET 50 LIMIT 10;

-- 14. Aliases
SELECT first_name || ' ' || last_name AS full_name 
FROM customers;

-- 15. NULL (checking nulls)
SELECT * FROM customers WHERE city IS NULL;

-- 16. IS NOT NULL
SELECT * FROM products WHERE stock IS NOT NULL;

-- 17. Arithmetic operations
SELECT product_name, selling_price - cost_price AS profit_margin 
FROM products;

-- 18. Concatenation
SELECT CONCAT(first_name, ' ', last_name, ' - ', email) AS customer_info 
FROM customers;

-- 19. CASE
SELECT order_id, status,
CASE 
    WHEN status = 'Completed' THEN 'Delivered'
    WHEN status = 'Pending' THEN 'Processing'
    ELSE 'Cancelled'
END AS order_status
FROM orders;

-- 20. COALESCE
SELECT product_name, COALESCE(category, 'Uncategorized') AS category 
FROM products;

-- 21. CAST
SELECT CAST(order_date AS TEXT) FROM orders;

-- 22. DATE functions - current date
SELECT CURRENT_DATE;

-- 23. EXTRACT
SELECT EXTRACT(YEAR FROM order_date) AS order_year 
FROM orders;

-- 24. AGE
SELECT first_name, AGE(join_date) AS membership_age 
FROM customers;

-- 25. Date arithmetic
SELECT order_date, order_date + INTERVAL '7 days' AS due_date 
FROM orders;