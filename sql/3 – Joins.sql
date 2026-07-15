-- 1. INNER JOIN
SELECT 
    o.order_id,
    c.first_name,
    c.last_name,
    o.order_date
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id;

-- 2. LEFT JOIN
SELECT 
    c.customer_id,
    c.first_name,
    o.order_id,
    o.order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id;

-- 3. RIGHT JOIN
SELECT 
    o.order_id,
    e.employee_name
FROM orders o
RIGHT JOIN employees e ON o.employee_id = e.employee_id;

-- 4. FULL OUTER JOIN
SELECT 
    c.first_name,
    o.order_id
FROM customers c
FULL OUTER JOIN orders o ON c.customer_id = o.customer_id;

-- 5. CROSS JOIN
SELECT 
    p.product_name,
    s.store_name
FROM products p
CROSS JOIN stores s
LIMIT 100;

-- 6. SELF JOIN
SELECT 
    e1.employee_name AS employee,
    e2.employee_name AS manager
FROM employees e1
LEFT JOIN employees e2 ON e1.employee_id = e2.employee_id;

-- 7. Multiple Table Join
SELECT 
    o.order_id,
    c.first_name,
    p.product_name,
    s.quantity,
    st.store_name,
    e.employee_name
FROM sales s
JOIN orders o ON s.order_id = o.order_id
JOIN customers c ON c.customer_id = o.customer_id
JOIN products p ON p.product_id = s.product_id
JOIN stores st ON st.store_id = o.store_id
JOIN employees e ON e.employee_id = o.employee_id;

-- 8. Join with conditions
SELECT 
    o.order_id,
    c.first_name,
    s.total
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN sales s ON o.order_id = s.order_id
WHERE o.status = 'Completed'
AND s.total > 100;

-- 9. Join with aggregate
SELECT 
    c.first_name,
    c.last_name,
    COUNT(o.order_id) AS order_count,
    SUM(s.total) AS total_spent
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
LEFT JOIN sales s ON o.order_id = s.order_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- 10. Join with date filter
SELECT 
    o.order_id,
    c.first_name,
    o.order_date
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2025-01-01';

-- 11. Join with multiple conditions
SELECT 
    s.sale_id,
    p.product_name,
    s.quantity
FROM sales s
INNER JOIN products p ON s.product_id = p.product_id
AND s.quantity > 5;

-- 12. Join with subquery
SELECT 
    o.order_id,
    c.first_name,
    (
        SELECT SUM(total) 
        FROM sales 
        WHERE order_id = o.order_id
    ) AS order_total
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id;

-- 13. Join with CTE
WITH CustomerOrders AS (
    SELECT 
        c.customer_id,
        c.first_name,
        COUNT(o.order_id) AS order_count
    FROM customers c
    LEFT JOIN orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.first_name
)
SELECT * FROM CustomerOrders WHERE order_count > 5;

-- 14-50. Additional join combinations (continued in other modules)