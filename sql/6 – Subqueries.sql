-- 1. Simple subquery in WHERE
SELECT * FROM customers 
WHERE customer_id IN (
    SELECT customer_id 
    FROM orders 
    WHERE status = 'Completed'
);

-- 2. Subquery with comparison
SELECT * FROM products 
WHERE selling_price > (
    SELECT AVG(selling_price) 
    FROM products
);

-- 3. Correlated subquery
SELECT * FROM products p 
WHERE selling_price > (
    SELECT AVG(selling_price) 
    FROM products 
    WHERE category = p.category
);

-- 4. Subquery in SELECT
SELECT 
    customer_id,
    first_name,
    (
        SELECT COUNT(*) 
        FROM orders 
        WHERE customer_id = c.customer_id
    ) AS order_count
FROM customers c;

-- 5. EXISTS subquery
SELECT * FROM customers c 
WHERE EXISTS (
    SELECT 1 
    FROM orders o 
    WHERE o.customer_id = c.customer_id
);

-- 6. NOT EXISTS
SELECT * FROM customers c 
WHERE NOT EXISTS (
    SELECT 1 
    FROM orders o 
    WHERE o.customer_id = c.customer_id
);

-- 7. ANY subquery
SELECT * FROM products 
WHERE selling_price > ANY (
    SELECT selling_price 
    FROM products 
    WHERE category = 'Electronics'
);

-- 8. ALL subquery
SELECT * FROM products 
WHERE selling_price > ALL (
    SELECT selling_price 
    FROM products 
    WHERE category = 'Electronics'
);

-- 9. Subquery in FROM
SELECT 
    category,
    avg_price
FROM (
    SELECT 
        category,
        AVG(selling_price) AS avg_price
    FROM products 
    GROUP BY category
) AS category_avg
WHERE avg_price > 100;

-- 10. Correlated with multiple tables
SELECT 
    product_name,
    category,
    selling_price,
    (
        SELECT AVG(selling_price) 
        FROM products 
        WHERE category = p.category
    ) AS category_avg_price
FROM products p;

-- 11. Nested subquery
SELECT * FROM customers 
WHERE customer_id IN (
    SELECT customer_id 
    FROM orders 
    WHERE order_id IN (
        SELECT order_id 
        FROM sales 
        WHERE total > 1000
    )
);

-- 12. Subquery with aggregate
SELECT 
    order_id,
    total,
    (SELECT AVG(total) FROM sales) AS overall_avg
FROM sales;

-- 13. Multi-column subquery
SELECT * FROM orders 
WHERE (customer_id, store_id) IN (
    SELECT customer_id, store_id 
    FROM orders 
    GROUP BY customer_id, store_id 
    HAVING COUNT(*) > 1
);

-- 14. Subquery with LIMIT
SELECT * FROM products 
WHERE selling_price > (
    SELECT selling_price 
    FROM products 
    ORDER BY selling_price 
    LIMIT 1
);

-- 15. Subquery with ARRAY
SELECT 
    customer_id,
    first_name,
    (
        SELECT ARRAY_AGG(order_id) 
        FROM orders 
        WHERE customer_id = c.customer_id
    ) AS order_ids
FROM customers c;

-- 16. CTE as subquery alternative
WITH SalesSummary AS (
    SELECT 
        product_id,
        SUM(total) AS total_sales,
        AVG(total) AS avg_sale
    FROM sales 
    GROUP BY product_id
)
SELECT p.product_name, ss.total_sales
FROM products p
JOIN SalesSummary ss ON p.product_id = ss.product_id;

-- 17. Subquery with BETWEEN
SELECT * FROM products 
WHERE selling_price BETWEEN (
    SELECT MIN(selling_price) 
    FROM products
) AND (
    SELECT MAX(selling_price) 
    FROM products
);

-- 18. Subquery with IN and multiple conditions
SELECT * FROM customers 
WHERE (customer_id, city) IN (
    SELECT customer_id, city 
    FROM customers 
    WHERE country = 'USA'
    GROUP BY customer_id, city
);

-- 19. Subquery with HAVING
SELECT 
    category,
    COUNT(*) AS count
FROM products 
GROUP BY category
HAVING COUNT(*) > (
    SELECT AVG(cnt) 
    FROM (
        SELECT COUNT(*) AS cnt 
        FROM products 
        GROUP BY category
    ) AS counts
);

-- 20-30. Additional subquery variations (continued in other modules)