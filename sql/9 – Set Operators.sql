-- 1. UNION
SELECT product_name, selling_price FROM products WHERE category = 'Electronics'
UNION
SELECT product_name, selling_price FROM products WHERE category = 'Clothing';

-- 2. UNION ALL
SELECT customer_id FROM customers WHERE country = 'USA'
UNION ALL
SELECT customer_id FROM customers WHERE country = 'Canada';

-- 3. INTERSECT
SELECT product_id FROM products WHERE category = 'Electronics'
INTERSECT
SELECT product_id FROM sales WHERE total > 100;

-- 4. EXCEPT
SELECT customer_id FROM customers WHERE country = 'USA'
EXCEPT
SELECT customer_id FROM orders;

-- 5. UNION with ORDER BY
SELECT first_name FROM customers WHERE country = 'USA'
UNION
SELECT last_name FROM customers WHERE country = 'Canada'
ORDER BY first_name;

-- 6. Multiple set operations
(SELECT product_id FROM products WHERE category = 'Electronics')
UNION
(SELECT product_id FROM products WHERE category = 'Clothing')
INTERSECT
(SELECT product_id FROM sales WHERE total > 50);

-- 7. Set operations with aggregate
SELECT category, COUNT(*) 
FROM products 
WHERE category IN (
    SELECT category FROM products
    INTERSECT
    SELECT category FROM sales
)
GROUP BY category;