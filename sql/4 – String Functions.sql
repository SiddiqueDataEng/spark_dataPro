-- 1. UPPER
SELECT UPPER(first_name) AS upper_name FROM customers;

-- 2. LOWER
SELECT LOWER(email) FROM customers;

-- 3. INITCAP (capitalize each word)
SELECT INITCAP(product_name) FROM products;

-- 4. LENGTH
SELECT product_name, LENGTH(product_name) AS name_length 
FROM products;

-- 5. SUBSTRING
SELECT SUBSTRING(first_name, 1, 3) AS first_3_chars 
FROM customers;

-- 6. LEFT
SELECT LEFT(first_name, 3) FROM customers;

-- 7. RIGHT
SELECT RIGHT(last_name, 2) FROM customers;

-- 8. POSITION
SELECT email, POSITION('@' IN email) AS at_position 
FROM customers;

-- 9. REPLACE
SELECT REPLACE(category, 'Electronics', 'Tech') 
FROM products;

-- 10. TRIM
SELECT TRIM(' ' FROM first_name) FROM customers;

-- 11. CONCAT
SELECT CONCAT(first_name, ' ', last_name) AS full_name 
FROM customers;

-- 12. SPLIT_PART
SELECT email, SPLIT_PART(email, '@', 1) AS username 
FROM customers;

-- 13. STRING_AGG (aggregate)
SELECT 
    category,
    STRING_AGG(product_name, ', ') AS product_list
FROM products
GROUP BY category;

-- 14. REGEXP_REPLACE
SELECT product_name, 
       REGEXP_REPLACE(product_name, '[0-9]', '', 'g') AS cleaned_name
FROM products;

-- 15. REGEXP_MATCH
SELECT product_name, 
       REGEXP_MATCH(product_name, '[A-Z][a-z]+') AS words
FROM products;

-- 16. REVERSE
SELECT REVERSE(first_name) AS reversed_name 
FROM customers;

-- 17. REPEAT
SELECT REPEAT('*', LENGTH(first_name)) AS hidden_name 
FROM customers;

-- 18. LPAD
SELECT LPAD(CAST(customer_id AS TEXT), 5, '0') AS padded_id 
FROM customers;

-- 19. RPAD
SELECT RPAD(first_name, 10, '*') FROM customers;

-- 20. OVERLAY
SELECT OVERLAY(product_name PLACING 'Laptop' FROM 1 FOR 4) 
FROM products 
WHERE product_name LIKE 'Lap%';

-- 21. BIT_LENGTH
SELECT BIT_LENGTH(email) FROM customers;

-- 22. CHAR_LENGTH
SELECT CHAR_LENGTH(product_name) FROM products;

-- 23. STRPOS
SELECT STRPOS(city, 'New') FROM customers;

-- 24. TRANSLATE
SELECT TRANSLATE(city, 'aeiou', '12345') FROM customers;

-- 25. UNNEST with string_to_array
SELECT 
    product_name,
    UNNEST(STRING_TO_ARRAY(category, ',')) AS category_part
FROM products;