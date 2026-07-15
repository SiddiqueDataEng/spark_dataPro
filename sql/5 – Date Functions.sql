-- 1. CURRENT_DATE
SELECT CURRENT_DATE AS today;

-- 2. NOW()
SELECT NOW() AS current_datetime;

-- 3. AGE()
SELECT AGE(CURRENT_DATE, join_date) AS membership_age 
FROM customers;

-- 4. DATE_PART
SELECT 
    DATE_PART('year', order_date) AS year,
    DATE_PART('month', order_date) AS month
FROM orders;

-- 5. EXTRACT
SELECT 
    EXTRACT(YEAR FROM order_date) AS order_year,
    EXTRACT(MONTH FROM order_date) AS order_month,
    EXTRACT(DAY FROM order_date) AS order_day
FROM orders;

-- 6. INTERVAL
SELECT order_date + INTERVAL '30 days' AS due_date 
FROM orders;

-- 7. DATE_TRUNC
SELECT 
    DATE_TRUNC('month', order_date) AS month_start,
    COUNT(*) AS orders_count
FROM orders
GROUP BY month_start;

-- 8. MAKE_DATE
SELECT MAKE_DATE(2025, 1, 15) AS created_date;

-- 9. TO_DATE
SELECT TO_DATE('2025-01-15', 'YYYY-MM-DD') AS converted_date;

-- 10. TO_TIMESTAMP
SELECT TO_TIMESTAMP('2025-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS') 
AS timestamp_converted;

-- 11. DATE difference
SELECT 
    order_date,
    CURRENT_DATE - order_date AS days_since_order
FROM orders;

-- 12. Date range queries
SELECT * FROM orders 
WHERE order_date BETWEEN '2025-01-01' AND '2025-12-31';

-- 13. Last 7 days
SELECT * FROM orders 
WHERE order_date >= CURRENT_DATE - INTERVAL '7 days';

-- 14. Year to date
SELECT * FROM orders 
WHERE order_date >= DATE_TRUNC('year', CURRENT_DATE);

-- 15. Month to date
SELECT * FROM orders 
WHERE order_date >= DATE_TRUNC('month', CURRENT_DATE);

-- 16. Quarter to date
SELECT * FROM orders 
WHERE order_date >= DATE_TRUNC('quarter', CURRENT_DATE);

-- 17. Last quarter
SELECT * FROM orders 
WHERE order_date >= DATE_TRUNC('quarter', CURRENT_DATE - INTERVAL '3 months')
AND order_date < DATE_TRUNC('quarter', CURRENT_DATE);

-- 18. Rolling 30 days
SELECT * FROM orders 
WHERE order_date BETWEEN CURRENT_DATE - INTERVAL '30 days' AND CURRENT_DATE;

-- 19. Same day last year
SELECT * FROM orders 
WHERE order_date = CURRENT_DATE - INTERVAL '1 year';

-- 20. DOW (Day of week)
SELECT 
    order_date,
    EXTRACT(DOW FROM order_date) AS day_of_week,
    TO_CHAR(order_date, 'Day') AS day_name
FROM orders;