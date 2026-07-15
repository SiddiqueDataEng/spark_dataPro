-- 1. Second Highest Salary
SELECT DISTINCT salary 
FROM employees 
ORDER BY salary DESC 
LIMIT 1 OFFSET 1;

-- Alternative using subquery
SELECT MAX(salary) 
FROM employees 
WHERE salary < (SELECT MAX(salary) FROM employees);

-- 2. Third Highest Sale
SELECT DISTINCT total 
FROM sales 
ORDER BY total DESC 
LIMIT 1 OFFSET 2;

-- 3. Top N Products per Category
WITH RankedProducts AS (
    SELECT 
        product_id,
        product_name,
        category,
        selling_price,
        ROW_NUMBER() OVER (PARTITION BY category ORDER BY selling_price DESC) AS rank
    FROM products
)
SELECT * FROM RankedProducts 
WHERE rank <= 5
ORDER BY category, rank;

-- 4. Duplicate Records
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    COUNT(*) AS duplicate_count
FROM customers
GROUP BY customer_id, first_name, last_name, email
HAVING COUNT(*) > 1;

-- 5. Remove Duplicates
DELETE FROM customers 
WHERE customer_id NOT IN (
    SELECT MIN(customer_id)
    FROM customers
    GROUP BY email
);

-- 6. Gaps and Islands (Consecutive Dates)
WITH DateGroups AS (
    SELECT 
        order_date,
        ROW_NUMBER() OVER (ORDER BY order_date) AS row_num,
        order_date - INTERVAL '1 day' * ROW_NUMBER() OVER (ORDER BY order_date) AS group_date
    FROM orders
)
SELECT 
    MIN(order_date) AS start_date,
    MAX(order_date) AS end_date,
    COUNT(*) AS consecutive_days
FROM DateGroups
GROUP BY group_date
ORDER BY start_date;

-- 7. Running Balance
SELECT 
    sale_id,
    order_id,
    total,
    SUM(total) OVER (ORDER BY sale_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance
FROM sales
ORDER BY sale_id;

-- 8. Median Salary
SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY salary) AS median_salary
FROM employees;

-- 9. Percentile Calculations
SELECT 
    employee_id,
    employee_name,
    salary,
    PERCENT_RANK() OVER (ORDER BY salary) AS percentile_rank,
    CUME_DIST() OVER (ORDER BY salary) AS cumulative_distribution
FROM employees;

-- 10. Cohort Analysis
WITH CustomerCohorts AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', MIN(order_date)) AS cohort_month
    FROM orders
    GROUP BY customer_id
)
SELECT 
    c.cohort_month,
    EXTRACT(MONTH FROM AGE(o.order_date, c.cohort_month)) AS months_since_first,
    COUNT(DISTINCT o.customer_id) AS customer_count,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(s.total) AS total_revenue
FROM CustomerCohorts c
JOIN orders o ON c.customer_id = o.customer_id
JOIN sales s ON o.order_id = s.order_id
WHERE o.order_date >= c.cohort_month
GROUP BY c.cohort_month, months_since_first
ORDER BY c.cohort_month, months_since_first;

-- 11. Market Basket Analysis
WITH ProductPairs AS (
    SELECT 
        o.order_id,
        p1.product_id AS product_a,
        p2.product_id AS product_b,
        p1.product_name AS product_a_name,
        p2.product_name AS product_b_name
    FROM orders o
    JOIN sales s1 ON o.order_id = s1.order_id
    JOIN products p1 ON s1.product_id = p1.product_id
    JOIN sales s2 ON o.order_id = s2.order_id AND s1.sale_id < s2.sale_id
    JOIN products p2 ON s2.product_id = p2.product_id
)
SELECT 
    product_a_name,
    product_b_name,
    COUNT(*) AS frequency,
    ROUND(COUNT(*)::DECIMAL / (SELECT COUNT(DISTINCT order_id) FROM orders) * 100, 2) AS support_pct
FROM ProductPairs
GROUP BY product_a_name, product_b_name
HAVING COUNT(*) > 1
ORDER BY frequency DESC
LIMIT 10;

-- 12. Funnel Analysis
WITH FunnelSteps AS (
    SELECT 
        'View' AS step,
        COUNT(DISTINCT customer_id) AS customers
    FROM page_views
    
    UNION ALL
    
    SELECT 
        'Cart' AS step,
        COUNT(DISTINCT customer_id) AS customers
    FROM cart_events
    
    UNION ALL
    
    SELECT 
        'Purchase' AS step,
        COUNT(DISTINCT customer_id) AS customers
    FROM orders
)
SELECT 
    step,
    customers,
    LAG(customers) OVER (ORDER BY step) AS previous_step_customers,
    ROUND(customers::DECIMAL / LAG(customers) OVER (ORDER BY step) * 100, 2) AS conversion_rate
FROM FunnelSteps;

-- 13. Slowly Changing Dimensions (SCD Type 2)
CREATE TABLE customer_dimension (
    customer_id INT,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(150),
    city VARCHAR(100),
    country VARCHAR(100),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN DEFAULT TRUE
);

-- Insert new customer
INSERT INTO customer_dimension (customer_id, first_name, last_name, email, city, country, valid_from, valid_to, is_current)
VALUES (1, 'John', 'Doe', 'john@email.com', 'New York', 'USA', '2025-01-01', '9999-12-31', TRUE);

-- Update customer (SCD Type 2)
UPDATE customer_dimension 
SET valid_to = CURRENT_DATE - INTERVAL '1 day', is_current = FALSE
WHERE customer_id = 1 AND is_current = TRUE;

INSERT INTO customer_dimension (customer_id, first_name, last_name, email, city, country, valid_from, valid_to, is_current)
VALUES (1, 'John', 'Doe', 'john@email.com', 'Los Angeles', 'USA', CURRENT_DATE, '9999-12-31', TRUE);

-- 14. Star Schema Query
SELECT 
    d.calendar_year,
    p.category,
    SUM(s.total) AS sales_amount,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT c.customer_id) AS customer_count
FROM fact_sales s
JOIN dim_orders o ON s.order_id = o.order_id
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN dim_products p ON s.product_id = p.product_id
JOIN dim_date d ON o.order_date = d.date
WHERE d.calendar_year = 2025
GROUP BY d.calendar_year, p.category
ORDER BY p.category;

-- 15. Data Warehouse Optimization - Pre-aggregated Table
CREATE TABLE agg_daily_sales AS
SELECT 
    DATE_TRUNC('day', o.order_date) AS sale_date,
    p.category,
    p.product_id,
    SUM(s.total) AS total_sales,
    SUM(s.quantity) AS units_sold,
    COUNT(DISTINCT o.order_id) AS order_count,
    AVG(s.total) AS avg_order_value
FROM sales s
JOIN orders o ON s.order_id = o.order_id
JOIN products p ON s.product_id = p.product_id
GROUP BY sale_date, category, product_id;

-- 16. Real-world Dashboard Query
SELECT 
    'Today' AS period,
    SUM(CASE WHEN order_date = CURRENT_DATE THEN total ELSE 0 END) AS sales_today,
    SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '7 days' THEN total ELSE 0 END) AS sales_week,
    SUM(CASE WHEN order_date >= CURRENT_DATE - INTERVAL '30 days' THEN total ELSE 0 END) AS sales_month,
    SUM(total) AS total_all_time
FROM sales
JOIN orders USING(order_id);

-- 17. Customer Segmentation RFM Analysis
WITH RFM AS (
    SELECT 
        customer_id,
        CURRENT_DATE - MAX(order_date) AS recency_days,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(total) AS monetary
    FROM orders o
    JOIN sales s ON o.order_id = s.order_id
    GROUP BY customer_id
),
RFM_Scores AS (
    SELECT 
        customer_id,
        NTILE(5) OVER (ORDER BY recency_days) AS recency_score,
        NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score,
        NTILE(5) OVER (ORDER BY monetary DESC) AS monetary_score
    FROM RFM
)
SELECT 
    customer_id,
    recency_score,
    frequency_score,
    monetary_score,
    recency_score + frequency_score + monetary_score AS rfm_total,
    CASE 
        WHEN recency_score >= 4 AND frequency_score >= 4 THEN 'Champions'
        WHEN recency_score >= 4 AND frequency_score >= 3 THEN 'Loyal Customers'
        WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Potential Loyalists'
        WHEN recency_score >= 3 AND frequency_score <= 2 THEN 'At Risk'
        ELSE 'Lost'
    END AS customer_segment
FROM RFM_Scores
ORDER BY rfm_total DESC;

-- 18. Sales Forecasting with Moving Average
SELECT 
    order_date,
    total_sales,
    AVG(total_sales) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
    ) AS seven_day_moving_avg,
    AVG(total_sales) OVER (
        ORDER BY order_date 
        ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) AS thirty_day_moving_avg
FROM (
    SELECT 
        order_date,
        SUM(total) AS total_sales
    FROM orders o
    JOIN sales s ON o.order_id = s.order_id
    GROUP BY order_date
) daily_sales
ORDER BY order_date;

-- 19. Year-over-Year Comparison
SELECT 
    EXTRACT(MONTH FROM order_date) AS month,
    SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2024 THEN total ELSE 0 END) AS sales_2024,
    SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2025 THEN total ELSE 0 END) AS sales_2025,
    (SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2025 THEN total ELSE 0 END) - 
     SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2024 THEN total ELSE 0 END)) / 
    NULLIF(SUM(CASE WHEN EXTRACT(YEAR FROM order_date) = 2024 THEN total ELSE 0 END), 0) * 100 AS growth_pct
FROM orders o
JOIN sales s ON o.order_id = s.order_id
WHERE EXTRACT(YEAR FROM order_date) IN (2024, 2025)
GROUP BY month
ORDER BY month;

-- 20. Recursive Category Tree (if categories table exists)
WITH RECURSIVE category_hierarchy AS (
    SELECT 
        category_id,
        category_name,
        parent_id,
        0 AS level,
        category_name AS path
    FROM categories
    WHERE parent_id IS NULL
    
    UNION ALL
    
    SELECT 
        c.category_id,
        c.category_name,
        c.parent_id,
        ch.level + 1,
        ch.path || ' > ' || c.category_name
    FROM categories c
    JOIN category_hierarchy ch ON c.parent_id = ch.category_id
)
SELECT * FROM category_hierarchy
ORDER BY path;