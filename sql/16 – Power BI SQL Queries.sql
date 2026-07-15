-- 1. Sales by Month
SELECT 
    DATE_TRUNC('month', o.order_date) AS month,
    SUM(s.total) AS total_sales,
    COUNT(DISTINCT o.order_id) AS order_count,
    AVG(s.total) AS avg_order_value
FROM orders o
JOIN sales s ON o.order_id = s.order_id
GROUP BY month
ORDER BY month;

-- 2. Top 10 Customers
SELECT 
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    SUM(s.total) AS total_spent,
    COUNT(DISTINCT o.order_id) AS order_count,
    AVG(s.total) AS avg_order_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN sales s ON o.order_id = s.order_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_spent DESC
LIMIT 10;

-- 3. Top Products
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    SUM(s.quantity) AS units_sold,
    SUM(s.total) AS total_revenue,
    AVG(s.unit_price) AS avg_price,
    COUNT(DISTINCT o.order_id) AS order_count
FROM products p
JOIN sales s ON p.product_id = s.product_id
JOIN orders o ON s.order_id = o.order_id
GROUP BY p.product_id, p.product_name, p.category
ORDER BY total_revenue DESC
LIMIT 10;

-- 4. Sales by Country
SELECT 
    c.country,
    SUM(s.total) AS total_sales,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    AVG(s.total) AS avg_sale_value
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN sales s ON o.order_id = s.order_id
GROUP BY c.country
ORDER BY total_sales DESC;

-- 5. Sales by Store
SELECT 
    st.store_name,
    st.city,
    st.country,
    SUM(s.total) AS total_sales,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    AVG(s.total) AS avg_sale_value
FROM stores st
JOIN orders o ON st.store_id = o.store_id
JOIN sales s ON o.order_id = s.order_id
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY st.store_name, st.city, st.country
ORDER BY total_sales DESC;

-- 6. Monthly Revenue with Trend
SELECT 
    DATE_TRUNC('month', o.order_date) AS month,
    SUM(s.total) AS revenue,
    LAG(SUM(s.total)) OVER (ORDER BY DATE_TRUNC('month', o.order_date)) AS prev_month_revenue,
    (SUM(s.total) - LAG(SUM(s.total)) OVER (ORDER BY DATE_TRUNC('month', o.order_date))) / 
    LAG(SUM(s.total)) OVER (ORDER BY DATE_TRUNC('month', o.order_date)) * 100 AS monthly_growth_pct
FROM orders o
JOIN sales s ON o.order_id = s.order_id
GROUP BY month
ORDER BY month;

-- 7. Profit Analysis
SELECT 
    p.category,
    SUM(s.profit) AS total_profit,
    SUM(s.total) AS total_revenue,
    (SUM(s.profit) / NULLIF(SUM(s.total), 0)) * 100 AS profit_margin_pct,
    AVG(s.profit / s.quantity) AS avg_profit_per_unit
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.category
ORDER BY total_profit DESC;

-- 8. Year-over-Year Growth
SELECT 
    EXTRACT(YEAR FROM o.order_date) AS year,
    SUM(s.total) AS total_sales,
    LAG(SUM(s.total)) OVER (ORDER BY EXTRACT(YEAR FROM o.order_date)) AS prev_year_sales,
    (SUM(s.total) - LAG(SUM(s.total)) OVER (ORDER BY EXTRACT(YEAR FROM o.order_date))) / 
    LAG(SUM(s.total)) OVER (ORDER BY EXTRACT(YEAR FROM o.order_date)) * 100 AS yoy_growth_pct
FROM orders o
JOIN sales s ON o.order_id = s.order_id
GROUP BY year
ORDER BY year;

-- 9. Running Total
SELECT 
    o.order_date,
    s.total,
    SUM(s.total) OVER (ORDER BY o.order_date) AS running_total
FROM orders o
JOIN sales s ON o.order_id = s.order_id
ORDER BY o.order_date;

-- 10. Pareto Analysis (80/20 Rule)
WITH CategorySales AS (
    SELECT 
        p.category,
        SUM(s.total) AS total_sales,
        SUM(s.profit) AS total_profit
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    GROUP BY p.category
),
SalesRank AS (
    SELECT 
        category,
        total_sales,
        total_profit,
        SUM(total_sales) OVER (ORDER BY total_sales DESC) AS running_total,
        SUM(total_sales) OVER () AS grand_total,
        ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS rank
    FROM CategorySales
)
SELECT 
    category,
    total_sales,
    total_profit,
    ROUND((running_total::DECIMAL / grand_total) * 100, 2) AS cumulative_pct
FROM SalesRank
ORDER BY rank;

-- 11. ABC Analysis
WITH ProductSales AS (
    SELECT 
        p.product_id,
        p.product_name,
        p.category,
        SUM(s.total) AS total_sales,
        SUM(s.quantity) AS units_sold
    FROM products p
    JOIN sales s ON p.product_id = s.product_id
    GROUP BY p.product_id, p.product_name, p.category
),
SalesRank AS (
    SELECT 
        *,
        SUM(total_sales) OVER (ORDER BY total_sales DESC) AS running_total,
        SUM(total_sales) OVER () AS grand_total,
        ROW_NUMBER() OVER (ORDER BY total_sales DESC) AS rank
    FROM ProductSales
)
SELECT 
    product_id,
    product_name,
    category,
    total_sales,
    units_sold,
    ROUND((running_total::DECIMAL / grand_total) * 100, 2) AS cumulative_pct,
    CASE 
        WHEN (running_total::DECIMAL / grand_total) * 100 <= 80 THEN 'A'
        WHEN (running_total::DECIMAL / grand_total) * 100 <= 95 THEN 'B'
        ELSE 'C'
    END AS abc_category
FROM SalesRank
ORDER BY cumulative_pct;

-- 12. Customer Cohort Analysis
WITH CustomerFirstOrder AS (
    SELECT 
        customer_id,
        MIN(order_date) AS first_order_date
    FROM orders
    GROUP BY customer_id
),
CohortData AS (
    SELECT 
        c.customer_id,
        DATE_TRUNC('month', c.first_order_date) AS cohort_month,
        o.order_date,
        s.total,
        EXTRACT(YEAR FROM AGE(o.order_date, c.first_order_date)) * 12 + 
        EXTRACT(MONTH FROM AGE(o.order_date, c.first_order_date)) AS months_since_first
    FROM CustomerFirstOrder c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN sales s ON o.order_id = s.order_id
)
SELECT 
    cohort_month,
    months_since_first,
    COUNT(DISTINCT customer_id) AS customers,
    SUM(total) AS revenue,
    AVG(total) AS avg_revenue
FROM CohortData
WHERE months_since_first IS NOT NULL
GROUP BY cohort_month, months_since_first
ORDER BY cohort_month, months_since_first;

-- 13. Customer Retention Rate
WITH MonthlyCustomers AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        customer_id
    FROM orders
    GROUP BY month, customer_id
),
Retention AS (
    SELECT 
        current_month.month,
        current_month.customer_id,
        LAG(current_month.month) OVER (PARTITION BY current_month.customer_id ORDER BY current_month.month) AS previous_month,
        LEAD(current_month.month) OVER (PARTITION BY current_month.customer_id ORDER BY current_month.month) AS next_month
    FROM MonthlyCustomers current_month
)
SELECT 
    month,
    COUNT(DISTINCT customer_id) AS active_customers,
    COUNT(DISTINCT CASE WHEN next_month IS NOT NULL THEN customer_id END) AS retained_customers,
    ROUND(COUNT(DISTINCT CASE WHEN next_month IS NOT NULL THEN customer_id END)::DECIMAL / 
          COUNT(DISTINCT customer_id) * 100, 2) AS retention_rate
FROM Retention
GROUP BY month
ORDER BY month;

-- 14. Churn Rate
WITH MonthlyActivity AS (
    SELECT 
        DATE_TRUNC('month', order_date) AS month,
        customer_id,
        COUNT(DISTINCT order_id) AS order_count
    FROM orders
    GROUP BY month, customer_id
),
Churn AS (
    SELECT 
        current_month.month,
        current_month.customer_id,
        CASE 
            WHEN next_month.customer_id IS NULL AND 
                 current_month.month < DATE_TRUNC('month', CURRENT_DATE) 
            THEN 1 
            ELSE 0 
        END AS churned
    FROM MonthlyActivity current_month
    LEFT JOIN MonthlyActivity next_month 
        ON current_month.customer_id = next_month.customer_id 
        AND next_month.month = current_month.month + INTERVAL '1 month'
)
SELECT 
    month,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(churned) AS churned_customers,
    ROUND(SUM(churned)::DECIMAL / COUNT(DISTINCT customer_id) * 100, 2) AS churn_rate
FROM Churn
GROUP BY month
ORDER BY month;

-- 15. Quarterly Sales Analysis
SELECT 
    EXTRACT(YEAR FROM o.order_date) AS year,
    EXTRACT(QUARTER FROM o.order_date) AS quarter,
    SUM(s.total) AS revenue,
    COUNT(DISTINCT o.order_id) AS orders,
    COUNT(DISTINCT c.customer_id) AS customers,
    AVG(s.total) AS avg_order_value
FROM orders o
JOIN sales s ON o.order_id = s.order_id
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY year, quarter
ORDER BY year, quarter;

-- 16. Sales by Day of Week
SELECT 
    EXTRACT(DOW FROM o.order_date) AS day_of_week,
    TO_CHAR(o.order_date, 'Day') AS day_name,
    SUM(s.total) AS total_sales,
    COUNT(DISTINCT o.order_id) AS order_count,
    AVG(s.total) AS avg_sale
FROM orders o
JOIN sales s ON o.order_id = s.order_id
GROUP BY day_of_week, day_name
ORDER BY day_of_week;

-- 17. Hourly Sales Pattern
SELECT 
    EXTRACT(HOUR FROM o.order_date) AS hour_of_day,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(s.total) AS total_sales
FROM orders o
JOIN sales s ON o.order_id = s.order_id
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- 18. Top Selling Products by Category
SELECT 
    category,
    product_name,
    total_sales,
    rank
FROM (
    SELECT 
        p.category,
        p.product_name,
        SUM(s.total) AS total_sales,
        ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(s.total) DESC) AS rank
    FROM products p
    JOIN sales s ON p.product_id = s.product_id
    GROUP BY p.category, p.product_name
) ranked_products
WHERE rank <= 5
ORDER BY category, rank;

-- 19. Customer Lifecycle Value
WITH CustomerMetrics AS (
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        MIN(o.order_date) AS first_order,
        MAX(o.order_date) AS last_order,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(s.total) AS lifetime_value,
        AVG(s.total) AS avg_order_value,
        EXTRACT(DAY FROM AGE(MAX(o.order_date), MIN(o.order_date))) AS customer_lifetime_days
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN sales s ON o.order_id = s.order_id
    WHERE o.status = 'Completed'
    GROUP BY c.customer_id, c.first_name, c.last_name
)
SELECT 
    *,
    CASE 
        WHEN lifetime_value > 5000 THEN 'High Value'
        WHEN lifetime_value > 1000 THEN 'Medium Value'
        ELSE 'Low Value'
    END AS customer_segment,
    CASE 
        WHEN customer_lifetime_days < 30 THEN 'New'
        WHEN customer_lifetime_days < 180 THEN 'Regular'
        ELSE 'Loyal'
    END AS customer_tenure
FROM CustomerMetrics
ORDER BY lifetime_value DESC;

-- 20. Product Cross-Selling Analysis
WITH OrderProducts AS (
    SELECT 
        o.order_id,
        p.product_name,
        p.category
    FROM orders o
    JOIN sales s ON o.order_id = s.order_id
    JOIN products p ON s.product_id = p.product_id
    GROUP BY o.order_id, p.product_name, p.category
)
SELECT 
    a.product_name AS product_a,
    b.product_name AS product_b,
    COUNT(*) AS purchased_together
FROM OrderProducts a
INNER JOIN OrderProducts b ON a.order_id = b.order_id
    AND a.product_name < b.product_name
GROUP BY product_a, product_b
ORDER BY purchased_together DESC
LIMIT 20;