-- 21. Customer Lifetime Value (CLTV) Prediction
WITH CustomerMetrics AS (
    SELECT 
        c.customer_id,
        c.first_name || ' ' || c.last_name AS customer_name,
        COUNT(DISTINCT o.order_id) AS order_count,
        SUM(s.total) AS total_spent,
        AVG(s.total) AS avg_order_value,
        MIN(o.order_date) AS first_order,
        MAX(o.order_date) AS last_order,
        EXTRACT(DAY FROM AGE(MAX(o.order_date), MIN(o.order_date))) AS days_active
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN sales s ON o.order_id = s.order_id
    GROUP BY c.customer_id, c.first_name, c.last_name
)
SELECT 
    *,
    ROUND(total_spent / NULLIF(days_active, 0) * 365, 2) AS annual_spend,
    ROUND(avg_order_value * (EXTRACT(YEAR FROM AGE(CURRENT_DATE, first_order)) + 1), 2) AS predicted_lifetime_value
FROM CustomerMetrics
ORDER BY predicted_lifetime_value DESC;

-- 22. Product Performance by Season
SELECT 
    p.product_name,
    p.category,
    CASE 
        WHEN EXTRACT(MONTH FROM o.order_date) IN (3,4,5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM o.order_date) IN (6,7,8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM o.order_date) IN (9,10,11) THEN 'Fall'
        ELSE 'Winter'
    END AS season,
    SUM(s.total) AS total_sales,
    SUM(s.quantity) AS units_sold
FROM sales s
JOIN products p ON s.product_id = p.product_id
JOIN orders o ON s.order_id = o.order_id
GROUP BY p.product_name, p.category, season
ORDER BY p.category, season, total_sales DESC;

-- 23. Inventory Turnover
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.stock AS current_stock,
    COALESCE(SUM(s.quantity), 0) AS units_sold,
    COALESCE(SUM(s.total), 0) AS revenue,
    CASE 
        WHEN COALESCE(SUM(s.quantity), 0) = 0 THEN NULL
        ELSE p.stock / NULLIF(SUM(s.quantity), 0) * 365
    END AS days_of_inventory
FROM products p
LEFT JOIN sales s ON p.product_id = s.product_id
LEFT JOIN orders o ON s.order_id = o.order_id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '365 days' OR o.order_date IS NULL
GROUP BY p.product_id, p.product_name, p.category, p.stock
ORDER BY days_of_inventory;

-- 24. Customer Purchase Pattern Analysis
SELECT 
    customer_id,
    order_date,
    total_spent,
    LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_order_date,
    EXTRACT(DAY FROM AGE(order_date, LAG(order_date) OVER (PARTITION BY customer_id ORDER BY order_date))) AS days_between_orders,
    LAG(total_spent) OVER (PARTITION BY customer_id ORDER BY order_date) AS previous_spent,
    total_spent - LAG(total_spent) OVER (PARTITION BY customer_id ORDER BY order_date) AS spending_change
FROM (
    SELECT 
        c.customer_id,
        o.order_date,
        SUM(s.total) AS total_spent
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
    JOIN sales s ON o.order_id = s.order_id
    GROUP BY c.customer_id, o.order_date
) customer_orders
ORDER BY customer_id, order_date;

-- 25. Store Performance Comparison
SELECT 
    st.store_name,
    st.city,
    st.country,
    COUNT(DISTINCT o.order_id) AS total_orders,
    SUM(s.total) AS total_revenue,
    AVG(s.total) AS avg_order_value,
    COUNT(DISTINCT c.customer_id) AS unique_customers,
    SUM(s.profit) AS total_profit,
    ROUND(SUM(s.profit) / NULLIF(SUM(s.total), 0) * 100, 2) AS profit_margin,
    RANK() OVER (ORDER BY SUM(s.total) DESC) AS revenue_rank,
    RANK() OVER (ORDER BY SUM(s.profit) DESC) AS profit_rank
FROM stores st
JOIN orders o ON st.store_id = o.store_id
JOIN sales s ON o.order_id = s.order_id
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY st.store_name, st.city, st.country
ORDER BY total_revenue DESC;