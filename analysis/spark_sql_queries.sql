-- analysis/spark_sql_queries.sql

-- 1. Sales Performance Dashboard
SELECT 
    sale_date,
    category,
    total_revenue,
    units_sold,
    order_count,
    unique_customers,
    avg_order_value,
    profit_margin
FROM gold.daily_sales_summary
WHERE sale_date >= '2025-01-01'
ORDER BY sale_date DESC, total_revenue DESC;

-- 2. Top Products by Revenue
SELECT 
    product_name,
    category,
    total_revenue,
    total_units_sold,
    profit_margin,
    revenue_rank,
    performance_score
FROM gold.product_performance
ORDER BY total_revenue DESC
LIMIT 20;

-- 3. Customer Segmentation
SELECT 
    customer_segment,
    COUNT(*) AS customer_count,
    AVG(lifetime_value) AS avg_lifetime_value,
    AVG(total_orders) AS avg_orders,
    AVG(avg_order_value) AS avg_order_value,
    SUM(lifetime_value) AS total_segment_value
FROM gold.customer_analytics
GROUP BY customer_segment
ORDER BY total_segment_value DESC;

-- 4. Store Performance Matrix
SELECT 
    store_name,
    city,
    region,
    total_revenue,
    total_orders,
    unique_customers,
    avg_order_value,
    profit_margin,
    revenue_rank,
    CASE 
        WHEN profit_margin > 30 AND revenue_rank <= 5 THEN 'Star'
        WHEN profit_margin > 30 THEN 'High Profit'
        WHEN revenue_rank <= 5 THEN 'High Revenue'
        WHEN profit_margin < 15 AND revenue_rank > 20 THEN 'Underperforming'
        ELSE 'Average'
    END AS store_category
FROM gold.store_performance
ORDER BY total_revenue DESC;

-- 5. Month-over-Month Growth Analysis
SELECT 
    year,
    month,
    category,
    monthly_revenue,
    prev_month_revenue,
    mom_growth_pct,
    growth_category
FROM gold.monthly_time_series
WHERE year = 2025
ORDER BY year, month, category;

-- 6. Customer Lifetime Value Distribution
SELECT 
    CASE 
        WHEN lifetime_value < 1000 THEN '0-1K'
        WHEN lifetime_value < 5000 THEN '1K-5K'
        WHEN lifetime_value < 10000 THEN '5K-10K'
        WHEN lifetime_value < 50000 THEN '10K-50K'
        ELSE '50K+'
    END AS lifetime_value_bucket,
    COUNT(*) AS customer_count,
    SUM(lifetime_value) AS total_clv,
    AVG(lifetime_value) AS avg_clv,
    AVG(total_orders) AS avg_orders
FROM gold.customer_analytics
GROUP BY lifetime_value_bucket
ORDER BY MIN(lifetime_value);

-- 7. Product Cross-Selling Patterns
WITH order_combinations AS (
    SELECT 
        o.order_id,
        COLLECT_LIST(p.product_name) AS products,
        COLLECT_LIST(p.category) AS categories
    FROM silver.orders o
    JOIN silver.sales s ON o.order_id = s.order_id
    JOIN silver.products p ON s.product_id = p.product_id
    GROUP BY o.order_id
    HAVING SIZE(COLLECT_LIST(p.product_id)) > 1
)
SELECT 
    products,
    categories,
    COUNT(*) AS order_count
FROM order_combinations
GROUP BY products, categories
ORDER BY order_count DESC
LIMIT 20;

-- 8. Seasonal Sales Patterns
SELECT 
    CASE 
        WHEN EXTRACT(MONTH FROM sale_date) IN (3,4,5) THEN 'Spring'
        WHEN EXTRACT(MONTH FROM sale_date) IN (6,7,8) THEN 'Summer'
        WHEN EXTRACT(MONTH FROM sale_date) IN (9,10,11) THEN 'Fall'
        ELSE 'Winter'
    END AS season,
    category,
    SUM(total_revenue) AS seasonal_revenue,
    AVG(total_revenue) AS avg_daily_revenue,
    SUM(units_sold) AS total_units
FROM gold.daily_sales_summary
GROUP BY season, category
ORDER BY season, seasonal_revenue DESC;

-- 9. Inventory Turnover Analysis
SELECT 
    p.product_name,
    p.category,
    p.stock,
    p.total_units_sold,
    p.profit_margin,
    p.inventory_turnover_days,
    CASE 
        WHEN p.inventory_turnover_days < 30 THEN 'Fast Moving'
        WHEN p.inventory_turnover_days < 90 THEN 'Medium Moving'
        ELSE 'Slow Moving'
    END AS inventory_category
FROM gold.product_performance p
WHERE p.inventory_turnover_days IS NOT NULL
ORDER BY inventory_turnover_days;

-- 10. Year-over-Year Comparison
WITH yearly_sales AS (
    SELECT 
        EXTRACT(YEAR FROM sale_date) AS year,
        EXTRACT(MONTH FROM sale_date) AS month,
        category,
        SUM(total_revenue) AS revenue
    FROM gold.daily_sales_summary
    GROUP BY year, month, category
)
SELECT 
    current.year,
    current.month,
    current.category,
    current.revenue AS current_year_revenue,
    previous.revenue AS previous_year_revenue,
    ROUND((current.revenue - previous.revenue) / previous.revenue * 100, 2) AS yoy_growth_pct
FROM yearly_sales current
LEFT JOIN yearly_sales previous 
    ON current.month = previous.month 
    AND current.category = previous.category
    AND current.year = previous.year + 1
ORDER BY current.year, current.month, current.category;