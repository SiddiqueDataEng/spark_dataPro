-- ============================================================================
-- Amazon Athena SQL Queries — Retail Medallion Data Lake
-- Account: 021891603670 (MSiddique)  |  Region: us-east-1
-- Dialect: Athena engine version 3 (Trino / Presto)
-- ============================================================================
-- Databases:
--   retail_bronze   → s3://retail-raw-021891603670-us-east-1/bronze/
--   retail_silver   → s3://retail-clean-021891603670-us-east-1/silver/
--   retail_gold     → s3://retail-curated-021891603670-us-east-1/gold/
-- ============================================================================

-- ── 0. Athena setup ──────────────────────────────────────────────────────────

-- Repair partitions after new data lands in S3
MSCK REPAIR TABLE retail_gold.sales;
MSCK REPAIR TABLE retail_gold.orders;


-- ── 1. Sales Dashboard ───────────────────────────────────────────────────────
-- Daily revenue by category, last 12 months

SELECT
    date_trunc('month', o.order_date)        AS month,
    p.category,
    ROUND(SUM(s.total_revenue), 2)           AS revenue,
    ROUND(SUM(s.profit), 2)                  AS profit,
    COUNT(DISTINCT o.order_id)               AS num_orders
FROM   retail_gold.sales     s
JOIN   retail_gold.orders    o  ON s.order_id  = o.order_id
JOIN   retail_gold.products  p  ON s.product_id = p.product_id
WHERE  o.order_date >= date_add('month', -12, current_date)
GROUP  BY 1, 2
ORDER  BY month DESC, revenue DESC;


-- ── 2. Top 20 Products by Revenue ────────────────────────────────────────────

SELECT
    p.product_id,
    p.product_name,
    p.category,
    ROUND(SUM(s.total_revenue), 2)   AS revenue,
    ROUND(SUM(s.profit), 2)           AS profit,
    SUM(s.quantity)                   AS units_sold,
    RANK() OVER (ORDER BY SUM(s.total_revenue) DESC) AS revenue_rank
FROM   retail_gold.sales    s
JOIN   retail_gold.products p ON s.product_id = p.product_id
GROUP  BY 1, 2, 3
ORDER  BY revenue DESC
LIMIT  20;


-- ── 3. Customer Segmentation ─────────────────────────────────────────────────
-- Platinum / Gold / Silver / Bronze / Standard tiers

WITH ltv AS (
    SELECT
        o.customer_id,
        ROUND(SUM(s.total_revenue), 2) AS lifetime_value,
        COUNT(DISTINCT o.order_id)     AS total_orders
    FROM   retail_gold.sales  s
    JOIN   retail_gold.orders o ON s.order_id = o.order_id
    GROUP  BY 1
)
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS full_name,
    c.country,
    l.lifetime_value,
    l.total_orders,
    CASE
        WHEN l.lifetime_value >= 5000 THEN 'Platinum'
        WHEN l.lifetime_value >= 2000 THEN 'Gold'
        WHEN l.lifetime_value >= 500  THEN 'Silver'
        WHEN l.lifetime_value >= 100  THEN 'Bronze'
        ELSE                               'Standard'
    END AS segment
FROM   retail_gold.customers c
JOIN   ltv l ON c.customer_id = l.customer_id
ORDER  BY lifetime_value DESC;


-- ── 4. RFM Quintile Scoring ───────────────────────────────────────────────────

WITH base AS (
    SELECT
        o.customer_id,
        DATE_DIFF('day', MAX(o.order_date), current_date) AS recency_days,
        COUNT(DISTINCT o.order_id)                         AS frequency,
        ROUND(SUM(s.total_revenue), 2)                     AS monetary
    FROM   retail_gold.orders o
    JOIN   retail_gold.sales  s ON s.order_id = o.order_id
    GROUP  BY 1
),
scored AS (
    SELECT *,
        NTILE(5) OVER (ORDER BY recency_days ASC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC)    AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC)     AS m_score
    FROM base
)
SELECT
    customer_id, recency_days, frequency, monetary,
    r_score, f_score, m_score,
    CONCAT(CAST(r_score AS VARCHAR),
           CAST(f_score AS VARCHAR),
           CAST(m_score AS VARCHAR))           AS rfm_score,
    CASE
        WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal'
        WHEN r_score >= 4 AND f_score < 3  THEN 'Recent'
        WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
        WHEN r_score >= 3 AND f_score <= 2 THEN 'Promising'
        ELSE                                    'Needs Attention'
    END                                        AS segment
FROM scored
ORDER BY monetary DESC;


-- ── 5. Cohort Retention Matrix ────────────────────────────────────────────────

WITH first_order AS (
    SELECT customer_id,
           date_trunc('month', MIN(order_date)) AS cohort_month
    FROM   retail_gold.orders
    GROUP  BY 1
),
orders_with_cohort AS (
    SELECT
        o.customer_id,
        f.cohort_month,
        DATE_DIFF('month', f.cohort_month,
            date_trunc('month', o.order_date)) AS months_since
    FROM   retail_gold.orders o
    JOIN   first_order f ON o.customer_id = f.customer_id
    WHERE  DATE_DIFF('month', f.cohort_month,
               date_trunc('month', o.order_date)) <= 11
)
SELECT
    cohort_month,
    months_since,
    COUNT(DISTINCT customer_id)               AS customers,
    ROUND(
        COUNT(DISTINCT customer_id) * 100.0 /
        FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
            PARTITION BY cohort_month
            ORDER BY months_since
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ), 2
    )                                         AS retention_pct
FROM   orders_with_cohort
GROUP  BY 1, 2
ORDER  BY cohort_month, months_since;


-- ── 6. ABC / Pareto Product Classification ────────────────────────────────────

WITH revenue AS (
    SELECT
        p.product_id, p.product_name, p.category,
        SUM(s.total_revenue) AS revenue
    FROM   retail_gold.sales    s
    JOIN   retail_gold.products p ON s.product_id = p.product_id
    GROUP  BY 1, 2, 3
),
cumulative AS (
    SELECT *,
        SUM(revenue) OVER (ORDER BY revenue DESC
            ROWS UNBOUNDED PRECEDING)       AS cum_revenue,
        SUM(revenue) OVER ()                AS total_revenue
    FROM revenue
)
SELECT
    product_id, product_name, category,
    ROUND(revenue, 2)                                AS revenue,
    ROUND(cum_revenue / total_revenue * 100, 2)      AS cum_pct,
    CASE
        WHEN cum_revenue / total_revenue <= 0.80 THEN 'A'
        WHEN cum_revenue / total_revenue <= 0.95 THEN 'B'
        ELSE                                          'C'
    END                                              AS abc_class
FROM   cumulative
ORDER  BY revenue DESC;


-- ── 7. Inventory Risk (Days of Cover) ────────────────────────────────────────

WITH daily_sales AS (
    SELECT
        s.product_id,
        SUM(s.quantity) / NULLIF(COUNT(DISTINCT o.order_date), 0) AS avg_daily_units
    FROM   retail_gold.sales  s
    JOIN   retail_gold.orders o ON s.order_id = o.order_id
    WHERE  o.order_date >= date_add('day', -90, current_date)
    GROUP  BY 1
)
SELECT
    p.product_id, p.product_name, p.category,
    p.stock,
    ROUND(d.avg_daily_units, 2)                    AS avg_daily_units,
    ROUND(p.stock / NULLIF(d.avg_daily_units, 0), 1) AS days_of_cover,
    CASE
        WHEN p.stock / NULLIF(d.avg_daily_units, 0) < 14  THEN 'STOCKOUT_RISK'
        WHEN p.stock / NULLIF(d.avg_daily_units, 0) > 180 THEN 'OVERSTOCKED'
        ELSE                                                    'OK'
    END                                            AS risk_flag
FROM   retail_bronze.products p
LEFT   JOIN daily_sales d ON p.product_id = d.product_id
ORDER  BY days_of_cover ASC NULLS FIRST;


-- ── 8. Month-over-Month Growth ────────────────────────────────────────────────

WITH monthly AS (
    SELECT
        date_trunc('month', o.order_date) AS month,
        p.category,
        SUM(s.total_revenue)              AS revenue
    FROM   retail_gold.sales    s
    JOIN   retail_gold.orders   o ON s.order_id  = o.order_id
    JOIN   retail_gold.products p ON s.product_id = p.product_id
    GROUP  BY 1, 2
)
SELECT
    m.month, m.category,
    ROUND(m.revenue, 2) AS revenue,
    ROUND(LAG(m.revenue) OVER (PARTITION BY m.category ORDER BY m.month), 2)
                        AS prev_revenue,
    ROUND(
        (m.revenue - LAG(m.revenue) OVER (PARTITION BY m.category ORDER BY m.month))
        / NULLIF(LAG(m.revenue) OVER (PARTITION BY m.category ORDER BY m.month), 0)
        * 100, 2
    )                   AS mom_growth_pct
FROM   monthly m
ORDER  BY month DESC, category;


-- ── 9. Revenue Anomaly Detection (|z-score| > 2.5) ───────────────────────────

WITH daily AS (
    SELECT
        p.category, o.order_date,
        SUM(s.total_revenue) AS revenue
    FROM   retail_gold.sales    s
    JOIN   retail_gold.orders   o ON s.order_id  = o.order_id
    JOIN   retail_gold.products p ON s.product_id = p.product_id
    GROUP  BY 1, 2
),
stats AS (
    SELECT *,
        AVG(revenue)    OVER (PARTITION BY category) AS mean_rev,
        STDDEV(revenue) OVER (PARTITION BY category) AS std_rev
    FROM daily
)
SELECT
    category, order_date,
    ROUND(revenue, 2)  AS revenue,
    ROUND(mean_rev, 2) AS mean_revenue,
    ROUND(ABS((revenue - mean_rev) / NULLIF(std_rev, 0)), 2) AS z_score
FROM   stats
WHERE  ABS((revenue - mean_rev) / NULLIF(std_rev, 0)) > 2.5
ORDER  BY z_score DESC;


-- ── 10. Employee Performance Ranking ─────────────────────────────────────────

SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name AS full_name,
    e.department,
    COUNT(DISTINCT o.order_id)              AS orders_handled,
    ROUND(SUM(s.total_revenue), 2)          AS revenue_generated,
    ROUND(SUM(s.profit), 2)                 AS profit_generated,
    RANK() OVER (ORDER BY SUM(s.total_revenue) DESC) AS revenue_rank
FROM   retail_gold.employees e
JOIN   retail_gold.orders    o ON o.employee_id = e.employee_id
JOIN   retail_gold.sales     s ON s.order_id    = o.order_id
GROUP  BY 1, 2, 3
ORDER  BY revenue_generated DESC;


-- ── 11. Cross-sell / Basket Analysis ─────────────────────────────────────────

WITH multi_item AS (
    SELECT order_id
    FROM   retail_gold.sales
    GROUP  BY order_id
    HAVING COUNT(DISTINCT product_id) >= 2
),
pairs AS (
    SELECT a.product_id AS product_a, b.product_id AS product_b
    FROM   retail_gold.sales a
    JOIN   retail_gold.sales b
        ON  a.order_id  = b.order_id
        AND a.product_id < b.product_id
    WHERE  a.order_id IN (SELECT order_id FROM multi_item)
)
SELECT
    pa.product_name AS product_a,
    pb.product_name AS product_b,
    pa.category     AS category_a,
    pb.category     AS category_b,
    COUNT(*)        AS co_occurrences
FROM   pairs
JOIN   retail_gold.products pa ON pairs.product_a = pa.product_id
JOIN   retail_gold.products pb ON pairs.product_b = pb.product_id
GROUP  BY 1, 2, 3, 4
ORDER  BY co_occurrences DESC
LIMIT  30;


-- ── 12. Year-over-Year Comparison ────────────────────────────────────────────

WITH monthly AS (
    SELECT
        YEAR(o.order_date)  AS yr,
        MONTH(o.order_date) AS mth,
        p.category,
        SUM(s.total_revenue) AS revenue
    FROM   retail_gold.sales    s
    JOIN   retail_gold.orders   o ON s.order_id  = o.order_id
    JOIN   retail_gold.products p ON s.product_id = p.product_id
    GROUP  BY 1, 2, 3
)
SELECT
    a.yr, a.mth, a.category,
    ROUND(a.revenue, 2) AS current_year_rev,
    ROUND(b.revenue, 2) AS prev_year_rev,
    ROUND((a.revenue - b.revenue) / NULLIF(b.revenue, 0) * 100, 2) AS yoy_growth_pct
FROM   monthly a
LEFT   JOIN monthly b
    ON  a.category = b.category
    AND a.mth      = b.mth
    AND a.yr       = b.yr + 1
ORDER  BY a.yr DESC, a.mth, a.category;


-- ── 13. LTV Distribution (Percentiles) ───────────────────────────────────────

WITH ltv AS (
    SELECT o.customer_id, SUM(s.total_revenue) AS lifetime_value
    FROM   retail_gold.sales  s
    JOIN   retail_gold.orders o ON s.order_id = o.order_id
    GROUP  BY 1
)
SELECT
    ROUND(approx_percentile(lifetime_value, 0.10), 2) AS p10,
    ROUND(approx_percentile(lifetime_value, 0.25), 2) AS p25,
    ROUND(approx_percentile(lifetime_value, 0.50), 2) AS median,
    ROUND(approx_percentile(lifetime_value, 0.75), 2) AS p75,
    ROUND(approx_percentile(lifetime_value, 0.90), 2) AS p90,
    ROUND(approx_percentile(lifetime_value, 0.95), 2) AS p95,
    ROUND(AVG(lifetime_value), 2)                     AS mean_ltv,
    ROUND(STDDEV(lifetime_value), 2)                  AS std_dev
FROM ltv;


-- ── 14. Discount Effectiveness ───────────────────────────────────────────────

SELECT
    CASE
        WHEN o.discount_pct = 0     THEN 'No Discount'
        WHEN o.discount_pct <= 0.10 THEN '1-10%'
        WHEN o.discount_pct <= 0.20 THEN '11-20%'
        WHEN o.discount_pct <= 0.30 THEN '21-30%'
        ELSE                             '31%+'
    END                                        AS discount_bucket,
    COUNT(DISTINCT o.order_id)                 AS num_orders,
    ROUND(AVG(s.total_revenue), 2)             AS avg_order_value,
    ROUND(AVG(s.profit / NULLIF(s.total_revenue, 0) * 100), 2) AS avg_margin_pct,
    ROUND(SUM(s.profit), 2)                    AS total_profit
FROM   retail_gold.orders o
JOIN   retail_gold.sales  s ON o.order_id = s.order_id
GROUP  BY 1
ORDER  BY discount_bucket;


-- ── 15. Store Performance ─────────────────────────────────────────────────────

SELECT
    st.store_id, st.store_name, st.city, st.country,
    ROUND(SUM(s.total_revenue), 2)  AS revenue,
    ROUND(SUM(s.profit), 2)          AS profit,
    COUNT(DISTINCT o.order_id)       AS num_orders,
    ROUND(AVG(s.total_revenue), 2)   AS avg_order_value,
    RANK() OVER (ORDER BY SUM(s.total_revenue) DESC) AS revenue_rank
FROM   retail_gold.sales   s
JOIN   retail_gold.orders  o  ON s.order_id = o.order_id
JOIN   retail_gold.stores  st ON o.store_id = st.store_id
GROUP  BY 1, 2, 3, 4
ORDER  BY revenue DESC;


-- ── 16. Day-of-Week Seasonality ───────────────────────────────────────────────

SELECT
    DAY_OF_WEEK(o.order_date)          AS day_of_week,
    MONTH(o.order_date)                AS month_num,
    p.category,
    ROUND(SUM(s.total_revenue), 2)     AS revenue,
    COUNT(DISTINCT o.order_id)         AS num_orders
FROM   retail_gold.sales    s
JOIN   retail_gold.orders   o ON s.order_id  = o.order_id
JOIN   retail_gold.products p ON s.product_id = p.product_id
GROUP  BY 1, 2, 3
ORDER  BY month_num, day_of_week, revenue DESC;


-- ── Glue Catalog / Lake Formation checks ─────────────────────────────────────

-- List all Athena databases
SHOW DATABASES;

-- List tables in Gold layer
SHOW TABLES IN retail_gold;

-- Check partitions
SHOW PARTITIONS retail_gold.sales;

-- Preview data
SELECT * FROM retail_gold.sales     LIMIT 10;
SELECT * FROM retail_gold.customers LIMIT 10;
SELECT * FROM retail_bronze.customers LIMIT 5;    -- includes CDC columns
