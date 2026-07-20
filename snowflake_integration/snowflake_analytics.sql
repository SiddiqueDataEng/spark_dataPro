-- snowflake/snowflake_analytics.sql
-- ============================================================
-- Retail Analytics — Snowflake SQL
-- ============================================================
-- All queries run against the tables loaded by pg_to_snowflake.py.
-- Schema context:  USE DATABASE RETAIL_DB; USE SCHEMA PUBLIC;
--
-- Sections
-- --------
--  1.  Sales Performance Dashboard
--  2.  Top Products by Revenue
--  3.  Customer Segmentation
--  4.  Store Performance Matrix
--  5.  Month-over-Month Growth
--  6.  Customer Lifetime Value (LTV) Distribution
--  7.  RFM Scoring (Recency · Frequency · Monetary)
--  8.  Cohort Retention Matrix
--  9.  ABC Product Classification (Pareto 80/15/5)
-- 10.  Inventory Risk (Days-of-Cover)
-- 11.  Year-over-Year Revenue Comparison
-- 12.  Seasonal Revenue Patterns
-- 13.  Discount Effectiveness
-- 14.  Employee Performance Ranking
-- 15.  Cross-Sell / Basket Analysis
-- 16.  Sales Anomaly Detection (Z-score)
-- ============================================================

USE DATABASE RETAIL_DB;
USE SCHEMA PUBLIC;


-- ============================================================
-- 1.  Sales Performance Dashboard
-- ============================================================
-- Daily revenue, units, order count, and average order value
-- per product category, ordered most-recent first.
-- ============================================================
SELECT
    o.ORDER_DATE                            AS sale_date,
    p.CATEGORY                              AS category,
    ROUND(SUM(s.TOTAL),        2)           AS total_revenue,
    SUM(s.QUANTITY)                         AS units_sold,
    COUNT(DISTINCT o.ORDER_ID)              AS order_count,
    COUNT(DISTINCT o.CUSTOMER_ID)           AS unique_customers,
    ROUND(SUM(s.TOTAL) / NULLIF(COUNT(DISTINCT o.ORDER_ID), 0), 2) AS avg_order_value,
    ROUND(SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) * 100, 2)       AS profit_margin_pct
FROM ORDERS   o
JOIN SALES    s ON o.ORDER_ID   = s.ORDER_ID
JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
WHERE o.ORDER_DATE >= DATEADD(year, -1, CURRENT_DATE())
GROUP BY o.ORDER_DATE, p.CATEGORY
ORDER BY o.ORDER_DATE DESC, total_revenue DESC;


-- ============================================================
-- 2.  Top 20 Products by Revenue
-- ============================================================
SELECT
    p.PRODUCT_NAME,
    p.CATEGORY,
    ROUND(SUM(s.TOTAL),            2)  AS total_revenue,
    SUM(s.QUANTITY)                    AS total_units_sold,
    ROUND(SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) * 100, 2) AS profit_margin_pct,
    RANK() OVER (ORDER BY SUM(s.TOTAL) DESC)                 AS revenue_rank
FROM PRODUCTS p
JOIN SALES    s ON p.PRODUCT_ID = s.PRODUCT_ID
GROUP BY p.PRODUCT_NAME, p.CATEGORY
ORDER BY total_revenue DESC
LIMIT 20;


-- ============================================================
-- 3.  Customer Segmentation by Lifetime Value
-- ============================================================
WITH customer_ltv AS (
    SELECT
        c.CUSTOMER_ID,
        c.FIRST_NAME,
        c.LAST_NAME,
        c.CITY,
        c.COUNTRY,
        ROUND(SUM(s.TOTAL),               2)  AS lifetime_value,
        COUNT(DISTINCT o.ORDER_ID)             AS total_orders,
        ROUND(SUM(s.TOTAL) / NULLIF(COUNT(DISTINCT o.ORDER_ID), 0), 2) AS avg_order_value
    FROM CUSTOMERS c
    JOIN ORDERS  o ON c.CUSTOMER_ID = o.CUSTOMER_ID
    JOIN SALES   s ON o.ORDER_ID    = s.ORDER_ID
    GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.CITY, c.COUNTRY
)
SELECT
    CASE
        WHEN lifetime_value >= 50000 THEN 'Platinum'
        WHEN lifetime_value >= 10000 THEN 'Gold'
        WHEN lifetime_value >=  5000 THEN 'Silver'
        WHEN lifetime_value >=  1000 THEN 'Bronze'
        ELSE 'Standard'
    END                                           AS customer_segment,
    COUNT(*)                                      AS customer_count,
    ROUND(AVG(lifetime_value), 2)                 AS avg_lifetime_value,
    ROUND(AVG(total_orders),   2)                 AS avg_orders,
    ROUND(AVG(avg_order_value), 2)                AS avg_order_value,
    ROUND(SUM(lifetime_value), 2)                 AS total_segment_revenue
FROM customer_ltv
GROUP BY customer_segment
ORDER BY total_segment_revenue DESC;


-- ============================================================
-- 4.  Store Performance Matrix
-- ============================================================
SELECT
    st.STORE_NAME,
    st.CITY,
    st.COUNTRY,
    st.REGION,
    ROUND(SUM(s.TOTAL),             2)  AS total_revenue,
    COUNT(DISTINCT o.ORDER_ID)          AS total_orders,
    COUNT(DISTINCT o.CUSTOMER_ID)       AS unique_customers,
    ROUND(SUM(s.TOTAL) / NULLIF(COUNT(DISTINCT o.ORDER_ID), 0), 2) AS avg_order_value,
    ROUND(SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) * 100, 2)        AS profit_margin_pct,
    RANK() OVER (ORDER BY SUM(s.TOTAL) DESC)                        AS revenue_rank,
    CASE
        WHEN SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) > 0.30
             AND RANK() OVER (ORDER BY SUM(s.TOTAL) DESC) <= 5  THEN 'Star'
        WHEN SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) > 0.30    THEN 'High Profit'
        WHEN RANK() OVER (ORDER BY SUM(s.TOTAL) DESC) <= 5      THEN 'High Revenue'
        WHEN SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) < 0.15
             AND RANK() OVER (ORDER BY SUM(s.TOTAL) DESC) > 20  THEN 'Underperforming'
        ELSE 'Average'
    END AS store_category
FROM STORES  st
JOIN ORDERS  o  ON st.STORE_ID   = o.STORE_ID
JOIN SALES   s  ON o.ORDER_ID    = s.ORDER_ID
GROUP BY st.STORE_NAME, st.CITY, st.COUNTRY, st.REGION
ORDER BY total_revenue DESC;


-- ============================================================
-- 5.  Month-over-Month Growth
-- ============================================================
WITH monthly_sales AS (
    SELECT
        DATE_TRUNC('month', o.ORDER_DATE)::DATE  AS sales_month,
        YEAR(o.ORDER_DATE)                        AS yr,
        MONTH(o.ORDER_DATE)                       AS mo,
        p.CATEGORY,
        ROUND(SUM(s.TOTAL), 2)                    AS monthly_revenue
    FROM ORDERS   o
    JOIN SALES    s ON o.ORDER_ID   = s.ORDER_ID
    JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY sales_month, yr, mo, p.CATEGORY
)
SELECT
    cur.sales_month,
    cur.yr                                AS year,
    cur.mo                                AS month,
    cur.CATEGORY,
    cur.monthly_revenue,
    prev.monthly_revenue                  AS prev_month_revenue,
    ROUND(
        (cur.monthly_revenue - prev.monthly_revenue)
        / NULLIF(prev.monthly_revenue, 0) * 100, 2
    )                                     AS mom_growth_pct,
    CASE
        WHEN (cur.monthly_revenue - prev.monthly_revenue)
             / NULLIF(prev.monthly_revenue, 0) >  0.10 THEN 'High Growth'
        WHEN (cur.monthly_revenue - prev.monthly_revenue)
             / NULLIF(prev.monthly_revenue, 0) >  0    THEN 'Growing'
        WHEN (cur.monthly_revenue - prev.monthly_revenue)
             / NULLIF(prev.monthly_revenue, 0) > -0.10 THEN 'Stable'
        ELSE 'Declining'
    END AS growth_category
FROM monthly_sales cur
LEFT JOIN monthly_sales prev
    ON  cur.mo       = prev.mo
    AND cur.CATEGORY = prev.CATEGORY
    AND cur.yr       = prev.yr + 1
ORDER BY cur.sales_month DESC, cur.CATEGORY;


-- ============================================================
-- 6.  Customer LTV Percentile Distribution
-- ============================================================
WITH customer_ltv AS (
    SELECT
        c.CUSTOMER_ID,
        ROUND(SUM(s.TOTAL), 2) AS lifetime_value
    FROM CUSTOMERS c
    JOIN ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
    JOIN SALES  s ON o.ORDER_ID    = s.ORDER_ID
    GROUP BY c.CUSTOMER_ID
)
SELECT
    PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY lifetime_value) AS p10,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY lifetime_value) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY lifetime_value) AS median,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY lifetime_value) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY lifetime_value) AS p90,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY lifetime_value) AS p95,
    ROUND(AVG(lifetime_value), 2)                                 AS mean,
    ROUND(STDDEV(lifetime_value), 2)                              AS std_dev,
    COUNT(*)                                                      AS total_customers
FROM customer_ltv;


-- ============================================================
-- 7.  RFM Scoring — Quintile-based, 9 Named Segments
-- ============================================================
WITH rfm_raw AS (
    SELECT
        c.CUSTOMER_ID,
        c.FIRST_NAME,
        c.LAST_NAME,
        c.CITY,
        c.COUNTRY,
        DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_DATE()) AS recency_days,
        COUNT(DISTINCT o.ORDER_ID)                          AS frequency,
        ROUND(SUM(s.TOTAL), 2)                              AS monetary
    FROM CUSTOMERS c
    JOIN ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
    JOIN SALES  s ON o.ORDER_ID    = s.ORDER_ID
    GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.CITY, c.COUNTRY
),
rfm_scores AS (
    SELECT
        *,
        -- Lower recency (more recent) = higher score, so invert ranking
        NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency)          AS f_score,
        NTILE(5) OVER (ORDER BY monetary)            AS m_score
    FROM rfm_raw
)
SELECT
    CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME,
    CITY,
    COUNTRY,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    r_score + f_score + m_score AS rfm_total,
    CONCAT(r_score::VARCHAR, f_score::VARCHAR, m_score::VARCHAR) AS rfm_code,
    CASE
        WHEN r_score = 5 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 4 AND f_score >= 3 AND m_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2                  THEN 'New Customers'
        WHEN r_score = 3  AND f_score >= 3                  THEN 'Potential Loyalists'
        WHEN r_score = 3  AND f_score <= 2 AND m_score >= 3 THEN 'Promising'
        WHEN r_score <= 2 AND f_score >= 4 AND m_score >= 4 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'Cant Lose Them'
        WHEN r_score <= 2 AND f_score <= 2 AND m_score >= 3 THEN 'Hibernating'
        ELSE 'Lost'
    END AS segment
FROM rfm_scores
ORDER BY rfm_total DESC;


-- ============================================================
-- 8.  Cohort Retention Matrix (months 0–11)
-- ============================================================
WITH first_order AS (
    SELECT
        CUSTOMER_ID,
        DATE_TRUNC('month', MIN(ORDER_DATE))::DATE AS cohort_month
    FROM ORDERS
    GROUP BY CUSTOMER_ID
),
activity AS (
    SELECT
        o.CUSTOMER_ID,
        f.cohort_month,
        DATE_TRUNC('month', o.ORDER_DATE)::DATE AS order_month
    FROM ORDERS o
    JOIN first_order f ON o.CUSTOMER_ID = f.CUSTOMER_ID
),
month_index AS (
    SELECT
        cohort_month,
        order_month,
        DATEDIFF('month', cohort_month, order_month) AS months_since_first,
        COUNT(DISTINCT CUSTOMER_ID)                  AS active_customers
    FROM activity
    GROUP BY cohort_month, order_month
),
cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT CUSTOMER_ID) AS total_customers
    FROM first_order
    GROUP BY cohort_month
)
SELECT
    mi.cohort_month,
    cs.total_customers AS cohort_size,
    mi.months_since_first,
    mi.active_customers,
    ROUND(mi.active_customers / cs.total_customers * 100, 1) AS retention_pct
FROM month_index mi
JOIN cohort_size cs ON mi.cohort_month = cs.cohort_month
WHERE mi.months_since_first BETWEEN 0 AND 11
ORDER BY mi.cohort_month, mi.months_since_first;


-- ============================================================
-- 9.  ABC Product Classification (Pareto 80 / 15 / 5)
-- ============================================================
WITH product_sales AS (
    SELECT
        p.PRODUCT_NAME,
        p.CATEGORY,
        ROUND(SUM(s.TOTAL), 2)  AS total_revenue,
        SUM(s.QUANTITY)          AS total_units
    FROM PRODUCTS p
    JOIN SALES    s ON p.PRODUCT_ID = s.PRODUCT_ID
    GROUP BY p.PRODUCT_NAME, p.CATEGORY
),
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC)                  AS rnk,
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)            AS running_total,
        SUM(total_revenue) OVER ()                                       AS grand_total
    FROM product_sales
)
SELECT
    PRODUCT_NAME,
    CATEGORY,
    total_revenue,
    total_units,
    rnk                                                      AS revenue_rank,
    ROUND(running_total / grand_total * 100, 2)              AS cumulative_pct,
    CASE
        WHEN running_total / grand_total <= 0.80 THEN 'A — Top 80%'
        WHEN running_total / grand_total <= 0.95 THEN 'B — Next 15%'
        ELSE                                          'C — Long Tail'
    END                                                      AS abc_class
FROM ranked
ORDER BY rnk;


-- ============================================================
-- 10.  Inventory Risk — Days-of-Cover
-- ============================================================
WITH demand AS (
    SELECT
        s.PRODUCT_ID,
        SUM(s.QUANTITY)                                         AS total_sold,
        GREATEST(DATEDIFF('day', MIN(o.ORDER_DATE),
                           MAX(o.ORDER_DATE)), 1)               AS sales_days
    FROM SALES  s
    JOIN ORDERS o ON s.ORDER_ID = o.ORDER_ID
    GROUP BY s.PRODUCT_ID
),
coverage AS (
    SELECT
        p.PRODUCT_ID,
        p.PRODUCT_NAME,
        p.CATEGORY,
        p.STOCK,
        d.total_sold,
        d.sales_days,
        ROUND(d.total_sold / d.sales_days, 3)                  AS daily_demand,
        ROUND(p.STOCK / NULLIF(d.total_sold / d.sales_days, 0), 0) AS days_of_cover
    FROM PRODUCTS p
    JOIN demand   d ON p.PRODUCT_ID = d.PRODUCT_ID
)
SELECT
    *,
    CASE
        WHEN days_of_cover < 30  THEN 'STOCKOUT RISK'
        WHEN days_of_cover > 365 THEN 'OVERSTOCKED'
        ELSE 'OK'
    END AS inventory_status
FROM coverage
ORDER BY days_of_cover NULLS LAST;


-- ============================================================
-- 11.  Year-over-Year Revenue Comparison
-- ============================================================
WITH yearly_sales AS (
    SELECT
        YEAR(o.ORDER_DATE)   AS yr,
        MONTH(o.ORDER_DATE)  AS mo,
        p.CATEGORY,
        ROUND(SUM(s.TOTAL), 2) AS revenue
    FROM ORDERS   o
    JOIN SALES    s ON o.ORDER_ID   = s.ORDER_ID
    JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY yr, mo, p.CATEGORY
)
SELECT
    cur.yr                                AS year,
    cur.mo                                AS month,
    cur.CATEGORY,
    cur.revenue                           AS current_year_revenue,
    prev.revenue                          AS previous_year_revenue,
    ROUND(
        (cur.revenue - prev.revenue) / NULLIF(prev.revenue, 0) * 100, 2
    )                                     AS yoy_growth_pct
FROM yearly_sales cur
LEFT JOIN yearly_sales prev
    ON  cur.mo       = prev.mo
    AND cur.CATEGORY = prev.CATEGORY
    AND cur.yr       = prev.yr + 1
ORDER BY cur.yr, cur.mo, cur.CATEGORY;


-- ============================================================
-- 12.  Seasonal Revenue Patterns
-- ============================================================
SELECT
    CASE
        WHEN MONTH(o.ORDER_DATE) IN (3, 4, 5) THEN 'Spring'
        WHEN MONTH(o.ORDER_DATE) IN (6, 7, 8) THEN 'Summer'
        WHEN MONTH(o.ORDER_DATE) IN (9,10,11) THEN 'Fall'
        ELSE 'Winter'
    END                                    AS season,
    p.CATEGORY,
    ROUND(SUM(s.TOTAL),            2)      AS seasonal_revenue,
    ROUND(AVG(s.TOTAL),            2)      AS avg_sale_revenue,
    SUM(s.QUANTITY)                        AS total_units,
    COUNT(DISTINCT o.ORDER_ID)             AS total_orders
FROM ORDERS   o
JOIN SALES    s ON o.ORDER_ID   = s.ORDER_ID
JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
GROUP BY season, p.CATEGORY
ORDER BY seasonal_revenue DESC;


-- ============================================================
-- 13.  Discount Effectiveness
-- ============================================================
SELECT
    CASE
        WHEN s.DISCOUNT = 0              THEN '0% — No Discount'
        WHEN s.DISCOUNT BETWEEN 1 AND 10 THEN '1–10%'
        WHEN s.DISCOUNT BETWEEN 11 AND 20 THEN '11–20%'
        WHEN s.DISCOUNT BETWEEN 21 AND 30 THEN '21–30%'
        ELSE '>30%'
    END                                                    AS discount_bucket,
    COUNT(*)                                               AS sales_count,
    ROUND(AVG(s.QUANTITY), 2)                              AS avg_qty,
    ROUND(AVG(s.TOTAL),    2)                              AS avg_order_value,
    ROUND(AVG(s.PROFIT),   2)                              AS avg_profit,
    ROUND(SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) * 100, 2) AS overall_margin_pct
FROM SALES s
GROUP BY discount_bucket
ORDER BY discount_bucket;


-- ============================================================
-- 14.  Employee Performance Ranking
-- ============================================================
SELECT
    e.EMPLOYEE_ID,
    e.EMPLOYEE_NAME,
    e.DEPARTMENT,
    e.SALARY,
    COUNT(DISTINCT o.ORDER_ID)                                   AS orders_handled,
    ROUND(SUM(s.TOTAL),  2)                                      AS revenue_generated,
    ROUND(AVG(s.TOTAL),  2)                                      AS avg_order_value,
    ROUND(SUM(s.PROFIT), 2)                                      AS total_profit,
    ROUND(SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) * 100, 2)     AS profit_margin_pct,
    ROUND(SUM(s.TOTAL)  / NULLIF(e.SALARY, 0), 4)               AS revenue_to_salary,
    RANK() OVER (ORDER BY SUM(s.TOTAL) DESC)                     AS revenue_rank,
    RANK() OVER (PARTITION BY e.DEPARTMENT ORDER BY SUM(s.TOTAL) DESC) AS dept_rank
FROM EMPLOYEES e
JOIN ORDERS    o ON e.EMPLOYEE_ID = o.EMPLOYEE_ID
JOIN SALES     s ON o.ORDER_ID    = s.ORDER_ID
GROUP BY e.EMPLOYEE_ID, e.EMPLOYEE_NAME, e.DEPARTMENT, e.SALARY
ORDER BY revenue_generated DESC;


-- ============================================================
-- 15.  Cross-Sell / Basket Analysis — Category Co-Occurrence
-- ============================================================
-- Finds the top category pairings that appear in the same order.
WITH order_cats AS (
    SELECT
        o.ORDER_ID,
        p.CATEGORY,
        COUNT(DISTINCT p.PRODUCT_ID) AS product_count
    FROM ORDERS   o
    JOIN SALES    s ON o.ORDER_ID   = s.ORDER_ID
    JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY o.ORDER_ID, p.CATEGORY
),
multi_cat_orders AS (
    SELECT ORDER_ID
    FROM order_cats
    GROUP BY ORDER_ID
    HAVING COUNT(DISTINCT CATEGORY) >= 2
),
pairs AS (
    SELECT
        a.ORDER_ID,
        a.CATEGORY AS cat_a,
        b.CATEGORY AS cat_b
    FROM order_cats a
    JOIN order_cats b
        ON  a.ORDER_ID = b.ORDER_ID
        AND a.CATEGORY < b.CATEGORY   -- avoid duplicate pairs
    JOIN multi_cat_orders m ON a.ORDER_ID = m.ORDER_ID
)
SELECT
    cat_a,
    cat_b,
    COUNT(DISTINCT ORDER_ID)   AS co_occurrence_count
FROM pairs
GROUP BY cat_a, cat_b
ORDER BY co_occurrence_count DESC
LIMIT 20;


-- ============================================================
-- 16.  Sales Anomaly Detection — Z-Score per Category
-- ============================================================
-- Flags daily revenue that deviates more than 2.5 std deviations
-- from the per-category mean.
WITH daily_cat AS (
    SELECT
        o.ORDER_DATE                  AS sale_date,
        p.CATEGORY,
        ROUND(SUM(s.TOTAL), 2)        AS daily_revenue
    FROM ORDERS   o
    JOIN SALES    s ON o.ORDER_ID   = s.ORDER_ID
    JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
    GROUP BY o.ORDER_DATE, p.CATEGORY
),
stats AS (
    SELECT
        CATEGORY,
        AVG(daily_revenue)    AS mu,
        STDDEV(daily_revenue) AS sigma
    FROM daily_cat
    GROUP BY CATEGORY
),
scored AS (
    SELECT
        d.sale_date,
        d.CATEGORY,
        d.daily_revenue,
        ROUND((d.daily_revenue - s.mu) / NULLIF(s.sigma, 0), 3) AS z_score
    FROM daily_cat d
    JOIN stats     s ON d.CATEGORY = s.CATEGORY
)
SELECT
    sale_date,
    CATEGORY,
    daily_revenue,
    z_score,
    CASE WHEN ABS(z_score) > 2.5 THEN 'ANOMALY' ELSE 'normal' END AS flag
FROM scored
WHERE ABS(z_score) > 2.5
ORDER BY ABS(z_score) DESC;
