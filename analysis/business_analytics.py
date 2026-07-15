# analysis/business_analytics.py
"""
BusinessAnalytics
==================
Core Spark SQL analytics against Gold and Silver Delta tables.

Views expected (registered by register_views):
  Gold  : daily_sales_summary, customer_analytics, product_performance,
          store_performance, monthly_time_series
  Silver: silver_customers, silver_products, silver_orders, silver_sales
"""
from __future__ import annotations

import os
import sys
import logging

# Ensure the project root (parent of this file's directory) is on sys.path so
# that `config`, `analysis`, etc. are importable when this file is run directly.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum, avg, count, max, min, round, rank, lag,
    countDistinct, datediff, current_date, when, lit,
    ntile, row_number, year, month, dayofmonth,
    quarter, weekofyear, collect_list, array_distinct, size,
)
from config.spark_config import SparkConfig

log = logging.getLogger(__name__)


class BusinessAnalytics:
    """Core business analytics using Spark SQL against Gold/Silver Delta tables."""

    def __init__(self, spark: SparkSession | None = None):
        # Use the supplied session; only create one if none is provided.
        if spark is not None:
            self.spark = spark
        else:
            self.spark = SparkConfig().get_spark_session()

        # Paths anchored to project root — safe regardless of cwd.
        self.gold_path   = os.path.join(_PROJECT_ROOT, "data", "gold")
        self.silver_path = os.path.join(_PROJECT_ROOT, "data", "silver")

    # ------------------------------------------------------------------
    # View registration
    # ------------------------------------------------------------------

    def register_views(self) -> None:
        """
        Register Gold and Silver Delta tables as Spark SQL temp views.
        Gold views  : <table_name>
        Silver views: silver_<table_name>
        Safe to call multiple times (createOrReplaceTempView is idempotent).
        """
        gold_tables = [
            "daily_sales_summary",
            "customer_analytics",
            "product_performance",
            "store_performance",
            "monthly_time_series",
        ]
        silver_tables = [
            "customers", "products", "employees",
            "stores", "orders", "sales",
        ]

        for table in gold_tables:
            path = os.path.join(self.gold_path, table)
            try:
                self.spark.read.format("delta").load(path).createOrReplaceTempView(table)
                print(f"✅ Registered gold view : {table}")
            except Exception as exc:
                print(f"⚠️  Could not register gold view {table}: {exc}")

        for table in silver_tables:
            path = os.path.join(self.silver_path, table)
            try:
                self.spark.read.format("delta").load(path).createOrReplaceTempView(f"silver_{table}")
                print(f"✅ Registered silver view: silver_{table}")
            except Exception as exc:
                print(f"⚠️  Could not register silver view {table}: {exc}")

    # ------------------------------------------------------------------
    # Sales analytics  (Gold views)
    # ------------------------------------------------------------------

    def run_sales_analytics(self) -> dict:
        """
        Run core sales analytics. Returns a dict of lazy Spark DataFrames.

        Views used: daily_sales_summary, customer_analytics,
                    product_performance, store_performance,
                    monthly_time_series
        """
        monthly_revenue = self.spark.sql("""
            SELECT
                year,
                month,
                category,
                SUM(total_revenue)  AS monthly_revenue,
                SUM(units_sold)     AS total_units,
                SUM(order_count)    AS total_orders,
                AVG(total_revenue)  AS avg_daily_revenue
            FROM daily_sales_summary
            GROUP BY year, month, category
            ORDER BY year, month, category
        """)

        top_customers = self.spark.sql("""
            SELECT
                customer_id,
                first_name,
                last_name,
                email,
                lifetime_value,
                total_orders,
                avg_order_value,
                customer_segment,
                churn_risk
            FROM customer_analytics
            ORDER BY lifetime_value DESC
            LIMIT 10
        """)

        product_ranking = self.spark.sql("""
            SELECT
                product_name,
                category,
                total_revenue,
                total_units_sold,
                profit_margin,
                revenue_rank,
                profit_rank,
                CASE
                    WHEN revenue_rank <= 10 AND profit_rank <= 10 THEN 'Star'
                    WHEN revenue_rank <= 10                        THEN 'Cash Cow'
                    WHEN profit_rank  <= 10                        THEN 'High Profit'
                    ELSE 'Average'
                END AS product_category
            FROM product_performance
            ORDER BY total_revenue DESC
        """)

        store_comparison = self.spark.sql("""
            SELECT
                store_name,
                city,
                country,
                region,
                total_revenue,
                total_orders,
                unique_customers,
                avg_order_value,
                profit_margin,
                revenue_rank
            FROM store_performance
            ORDER BY total_revenue DESC
        """)

        time_series_analysis = self.spark.sql("""
            SELECT
                year,
                month,
                category,
                monthly_revenue,
                prev_month_revenue,
                mom_growth_pct,
                CASE
                    WHEN mom_growth_pct >  10 THEN 'High Growth'
                    WHEN mom_growth_pct >   0 THEN 'Growing'
                    WHEN mom_growth_pct > -10 THEN 'Stable'
                    ELSE 'Declining'
                END AS growth_category
            FROM monthly_time_series
            WHERE prev_month_revenue IS NOT NULL
            ORDER BY year, month, category
        """)

        return {
            "monthly_revenue":      monthly_revenue,
            "top_customers":        top_customers,
            "product_ranking":      product_ranking,
            "store_comparison":     store_comparison,
            "time_series_analysis": time_series_analysis,
        }

    # ------------------------------------------------------------------
    # Complex analytics  (Silver views)
    # ------------------------------------------------------------------

    def run_complex_analytics(self) -> dict:
        """
        Cohort retention, cross-sell analysis, and RFM scoring.

        Views used: silver_customers, silver_orders, silver_sales,
                    silver_products
        """
        cohort_retention = self.spark.sql("""
            WITH cohort_data AS (
                SELECT
                    c.customer_id,
                    YEAR(c.join_date)   AS cohort_year,
                    MONTH(c.join_date)  AS cohort_month,
                    YEAR(o.order_date)  AS order_year,
                    MONTH(o.order_date) AS order_month
                FROM silver_customers c
                JOIN silver_orders  o ON c.customer_id = o.customer_id
            ),
            cohort_metrics AS (
                SELECT
                    cohort_year,
                    cohort_month,
                    COUNT(DISTINCT customer_id) AS cohort_size,
                    COUNT(DISTINCT CASE
                        WHEN order_year = cohort_year AND order_month = cohort_month
                        THEN customer_id END) AS first_month_customers,
                    COUNT(DISTINCT CASE
                        WHEN order_year > cohort_year OR
                             (order_year = cohort_year AND order_month > cohort_month)
                        THEN customer_id END) AS retained_customers
                FROM cohort_data
                GROUP BY cohort_year, cohort_month
            )
            SELECT
                *,
                ROUND(
                    retained_customers / NULLIF(first_month_customers, 0) * 100, 2
                ) AS retention_rate
            FROM cohort_metrics
            ORDER BY cohort_year DESC, cohort_month DESC
        """)

        cross_sell_analysis = self.spark.sql("""
            WITH order_products AS (
                SELECT
                    o.order_id,
                    COLLECT_LIST(p.product_name) AS products_list,
                    COLLECT_LIST(p.category)     AS categories_list
                FROM silver_orders   o
                JOIN silver_sales    s ON o.order_id   = s.order_id
                JOIN silver_products p ON s.product_id = p.product_id
                GROUP BY o.order_id
                HAVING COUNT(DISTINCT s.product_id) >= 2
            )
            SELECT
                order_id,
                products_list,
                categories_list,
                SIZE(products_list)                   AS product_count,
                SIZE(ARRAY_DISTINCT(categories_list)) AS category_count
            FROM order_products
            ORDER BY product_count DESC
            LIMIT 20
        """)

        rfm_analysis = self.spark.sql("""
            WITH rfm_calc AS (
                SELECT
                    c.customer_id,
                    c.first_name,
                    c.last_name,
                    DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) AS recency,
                    COUNT(DISTINCT o.order_id)                  AS frequency,
                    SUM(s.total)                                AS monetary
                FROM silver_customers c
                JOIN silver_orders  o ON c.customer_id = o.customer_id
                JOIN silver_sales   s ON o.order_id    = s.order_id
                GROUP BY c.customer_id, c.first_name, c.last_name
            ),
            rfm_scores AS (
                SELECT
                    *,
                    NTILE(5) OVER (ORDER BY recency)        AS recency_score,
                    NTILE(5) OVER (ORDER BY frequency DESC) AS frequency_score,
                    NTILE(5) OVER (ORDER BY monetary DESC)  AS monetary_score
                FROM rfm_calc
            )
            SELECT
                customer_id,
                first_name,
                last_name,
                recency,
                frequency,
                ROUND(monetary, 2)                              AS monetary,
                recency_score,
                frequency_score,
                monetary_score,
                recency_score + frequency_score + monetary_score AS total_rfm_score,
                CASE
                    WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4
                        THEN 'Champions'
                    WHEN recency_score >= 4 AND frequency_score >= 3 AND monetary_score >= 3
                        THEN 'Loyal'
                    WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3
                        THEN 'Potential'
                    WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score >= 2
                        THEN 'At Risk'
                    WHEN recency_score <= 2 AND frequency_score >= 2 AND monetary_score >= 2
                        THEN 'Needs Attention'
                    ELSE 'Lost'
                END AS rfm_segment
            FROM rfm_scores
            ORDER BY total_rfm_score DESC
        """)

        return {
            "cohort_retention":    cohort_retention,
            "cross_sell_analysis": cross_sell_analysis,
            "rfm_analysis":        rfm_analysis,
        }

    # ------------------------------------------------------------------
    # Advanced analytics  (window functions)
    # ------------------------------------------------------------------

    def run_advanced_analytics(self) -> dict:
        """
        Moving averages, YoY comparison, and Pareto / ABC analysis.

        Views used: daily_sales_summary, monthly_time_series,
                    silver_products, silver_sales
        """
        moving_averages = self.spark.sql("""
            WITH daily_sales AS (
                SELECT
                    sale_date,
                    SUM(total_revenue) AS daily_revenue
                FROM daily_sales_summary
                GROUP BY sale_date
            )
            SELECT
                sale_date,
                daily_revenue,
                AVG(daily_revenue) OVER (
                    ORDER BY sale_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ) AS moving_avg_7days,
                AVG(daily_revenue) OVER (
                    ORDER BY sale_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS moving_avg_30days,
                RANK() OVER (ORDER BY daily_revenue DESC) AS revenue_rank
            FROM daily_sales
            ORDER BY sale_date DESC
        """)

        yoy_comparison = self.spark.sql("""
            WITH monthly_sales AS (
                SELECT
                    year,
                    month,
                    category,
                    SUM(monthly_revenue) AS revenue
                FROM monthly_time_series
                GROUP BY year, month, category
            )
            SELECT
                cur.year,
                cur.month,
                cur.category,
                cur.revenue                                           AS current_year_revenue,
                prev.revenue                                          AS previous_year_revenue,
                ROUND(
                    (cur.revenue - prev.revenue) / NULLIF(prev.revenue, 0) * 100, 2
                )                                                     AS yoy_growth_pct
            FROM monthly_sales cur
            LEFT JOIN monthly_sales prev
                ON  cur.month    = prev.month
                AND cur.category = prev.category
                AND cur.year     = prev.year + 1
            ORDER BY cur.year, cur.month, cur.category
        """)

        pareto_analysis = self.spark.sql("""
            WITH product_sales AS (
                SELECT
                    p.product_name,
                    p.category,
                    SUM(s.total) AS total_sales
                FROM silver_products p
                JOIN silver_sales    s ON p.product_id = s.product_id
                GROUP BY p.product_name, p.category
            ),
            ranked_sales AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (ORDER BY total_sales DESC)     AS rank,
                    SUM(total_sales) OVER (ORDER BY total_sales DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                                                      AS running_total,
                    SUM(total_sales) OVER ()                          AS grand_total
                FROM product_sales
            )
            SELECT
                product_name,
                category,
                ROUND(total_sales, 2)                          AS total_sales,
                rank,
                ROUND(running_total / grand_total * 100, 2)   AS cumulative_percentage,
                CASE
                    WHEN running_total / grand_total <= 0.80 THEN 'A'
                    WHEN running_total / grand_total <= 0.95 THEN 'B'
                    ELSE 'C'
                END AS pareto_category
            FROM ranked_sales
            ORDER BY rank
        """)

        return {
            "moving_averages":  moving_averages,
            "yoy_comparison":   yoy_comparison,
            "pareto_analysis":  pareto_analysis,
        }


# ── Standalone entry point ────────────────────────────────────────────────────
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    ba = BusinessAnalytics()
    ba.register_views()
    sales   = ba.run_sales_analytics()
    complex_ = ba.run_complex_analytics()
    advanced = ba.run_advanced_analytics()

    print("\n── Top 10 Customers by LTV ──")
    sales["top_customers"].show(10, truncate=False)
    print("\n── Product Performance ──")
    sales["product_ranking"].show(10, truncate=False)
    print("\n── RFM Sample ──")
    complex_["rfm_analysis"].show(5, truncate=False)
    print("\n── Pareto Top 20 ──")
    advanced["pareto_analysis"].show(20, truncate=False)

    ba.spark.stop()
