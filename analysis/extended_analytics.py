# analysis/extended_analytics.py
"""
ExtendedAnalytics
==================
Deep analytical methods on top of the Silver / Gold Delta layers.

12 analyses:
  1.  Cohort retention matrix
  2.  RFM segmentation (quintile-scored, 9 named segments)
  3.  ABC product classification (Pareto 80 / 15 / 5)
  4.  Market basket / cross-sell affinity
  5.  Revenue trend with 7-day and 30-day moving averages
  6.  Customer LTV percentile distribution
  7.  Category × country revenue heat-map
  8.  Discount effectiveness (order value, profit, margin by bucket)
  9.  Day-of-week and month-over-month seasonality
  10. Sales anomaly detection (z-score per category)
  11. Employee performance ranking
  12. Inventory risk (days-of-cover, stockout / overstock flags)

Views expected (registered by register_views):
  Silver: s_customers, s_products, s_employees, s_stores, s_orders, s_sales
  Gold  : g_daily_sales_summary, g_customer_analytics, g_product_performance,
          g_store_performance, g_monthly_time_series
"""
from __future__ import annotations

import os
import sys

# Ensure the project root (parent of this file's directory) is on sys.path so
# that `config`, `analysis`, etc. are importable when this file is run directly.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum, avg, count, max, min, round, rank, lag,
    countDistinct, datediff, current_date, when, lit,
    ntile, row_number, stddev, greatest, concat,
    collect_list, array_distinct, size,
)
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window


class ExtendedAnalytics:
    """Run all extended analytics and return Spark DataFrames."""

    def __init__(self, spark: SparkSession | None = None):
        if spark is None:
            from config.spark_config import SparkConfig
            self.spark = SparkConfig().get_spark_session()
        else:
            self.spark = spark

        # Anchored to project root — safe regardless of cwd.
        self.silver_path = os.path.join(_PROJECT_ROOT, "data", "silver")
        self.gold_path   = os.path.join(_PROJECT_ROOT, "data", "gold")

    # ------------------------------------------------------------------
    # View registration
    # ------------------------------------------------------------------

    def register_views(self) -> None:
        """
        Register Silver and Gold Delta tables as Spark SQL temp views.
        Silver views: s_<table>
        Gold views  : g_<table>
        Safe to call multiple times (idempotent).
        """
        for table in ["customers", "products", "employees", "stores", "orders", "sales"]:
            path = os.path.join(self.silver_path, table)
            try:
                self.spark.read.format("delta").load(path).createOrReplaceTempView(f"s_{table}")
                print(f"✅ Registered silver view: s_{table}")
            except Exception as exc:
                print(f"⚠️  Could not register s_{table}: {exc}")

        for table in ["daily_sales_summary", "customer_analytics",
                      "product_performance", "store_performance", "monthly_time_series"]:
            path = os.path.join(self.gold_path, table)
            try:
                self.spark.read.format("delta").load(path).createOrReplaceTempView(f"g_{table}")
                print(f"✅ Registered gold view  : g_{table}")
            except Exception as exc:
                print(f"⚠️  Could not register g_{table}: {exc}")

    # ------------------------------------------------------------------
    # 1. Cohort retention matrix
    # ------------------------------------------------------------------

    def cohort_retention_matrix(self) -> DataFrame:
        """
        Monthly cohort retention matrix.
        Rows = cohort month, columns = months-since-first-purchase (0–11).
        Uses: s_orders
        """
        df = self.spark.sql("""
            WITH first_order AS (
                SELECT customer_id,
                       DATE_TRUNC('month', MIN(order_date)) AS cohort_month
                FROM s_orders
                GROUP BY customer_id
            ),
            activity AS (
                SELECT o.customer_id,
                       f.cohort_month,
                       DATE_TRUNC('month', o.order_date) AS order_month
                FROM s_orders o
                JOIN first_order f ON o.customer_id = f.customer_id
            ),
            month_index AS (
                SELECT cohort_month,
                       order_month,
                       CAST(MONTHS_BETWEEN(order_month, cohort_month) AS INT) AS month_num,
                       COUNT(DISTINCT customer_id) AS active_customers
                FROM activity
                GROUP BY cohort_month, order_month
            ),
            cohort_size AS (
                SELECT cohort_month,
                       COUNT(DISTINCT customer_id) AS total_customers
                FROM first_order
                GROUP BY cohort_month
            )
            SELECT
                mi.cohort_month,
                cs.total_customers AS cohort_size,
                mi.month_num       AS months_since_first,
                mi.active_customers,
                ROUND(mi.active_customers / cs.total_customers * 100, 1) AS retention_pct
            FROM month_index mi
            JOIN cohort_size cs ON mi.cohort_month = cs.cohort_month
            WHERE mi.month_num >= 0 AND mi.month_num <= 11
            ORDER BY mi.cohort_month, mi.month_num
        """)
        print("\n━━━ 1. Cohort Retention Matrix (first 20 rows) ━━━")
        df.show(20, truncate=False)
        return df

    # ------------------------------------------------------------------
    # 2. RFM segmentation
    # ------------------------------------------------------------------

    def rfm_segmentation(self) -> DataFrame:
        """
        Full RFM quintile scoring with 9 named business segments.
        Uses: s_customers, s_orders, s_sales views.
        Returns a DataFrame with one row per customer.
        """
        rfm_base = (
            self.spark.sql("""
                SELECT
                    c.customer_id,
                    c.first_name,
                    c.last_name,
                    c.city,
                    c.country,
                    DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) AS recency_days,
                    COUNT(DISTINCT o.order_id)                  AS frequency,
                    ROUND(SUM(s.total), 2)                      AS monetary
                FROM s_customers c
                JOIN s_orders o ON c.customer_id = o.customer_id
                JOIN s_sales  s ON o.order_id    = s.order_id
                GROUP BY c.customer_id, c.first_name, c.last_name, c.city, c.country
            """)
        )

        w_r = Window.orderBy(col("recency_days").desc())
        w_f = Window.orderBy(col("frequency"))
        w_m = Window.orderBy(col("monetary"))

        df = (
            rfm_base
            .withColumn("r_score", ntile(5).over(w_r))
            .withColumn("f_score", ntile(5).over(w_f))
            .withColumn("m_score", ntile(5).over(w_m))
            .withColumn("rfm_total", col("r_score") + col("f_score") + col("m_score"))
            .withColumn("rfm_code",
                concat(col("r_score").cast("string"),
                       col("f_score").cast("string"),
                       col("m_score").cast("string")))
            .withColumn("segment",
                when((col("r_score") == 5) & (col("f_score") >= 4) & (col("m_score") >= 4),
                     "Champions")
                .when((col("r_score") >= 4) & (col("f_score") >= 3) & (col("m_score") >= 3),
                      "Loyal Customers")
                .when((col("r_score") >= 4) & (col("f_score") <= 2),
                      "New Customers")
                .when((col("r_score") == 3) & (col("f_score") >= 3),
                      "Potential Loyalists")
                .when((col("r_score") == 3) & (col("f_score") <= 2) & (col("m_score") >= 3),
                      "Promising")
                .when((col("r_score") <= 2) & (col("f_score") >= 4) & (col("m_score") >= 4),
                      "At Risk")
                .when((col("r_score") <= 2) & (col("f_score") >= 3) & (col("m_score") >= 3),
                      "Cant Lose Them")
                .when((col("r_score") <= 2) & (col("f_score") <= 2) & (col("m_score") >= 3),
                      "Hibernating")
                .otherwise("Lost"))
            .orderBy(col("rfm_total").desc())
        )

        print("\n━━━ 2. RFM — Champions ━━━")
        df.filter(col("segment") == "Champions").show(10, truncate=False)

        summary = (
            df.groupBy("segment")
            .agg(
                count("*").alias("customers"),
                round(avg("monetary"), 2).alias("avg_revenue"),
                round(avg("recency_days"), 1).alias("avg_recency_days"),
            )
            .orderBy(col("avg_revenue").desc())
        )
        print("\n  RFM Segment Summary:")
        summary.show(truncate=False)
        return df

    # ------------------------------------------------------------------
    # 3. ABC product classification
    # ------------------------------------------------------------------

    def abc_product_classification(self) -> DataFrame:
        """
        Rank products by cumulative revenue share.
        A = top 80 %, B = next 15 %, C = remaining 5 %.
        Uses: g_product_performance view.
        """
        perf = self.spark.sql("""
            SELECT product_name, category, total_revenue,
                   total_units_sold, profit_margin
            FROM g_product_performance
        """)

        w_desc  = Window.orderBy(col("total_revenue").desc()).rowsBetween(
            Window.unboundedPreceding, Window.currentRow)
        w_total = Window.orderBy(lit(1)).rowsBetween(
            Window.unboundedPreceding, Window.unboundedFollowing)

        df = (
            perf
            .withColumn("cum_revenue",   sum("total_revenue").over(w_desc))
            .withColumn("grand_total",   sum("total_revenue").over(w_total))
            .withColumn("cumulative_pct",
                round(col("cum_revenue") / col("grand_total") * 100, 2))
            .withColumn("abc_class",
                when(col("cum_revenue") / col("grand_total") <= 0.80, "A")
                .when(col("cum_revenue") / col("grand_total") <= 0.95, "B")
                .otherwise("C"))
            .withColumn("revenue", round(col("total_revenue"), 2))
            .drop("cum_revenue", "grand_total", "total_revenue")
            .orderBy(col("revenue").desc())
        )

        print("\n━━━ 3. ABC Product Classification ━━━")
        df.groupBy("abc_class").agg(
            count("*").alias("product_count"),
            round(sum("revenue"), 2).alias("total_revenue"),
            round(avg("profit_margin"), 2).alias("avg_margin"),
        ).orderBy("abc_class").show(truncate=False)
        return df

    # ------------------------------------------------------------------
    # 4. Basket / cross-sell analysis
    # ------------------------------------------------------------------

    def basket_analysis(self) -> DataFrame:
        """
        Orders with 2+ distinct products — category co-occurrence.
        Uses: s_orders, s_sales, s_products views.
        """
        df = self.spark.sql("""
            WITH multi_item_orders AS (
                SELECT
                    o.order_id,
                    COLLECT_LIST(DISTINCT p.category) AS cats
                FROM s_orders   o
                JOIN s_sales    s ON o.order_id   = s.order_id
                JOIN s_products p ON s.product_id = p.product_id
                GROUP BY o.order_id
                HAVING COUNT(DISTINCT p.product_id) >= 2
            )
            SELECT
                cats,
                SIZE(ARRAY_DISTINCT(cats)) AS distinct_categories,
                COUNT(*)                   AS order_count
            FROM multi_item_orders
            GROUP BY cats
            ORDER BY order_count DESC
            LIMIT 20
        """)
        print("\n━━━ 4. Basket Analysis — Top 20 Category Combos ━━━")
        df.show(20, truncate=False)
        return df

    # ------------------------------------------------------------------
    # 5. Revenue trend
    # ------------------------------------------------------------------

    def revenue_trend(self) -> DataFrame:
        """
        Daily revenue with 7-day and 30-day moving averages, plus
        day-over-day delta.
        Uses: g_daily_sales_summary view.
        """
        df = self.spark.sql("""
            WITH daily AS (
                SELECT sale_date,
                       SUM(total_revenue) AS revenue
                FROM g_daily_sales_summary
                GROUP BY sale_date
            )
            SELECT
                sale_date,
                ROUND(revenue, 2) AS daily_revenue,
                ROUND(AVG(revenue) OVER (
                    ORDER BY sale_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2)   AS ma_7d,
                ROUND(AVG(revenue) OVER (
                    ORDER BY sale_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW), 2)  AS ma_30d,
                ROUND(revenue - LAG(revenue) OVER (ORDER BY sale_date), 2) AS day_over_day
            FROM daily
            ORDER BY sale_date DESC
        """)
        print("\n━━━ 5. Revenue Trend (latest 14 days) ━━━")
        df.show(14, truncate=False)
        return df

    # ------------------------------------------------------------------
    # 6. Customer LTV distribution
    # ------------------------------------------------------------------

    def ltv_distribution(self) -> DataFrame:
        """
        Percentile summary of customer lifetime value.
        Uses: g_customer_analytics view.
        """
        df = self.spark.sql("""
            SELECT
                ROUND(PERCENTILE_APPROX(lifetime_value, 0.10), 2) AS p10,
                ROUND(PERCENTILE_APPROX(lifetime_value, 0.25), 2) AS p25,
                ROUND(PERCENTILE_APPROX(lifetime_value, 0.50), 2) AS median,
                ROUND(PERCENTILE_APPROX(lifetime_value, 0.75), 2) AS p75,
                ROUND(PERCENTILE_APPROX(lifetime_value, 0.90), 2) AS p90,
                ROUND(PERCENTILE_APPROX(lifetime_value, 0.95), 2) AS p95,
                ROUND(AVG(lifetime_value),    2) AS mean,
                ROUND(STDDEV(lifetime_value), 2) AS std_dev,
                COUNT(*)                         AS total_customers
            FROM g_customer_analytics
        """)
        print("\n━━━ 6. Customer LTV Distribution ━━━")
        df.show(truncate=False)
        return df

    # ------------------------------------------------------------------
    # 7. Category × country heat-map
    # ------------------------------------------------------------------

    def category_store_heatmap(self) -> DataFrame:
        """
        Revenue matrix aggregated by product category and store country.
        Uses: g_daily_sales_summary view.
        """
        df = self.spark.sql("""
            SELECT
                category,
                store_country,
                ROUND(SUM(total_revenue), 2) AS revenue,
                SUM(units_sold)              AS units,
                ROUND(AVG(profit_margin), 2) AS avg_margin
            FROM g_daily_sales_summary
            GROUP BY category, store_country
            ORDER BY revenue DESC
        """)
        print("\n━━━ 7. Category × Country Heatmap (top 20) ━━━")
        df.show(20, truncate=False)
        return df

    # ------------------------------------------------------------------
    # 8. Discount effectiveness
    # ------------------------------------------------------------------

    def discount_effectiveness(self) -> DataFrame:
        """
        Average order value, profit, and margin broken down by discount
        bucket: 0%, 1-10%, 11-20%, 21-30%, >30%.
        Uses: s_sales view.
        """
        df = self.spark.sql("""
            SELECT
                CASE
                    WHEN discount = 0               THEN '0%'
                    WHEN discount BETWEEN 1  AND 10 THEN '1-10%'
                    WHEN discount BETWEEN 11 AND 20 THEN '11-20%'
                    WHEN discount BETWEEN 21 AND 30 THEN '21-30%'
                    ELSE '>30%'
                END                                         AS discount_bucket,
                COUNT(*)                                    AS sales_count,
                ROUND(AVG(quantity), 2)                     AS avg_qty,
                ROUND(AVG(total), 2)                        AS avg_order_value,
                ROUND(AVG(profit), 2)                       AS avg_profit,
                ROUND(SUM(profit) / NULLIF(SUM(total), 0) * 100, 2) AS overall_margin_pct
            FROM s_sales
            GROUP BY discount_bucket
            ORDER BY discount_bucket
        """)
        print("\n━━━ 8. Discount Effectiveness ━━━")
        df.show(truncate=False)
        return df

    # ------------------------------------------------------------------
    # 9. Seasonality
    # ------------------------------------------------------------------

    def seasonality_analysis(self) -> tuple[DataFrame, DataFrame]:
        """
        Day-of-week and month-over-month seasonality.
        Uses: g_daily_sales_summary, g_monthly_time_series views.
        Returns: (dow_df, monthly_df) — both DataFrames.
        """
        dow = self.spark.sql("""
            SELECT
                weekday,
                COUNT(*)                     AS data_points,
                ROUND(AVG(total_revenue), 2) AS avg_daily_revenue,
                ROUND(SUM(total_revenue), 2) AS total_revenue,
                ROUND(AVG(units_sold), 2)    AS avg_units
            FROM g_daily_sales_summary
            GROUP BY weekday
            ORDER BY CASE weekday
                WHEN 'Monday'    THEN 1
                WHEN 'Tuesday'   THEN 2
                WHEN 'Wednesday' THEN 3
                WHEN 'Thursday'  THEN 4
                WHEN 'Friday'    THEN 5
                WHEN 'Saturday'  THEN 6
                ELSE 7
            END
        """)
        print("\n━━━ 9. Day-of-Week Seasonality ━━━")
        dow.show(truncate=False)

        monthly = self.spark.sql("""
            SELECT
                year,
                month,
                ROUND(SUM(monthly_revenue), 2) AS revenue,
                SUM(monthly_units)             AS units
            FROM g_monthly_time_series
            GROUP BY year, month
            ORDER BY year, month
        """)
        print("  Monthly Revenue Trend:")
        monthly.show(24, truncate=False)

        # Return both DataFrames so callers can use either
        return dow, monthly

    # ------------------------------------------------------------------
    # 10. Anomaly detection
    # ------------------------------------------------------------------

    def anomaly_detection(self) -> DataFrame:
        """
        Flag daily revenue records where |z-score| > 2.5 per category.
        Uses: g_daily_sales_summary view.
        """
        df = self.spark.sql("""
            WITH stats AS (
                SELECT category,
                       AVG(total_revenue)    AS mu,
                       STDDEV(total_revenue) AS sigma
                FROM g_daily_sales_summary
                GROUP BY category
            ),
            scored AS (
                SELECT
                    d.sale_date,
                    d.category,
                    ROUND(d.total_revenue, 2)                                AS revenue,
                    ROUND((d.total_revenue - s.mu) / NULLIF(s.sigma, 0), 3) AS z_score
                FROM g_daily_sales_summary d
                JOIN stats s ON d.category = s.category
            )
            SELECT
                *,
                CASE WHEN ABS(z_score) > 2.5 THEN 'ANOMALY' ELSE 'normal' END AS flag
            FROM scored
            WHERE ABS(z_score) > 2.5
            ORDER BY ABS(z_score) DESC
        """)
        print("\n━━━ 10. Sales Anomalies (|z| > 2.5) ━━━")
        df.show(20, truncate=False)
        return df

    # ------------------------------------------------------------------
    # 11. Employee performance
    # ------------------------------------------------------------------

    def employee_performance(self) -> DataFrame:
        """
        Rank employees by orders handled, revenue generated, and
        revenue-to-salary ratio.
        Uses: s_employees, s_orders, s_sales views.
        """
        df = self.spark.sql("""
            SELECT
                e.employee_id,
                e.employee_name,
                e.department,
                e.salary,
                COUNT(DISTINCT o.order_id)                         AS orders_handled,
                ROUND(SUM(s.total), 2)                             AS revenue_generated,
                ROUND(AVG(s.total), 2)                             AS avg_order_value,
                ROUND(SUM(s.profit), 2)                            AS total_profit,
                ROUND(SUM(s.profit) / NULLIF(SUM(s.total), 0) * 100, 2) AS profit_margin_pct,
                ROUND(SUM(s.total)  / NULLIF(e.salary, 0), 4)     AS revenue_to_salary_ratio,
                RANK() OVER (ORDER BY SUM(s.total) DESC)           AS revenue_rank
            FROM s_employees e
            JOIN s_orders    o ON e.employee_id = o.employee_id
            JOIN s_sales     s ON o.order_id    = s.order_id
            GROUP BY e.employee_id, e.employee_name, e.department, e.salary
            ORDER BY revenue_generated DESC
        """)
        print("\n━━━ 11. Top 10 Employees by Revenue ━━━")
        df.show(10, truncate=False)
        return df

    # ------------------------------------------------------------------
    # 12. Inventory risk
    # ------------------------------------------------------------------

    def inventory_risk(self) -> DataFrame:
        """
        Days-of-cover per product based on observed daily demand.
        Flags STOCKOUT RISK (< 30 days) and OVERSTOCKED (> 365 days).
        Uses: s_sales, s_orders, s_products views.
        """
        demand = self.spark.sql("""
            SELECT
                s.product_id,
                SUM(s.quantity)                                   AS total_sold,
                COUNT(DISTINCT s.order_id)                        AS order_count,
                DATEDIFF(MAX(o.order_date), MIN(o.order_date))    AS sales_days
            FROM s_sales   s
            JOIN s_orders  o ON s.order_id = o.order_id
            GROUP BY s.product_id
        """)

        products = self.spark.sql("""
            SELECT product_id, product_name, category, stock
            FROM s_products
        """)

        df = (
            products.join(demand, "product_id")
            .withColumn("sales_days_safe",
                greatest(col("sales_days"), lit(1)).cast(DoubleType()))
            .withColumn("daily_demand",
                round(col("total_sold") / col("sales_days_safe"), 3))
            .withColumn("daily_demand_safe",
                greatest(col("daily_demand"), lit(0.01)))
            .withColumn("days_of_cover",
                round(col("stock") / col("daily_demand_safe"), 0))
            .withColumn("inventory_status",
                when(col("days_of_cover") < 30,   "STOCKOUT RISK")
                .when(col("days_of_cover") > 365,  "OVERSTOCKED")
                .otherwise("OK"))
            .drop("sales_days_safe", "daily_demand_safe")
            .orderBy("days_of_cover")
        )

        print("\n━━━ 12. Inventory Risk ━━━")
        df.filter(col("inventory_status") != "OK").show(20, truncate=False)
        return df

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def run_all(self) -> dict:
        """Run all 12 analyses and return results as a dict."""
        print("\n" + "═" * 70)
        print("  EXTENDED ANALYTICS")
        print("═" * 70)

        results = {}
        steps = [
            ("cohort_retention",       self.cohort_retention_matrix),
            ("rfm_segmentation",       self.rfm_segmentation),
            ("abc_classification",     self.abc_product_classification),
            ("basket_analysis",        self.basket_analysis),
            ("revenue_trend",          self.revenue_trend),
            ("ltv_distribution",       self.ltv_distribution),
            ("category_heatmap",       self.category_store_heatmap),
            ("discount_effectiveness", self.discount_effectiveness),
            ("seasonality",            self.seasonality_analysis),
            ("anomaly_detection",      self.anomaly_detection),
            ("employee_performance",   self.employee_performance),
            ("inventory_risk",         self.inventory_risk),
        ]
        for name, fn in steps:
            try:
                results[name] = fn()
            except Exception as exc:
                print(f"⚠️  {name} failed: {exc}")

        print("\n✅ Extended analytics complete!")
        return results


# ── Standalone entry point ────────────────────────────────────────────────────
if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")

    ea = ExtendedAnalytics()
    ea.register_views()
    ea.run_all()
    ea.spark.stop()
