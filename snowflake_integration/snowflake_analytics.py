# snowflake/snowflake_analytics.py
"""
SnowflakeAnalytics
==================
Executes all 16 retail analytics queries against Snowflake and
returns results as pandas DataFrames.

Usage (standalone)
------------------
    python -m snowflake_integration.snowflake_analytics              # run all analyses
    python -m snowflake_integration.snowflake_analytics --query rfm  # single analysis
    python -m snowflake_integration.snowflake_analytics --list       # list available queries

Available query names
---------------------
    sales_dashboard   top_products      customer_segmentation
    store_performance  mom_growth        ltv_distribution
    rfm               cohort_retention   abc_products
    inventory_risk    yoy_comparison     seasonality
    discount          employee           cross_sell
    anomaly_detection

Requirements
------------
    pip install snowflake-connector-python[pandas] pandas tabulate
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Callable

import pandas as pd
from dotenv import load_dotenv

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

load_dotenv()

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Individual query definitions
# ---------------------------------------------------------------------------
# Each function receives a snowflake.connector cursor and returns a DataFrame.

def _q_sales_dashboard(cur) -> pd.DataFrame:
    cur.execute("""
        SELECT
            o.ORDER_DATE                                              AS sale_date,
            p.CATEGORY,
            ROUND(SUM(s.TOTAL),        2)                            AS total_revenue,
            SUM(s.QUANTITY)                                           AS units_sold,
            COUNT(DISTINCT o.ORDER_ID)                                AS order_count,
            COUNT(DISTINCT o.CUSTOMER_ID)                             AS unique_customers,
            ROUND(SUM(s.TOTAL) / NULLIF(COUNT(DISTINCT o.ORDER_ID), 0), 2) AS avg_order_value,
            ROUND(SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) * 100, 2) AS profit_margin_pct
        FROM ORDERS   o
        JOIN SALES    s ON o.ORDER_ID   = s.ORDER_ID
        JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
        WHERE o.ORDER_DATE >= DATEADD(year, -1, CURRENT_DATE())
        GROUP BY o.ORDER_DATE, p.CATEGORY
        ORDER BY o.ORDER_DATE DESC, total_revenue DESC
    """)
    return cur.fetch_pandas_all()


def _q_top_products(cur) -> pd.DataFrame:
    cur.execute("""
        SELECT
            p.PRODUCT_NAME,
            p.CATEGORY,
            ROUND(SUM(s.TOTAL),  2)                                  AS total_revenue,
            SUM(s.QUANTITY)                                           AS total_units_sold,
            ROUND(SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) * 100, 2) AS profit_margin_pct,
            RANK() OVER (ORDER BY SUM(s.TOTAL) DESC)                 AS revenue_rank
        FROM PRODUCTS p
        JOIN SALES    s ON p.PRODUCT_ID = s.PRODUCT_ID
        GROUP BY p.PRODUCT_NAME, p.CATEGORY
        ORDER BY total_revenue DESC
        LIMIT 20
    """)
    return cur.fetch_pandas_all()


def _q_customer_segmentation(cur) -> pd.DataFrame:
    cur.execute("""
        WITH customer_ltv AS (
            SELECT
                c.CUSTOMER_ID,
                ROUND(SUM(s.TOTAL), 2)                                        AS lifetime_value,
                COUNT(DISTINCT o.ORDER_ID)                                    AS total_orders,
                ROUND(SUM(s.TOTAL) / NULLIF(COUNT(DISTINCT o.ORDER_ID), 0), 2) AS avg_order_value
            FROM CUSTOMERS c
            JOIN ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
            JOIN SALES  s ON o.ORDER_ID    = s.ORDER_ID
            GROUP BY c.CUSTOMER_ID
        )
        SELECT
            CASE
                WHEN lifetime_value >= 50000 THEN 'Platinum'
                WHEN lifetime_value >= 10000 THEN 'Gold'
                WHEN lifetime_value >=  5000 THEN 'Silver'
                WHEN lifetime_value >=  1000 THEN 'Bronze'
                ELSE 'Standard'
            END                                   AS customer_segment,
            COUNT(*)                              AS customer_count,
            ROUND(AVG(lifetime_value), 2)         AS avg_lifetime_value,
            ROUND(AVG(total_orders),   2)         AS avg_orders,
            ROUND(SUM(lifetime_value), 2)         AS total_segment_revenue
        FROM customer_ltv
        GROUP BY customer_segment
        ORDER BY total_segment_revenue DESC
    """)
    return cur.fetch_pandas_all()


def _q_store_performance(cur) -> pd.DataFrame:
    cur.execute("""
        SELECT
            st.STORE_NAME,
            st.CITY,
            st.COUNTRY,
            st.REGION,
            ROUND(SUM(s.TOTAL),                2)                    AS total_revenue,
            COUNT(DISTINCT o.ORDER_ID)                                AS total_orders,
            COUNT(DISTINCT o.CUSTOMER_ID)                             AS unique_customers,
            ROUND(SUM(s.TOTAL) / NULLIF(COUNT(DISTINCT o.ORDER_ID), 0), 2) AS avg_order_value,
            ROUND(SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) * 100, 2)  AS profit_margin_pct,
            RANK() OVER (ORDER BY SUM(s.TOTAL) DESC)                  AS revenue_rank
        FROM STORES  st
        JOIN ORDERS  o  ON st.STORE_ID  = o.STORE_ID
        JOIN SALES   s  ON o.ORDER_ID   = s.ORDER_ID
        GROUP BY st.STORE_NAME, st.CITY, st.COUNTRY, st.REGION
        ORDER BY total_revenue DESC
    """)
    return cur.fetch_pandas_all()


def _q_mom_growth(cur) -> pd.DataFrame:
    cur.execute("""
        WITH monthly_sales AS (
            SELECT
                DATE_TRUNC('month', o.ORDER_DATE)::DATE AS sales_month,
                YEAR(o.ORDER_DATE)                       AS yr,
                MONTH(o.ORDER_DATE)                      AS mo,
                p.CATEGORY,
                ROUND(SUM(s.TOTAL), 2)                   AS monthly_revenue
            FROM ORDERS   o
            JOIN SALES    s ON o.ORDER_ID   = s.ORDER_ID
            JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
            GROUP BY sales_month, yr, mo, p.CATEGORY
        )
        SELECT
            cur.sales_month,
            cur.yr                 AS year,
            cur.mo                 AS month,
            cur.CATEGORY,
            cur.monthly_revenue,
            prev.monthly_revenue   AS prev_month_revenue,
            ROUND(
                (cur.monthly_revenue - prev.monthly_revenue)
                / NULLIF(prev.monthly_revenue, 0) * 100, 2
            )                      AS mom_growth_pct
        FROM monthly_sales cur
        LEFT JOIN monthly_sales prev
            ON  cur.mo       = prev.mo
            AND cur.CATEGORY = prev.CATEGORY
            AND cur.yr       = prev.yr + 1
        ORDER BY cur.sales_month DESC, cur.CATEGORY
    """)
    return cur.fetch_pandas_all()


def _q_ltv_distribution(cur) -> pd.DataFrame:
    cur.execute("""
        WITH customer_ltv AS (
            SELECT c.CUSTOMER_ID, ROUND(SUM(s.TOTAL), 2) AS lifetime_value
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
            ROUND(AVG(lifetime_value),    2) AS mean,
            ROUND(STDDEV(lifetime_value), 2) AS std_dev,
            COUNT(*)                         AS total_customers
        FROM customer_ltv
    """)
    return cur.fetch_pandas_all()


def _q_rfm(cur) -> pd.DataFrame:
    cur.execute("""
        WITH rfm_raw AS (
            SELECT
                c.CUSTOMER_ID,
                c.FIRST_NAME,
                c.LAST_NAME,
                DATEDIFF('day', MAX(o.ORDER_DATE), CURRENT_DATE()) AS recency_days,
                COUNT(DISTINCT o.ORDER_ID)                          AS frequency,
                ROUND(SUM(s.TOTAL), 2)                              AS monetary
            FROM CUSTOMERS c
            JOIN ORDERS o ON c.CUSTOMER_ID = o.CUSTOMER_ID
            JOIN SALES  s ON o.ORDER_ID    = s.ORDER_ID
            GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME
        ),
        rfm_scores AS (
            SELECT *,
                NTILE(5) OVER (ORDER BY recency_days DESC) AS r_score,
                NTILE(5) OVER (ORDER BY frequency)          AS f_score,
                NTILE(5) OVER (ORDER BY monetary)            AS m_score
            FROM rfm_raw
        )
        SELECT
            CUSTOMER_ID, FIRST_NAME, LAST_NAME,
            recency_days, frequency, monetary,
            r_score, f_score, m_score,
            r_score + f_score + m_score AS rfm_total,
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
        ORDER BY rfm_total DESC
    """)
    return cur.fetch_pandas_all()


def _q_cohort_retention(cur) -> pd.DataFrame:
    cur.execute("""
        WITH first_order AS (
            SELECT CUSTOMER_ID,
                   DATE_TRUNC('month', MIN(ORDER_DATE))::DATE AS cohort_month
            FROM ORDERS GROUP BY CUSTOMER_ID
        ),
        activity AS (
            SELECT o.CUSTOMER_ID, f.cohort_month,
                   DATE_TRUNC('month', o.ORDER_DATE)::DATE AS order_month
            FROM ORDERS o JOIN first_order f ON o.CUSTOMER_ID = f.CUSTOMER_ID
        ),
        month_index AS (
            SELECT cohort_month, order_month,
                   DATEDIFF('month', cohort_month, order_month) AS months_since_first,
                   COUNT(DISTINCT CUSTOMER_ID) AS active_customers
            FROM activity
            GROUP BY cohort_month, order_month
        ),
        cohort_size AS (
            SELECT cohort_month, COUNT(DISTINCT CUSTOMER_ID) AS total_customers
            FROM first_order GROUP BY cohort_month
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
        ORDER BY mi.cohort_month, mi.months_since_first
    """)
    return cur.fetch_pandas_all()


def _q_abc_products(cur) -> pd.DataFrame:
    cur.execute("""
        WITH product_sales AS (
            SELECT p.PRODUCT_NAME, p.CATEGORY,
                   ROUND(SUM(s.TOTAL), 2) AS total_revenue,
                   SUM(s.QUANTITY)         AS total_units
            FROM PRODUCTS p JOIN SALES s ON p.PRODUCT_ID = s.PRODUCT_ID
            GROUP BY p.PRODUCT_NAME, p.CATEGORY
        ),
        ranked AS (
            SELECT *,
                   ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS rnk,
                   SUM(total_revenue) OVER (ORDER BY total_revenue DESC
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total,
                   SUM(total_revenue) OVER ()                            AS grand_total
            FROM product_sales
        )
        SELECT PRODUCT_NAME, CATEGORY, total_revenue, total_units, rnk,
               ROUND(running_total / grand_total * 100, 2) AS cumulative_pct,
               CASE
                   WHEN running_total / grand_total <= 0.80 THEN 'A'
                   WHEN running_total / grand_total <= 0.95 THEN 'B'
                   ELSE 'C'
               END AS abc_class
        FROM ranked ORDER BY rnk
    """)
    return cur.fetch_pandas_all()


def _q_inventory_risk(cur) -> pd.DataFrame:
    cur.execute("""
        WITH demand AS (
            SELECT s.PRODUCT_ID,
                   SUM(s.QUANTITY)                                    AS total_sold,
                   GREATEST(DATEDIFF('day', MIN(o.ORDER_DATE),
                                     MAX(o.ORDER_DATE)), 1)           AS sales_days
            FROM SALES s JOIN ORDERS o ON s.ORDER_ID = o.ORDER_ID
            GROUP BY s.PRODUCT_ID
        )
        SELECT
            p.PRODUCT_NAME, p.CATEGORY, p.STOCK,
            d.total_sold, d.sales_days,
            ROUND(d.total_sold / d.sales_days, 3) AS daily_demand,
            ROUND(p.STOCK / NULLIF(d.total_sold / d.sales_days, 0), 0) AS days_of_cover,
            CASE
                WHEN p.STOCK / NULLIF(d.total_sold / d.sales_days, 0) < 30  THEN 'STOCKOUT RISK'
                WHEN p.STOCK / NULLIF(d.total_sold / d.sales_days, 0) > 365 THEN 'OVERSTOCKED'
                ELSE 'OK'
            END AS inventory_status
        FROM PRODUCTS p JOIN demand d ON p.PRODUCT_ID = d.PRODUCT_ID
        ORDER BY days_of_cover NULLS LAST
    """)
    return cur.fetch_pandas_all()


def _q_yoy_comparison(cur) -> pd.DataFrame:
    cur.execute("""
        WITH yearly_sales AS (
            SELECT YEAR(o.ORDER_DATE) AS yr, MONTH(o.ORDER_DATE) AS mo,
                   p.CATEGORY, ROUND(SUM(s.TOTAL), 2) AS revenue
            FROM ORDERS o
            JOIN SALES s ON o.ORDER_ID = s.ORDER_ID
            JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
            GROUP BY yr, mo, p.CATEGORY
        )
        SELECT cur.yr AS year, cur.mo AS month, cur.CATEGORY,
               cur.revenue AS current_revenue,
               prev.revenue AS previous_revenue,
               ROUND((cur.revenue - prev.revenue) / NULLIF(prev.revenue, 0) * 100, 2) AS yoy_pct
        FROM yearly_sales cur
        LEFT JOIN yearly_sales prev
            ON cur.mo = prev.mo AND cur.CATEGORY = prev.CATEGORY AND cur.yr = prev.yr + 1
        ORDER BY cur.yr, cur.mo, cur.CATEGORY
    """)
    return cur.fetch_pandas_all()


def _q_seasonality(cur) -> pd.DataFrame:
    cur.execute("""
        SELECT
            CASE
                WHEN MONTH(o.ORDER_DATE) IN (3,4,5) THEN 'Spring'
                WHEN MONTH(o.ORDER_DATE) IN (6,7,8) THEN 'Summer'
                WHEN MONTH(o.ORDER_DATE) IN (9,10,11) THEN 'Fall'
                ELSE 'Winter'
            END AS season,
            p.CATEGORY,
            ROUND(SUM(s.TOTAL), 2)         AS seasonal_revenue,
            ROUND(AVG(s.TOTAL), 2)         AS avg_sale_revenue,
            SUM(s.QUANTITY)                AS total_units,
            COUNT(DISTINCT o.ORDER_ID)     AS total_orders
        FROM ORDERS o
        JOIN SALES s ON o.ORDER_ID = s.ORDER_ID
        JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
        GROUP BY season, p.CATEGORY
        ORDER BY seasonal_revenue DESC
    """)
    return cur.fetch_pandas_all()


def _q_discount(cur) -> pd.DataFrame:
    cur.execute("""
        SELECT
            CASE
                WHEN DISCOUNT = 0              THEN '0% — No Discount'
                WHEN DISCOUNT BETWEEN 1 AND 10 THEN '1–10%'
                WHEN DISCOUNT BETWEEN 11 AND 20 THEN '11–20%'
                WHEN DISCOUNT BETWEEN 21 AND 30 THEN '21–30%'
                ELSE '>30%'
            END                                                         AS discount_bucket,
            COUNT(*)                                                    AS sales_count,
            ROUND(AVG(QUANTITY), 2)                                     AS avg_qty,
            ROUND(AVG(TOTAL),    2)                                     AS avg_order_value,
            ROUND(AVG(PROFIT),   2)                                     AS avg_profit,
            ROUND(SUM(PROFIT) / NULLIF(SUM(TOTAL), 0) * 100, 2)        AS margin_pct
        FROM SALES
        GROUP BY discount_bucket
        ORDER BY discount_bucket
    """)
    return cur.fetch_pandas_all()


def _q_employee(cur) -> pd.DataFrame:
    cur.execute("""
        SELECT
            e.EMPLOYEE_NAME, e.DEPARTMENT, e.SALARY,
            COUNT(DISTINCT o.ORDER_ID)                                   AS orders_handled,
            ROUND(SUM(s.TOTAL),  2)                                      AS revenue_generated,
            ROUND(AVG(s.TOTAL),  2)                                      AS avg_order_value,
            ROUND(SUM(s.PROFIT), 2)                                      AS total_profit,
            ROUND(SUM(s.PROFIT) / NULLIF(SUM(s.TOTAL), 0) * 100, 2)     AS profit_margin_pct,
            ROUND(SUM(s.TOTAL)  / NULLIF(e.SALARY, 0), 4)               AS revenue_to_salary,
            RANK() OVER (ORDER BY SUM(s.TOTAL) DESC)                     AS revenue_rank
        FROM EMPLOYEES e
        JOIN ORDERS    o ON e.EMPLOYEE_ID = o.EMPLOYEE_ID
        JOIN SALES     s ON o.ORDER_ID    = s.ORDER_ID
        GROUP BY e.EMPLOYEE_NAME, e.DEPARTMENT, e.SALARY
        ORDER BY revenue_generated DESC
    """)
    return cur.fetch_pandas_all()


def _q_cross_sell(cur) -> pd.DataFrame:
    cur.execute("""
        WITH order_cats AS (
            SELECT o.ORDER_ID, p.CATEGORY
            FROM ORDERS o JOIN SALES s ON o.ORDER_ID = s.ORDER_ID
            JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
            GROUP BY o.ORDER_ID, p.CATEGORY
        ),
        multi_cat_orders AS (
            SELECT ORDER_ID FROM order_cats
            GROUP BY ORDER_ID HAVING COUNT(DISTINCT CATEGORY) >= 2
        ),
        pairs AS (
            SELECT a.ORDER_ID, a.CATEGORY AS cat_a, b.CATEGORY AS cat_b
            FROM order_cats a JOIN order_cats b
                ON a.ORDER_ID = b.ORDER_ID AND a.CATEGORY < b.CATEGORY
            JOIN multi_cat_orders m ON a.ORDER_ID = m.ORDER_ID
        )
        SELECT cat_a, cat_b, COUNT(DISTINCT ORDER_ID) AS co_occurrence_count
        FROM pairs
        GROUP BY cat_a, cat_b
        ORDER BY co_occurrence_count DESC
        LIMIT 20
    """)
    return cur.fetch_pandas_all()


def _q_anomaly_detection(cur) -> pd.DataFrame:
    cur.execute("""
        WITH daily_cat AS (
            SELECT o.ORDER_DATE AS sale_date, p.CATEGORY,
                   ROUND(SUM(s.TOTAL), 2) AS daily_revenue
            FROM ORDERS o
            JOIN SALES s ON o.ORDER_ID = s.ORDER_ID
            JOIN PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID
            GROUP BY o.ORDER_DATE, p.CATEGORY
        ),
        stats AS (
            SELECT CATEGORY, AVG(daily_revenue) AS mu, STDDEV(daily_revenue) AS sigma
            FROM daily_cat GROUP BY CATEGORY
        )
        SELECT
            d.sale_date, d.CATEGORY, d.daily_revenue,
            ROUND((d.daily_revenue - s.mu) / NULLIF(s.sigma, 0), 3) AS z_score,
            'ANOMALY' AS flag
        FROM daily_cat d JOIN stats s ON d.CATEGORY = s.CATEGORY
        WHERE ABS((d.daily_revenue - s.mu) / NULLIF(s.sigma, 0)) > 2.5
        ORDER BY ABS(z_score) DESC
    """)
    return cur.fetch_pandas_all()


# ---------------------------------------------------------------------------
# Query registry
# ---------------------------------------------------------------------------

QUERIES: dict[str, tuple[str, Callable]] = {
    "sales_dashboard":       ("Sales Performance Dashboard",          _q_sales_dashboard),
    "top_products":          ("Top 20 Products by Revenue",           _q_top_products),
    "customer_segmentation": ("Customer LTV Segmentation",            _q_customer_segmentation),
    "store_performance":     ("Store Performance Matrix",             _q_store_performance),
    "mom_growth":            ("Month-over-Month Growth",              _q_mom_growth),
    "ltv_distribution":      ("Customer LTV Percentile Distribution", _q_ltv_distribution),
    "rfm":                   ("RFM Segmentation",                     _q_rfm),
    "cohort_retention":      ("Cohort Retention Matrix",              _q_cohort_retention),
    "abc_products":          ("ABC Product Classification",           _q_abc_products),
    "inventory_risk":        ("Inventory Risk / Days-of-Cover",       _q_inventory_risk),
    "yoy_comparison":        ("Year-over-Year Revenue Comparison",    _q_yoy_comparison),
    "seasonality":           ("Seasonal Revenue Patterns",            _q_seasonality),
    "discount":              ("Discount Effectiveness",               _q_discount),
    "employee":              ("Employee Performance Ranking",         _q_employee),
    "cross_sell":            ("Cross-Sell / Basket Analysis",         _q_cross_sell),
    "anomaly_detection":     ("Sales Anomaly Detection",              _q_anomaly_detection),
}


# ---------------------------------------------------------------------------
# Main analytics class
# ---------------------------------------------------------------------------

class SnowflakeAnalytics:
    """
    Execute retail analytics queries against Snowflake.

    Quick start:
        analytics = SnowflakeAnalytics()
        results   = analytics.run_all()          # dict of DataFrames
        df        = analytics.run("rfm")         # single DataFrame
    """

    def __init__(self) -> None:
        from config.snowflake_config import SnowflakeConfig
        self._cfg = SnowflakeConfig()

    # ------------------------------------------------------------------
    # Connection helper
    # ------------------------------------------------------------------

    def _connect(self):
        """Open and return a Snowflake connection."""
        return self._cfg.get_connection()

    # ------------------------------------------------------------------
    # Run a single named analysis
    # ------------------------------------------------------------------

    def run(self, query_name: str) -> pd.DataFrame:
        """
        Execute a single named analysis and return a DataFrame.

        Parameters
        ----------
        query_name : str
            One of the keys in QUERIES (e.g. 'rfm', 'top_products').
        """
        if query_name not in QUERIES:
            raise ValueError(
                f"Unknown query: '{query_name}'. "
                f"Available: {', '.join(QUERIES)}"
            )

        label, fn = QUERIES[query_name]
        log.info("Running: %s …", label)

        conn = self._connect()
        try:
            db  = self._cfg.database
            sch = self._cfg.schema
            wh  = self._cfg.warehouse
            cur = conn.cursor()
            cur.execute(f"USE WAREHOUSE {wh}")
            cur.execute(f"USE DATABASE {db}")
            cur.execute(f"USE SCHEMA {sch}")
            df = fn(cur)
        finally:
            conn.close()

        log.info("  → %d rows returned", len(df))
        return df

    # ------------------------------------------------------------------
    # Run all analyses
    # ------------------------------------------------------------------

    def run_all(
        self,
        print_results: bool = True,
        max_rows: int = 10,
    ) -> dict[str, pd.DataFrame]:
        """
        Execute all 16 analyses in a single Snowflake session.

        Parameters
        ----------
        print_results : bool
            Print a summary table for each analysis to stdout.
        max_rows : int
            How many rows to print per analysis.

        Returns
        -------
        dict mapping query_name → DataFrame.
        """
        results: dict[str, pd.DataFrame] = {}

        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.execute(f"USE WAREHOUSE {self._cfg.warehouse}")
            cur.execute(f"USE DATABASE {self._cfg.database}")
            cur.execute(f"USE SCHEMA {self._cfg.schema}")

            for name, (label, fn) in QUERIES.items():
                print(f"\n{'━' * 60}")
                print(f"  {label}")
                print(f"{'━' * 60}")
                try:
                    df = fn(cur)
                    results[name] = df
                    if print_results:
                        try:
                            print(df.head(max_rows).to_string(index=False))
                        except Exception:
                            print(df.head(max_rows))
                    print(f"  → {len(df):,} rows")
                except Exception as exc:
                    log.warning("  ⚠️  %s failed: %s", name, exc)

        finally:
            conn.close()

        print(f"\n✅ All analyses complete. {len(results)}/{len(QUERIES)} succeeded.")
        return results


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Run retail analytics queries against Snowflake."
    )
    p.add_argument(
        "--query",
        default="",
        help=(
            "Name of a single analysis to run "
            "(default: all). Use --list to see options."
        ),
    )
    p.add_argument(
        "--list",
        action="store_true",
        help="Print all available query names and exit.",
    )
    p.add_argument(
        "--max-rows",
        type=int,
        default=10,
        help="Maximum rows to print per result set (default: 10).",
    )
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _parse_args()

    if args.list:
        print("\nAvailable query names:")
        for name, (label, _) in QUERIES.items():
            print(f"  {name:<26}  {label}")
        sys.exit(0)

    analytics = SnowflakeAnalytics()

    if args.query:
        df = analytics.run(args.query)
        print(df.to_string(index=False))
    else:
        analytics.run_all(print_results=True, max_rows=args.max_rows)
