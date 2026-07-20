# aws/athena/athena_analytics.py
"""
AthenaAnalytics
===============
Serverless SQL analytics over the S3 Data Lake using Amazon Athena.

Mirrors the full Spark SQL analytics layer (BusinessAnalytics +
ExtendedAnalytics) but executes natively in Athena against Parquet in S3.

All 16 analyses from the Snowflake layer are re-implemented here using
Athena-compatible SQL (Presto/Trino dialect).

Key Athena differences vs Spark SQL / Snowflake:
  - date_trunc / date_format work similarly
  - INTERVAL literals: INTERVAL '1' DAY  (not INTERVAL 1 DAY)
  - approx_percentile() instead of PERCENTILE_APPROX()
  - No PIVOT — use conditional aggregation
  - Results written to S3 and returned as Pandas DataFrames

Usage:
    from aws.athena.athena_analytics import AthenaAnalytics
    ath = AthenaAnalytics()
    ath.setup_workgroup()
    results = ath.run_all()          # dict[name → pd.DataFrame]
    df = ath.run("rfm")
    ath.run_all(save_csv=True)       # saves CSVs to local results/
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Optional

import pandas as pd

from aws.config.aws_config import AWSConfig

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─────────────────────────────────────────────────────────────────────────────
# SQL queries  (Athena / Presto dialect)
# ─────────────────────────────────────────────────────────────────────────────

def _queries(glue_db_raw: str, glue_db_curated: str) -> dict[str, str]:
    """Return all analytical queries keyed by a short name."""
    bronze = glue_db_raw
    gold   = glue_db_curated

    return {

        # ── 1. Sales dashboard — daily revenue by category, last 12 months ──
        "sales_dashboard": f"""
            SELECT
                date_trunc('month', o.order_date)        AS month,
                p.category,
                ROUND(SUM(s.total_revenue), 2)           AS revenue,
                ROUND(SUM(s.profit), 2)                  AS profit,
                COUNT(DISTINCT o.order_id)               AS num_orders
            FROM   "{gold}"."sales"     s
            JOIN   "{gold}"."orders"    o  ON s.order_id  = o.order_id
            JOIN   "{gold}"."products"  p  ON s.product_id = p.product_id
            WHERE  o.order_date >= date_add('month', -12, current_date)
            GROUP  BY 1, 2
            ORDER  BY month DESC, revenue DESC
        """,

        # ── 2. Top 20 products by revenue ────────────────────────────────────
        "top_products": f"""
            SELECT
                p.product_id,
                p.product_name,
                p.category,
                ROUND(SUM(s.total_revenue), 2)   AS revenue,
                ROUND(SUM(s.profit), 2)           AS profit,
                ROUND(AVG(s.unit_price), 2)       AS avg_unit_price,
                SUM(s.quantity)                   AS units_sold,
                RANK() OVER (ORDER BY SUM(s.total_revenue) DESC) AS revenue_rank
            FROM   "{gold}"."sales"    s
            JOIN   "{gold}"."products" p ON s.product_id = p.product_id
            GROUP  BY 1, 2, 3
            ORDER  BY revenue DESC
            LIMIT  20
        """,

        # ── 3. Customer segmentation (Platinum / Gold / Silver / Bronze) ─────
        "customer_segmentation": f"""
            WITH ltv AS (
                SELECT
                    s.customer_id,
                    ROUND(SUM(s.total_revenue), 2) AS lifetime_value,
                    COUNT(DISTINCT o.order_id)     AS total_orders
                FROM   "{gold}"."sales"  s
                JOIN   "{gold}"."orders" o ON s.order_id = o.order_id
                GROUP  BY 1
            )
            SELECT
                c.customer_id,
                c.first_name || ' ' || c.last_name AS full_name,
                c.country,
                l.lifetime_value,
                l.total_orders,
                CASE
                    WHEN l.lifetime_value >= 5000                  THEN 'Platinum'
                    WHEN l.lifetime_value >= 2000                  THEN 'Gold'
                    WHEN l.lifetime_value >= 500                   THEN 'Silver'
                    WHEN l.lifetime_value >= 100                   THEN 'Bronze'
                    ELSE                                                'Standard'
                END AS segment
            FROM   "{gold}"."customers" c
            JOIN   ltv l ON c.customer_id = l.customer_id
            ORDER  BY lifetime_value DESC
        """,

        # ── 4. Store performance ──────────────────────────────────────────────
        "store_performance": f"""
            SELECT
                st.store_id,
                st.store_name,
                st.city,
                st.country,
                ROUND(SUM(s.total_revenue), 2)  AS revenue,
                ROUND(SUM(s.profit), 2)          AS profit,
                COUNT(DISTINCT o.order_id)       AS num_orders,
                ROUND(AVG(s.total_revenue), 2)   AS avg_order_value,
                RANK() OVER (ORDER BY SUM(s.total_revenue) DESC) AS revenue_rank
            FROM   "{gold}"."sales"   s
            JOIN   "{gold}"."orders"  o  ON s.order_id = o.order_id
            JOIN   "{gold}"."stores"  st ON o.store_id = st.store_id
            GROUP  BY 1, 2, 3, 4
            ORDER  BY revenue DESC
        """,

        # ── 5. Month-over-month growth by category ────────────────────────────
        "mom_growth": f"""
            WITH monthly AS (
                SELECT
                    date_trunc('month', o.order_date) AS month,
                    p.category,
                    SUM(s.total_revenue)              AS revenue
                FROM   "{gold}"."sales"    s
                JOIN   "{gold}"."orders"   o ON s.order_id  = o.order_id
                JOIN   "{gold}"."products" p ON s.product_id = p.product_id
                GROUP  BY 1, 2
            )
            SELECT
                m.month,
                m.category,
                ROUND(m.revenue, 2)                                AS revenue,
                ROUND(LAG(m.revenue) OVER (
                    PARTITION BY m.category ORDER BY m.month
                ), 2)                                              AS prev_revenue,
                ROUND(
                    (m.revenue - LAG(m.revenue) OVER (
                        PARTITION BY m.category ORDER BY m.month
                    )) / NULLIF(LAG(m.revenue) OVER (
                        PARTITION BY m.category ORDER BY m.month
                    ), 0) * 100, 2
                )                                                  AS mom_growth_pct
            FROM   monthly m
            ORDER  BY month DESC, category
        """,

        # ── 6. Customer LTV distribution (percentiles) ───────────────────────
        "ltv_distribution": f"""
            WITH ltv AS (
                SELECT
                    o.customer_id,
                    SUM(s.total_revenue) AS lifetime_value
                FROM   "{gold}"."sales"  s
                JOIN   "{gold}"."orders" o ON s.order_id = o.order_id
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
                ROUND(
                    stddev(lifetime_value), 2
                )                                                  AS std_dev
            FROM ltv
        """,

        # ── 7. RFM quintile scoring ───────────────────────────────────────────
        "rfm": f"""
            WITH base AS (
                SELECT
                    o.customer_id,
                    DATE_DIFF('day', MAX(o.order_date), current_date) AS recency_days,
                    COUNT(DISTINCT o.order_id)                         AS frequency,
                    ROUND(SUM(s.total_revenue), 2)                     AS monetary
                FROM   "{gold}"."orders" o
                JOIN   "{gold}"."sales"  s ON s.order_id = o.order_id
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
                    WHEN r_score >= 4 AND f_score >= 4             THEN 'Champions'
                    WHEN r_score >= 3 AND f_score >= 3             THEN 'Loyal'
                    WHEN r_score >= 4 AND f_score < 3              THEN 'Recent'
                    WHEN r_score <= 2 AND f_score >= 3             THEN 'At Risk'
                    WHEN r_score <= 2 AND f_score <= 2             THEN 'Lost'
                    WHEN r_score >= 3 AND f_score <= 2             THEN 'Promising'
                    ELSE 'Needs Attention'
                END                                                AS segment
            FROM scored
            ORDER BY monetary DESC
        """,

        # ── 8. Cohort retention matrix ────────────────────────────────────────
        "cohort_retention": f"""
            WITH first_order AS (
                SELECT
                    customer_id,
                    date_trunc('month', MIN(order_date)) AS cohort_month
                FROM "{gold}"."orders"
                GROUP BY 1
            ),
            orders_with_cohort AS (
                SELECT
                    o.customer_id,
                    f.cohort_month,
                    DATE_DIFF('month', f.cohort_month,
                        date_trunc('month', o.order_date)) AS months_since
                FROM   "{gold}"."orders" o
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
            FROM orders_with_cohort
            GROUP BY 1, 2
            ORDER BY cohort_month, months_since
        """,

        # ── 9. ABC / Pareto product classification ────────────────────────────
        "abc_products": f"""
            WITH revenue AS (
                SELECT
                    p.product_id,
                    p.product_name,
                    p.category,
                    SUM(s.total_revenue) AS revenue
                FROM   "{gold}"."sales"    s
                JOIN   "{gold}"."products" p ON s.product_id = p.product_id
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
                product_id,
                product_name,
                category,
                ROUND(revenue, 2)                                        AS revenue,
                ROUND(cum_revenue / total_revenue * 100, 2)              AS cum_pct,
                CASE
                    WHEN cum_revenue / total_revenue <= 0.80 THEN 'A'
                    WHEN cum_revenue / total_revenue <= 0.95 THEN 'B'
                    ELSE                                          'C'
                END                                                      AS abc_class
            FROM cumulative
            ORDER BY revenue DESC
        """,

        # ── 10. Inventory risk (days-of-cover) ────────────────────────────────
        "inventory_risk": f"""
            WITH daily_sales AS (
                SELECT
                    s.product_id,
                    SUM(s.quantity) / NULLIF(COUNT(DISTINCT o.order_date), 0) AS avg_daily_units
                FROM   "{gold}"."sales"  s
                JOIN   "{gold}"."orders" o ON s.order_id = o.order_id
                WHERE  o.order_date >= date_add('day', -90, current_date)
                GROUP  BY 1
            )
            SELECT
                p.product_id,
                p.product_name,
                p.category,
                p.stock,
                ROUND(d.avg_daily_units, 2)                    AS avg_daily_units,
                ROUND(p.stock / NULLIF(d.avg_daily_units, 0), 1) AS days_of_cover,
                CASE
                    WHEN p.stock / NULLIF(d.avg_daily_units, 0) < 14  THEN 'STOCKOUT_RISK'
                    WHEN p.stock / NULLIF(d.avg_daily_units, 0) > 180 THEN 'OVERSTOCKED'
                    ELSE                                                    'OK'
                END                                            AS risk_flag
            FROM   "{bronze}"."products" p
            LEFT   JOIN daily_sales d ON p.product_id = d.product_id
            ORDER  BY days_of_cover ASC NULLS FIRST
        """,

        # ── 11. Year-over-year comparison ─────────────────────────────────────
        "yoy_comparison": f"""
            WITH monthly AS (
                SELECT
                    YEAR(o.order_date)                          AS yr,
                    MONTH(o.order_date)                         AS mth,
                    p.category,
                    SUM(s.total_revenue)                        AS revenue
                FROM   "{gold}"."sales"    s
                JOIN   "{gold}"."orders"   o ON s.order_id  = o.order_id
                JOIN   "{gold}"."products" p ON s.product_id = p.product_id
                GROUP  BY 1, 2, 3
            )
            SELECT
                a.yr, a.mth, a.category,
                ROUND(a.revenue, 2)                           AS current_year_rev,
                ROUND(b.revenue, 2)                           AS prev_year_rev,
                ROUND((a.revenue - b.revenue) /
                      NULLIF(b.revenue, 0) * 100, 2)          AS yoy_growth_pct
            FROM   monthly a
            LEFT   JOIN monthly b
                ON  a.category = b.category
                AND a.mth      = b.mth
                AND a.yr       = b.yr + 1
            ORDER  BY a.yr DESC, a.mth, a.category
        """,

        # ── 12. Discount effectiveness ────────────────────────────────────────
        "discount": f"""
            SELECT
                CASE
                    WHEN o.discount_pct = 0          THEN 'No Discount'
                    WHEN o.discount_pct <= 0.10      THEN '1-10%'
                    WHEN o.discount_pct <= 0.20      THEN '11-20%'
                    WHEN o.discount_pct <= 0.30      THEN '21-30%'
                    ELSE                                  '31%+'
                END                                        AS discount_bucket,
                COUNT(DISTINCT o.order_id)                 AS num_orders,
                ROUND(AVG(s.total_revenue), 2)             AS avg_order_value,
                ROUND(AVG(s.profit / NULLIF(s.total_revenue, 0) * 100), 2)
                                                           AS avg_margin_pct,
                ROUND(SUM(s.profit), 2)                    AS total_profit
            FROM   "{gold}"."orders" o
            JOIN   "{gold}"."sales"  s ON o.order_id = s.order_id
            GROUP  BY 1
            ORDER  BY discount_bucket
        """,

        # ── 13. Employee performance ──────────────────────────────────────────
        "employee": f"""
            SELECT
                e.employee_id,
                e.first_name || ' ' || e.last_name AS full_name,
                e.department,
                e.salary,
                COUNT(DISTINCT o.order_id)                AS orders_handled,
                ROUND(SUM(s.total_revenue), 2)            AS revenue_generated,
                ROUND(SUM(s.profit), 2)                   AS profit_generated,
                ROUND(SUM(s.total_revenue) / NULLIF(e.salary, 0), 2)
                                                          AS revenue_to_salary_ratio,
                RANK() OVER (ORDER BY SUM(s.total_revenue) DESC) AS revenue_rank
            FROM   "{gold}"."employees" e
            JOIN   "{gold}"."orders"    o ON o.employee_id = e.employee_id
            JOIN   "{gold}"."sales"     s ON s.order_id    = o.order_id
            GROUP  BY 1, 2, 3, 4
            ORDER  BY revenue_generated DESC
        """,

        # ── 14. Cross-sell / basket analysis ─────────────────────────────────
        "cross_sell": f"""
            WITH multi_item AS (
                SELECT order_id
                FROM   "{gold}"."sales"
                GROUP  BY order_id
                HAVING COUNT(DISTINCT product_id) >= 2
            ),
            pairs AS (
                SELECT
                    a.product_id AS product_a,
                    b.product_id AS product_b
                FROM   "{gold}"."sales" a
                JOIN   "{gold}"."sales" b
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
            JOIN   "{gold}"."products" pa ON pairs.product_a = pa.product_id
            JOIN   "{gold}"."products" pb ON pairs.product_b = pb.product_id
            GROUP  BY 1, 2, 3, 4
            ORDER  BY co_occurrences DESC
            LIMIT  30
        """,

        # ── 15. Revenue anomaly detection (|z-score| > 2.5) ──────────────────
        "anomaly_detection": f"""
            WITH daily AS (
                SELECT
                    p.category,
                    o.order_date,
                    SUM(s.total_revenue) AS revenue
                FROM   "{gold}"."sales"    s
                JOIN   "{gold}"."orders"   o ON s.order_id  = o.order_id
                JOIN   "{gold}"."products" p ON s.product_id = p.product_id
                GROUP  BY 1, 2
            ),
            stats AS (
                SELECT
                    category,
                    order_date,
                    revenue,
                    AVG(revenue)    OVER (PARTITION BY category) AS mean_rev,
                    STDDEV(revenue) OVER (PARTITION BY category) AS std_rev
                FROM daily
            )
            SELECT
                category,
                order_date,
                ROUND(revenue, 2)     AS revenue,
                ROUND(mean_rev, 2)    AS mean_revenue,
                ROUND(std_rev, 2)     AS std_revenue,
                ROUND(ABS((revenue - mean_rev) /
                          NULLIF(std_rev, 0)), 2)  AS z_score,
                TRUE                               AS is_anomaly
            FROM stats
            WHERE ABS((revenue - mean_rev) / NULLIF(std_rev, 0)) > 2.5
            ORDER BY z_score DESC
        """,

        # ── 16. Seasonality (revenue by day-of-week and month) ───────────────
        "seasonality": f"""
            SELECT
                DAY_OF_WEEK(o.order_date)          AS day_of_week,
                MONTH(o.order_date)                AS month_num,
                p.category,
                ROUND(SUM(s.total_revenue), 2)     AS revenue,
                COUNT(DISTINCT o.order_id)         AS num_orders
            FROM   "{gold}"."sales"    s
            JOIN   "{gold}"."orders"   o ON s.order_id  = o.order_id
            JOIN   "{gold}"."products" p ON s.product_id = p.product_id
            GROUP  BY 1, 2, 3
            ORDER  BY month_num, day_of_week, revenue DESC
        """,
    }


# ─────────────────────────────────────────────────────────────────────────────
# AthenaAnalytics class
# ─────────────────────────────────────────────────────────────────────────────

class AthenaAnalytics:
    """Execute SQL analytics via Amazon Athena over the S3 Data Lake."""

    def __init__(self, cfg: Optional[AWSConfig] = None) -> None:
        self.cfg    = cfg or AWSConfig()
        self.athena = self.cfg.athena_client()
        self.s3     = self.cfg.s3_client()
        self._queries = _queries(self.cfg.glue_db_raw, self.cfg.glue_db_curated)

    # ──────────────────────────────────────────────────────────────────────────
    # Workgroup setup
    # ──────────────────────────────────────────────────────────────────────────

    def setup_workgroup(self) -> None:
        """Create the Athena workgroup with per-query cost controls."""
        try:
            self.athena.create_work_group(
                Name=self.cfg.athena_workgroup,
                Description="Retail Medallion Analytics workgroup",
                Configuration={
                    "ResultConfiguration": {
                        "OutputLocation":    self.cfg.athena_output,
                        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                    },
                    "EnforceWorkGroupConfiguration": True,
                    "PublishCloudWatchMetricsEnabled": True,
                    "BytesScannedCutoffPerQuery":  10_737_418_240,  # 10 GB
                    "RequesterPaysEnabled": False,
                    "EngineVersion": {
                        "SelectedEngineVersion": "Athena engine version 3",
                    },
                },
                Tags=[
                    {"Key": "Project", "Value": "retail-medallion"},
                    {"Key": "Owner",   "Value": "MSiddique"},
                ],
            )
            log.info("Created Athena workgroup: %s", self.cfg.athena_workgroup)
        except self.athena.exceptions.InvalidRequestException:
            log.info("Athena workgroup already exists: %s", self.cfg.athena_workgroup)

    # ──────────────────────────────────────────────────────────────────────────
    # Query execution
    # ──────────────────────────────────────────────────────────────────────────

    def execute(self, sql: str, database: Optional[str] = None) -> str:
        """Submit a query and return the QueryExecutionId."""
        kwargs: dict = {
            "QueryString":  sql.strip(),
            "WorkGroup":    self.cfg.athena_workgroup,
            "ResultConfiguration": {
                "OutputLocation": self.cfg.athena_output,
            },
        }
        if database:
            kwargs["QueryExecutionContext"] = {"Database": database}

        resp = self.athena.start_query_execution(**kwargs)
        return resp["QueryExecutionId"]

    def wait(self, execution_id: str, timeout: int = 120) -> str:
        """Poll until the query completes. Returns final state."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp  = self.athena.get_query_execution(
                QueryExecutionId=execution_id
            )
            state = resp["QueryExecution"]["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                if state != "SUCCEEDED":
                    reason = resp["QueryExecution"]["Status"].get(
                        "StateChangeReason", ""
                    )
                    log.error("Query %s  state=%s  reason=%s",
                              execution_id, state, reason)
                return state
            time.sleep(2)
        return "TIMEOUT"

    def fetch(self, execution_id: str, max_rows: int = 1000) -> pd.DataFrame:
        """Fetch query results as a Pandas DataFrame."""
        paginator = self.athena.get_paginator("get_query_results")
        rows: list[list] = []
        columns: list[str] = []

        for page in paginator.paginate(QueryExecutionId=execution_id):
            result_set = page["ResultSet"]
            if not columns:
                columns = [
                    c["Label"]
                    for c in result_set["ResultSetMetadata"]["ColumnInfo"]
                ]
            for row in result_set["Rows"][1:]:  # skip header row
                rows.append([d.get("VarCharValue", None) for d in row["Data"]])
                if len(rows) >= max_rows:
                    break

        return pd.DataFrame(rows, columns=columns)

    def run(
        self,
        query_name: str,
        database: Optional[str] = None,
        max_rows: int = 1000,
    ) -> pd.DataFrame:
        """Execute a named analysis and return results as a DataFrame."""
        if query_name not in self._queries:
            raise ValueError(
                f"Unknown query '{query_name}'. "
                f"Available: {list(self._queries.keys())}"
            )
        sql = self._queries[query_name]
        db  = database or self.cfg.glue_db_curated
        log.info("Running Athena query: %s", query_name)

        exec_id = self.execute(sql, database=db)
        state   = self.wait(exec_id)

        if state != "SUCCEEDED":
            log.error("Query %s failed  state=%s", query_name, state)
            return pd.DataFrame()

        df = self.fetch(exec_id, max_rows=max_rows)
        log.info("  %s: %d rows", query_name, len(df))
        return df

    def run_all(
        self,
        max_rows: int = 1000,
        save_csv: bool = False,
    ) -> dict[str, pd.DataFrame]:
        """Run all 16 analyses. Returns dict[name → DataFrame]."""
        log.info("=== Athena Analytics (%d queries) ===", len(self._queries))
        results: dict[str, pd.DataFrame] = {}

        for name in self._queries:
            try:
                df = self.run(name, max_rows=max_rows)
                results[name] = df
                if save_csv:
                    out_dir = Path("results") / "athena"
                    out_dir.mkdir(parents=True, exist_ok=True)
                    df.to_csv(out_dir / f"{name}.csv", index=False)
            except Exception as exc:
                log.error("Query %s failed: %s", name, exc)
                results[name] = pd.DataFrame()

        log.info("=== Athena Analytics complete ===")
        return results

    def list_queries(self) -> list[str]:
        """Return all available query names."""
        return list(self._queries.keys())

    def run_ddl(self, sql: str) -> None:
        """Execute a DDL statement (CREATE TABLE, MSCK REPAIR, etc.)."""
        exec_id = self.execute(sql)
        self.wait(exec_id, timeout=60)
