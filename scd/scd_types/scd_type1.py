"""
SCD Type 1 — Overwrite (no history)
=====================================
The most common SCD strategy: **overwrite the current row** with new values.
No historical record is kept — the dimension always reflects the latest state.

When to use
-----------
* Attribute corrections (typo fix, data quality remediation).
* Attributes where history has no analytical value, e.g.
  - customer phone number
  - product description text
  - employee name (legal name change)
* When storage / query simplicity matters more than auditability.

How it works
------------
1. Delta MERGE: whenMatchedUpdate(set=…), whenNotMatchedInsert(values=…)
2. For every matched row the entire row is replaced with the new values.
3. No extra columns needed — schema stays identical to the source table.

Trade-offs
----------
✅ Simple, cheap — same row count, no extra columns.
❌ History is permanently lost; reports that query past data will see
   the current value retroactively (silently wrong).

Output
------
  data/scd/type1/<table>/          — SCD-1 dimension Delta table
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class SCD1:
    """
    Applies SCD Type-1 (overwrite) semantics via Delta MERGE.

    Parameters
    ----------
    spark      : active SparkSession
    table_name : dimension table name
    pk_col     : primary-key column name (auto-detected if omitted)
    scd_path   : root path for SCD Delta tables
    """

    PK_MAP = {
        "customers": "customer_id",
        "products":  "product_id",
        "employees": "employee_id",
        "stores":    "store_id",
    }

    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
        pk_col: str | None = None,
        scd_path: str | None = None,
    ):
        self.spark      = spark
        self.table_name = table_name
        self.pk_col     = pk_col or self.PK_MAP.get(table_name, "id")
        self.scd_path   = scd_path or str(
            _PROJECT_ROOT / "data" / "scd" / "type1" / table_name
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def apply(self, events: list[dict]) -> dict[str, int]:
        """
        UPSERT: on match overwrite ALL columns; on no-match insert.
        This is the 'last-write-wins' strategy — history is discarded.

        Returns counts dict: {inserted, updated, errors}.
        """
        from delta.tables import DeltaTable
        from scd.scd_utils import rows_to_df, ensure_delta_table

        relevant = [e for e in events if e.get("table") == self.table_name]
        if not relevant:
            return {"inserted": 0, "updated": 0, "errors": 0}

        rows = [e["after"] for e in relevant if e.get("after")]
        if not rows:
            return {"inserted": 0, "updated": 0, "errors": 0}

        counts = {"inserted": 0, "updated": 0, "errors": 0}
        stage  = None
        try:
            df, stage = rows_to_df(self.spark, rows)
            df = df.withColumn(self.pk_col, F.col(self.pk_col).cast("long"))

            ensure_delta_table(df, self.scd_path)
            dt = DeltaTable.forPath(self.spark, self.scd_path)

            target_cols = set(dt.toDF().columns)
            source_cols = set(df.columns)
            shared      = target_cols & source_cols

            upsert_set = {c: F.col(f"s.{c}") for c in shared}

            (dt.alias("t")
               .merge(df.alias("s"),
                      f"t.{self.pk_col} = s.{self.pk_col}")
               .whenMatchedUpdate(set=upsert_set)
               .whenNotMatchedInsert(values=upsert_set)
               .execute())

            counts["inserted"] = sum(1 for e in relevant if e.get("op") == "INSERT")
            counts["updated"]  = sum(1 for e in relevant if e.get("op") == "UPDATE")
            log.info("SCD1 %s: %s", self.table_name, counts)

        except Exception as exc:
            log.error("SCD1 failed for %s: %s", self.table_name, exc)
            counts["errors"] = len(rows)
        finally:
            if stage:
                stage.unlink(missing_ok=True)

        return counts

    def read(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.scd_path)
