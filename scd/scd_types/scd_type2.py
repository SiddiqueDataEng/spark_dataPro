"""
SCD Type 2 — Add New Row (Full History)
========================================
The most widely used SCD type.  Every time a tracked attribute changes,
the current row is **expired** (end_date set, is_current=False) and a
brand-new row is **inserted** for the new values.

This gives a complete, queryable history of every version of every
dimension member.

Schema additions (vs. source table)
-------------------------------------
  scd_start_date  DATE         — when this version became effective
  scd_end_date    DATE         — when this version was superseded (NULL = current)
  is_current      BOOLEAN      — True if this is the latest version
  scd_version     INT          — version counter (1, 2, 3 …)

When to use
-----------
* Full historical visibility: "What was the customer's city on 2023-01-15?"
* Slowly changing attributes with analytical value:
  employee department transfers, product category reclassifications.

How it works
------------
1. INSERT → insert row: scd_start_date=today, scd_end_date=NULL,
            is_current=True, scd_version=1
2. UPDATE → expire current row (end_date=today-1, is_current=False),
            then insert a new row with incremented scd_version

Output
------
  data/scd/type2/<table>/
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class SCD2:
    """Applies SCD Type-2 (add new row / full history) semantics."""

    DEFAULT_TRACK: dict[str, list[str]] = {
        "customers": ["city", "country", "email"],
        "products":  ["selling_price", "category"],
        "employees": ["department", "salary"],
        "stores":    ["city", "country", "store_name"],
    }

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
        track_cols: list[str] | None = None,
        pk_col: str | None = None,
        scd_path: str | None = None,
    ):
        self.spark      = spark
        self.table_name = table_name
        self.track_cols = track_cols or self.DEFAULT_TRACK.get(table_name, [])
        self.pk_col     = pk_col or self.PK_MAP.get(table_name, "id")
        self.scd_path   = scd_path or str(
            _PROJECT_ROOT / "data" / "scd" / "type2" / table_name
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def apply(self, events: list[dict]) -> dict[str, int]:
        from delta.tables import DeltaTable
        from scd.scd_utils import rows_to_df, ensure_delta_table

        relevant = [e for e in events if e.get("table") == self.table_name]
        if not relevant:
            return {"inserted": 0, "expired": 0, "errors": 0}

        counts = {"inserted": 0, "expired": 0, "errors": 0}
        today  = date.today()

        for evt in relevant:
            op    = evt.get("op")
            after = evt.get("after") or {}
            pk    = evt.get("pk")
            stage = None

            try:
                if op == "INSERT":
                    row = {
                        **after,
                        "scd_start_date": str(today),
                        "scd_end_date":   None,
                        "is_current":     True,
                        "scd_version":    1,
                    }
                    df, stage = rows_to_df(self.spark, [row])
                    df = self._cast(df)
                    ensure_delta_table(df, self.scd_path)

                    dt = DeltaTable.forPath(self.spark, self.scd_path)
                    shared = set(dt.toDF().columns) & set(df.columns)
                    (dt.alias("t")
                       .merge(df.alias("s"),
                              f"t.{self.pk_col} = s.{self.pk_col} "
                              "AND t.is_current = true")
                       .whenNotMatchedInsert(
                           values={c: F.col(f"s.{c}") for c in shared})
                       .execute())
                    counts["inserted"] += 1

                elif op == "UPDATE":
                    # Check table exists; if not, treat as insert
                    try:
                        dt = DeltaTable.forPath(self.spark, self.scd_path)
                    except Exception:
                        row = {
                            **after,
                            "scd_start_date": str(today),
                            "scd_end_date":   None,
                            "is_current":     True,
                            "scd_version":    1,
                        }
                        df, stage = rows_to_df(self.spark, [row])
                        df = self._cast(df)
                        ensure_delta_table(df, self.scd_path)
                        counts["inserted"] += 1
                        continue

                    # Expire current version
                    dt.update(
                        condition=(
                            (F.col(self.pk_col) == int(pk)) &
                            F.col("is_current")
                        ),
                        set={
                            "scd_end_date": F.lit(str(today - timedelta(days=1))),
                            "is_current":   F.lit(False),
                        },
                    )
                    counts["expired"] += 1

                    # Determine next version number
                    max_v = (
                        dt.toDF()
                          .filter(F.col(self.pk_col) == int(pk))
                          .agg(F.max("scd_version").alias("mv"))
                          .collect()[0]["mv"] or 0
                    )
                    new_v = max_v + 1

                    # Insert new current version
                    row = {
                        **after,
                        "scd_start_date": str(today),
                        "scd_end_date":   None,
                        "is_current":     True,
                        "scd_version":    new_v,
                    }
                    df, stage = rows_to_df(self.spark, [row])
                    df = self._cast(df)

                    dt2 = DeltaTable.forPath(self.spark, self.scd_path)
                    shared = set(dt2.toDF().columns) & set(df.columns)
                    (dt2.alias("t")
                        .merge(df.alias("s"),
                               f"t.{self.pk_col} = s.{self.pk_col} "
                               "AND t.scd_version = s.scd_version "
                               "AND t.is_current = true")
                        .whenNotMatchedInsert(
                            values={c: F.col(f"s.{c}") for c in shared})
                        .execute())
                    counts["inserted"] += 1

            except Exception as exc:
                log.error("SCD2 %s pk=%s: %s", self.table_name, pk, exc)
                counts["errors"] += 1
            finally:
                if stage:
                    stage.unlink(missing_ok=True)

        log.info("SCD2 %s: %s", self.table_name, counts)
        return counts

    def read(self, current_only: bool = False) -> DataFrame:
        df = self.spark.read.format("delta").load(self.scd_path)
        return df.filter(F.col("is_current")) if current_only else df

    def read_as_of(self, as_of_date: str) -> DataFrame:
        """Point-in-time query."""
        df = self.spark.read.format("delta").load(self.scd_path)
        return df.filter(
            (F.col("scd_start_date") <= as_of_date) &
            (
                F.col("scd_end_date").isNull() |
                (F.col("scd_end_date") >= as_of_date)
            )
        )

    def _cast(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(self.pk_col, F.col(self.pk_col).cast("long"))
        if "scd_version" in df.columns:
            df = df.withColumn("scd_version", F.col("scd_version").cast("int"))
        return df
