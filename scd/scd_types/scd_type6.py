"""
SCD Type 6 — Hybrid SCD 1 + 2 + 3
====================================
The most comprehensive SCD type, combining:
  * SCD-2 rows        — new row per change (full history)
  * SCD-3 columns     — previous_X column per tracked attribute
  * SCD-1 retroactive — current_X column retroactively updated on ALL rows

Schema (per tracked column X)
------------------------------
  X                  — historical value (as of this row's effective date)
  previous_X         — value before this change
  current_X          — ALWAYS the latest value (retroactively updated on ALL rows)

Plus standard SCD-2 columns:
  scd_start_date  DATE
  scd_end_date    DATE       (NULL = current)
  is_current      BOOLEAN
  scd_version     INT

The retroactive update is what makes SCD-6 unique:
  When employee 42 moves from Finance → Marketing, ALL their historical
  rows get current_department='Marketing' retroactively.  But each row
  still keeps department='Finance' (what it was at the time).

  This lets you answer: "Who was EVER in Finance and is NOW in Marketing?"
  in a single table scan with no joins.

Output
------
  data/scd/type6/<table>/
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class SCD6:
    """Applies SCD Type-6 (hybrid SCD1+2+3) semantics."""

    DEFAULT_TRACK: dict[str, list[str]] = {
        "customers": ["city", "country"],
        "products":  ["selling_price", "category"],
        "employees": ["department", "salary"],
        "stores":    ["city", "country"],
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
            _PROJECT_ROOT / "data" / "scd" / "type6" / table_name
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def apply(self, events: list[dict]) -> dict[str, int]:
        from delta.tables import DeltaTable
        from scd.scd_utils import rows_to_df, ensure_delta_table

        relevant = [e for e in events if e.get("table") == self.table_name]
        if not relevant:
            return {"inserted": 0, "expired": 0, "retroactive_updates": 0, "errors": 0}

        counts = {"inserted": 0, "expired": 0, "retroactive_updates": 0, "errors": 0}
        today  = date.today()

        for evt in relevant:
            op     = evt.get("op")
            after  = evt.get("after") or {}
            before = evt.get("before") or {}
            pk     = evt.get("pk")
            stage  = None

            try:
                if op == "INSERT":
                    row = self._first_version(after, today)
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
                    try:
                        dt = DeltaTable.forPath(self.spark, self.scd_path)
                    except Exception:
                        row = self._first_version(after, today)
                        df, stage = rows_to_df(self.spark, [row])
                        df = self._cast(df)
                        ensure_delta_table(df, self.scd_path)
                        counts["inserted"] += 1
                        continue

                    # Step 1: expire current version
                    dt.update(
                        condition=(
                            (F.col(self.pk_col) == int(pk)) &
                            F.col("is_current")
                        ),
                        set={
                            "is_current":   F.lit(False),
                            "scd_end_date": F.lit(str(today - timedelta(days=1))),
                        },
                    )
                    counts["expired"] += 1

                    # Step 2: retroactively update current_X on ALL rows for this member
                    retro: dict = {}
                    for col_name in self.track_cols:
                        if col_name in (evt.get("changed_attrs") or []):
                            retro[f"current_{col_name}"] = F.lit(
                                str(after.get(col_name, ""))
                            )
                    if retro:
                        dt.update(
                            condition=F.col(self.pk_col) == int(pk),
                            set=retro,
                        )
                        counts["retroactive_updates"] += 1

                    # Step 3: get next version number
                    max_v = (
                        dt.toDF()
                          .filter(F.col(self.pk_col) == int(pk))
                          .agg(F.max("scd_version").alias("mv"))
                          .collect()[0]["mv"] or 0
                    )
                    new_v = max_v + 1

                    # Step 4: insert new current row
                    prev_vals = {
                        f"previous_{c}": str(before.get(c, ""))
                        for c in self.track_cols
                    }
                    row = self._new_version(after, today, new_v, prev_vals)
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
                log.error("SCD6 %s pk=%s: %s", self.table_name, pk, exc)
                counts["errors"] += 1
            finally:
                if stage:
                    stage.unlink(missing_ok=True)

        log.info("SCD6 %s: %s", self.table_name, counts)
        return counts

    def read(self, current_only: bool = False) -> DataFrame:
        df = self.spark.read.format("delta").load(self.scd_path)
        return df.filter(F.col("is_current")) if current_only else df

    def read_as_of(self, as_of_date: str) -> DataFrame:
        df = self.spark.read.format("delta").load(self.scd_path)
        return df.filter(
            (F.col("scd_start_date") <= as_of_date) &
            (
                F.col("scd_end_date").isNull() |
                (F.col("scd_end_date") >= as_of_date)
            )
        )

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _first_version(self, after: dict, start: date) -> dict:
        row = dict(after)
        for c in self.track_cols:
            row[f"previous_{c}"] = None
            row[f"current_{c}"]  = str(after.get(c, ""))
        row.update({
            "scd_start_date": str(start),
            "scd_end_date":   None,
            "is_current":     True,
            "scd_version":    1,
        })
        return row

    def _new_version(
        self, after: dict, start: date, version: int, prev_vals: dict
    ) -> dict:
        row = dict(after)
        for c in self.track_cols:
            row[f"previous_{c}"] = prev_vals.get(f"previous_{c}")
            row[f"current_{c}"]  = str(after.get(c, ""))
        row.update({
            "scd_start_date": str(start),
            "scd_end_date":   None,
            "is_current":     True,
            "scd_version":    version,
            **prev_vals,
        })
        return row

    def _cast(self, df: DataFrame) -> DataFrame:
        df = df.withColumn(self.pk_col, F.col(self.pk_col).cast("long"))
        if "scd_version" in df.columns:
            df = df.withColumn("scd_version", F.col("scd_version").cast("int"))
        return df
