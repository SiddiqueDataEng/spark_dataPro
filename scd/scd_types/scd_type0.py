"""
SCD Type 0 — Fixed / Retain Original
=====================================
The simplest strategy: **do nothing when a change arrives for designated
columns**.  Original values are preserved forever; incoming updates to
"fixed" columns are silently dropped.

When to use
-----------
* Attributes that should never change once set, e.g.
  - customer's original join_date / gender-at-signup
  - product's original cost_price / launch_date
  - employee's hire_date / original employee number
* Regulatory or contractual obligation to keep the original value.

How it works
------------
1. On first INSERT the row lands in the dimension table normally.
2. On UPDATE the engine checks which columns have changed.
   - Changed columns NOT in fixed_cols → allowed through (mutable update).
   - Changed columns IN fixed_cols     → silently dropped.
3. A Delta MERGE is used: whenMatchedUpdate only touches non-fixed columns.

Output
------
  data/scd/type0/<table>/          — SCD-0 dimension Delta table
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class SCD0:
    """Applies SCD Type-0 (retain original) semantics."""

    DEFAULT_FIXED: dict[str, list[str]] = {
        "customers": ["join_date", "gender"],
        "products":  ["cost_price"],
        "employees": [],
        "stores":    [],
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
        fixed_cols: list[str] | None = None,
        pk_col: str | None = None,
        scd_path: str | None = None,
    ):
        self.spark      = spark
        self.table_name = table_name
        self.fixed_cols = fixed_cols if fixed_cols is not None \
                          else self.DEFAULT_FIXED.get(table_name, [])
        self.pk_col     = pk_col or self.PK_MAP.get(table_name, "id")
        self.scd_path   = scd_path or str(
            _PROJECT_ROOT / "data" / "scd" / "type0" / table_name
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def apply(self, events: list[dict]) -> dict[str, int]:
        """Process INSERT (full insert) and UPDATE (non-fixed cols only)."""
        from delta.tables import DeltaTable
        from scd.scd_utils import rows_to_df, ensure_delta_table

        counts = {"inserted": 0, "updated": 0, "skipped_fixed": 0, "errors": 0}

        inserts = [e for e in events
                   if e.get("table") == self.table_name and e.get("op") == "INSERT"]
        updates = [e for e in events
                   if e.get("table") == self.table_name and e.get("op") == "UPDATE"]

        # ── INSERTs ────────────────────────────────────────────────────────────
        if inserts:
            rows   = [e["after"] for e in inserts if e.get("after")]
            stage  = None
            if rows:
                try:
                    df, stage = rows_to_df(self.spark, rows)
                    df = df.withColumn(self.pk_col, F.col(self.pk_col).cast("long"))
                    ensure_delta_table(df, self.scd_path)

                    dt = DeltaTable.forPath(self.spark, self.scd_path)
                    shared = set(dt.toDF().columns) & set(df.columns)
                    vals   = {c: F.col(f"s.{c}") for c in shared}

                    (dt.alias("t")
                       .merge(df.alias("s"),
                              f"t.{self.pk_col} = s.{self.pk_col}")
                       .whenNotMatchedInsert(values=vals)
                       .execute())
                    counts["inserted"] += len(rows)
                except Exception as exc:
                    log.error("SCD0 insert %s: %s", self.table_name, exc)
                    counts["errors"] += len(rows)
                finally:
                    if stage:
                        stage.unlink(missing_ok=True)

        # ── UPDATEs ────────────────────────────────────────────────────────────
        # Skip updates if the SCD table doesn't exist yet (no INSERTs seen yet)
        scd_exists = Path(self.scd_path).exists() and any(Path(self.scd_path).iterdir())
        if updates and not scd_exists:
            log.info("SCD0 %s: skipping %d updates — table not yet bootstrapped",
                     self.table_name, len(updates))
            return counts

        for evt in updates:
            after   = evt.get("after") or {}
            changed = evt.get("changed_attrs") or list(after.keys())
            fixed   = [c for c in changed if c in self.fixed_cols]
            mutable = [c for c in changed if c not in self.fixed_cols
                       and c != self.pk_col]

            if fixed:
                log.info("SCD0 %s pk=%s: dropping fixed cols %s",
                         self.table_name, evt.get("pk"), fixed)
                counts["skipped_fixed"] += 1

            if not mutable:
                continue

            stage = None
            try:
                row  = {k: after[k] for k in mutable + [self.pk_col] if k in after}
                df, stage = rows_to_df(self.spark, [row])
                df   = df.withColumn(self.pk_col, F.col(self.pk_col).cast("long"))

                dt = DeltaTable.forPath(self.spark, self.scd_path)
                shared     = set(dt.toDF().columns) & set(df.columns)
                update_set = {
                    c: F.col(f"s.{c}")
                    for c in (shared - {self.pk_col})
                    if c not in self.fixed_cols
                }
                if update_set:
                    (dt.alias("t")
                       .merge(df.alias("s"),
                              f"t.{self.pk_col} = s.{self.pk_col}")
                       .whenMatchedUpdate(set=update_set)
                       .execute())
                    counts["updated"] += 1
            except Exception as exc:
                log.error("SCD0 update %s pk=%s: %s",
                          self.table_name, evt.get("pk"), exc)
                counts["errors"] += 1
            finally:
                if stage:
                    stage.unlink(missing_ok=True)

        log.info("SCD0 %s: %s", self.table_name, counts)
        return counts

    def read(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.scd_path)
