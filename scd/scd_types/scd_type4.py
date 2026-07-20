"""
SCD Type 4 — History Table (Current + Separate Archive)
========================================================
SCD-4 separates current state from historical archive into two tables:

  1. **Current table** — always the latest value, fast lookups, same schema
     as source (SCD-1 semantics).
  2. **History table** — append-only archive of every version, with
     effective date timestamps.

When to use
-----------
* Consumers mostly query current state but you still need full history.
* Legacy systems consume the current table (can't handle extra SCD columns).
* When you don't want to pollute the main table with SCD columns.

How it works
------------
  INSERT → upsert into current + append to history with scd_action='INSERT'
  UPDATE → upsert into current + archive old + append new to history

Output tables
-------------
  data/scd/type4/<table>/current/     — current snapshot
  data/scd/type4/<table>/history/     — append-only archive
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class SCD4:
    """Applies SCD Type-4 (history table) semantics."""

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
        self.spark        = spark
        self.table_name   = table_name
        self.pk_col       = pk_col or self.PK_MAP.get(table_name, "id")
        base              = scd_path or str(
            _PROJECT_ROOT / "data" / "scd" / "type4" / table_name
        )
        self.current_path = str(Path(base) / "current")
        self.history_path = str(Path(base) / "history")

    # ── Public API ─────────────────────────────────────────────────────────────

    def apply(self, events: list[dict]) -> dict[str, int]:
        from delta.tables import DeltaTable
        from scd.scd_utils import rows_to_df, ensure_delta_table

        relevant = [e for e in events if e.get("table") == self.table_name]
        if not relevant:
            return {"current_upserted": 0, "history_rows": 0, "errors": 0}

        counts       = {"current_upserted": 0, "history_rows": 0, "errors": 0}
        now          = datetime.utcnow().isoformat()
        history_rows: list[dict] = []

        for evt in relevant:
            op     = evt.get("op")
            after  = evt.get("after") or {}
            before = evt.get("before") or {}
            stage  = None

            try:
                # Upsert current table
                df, stage = rows_to_df(self.spark, [dict(after)])
                df = df.withColumn(self.pk_col, F.col(self.pk_col).cast("long"))
                ensure_delta_table(df, self.current_path)

                dt = DeltaTable.forPath(self.spark, self.current_path)
                shared = set(dt.toDF().columns) & set(df.columns)
                upsert = {c: F.col(f"s.{c}") for c in shared}

                (dt.alias("t")
                   .merge(df.alias("s"),
                          f"t.{self.pk_col} = s.{self.pk_col}")
                   .whenMatchedUpdate(set=upsert)
                   .whenNotMatchedInsert(values=upsert)
                   .execute())
                counts["current_upserted"] += 1

                # Queue history rows
                if op == "INSERT":
                    history_rows.append({**after, "scd_action": "INSERT",
                                         "scd_recorded_at": now})
                elif op == "UPDATE" and before:
                    history_rows.append({**before, "scd_action": "EXPIRE",
                                         "scd_recorded_at": now})
                    history_rows.append({**after, "scd_action": "CURRENT",
                                         "scd_recorded_at": now})
                else:
                    history_rows.append({**after, "scd_action": op or "UNKNOWN",
                                         "scd_recorded_at": now})
            except Exception as exc:
                log.error("SCD4 current upsert %s: %s", self.table_name, exc)
                counts["errors"] += 1
            finally:
                if stage:
                    stage.unlink(missing_ok=True)

        # Bulk-write history rows
        if history_rows:
            h_stage = None
            try:
                h_df, h_stage = rows_to_df(self.spark, history_rows)
                h_df = h_df.withColumn(self.pk_col, F.col(self.pk_col).cast("long"))
                (h_df.write.format("delta").mode("append")
                     .option("mergeSchema", "true")
                     .save(self.history_path))
                counts["history_rows"] += len(history_rows)
            except Exception as exc:
                log.error("SCD4 history write %s: %s", self.table_name, exc)
                counts["errors"] += len(history_rows)
            finally:
                if h_stage:
                    h_stage.unlink(missing_ok=True)

        log.info("SCD4 %s: %s", self.table_name, counts)
        return counts

    def read_current(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.current_path)

    def read_history(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.history_path)
