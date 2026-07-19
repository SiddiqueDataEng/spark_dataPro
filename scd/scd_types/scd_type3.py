"""
SCD Type 3 — Add New Column (Limited History)
===============================================
Instead of adding rows, SCD-3 adds **extra columns** to store the
previous value alongside the current value.

Schema additions (per tracked column X)
-----------------------------------------
  current_<X>       — current value (renamed from original column)
  previous_<X>      — the value before the last change (NULL on first load)
  scd_changed_at    TIMESTAMP  — when the most recent change occurred
  scd_change_count  INT        — how many changes this row has seen

When to use
-----------
* "Before/after" reporting on a small number of attributes.
* You want a flat, single-row view and just need "what was it before?".
* Examples: current vs. previous department, current vs. previous price.

Limitations
-----------
❌ Only tracks ONE prior value — further changes overwrite previous_X.
❌ Can't answer "what was it two changes ago?".

Output
------
  data/scd/type3/<table>/
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class SCD3:
    """Applies SCD Type-3 (add new column) semantics."""

    DEFAULT_TRACK: dict[str, list[str]] = {
        "customers": ["city", "country"],
        "products":  ["category", "selling_price"],
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
            _PROJECT_ROOT / "data" / "scd" / "type3" / table_name
        )

    # ── Public API ─────────────────────────────────────────────────────────────

    def apply(self, events: list[dict]) -> dict[str, int]:
        from delta.tables import DeltaTable
        from scd.scd_utils import rows_to_df, ensure_delta_table

        relevant = [e for e in events if e.get("table") == self.table_name]
        if not relevant:
            return {"inserted": 0, "updated": 0, "errors": 0}

        counts = {"inserted": 0, "updated": 0, "errors": 0}
        now    = datetime.utcnow().isoformat()

        inserts = [e for e in relevant if e.get("op") == "INSERT"]
        updates = [e for e in relevant if e.get("op") == "UPDATE"]

        # ── INSERTs ────────────────────────────────────────────────────────────
        if inserts:
            rows  = [self._initial_row(e.get("after") or {}, now) for e in inserts]
            stage = None
            try:
                df, stage = rows_to_df(self.spark, rows)
                df = df.withColumn(self.pk_col, F.col(self.pk_col).cast("long"))
                ensure_delta_table(df, self.scd_path)

                dt = DeltaTable.forPath(self.spark, self.scd_path)
                shared = set(dt.toDF().columns) & set(df.columns)
                (dt.alias("t")
                   .merge(df.alias("s"),
                          f"t.{self.pk_col} = s.{self.pk_col}")
                   .whenNotMatchedInsert(
                       values={c: F.col(f"s.{c}") for c in shared})
                   .execute())
                counts["inserted"] += len(rows)
            except Exception as exc:
                log.error("SCD3 insert %s: %s", self.table_name, exc)
                counts["errors"] += len(inserts)
            finally:
                if stage:
                    stage.unlink(missing_ok=True)

        # ── UPDATEs ────────────────────────────────────────────────────────────
        # Skip updates if the SCD table doesn't exist yet
        scd_exists = Path(self.scd_path).exists() and any(Path(self.scd_path).iterdir())
        if updates and not scd_exists:
            log.info("SCD3 %s: skipping %d updates — table not yet bootstrapped",
                     self.table_name, len(updates))
            return counts

        for evt in updates:
            after   = evt.get("after") or {}
            pk      = evt.get("pk")
            changed = evt.get("changed_attrs") or list(after.keys())
            tracked = [c for c in changed if c in self.track_cols]
            if not tracked:
                continue

            try:
                dt = DeltaTable.forPath(self.spark, self.scd_path)

                update_set: dict = {
                    "scd_changed_at": F.lit(now),
                    "scd_change_count": (
                        F.coalesce(F.col("scd_change_count"), F.lit(0)) + 1
                    ),
                }
                for col_name in tracked:
                    update_set[f"previous_{col_name}"] = F.col(f"current_{col_name}")
                    update_set[f"current_{col_name}"]  = F.lit(str(after.get(col_name, "")))

                dt.update(
                    condition=F.col(self.pk_col) == int(pk),
                    set=update_set,
                )
                counts["updated"] += 1
            except Exception as exc:
                log.error("SCD3 update %s pk=%s: %s", self.table_name, pk, exc)
                counts["errors"] += 1

        log.info("SCD3 %s: %s", self.table_name, counts)
        return counts

    def read(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.scd_path)

    def _initial_row(self, after: dict, now: str) -> dict:
        row: dict = {}
        for k, v in after.items():
            if k in self.track_cols:
                row[f"current_{k}"]  = v
                row[f"previous_{k}"] = None
            else:
                row[k] = v
        row["scd_changed_at"]   = now
        row["scd_change_count"] = 0
        return row
