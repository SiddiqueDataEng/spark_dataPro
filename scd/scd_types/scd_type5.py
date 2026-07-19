"""
SCD Type 5 — Mini-Dimension + Current Snapshot (SCD4 + SCD1)
=============================================================
SCD-5 = SCD-4 (separate history) + SCD-1 (current snapshot) + a compact
**mini-dimension** table that stores unique combinations of tracked attributes.

The current table references mini_dim_key so fact queries can filter by
attribute profile WITHOUT joining the large history table.

Example: "sales where customer_segment='Premium'" → mini_dim_key='a3b9...'
→ fast lookup in mini_dim, no full history scan.

Output tables
-------------
  data/scd/type5/<table>/current/      — current snapshot + mini_dim_key FK
  data/scd/type5/<table>/history/      — append-only archive
  data/scd/type5/<table>/mini_dim/     — compact attribute profile lookup
"""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

log = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


class SCD5:
    """Applies SCD Type-5 (mini-dimension) semantics."""

    DEFAULT_PROFILE: dict[str, list[str]] = {
        "customers": ["city", "country"],
        "products":  ["category"],
        "employees": ["department"],
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
        profile_cols: list[str] | None = None,
        pk_col: str | None = None,
        scd_path: str | None = None,
    ):
        self.spark        = spark
        self.table_name   = table_name
        self.profile_cols = profile_cols or self.DEFAULT_PROFILE.get(table_name, [])
        self.pk_col       = pk_col or self.PK_MAP.get(table_name, "id")
        base              = scd_path or str(
            _PROJECT_ROOT / "data" / "scd" / "type5" / table_name
        )
        self.current_path  = str(Path(base) / "current")
        self.history_path  = str(Path(base) / "history")
        self.mini_dim_path = str(Path(base) / "mini_dim")

    # ── Public API ─────────────────────────────────────────────────────────────

    def apply(self, events: list[dict]) -> dict[str, int]:
        from delta.tables import DeltaTable
        from scd.scd_utils import rows_to_df, ensure_delta_table

        relevant = [e for e in events if e.get("table") == self.table_name]
        if not relevant:
            return {"current_upserted": 0, "history_rows": 0,
                    "mini_dim_keys": 0, "errors": 0}

        counts        = {"current_upserted": 0, "history_rows": 0,
                         "mini_dim_keys": 0, "errors": 0}
        now           = datetime.utcnow().isoformat()
        history_rows:  list[dict] = []
        mini_profiles: list[dict] = []

        for evt in relevant:
            op     = evt.get("op")
            after  = evt.get("after") or {}
            before = evt.get("before") or {}
            stage  = None

            try:
                profile     = {c: str(after.get(c, "")) for c in self.profile_cols}
                mini_key    = self._profile_key(profile)
                mini_profiles.append({**profile, "mini_dim_key": mini_key})

                # Upsert current table with mini_dim_key
                row = {**after, "mini_dim_key": mini_key}
                df, stage = rows_to_df(self.spark, [row])
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

                action = "INSERT" if op == "INSERT" else "CURRENT"
                history_rows.append({**row, "scd_action": action,
                                      "scd_recorded_at": now})
                if op == "UPDATE" and before:
                    old_key = self._profile_key(
                        {c: str(before.get(c, "")) for c in self.profile_cols}
                    )
                    history_rows.append({**before, "mini_dim_key": old_key,
                                          "scd_action": "EXPIRE",
                                          "scd_recorded_at": now})
            except Exception as exc:
                log.error("SCD5 %s: %s", self.table_name, exc)
                counts["errors"] += 1
            finally:
                if stage:
                    stage.unlink(missing_ok=True)

        # Write history
        if history_rows:
            h_stage = None
            try:
                h_df, h_stage = rows_to_df(self.spark, history_rows)
                h_df = h_df.withColumn(self.pk_col, F.col(self.pk_col).cast("long"))
                (h_df.write.format("delta").mode("append")
                     .option("mergeSchema", "true").save(self.history_path))
                counts["history_rows"] += len(history_rows)
            except Exception as exc:
                log.error("SCD5 history %s: %s", self.table_name, exc)
            finally:
                if h_stage:
                    h_stage.unlink(missing_ok=True)

        # Upsert mini-dimension
        if mini_profiles:
            m_stage = None
            try:
                m_df, m_stage = rows_to_df(self.spark, mini_profiles)
                m_df = m_df.dropDuplicates(["mini_dim_key"])
                ensure_delta_table(m_df, self.mini_dim_path)

                m_dt = DeltaTable.forPath(self.spark, self.mini_dim_path)
                m_shared = set(m_dt.toDF().columns) & set(m_df.columns)
                (m_dt.alias("t")
                     .merge(m_df.alias("s"),
                            "t.mini_dim_key = s.mini_dim_key")
                     .whenNotMatchedInsert(
                         values={c: F.col(f"s.{c}") for c in m_shared})
                     .execute())
                counts["mini_dim_keys"] += len(mini_profiles)
            except Exception as exc:
                log.error("SCD5 mini-dim %s: %s", self.table_name, exc)
            finally:
                if m_stage:
                    m_stage.unlink(missing_ok=True)

        log.info("SCD5 %s: %s", self.table_name, counts)
        return counts

    def read_current(self)  -> DataFrame:
        return self.spark.read.format("delta").load(self.current_path)

    def read_history(self)  -> DataFrame:
        return self.spark.read.format("delta").load(self.history_path)

    def read_mini_dim(self) -> DataFrame:
        return self.spark.read.format("delta").load(self.mini_dim_path)

    @staticmethod
    def _profile_key(profile: dict) -> str:
        raw = "|".join(f"{k}={v}" for k, v in sorted(profile.items()))
        return hashlib.md5(raw.encode()).hexdigest()[:16]
