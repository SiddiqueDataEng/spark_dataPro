"""
SCD Pipeline Orchestrator
==========================
End-to-end SCD pipeline that ties together:

  1. SCDDataGenerator — generates realistic dimension changes in PostgreSQL
  2. SCD Types 0–6    — applies each strategy to the generated events
  3. Reporting        — shows before/after counts and sample data per type

Usage
-----
  python -m scd.scd_pipeline                      # run all SCD types (default)
  python -m scd.scd_pipeline --scd-type 2         # run only SCD Type-2
  python -m scd.scd_pipeline --scd-type 0,2,6     # run specific types
  python -m scd.scd_pipeline --changes 30         # custom change count
  python -m scd.scd_pipeline --table employees    # restrict to one dimension
  python -m scd.scd_pipeline --no-generate        # skip data generation
    (use existing scd_events/scd_changes.jsonl)
"""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path

log = logging.getLogger(__name__)

TABLES = ["customers", "products", "employees", "stores"]


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────

class SCDPipeline:
    """
    Orchestrates the complete SCD lifecycle for all dimension tables.

    Steps
    -----
    1. Generate dimension changes in PostgreSQL via SCDDataGenerator.
    2. Load the generated events from the JSONL change log.
    3. Apply each requested SCD type to every dimension table.
    4. Print a comparative summary table.
    """

    def __init__(self, n_changes: int = 30, tables: list[str] | None = None):
        self.n_changes = n_changes
        self.tables    = tables or TABLES
        from config.spark_config import SparkConfig
        self.spark = SparkConfig().get_spark_session()

    # ── Step 1: Generate ─────────────────────────────────────────────────────

    def step_generate(self) -> list[dict]:
        self._header("Step 1 — SCD Data Generator (PostgreSQL dimension changes)")
        from scd.scd_data_generator import SCDDataGenerator
        gen = SCDDataGenerator(n=self.n_changes, append_log=False)
        try:
            events = gen.generate()
        finally:
            gen.close()
        return events

    def step_load_events(self) -> list[dict]:
        """Load events from existing change log without generating new ones."""
        from scd.scd_data_generator import SCDDataGenerator, SCD_CHANGES_LOG
        if not SCD_CHANGES_LOG.exists():
            print("⚠️  No existing change log found. Run with --generate first.")
            return []
        gen    = SCDDataGenerator.__new__(SCDDataGenerator)
        events = gen.load_events()
        print(f"✅ Loaded {len(events)} events from {SCD_CHANGES_LOG}")
        return events

    # ── Step 2–7: Apply SCD types ─────────────────────────────────────────────

    def step_scd0(self, events: list[dict]) -> dict:
        self._header("Step 2 — SCD Type 0: Fixed / Retain Original")
        from scd.scd_types.scd_type0 import SCD0
        totals: dict = {}
        for table in self.tables:
            scd = SCD0(self.spark, table)
            counts = scd.apply(events)
            totals[table] = counts
            self._print_counts(table, counts)
            if counts.get("inserted", 0) + counts.get("updated", 0) > 0:
                print(f"  📋 Sample ({table}):")
                try:
                    scd.read().show(3, truncate=60)
                except Exception:
                    pass
        return totals

    def step_scd1(self, events: list[dict]) -> dict:
        self._header("Step 3 — SCD Type 1: Overwrite (no history)")
        from scd.scd_types.scd_type1 import SCD1
        totals: dict = {}
        for table in self.tables:
            scd = SCD1(self.spark, table)
            counts = scd.apply(events)
            totals[table] = counts
            self._print_counts(table, counts)
            if counts.get("inserted", 0) + counts.get("updated", 0) > 0:
                print(f"  📋 Sample ({table}):")
                try:
                    scd.read().show(3, truncate=60)
                except Exception:
                    pass
        return totals

    def step_scd2(self, events: list[dict]) -> dict:
        self._header("Step 4 — SCD Type 2: Add New Row (full history)")
        from scd.scd_types.scd_type2 import SCD2
        totals: dict = {}
        for table in self.tables:
            scd = SCD2(self.spark, table)
            counts = scd.apply(events)
            totals[table] = counts
            self._print_counts(table, counts)
            if counts.get("inserted", 0) > 0:
                print(f"  📋 History sample ({table}) — all versions:")
                try:
                    scd.read().select(
                        scd.pk_col, "scd_version", "scd_start_date",
                        "scd_end_date", "is_current",
                        *[c for c in scd.track_cols
                          if c in scd.read().columns][:2],
                    ).orderBy(scd.pk_col, "scd_version").show(6, truncate=40)
                except Exception:
                    pass
        return totals

    def step_scd3(self, events: list[dict]) -> dict:
        self._header("Step 5 — SCD Type 3: Add Column (prev + current value)")
        from scd.scd_types.scd_type3 import SCD3
        totals: dict = {}
        for table in self.tables:
            scd = SCD3(self.spark, table)
            counts = scd.apply(events)
            totals[table] = counts
            self._print_counts(table, counts)
            if counts.get("inserted", 0) + counts.get("updated", 0) > 0:
                print(f"  📋 Sample ({table}) — current vs previous:")
                try:
                    cols = [scd.pk_col, "scd_change_count"]
                    for c in scd.track_cols[:2]:
                        cols += [f"current_{c}", f"previous_{c}"]
                    scd.read().select(*cols).show(5, truncate=40)
                except Exception:
                    pass
        return totals

    def step_scd4(self, events: list[dict]) -> dict:
        self._header("Step 6 — SCD Type 4: History Table")
        from scd.scd_types.scd_type4 import SCD4
        totals: dict = {}
        for table in self.tables:
            scd = SCD4(self.spark, table)
            counts = scd.apply(events)
            totals[table] = counts
            self._print_counts(table, counts)
            if counts.get("history_rows", 0) > 0:
                print(f"  📋 History sample ({table}):")
                try:
                    scd.read_history().select(
                        scd.pk_col, "scd_action", "scd_recorded_at"
                    ).show(5, truncate=60)
                except Exception:
                    pass
        return totals

    def step_scd5(self, events: list[dict]) -> dict:
        self._header("Step 7 — SCD Type 5: Mini-Dimension")
        from scd.scd_types.scd_type5 import SCD5
        totals: dict = {}
        for table in self.tables:
            scd = SCD5(self.spark, table)
            counts = scd.apply(events)
            totals[table] = counts
            self._print_counts(table, counts)
            if counts.get("mini_dim_keys", 0) > 0:
                print(f"  📋 Mini-dimension ({table}):")
                try:
                    scd.read_mini_dim().show(5, truncate=60)
                except Exception:
                    pass
        return totals

    def step_scd6(self, events: list[dict]) -> dict:
        self._header("Step 8 — SCD Type 6: Hybrid (SCD1+2+3)")
        from scd.scd_types.scd_type6 import SCD6
        totals: dict = {}
        for table in self.tables:
            scd = SCD6(self.spark, table)
            counts = scd.apply(events)
            totals[table] = counts
            self._print_counts(table, counts)
            if counts.get("inserted", 0) > 0:
                print(f"  📋 Hybrid history sample ({table}):")
                try:
                    cols = [scd.pk_col, "scd_version", "is_current",
                            "scd_start_date"]
                    for c in scd.track_cols[:1]:
                        cols += [c, f"previous_{c}", f"current_{c}"]
                    scd.read().select(*cols).orderBy(scd.pk_col, "scd_version").show(6, truncate=40)
                except Exception:
                    pass
        return totals

    # ── Full run ──────────────────────────────────────────────────────────────

    def run(
        self,
        scd_types: list[int] | None = None,
        generate: bool = True,
    ) -> dict:
        """
        Execute the SCD pipeline.

        Parameters
        ----------
        scd_types : list of SCD type numbers to run (default: [0,1,2,3,4,5,6])
        generate  : if True, generate fresh changes; else use existing log
        """
        scd_types = scd_types or [0, 1, 2, 3, 4, 5, 6]
        t_start   = time.time()
        summary: dict = {}

        self._banner()

        # Generate or load events
        events = self.step_generate() if generate else self.step_load_events()
        if not events:
            print("⚠️  No events to process.")
            return {}
        summary["events_generated"] = len(events)

        # Apply selected SCD types
        dispatch = {
            0: ("scd0", self.step_scd0),
            1: ("scd1", self.step_scd1),
            2: ("scd2", self.step_scd2),
            3: ("scd3", self.step_scd3),
            4: ("scd4", self.step_scd4),
            5: ("scd5", self.step_scd5),
            6: ("scd6", self.step_scd6),
        }
        for scd_type in sorted(scd_types):
            if scd_type in dispatch:
                key, fn = dispatch[scd_type]
                summary[key] = fn(events)

        elapsed = time.time() - t_start
        self._print_summary(summary, elapsed, scd_types)
        return summary

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _header(title: str):
        print(f"\n{'─' * 65}")
        print(f"  {title}")
        print(f"{'─' * 65}")

    @staticmethod
    def _banner():
        print("\n" + "═" * 65)
        print("  SCD PIPELINE  —  Slowly Changing Dimensions (Types 0–6)")
        print("  " + "─" * 61)
        print("  Generator → Events → SCD-0/1/2/3/4/5/6 → Delta Tables")
        print("═" * 65)

    @staticmethod
    def _print_counts(table: str, counts: dict):
        parts = ", ".join(f"{k}={v}" for k, v in counts.items() if v != 0)
        if parts:
            print(f"  ✅ {table:<15}  {parts}")
        else:
            print(f"  ⬜ {table:<15}  (no matching events)")

    @staticmethod
    def _print_summary(summary: dict, elapsed: float, scd_types: list[int]):
        print("\n" + "═" * 65)
        print("  SCD PIPELINE COMPLETE")
        print("═" * 65)
        print(f"  Events processed  : {summary.get('events_generated', 0)}")
        print(f"  Elapsed time      : {elapsed:.1f}s")
        print(f"  SCD types run     : {scd_types}")
        print()

        labels = {
            "scd0": "Type 0  Fixed",
            "scd1": "Type 1  Overwrite",
            "scd2": "Type 2  Add Row",
            "scd3": "Type 3  Add Column",
            "scd4": "Type 4  History Table",
            "scd5": "Type 5  Mini-Dimension",
            "scd6": "Type 6  Hybrid 1+2+3",
        }
        for key, label in labels.items():
            if key in summary:
                results = summary[key]
                total_ops = sum(
                    v for table_counts in results.values()
                    for k, v in table_counts.items()
                    if k not in ("errors",)
                )
                total_err = sum(
                    c.get("errors", 0) for c in results.values()
                )
                status = "✅" if total_err == 0 else "⚠️ "
                print(f"  {status} {label:<24}  ops={total_ops:>5}  errors={total_err}")
        print("═" * 65 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="SCD Pipeline — Slowly Changing Dimensions (Types 0–6)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
SCD Types
---------
  0  Fixed / Retain Original
  1  Overwrite (no history)
  2  Add New Row (full history)
  3  Add Column (prev + current value)
  4  History Table (current + archive)
  5  Mini-Dimension (SCD4 + profile lookup)
  6  Hybrid SCD1+2+3 (retroactive current_X)

Examples
--------
  python -m scd.scd_pipeline                     # all types, 30 changes
  python -m scd.scd_pipeline --scd-type 2        # only SCD-2
  python -m scd.scd_pipeline --scd-type 0,2,6   # specific types
  python -m scd.scd_pipeline --changes 50        # 50 changes
  python -m scd.scd_pipeline --table employees   # single dimension
  python -m scd.scd_pipeline --no-generate       # reuse last event log
        """,
    )
    parser.add_argument(
        "--scd-type",
        type=str,
        default="all",
        help="SCD types to run: 'all' or comma-separated (e.g. '0,2,6')",
    )
    parser.add_argument("--changes", type=int, default=30,
                        help="Number of dimension changes to generate")
    parser.add_argument("--table", type=str, default=None,
                        help="Restrict to a single dimension table")
    parser.add_argument("--no-generate", action="store_true",
                        help="Skip generation, use existing change log")
    args = parser.parse_args()

    if args.scd_type == "all":
        types = [0, 1, 2, 3, 4, 5, 6]
    else:
        types = [int(t.strip()) for t in args.scd_type.split(",")]

    tables = [args.table] if args.table else None

    pipeline = SCDPipeline(n_changes=args.changes, tables=tables)
    pipeline.run(scd_types=types, generate=not args.no_generate)
