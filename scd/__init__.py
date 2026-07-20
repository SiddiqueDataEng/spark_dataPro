"""
Slowly Changing Dimensions (SCD) — Package
===========================================
Implements all major SCD types (0 through 6) on top of the Delta Lake
Medallion architecture.

SCD Types implemented
---------------------
  SCD Type 0  — Fixed / Retain Original
  SCD Type 1  — Overwrite (no history)
  SCD Type 2  — Add New Row (full history)
  SCD Type 3  — Add New Column (limited history: previous value only)
  SCD Type 4  — History Table (current + separate history table)
  SCD Type 5  — SCD5 = SCD4 + SCD1 (mini-dimension + current snapshot)
  SCD Type 6  — Hybrid SCD1 + SCD2 + SCD3 (surrogate key + current flag
                + previous value column)

Usage
-----
  python -m scd.scd_pipeline --scd-type all
  python -m scd.scd_pipeline --scd-type 2
  python -m scd.scd_pipeline --changes 30
"""

from scd.scd_pipeline import SCDPipeline

__all__ = ["SCDPipeline"]
