"""
SCD Utilities
=============
Shared helpers used by all SCD type implementations.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Tuple

from pyspark.sql import SparkSession, DataFrame

log = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_STAGING_DIR  = _PROJECT_ROOT / "data" / "scd" / "_staging"


def rows_to_df(spark: SparkSession, rows: list[dict]) -> Tuple[DataFrame, Path]:
    """
    Convert a list of dicts to a Spark DataFrame WITHOUT spawning Python workers.

    Writes rows to a stable JSONL staging file in the project directory, then
    reads back via spark.read.json() (pure JVM, no Python serialisation workers).

    Returns (DataFrame, staging_file_path).  The caller MUST keep the file alive
    until any Spark action that consumes the DataFrame completes — Delta Lake 4.x
    materialises (checkpoints) the source RDD and re-reads the file mid-merge.
    After the action completes, call  staging_path.unlink(missing_ok=True).
    """
    _STAGING_DIR.mkdir(parents=True, exist_ok=True)
    ts   = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    path = _STAGING_DIR / f"scd_stage_{ts}.jsonl"

    with open(path, "w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, default=str) + "\n")

    df = spark.read.json(str(path))
    return df, path


def ensure_delta_table(df: DataFrame, delta_path: str):
    """
    Bootstrap a Delta table if it doesn't exist yet.

    Writes the DataFrame with overwrite so the schema is established on first
    run.  Subsequent calls (when the path already has data) are a no-op.
    """
    p = Path(delta_path)
    if p.exists() and any(p.iterdir()):
        return  # already initialised
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")
       .save(delta_path))
    log.info("Bootstrapped Delta table at %s", delta_path)
