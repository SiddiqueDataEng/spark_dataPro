# ml/ml_pipeline.py
"""
ML Pipeline Orchestrator
========================
Runs the full machine learning workflow:

  1. Feature Engineering  — build feature DataFrames from Delta tables
  2. Churn Classifier     — predict customer churn (binary)
  3. LTV Regressor        — predict customer lifetime value
  4. Demand Forecaster    — predict weekly product demand
  5. Segmentation         — cluster customers into behavioural groups

Results are logged to MLflow (./mlruns/) and printed to stdout.

Usage
-----
  python -m ml.ml_pipeline          # standalone
  from ml.ml_pipeline import MLPipeline; MLPipeline().run()
"""

from __future__ import annotations

import os
import time
import logging
from pyspark.sql import SparkSession

from ml.feature_engineering import FeatureEngineering
from ml.models import (
    train_churn_model,
    train_ltv_model,
    train_demand_model,
    train_segmentation_model,
)

os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")
log = logging.getLogger(__name__)


class MLPipeline:
    """Orchestrates feature engineering and model training."""

    def __init__(self, spark: SparkSession | None = None):
        if spark is None:
            from config.spark_config import SparkConfig
            self.spark = SparkConfig().get_spark_session()
        else:
            self.spark = spark

    # ------------------------------------------------------------------

    def run(self) -> dict[str, dict]:
        """Run all ML steps and return a dict of {model_name: metrics}."""
        total_start = time.time()

        print("\n" + "═" * 70)
        print("  ML PIPELINE")
        print("═" * 70)

        # ── Feature Engineering ──────────────────────────────────────────
        fe = FeatureEngineering(spark=self.spark)
        features = fe.build_all()

        customer_df = features["customer"]
        product_df  = features["product"]

        results: dict[str, dict] = {}

        # ── 1. Churn Classifier ──────────────────────────────────────────
        t0 = time.time()
        try:
            results["churn"] = train_churn_model(customer_df)
            log.info("Churn model trained in %.1fs", time.time() - t0)
        except Exception as e:
            log.error("Churn model failed: %s", e)
            print(f"⚠️  Churn model error: {e}")

        # ── 2. LTV Regressor ─────────────────────────────────────────────
        t0 = time.time()
        try:
            results["ltv"] = train_ltv_model(customer_df)
            log.info("LTV model trained in %.1fs", time.time() - t0)
        except Exception as e:
            log.error("LTV model failed: %s", e)
            print(f"⚠️  LTV model error: {e}")

        # ── 3. Demand Forecaster ──────────────────────────────────────────
        t0 = time.time()
        try:
            results["demand"] = train_demand_model(product_df)
            log.info("Demand model trained in %.1fs", time.time() - t0)
        except Exception as e:
            log.error("Demand model failed: %s", e)
            print(f"⚠️  Demand model error: {e}")

        # ── 4. Customer Segmentation ──────────────────────────────────────
        t0 = time.time()
        try:
            results["segmentation"] = train_segmentation_model(customer_df)
            log.info("Segmentation trained in %.1fs", time.time() - t0)
        except Exception as e:
            log.error("Segmentation failed: %s", e)
            print(f"⚠️  Segmentation error: {e}")

        # ── Summary ───────────────────────────────────────────────────────
        total = time.time() - total_start
        print("\n" + "═" * 70)
        print(f"  ML PIPELINE COMPLETE  ({total:.1f}s)")
        print("═" * 70)
        for model, metrics in results.items():
            print(f"  {model:15s}  {metrics}")

        print("\n  MLflow UI: run  mlflow ui --backend-store-uri ./mlruns")
        print("             then open http://localhost:5000\n")

        return results


# ── Standalone entry point ────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s - %(levelname)s - %(message)s")
    pipeline = MLPipeline()
    pipeline.run()
    pipeline.spark.stop()
