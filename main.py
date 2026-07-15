# main.py
"""
Medallion ETL Pipeline — Standalone Runner
==========================================
Runs the full pipeline in order:
  Bronze → Silver → Gold → Core Analytics → Extended Analytics → ML

Usage:
  JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 python main.py [--layer LAYER]

  LAYER options: bronze | silver | gold | analytics | ml | all (default: all)
"""

import os
import sys
import time
import logging
import argparse

os.environ.setdefault("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline controller
# ─────────────────────────────────────────────────────────────────────────────

class ETLPipeline:
    """Main ETL + Analytics + ML Pipeline Controller."""

    def __init__(self):
        from config.spark_config import SparkConfig
        self.spark = SparkConfig().get_spark_session()

        from etl.bronze.bronze_etl      import BronzeETL
        from etl.silver.silver_etl      import SilverETL
        from etl.gold.gold_etl          import GoldETL
        from analysis.business_analytics  import BusinessAnalytics
        from analysis.extended_analytics  import ExtendedAnalytics
        from ml.ml_pipeline              import MLPipeline

        self.bronze    = BronzeETL()
        self.silver    = SilverETL()
        self.gold      = GoldETL()
        self.analytics = BusinessAnalytics(self.spark)
        self.extended  = ExtendedAnalytics(self.spark)
        self.ml        = MLPipeline(self.spark)

    # ── Layer runners ─────────────────────────────────────────────────────────

    def run_bronze_layer(self):
        logger.info("🚀 Starting Bronze Layer ETL…")
        t = time.time()
        try:
            data = self.bronze.ingest_all()
            logger.info("✅ Bronze Layer completed in %.2fs", time.time() - t)
            return data
        except Exception as e:
            logger.error("❌ Bronze Layer failed: %s", e)
            raise

    def run_silver_layer(self):
        logger.info("🚀 Starting Silver Layer ETL…")
        t = time.time()
        try:
            data = self.silver.process_all()
            logger.info("✅ Silver Layer completed in %.2fs", time.time() - t)
            return data
        except Exception as e:
            logger.error("❌ Silver Layer failed: %s", e)
            raise

    def run_gold_layer(self):
        logger.info("🚀 Starting Gold Layer ETL…")
        t = time.time()
        try:
            data = self.gold.process_all()
            logger.info("✅ Gold Layer completed in %.2fs", time.time() - t)
            return data
        except Exception as e:
            logger.error("❌ Gold Layer failed: %s", e)
            raise

    def run_analytics(self):
        logger.info("📊 Running Core + Extended Business Analytics…")
        t = time.time()
        try:
            # Core analytics
            self.analytics.register_views()
            core = {
                "basic":   self.analytics.run_sales_analytics(),
                "complex": self.analytics.run_complex_analytics(),
                "advanced":self.analytics.run_advanced_analytics(),
            }

            # Show key results
            logger.info("Top 10 Customers by Lifetime Value:")
            core["basic"]["top_customers"].show(10, truncate=False)
            logger.info("Product Performance Ranking:")
            core["basic"]["product_ranking"].show(10, truncate=False)
            logger.info("RFM Analysis Sample:")
            core["complex"]["rfm_analysis"].show(5, truncate=False)

            # Extended analytics
            self.extended.register_views()
            ext = self.extended.run_all()

            logger.info("✅ Analytics completed in %.2fs", time.time() - t)
            return {"core": core, "extended": ext}
        except Exception as e:
            logger.error("❌ Analytics failed: %s", e)
            raise

    def run_ml(self):
        logger.info("🤖 Running ML Pipeline…")
        t = time.time()
        try:
            results = self.ml.run()
            logger.info("✅ ML Pipeline completed in %.2fs", time.time() - t)
            return results
        except Exception as e:
            logger.error("❌ ML Pipeline failed: %s", e)
            raise

    # ── Full pipeline ─────────────────────────────────────────────────────────

    def run_full_pipeline(self):
        logger.info("=" * 70)
        logger.info("🚀 Starting Full Medallion ETL + Analytics + ML Pipeline")
        logger.info("=" * 70)
        t = time.time()

        bronze    = self.run_bronze_layer()
        silver    = self.run_silver_layer()
        gold      = self.run_gold_layer()
        analytics = self.run_analytics()
        ml        = self.run_ml()

        logger.info("=" * 70)
        logger.info("✅ Full Pipeline completed in %.2fs", time.time() - t)
        logger.info("=" * 70)

        return {
            "bronze": bronze, "silver": silver, "gold": gold,
            "analytics": analytics, "ml": ml,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Medallion ETL Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "analytics", "ml", "all"],
        default="all",
        help="Which layer to run (default: all)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    pipeline = ETLPipeline()

    try:
        if args.layer == "all":
            pipeline.run_full_pipeline()
        elif args.layer == "bronze":
            pipeline.run_bronze_layer()
        elif args.layer == "silver":
            pipeline.run_silver_layer()
        elif args.layer == "gold":
            pipeline.run_gold_layer()
        elif args.layer == "analytics":
            pipeline.run_analytics()
        elif args.layer == "ml":
            pipeline.run_ml()
    finally:
        pipeline.spark.stop()


if __name__ == "__main__":
    main()
