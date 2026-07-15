# config/spark_config.py
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os
from dotenv import load_dotenv

load_dotenv()

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_JDBC_JAR     = os.path.join(_PROJECT_ROOT, "jars", "postgresql-42.7.4.jar")

# Delta jars — pre-downloaded to ~/.ivy2.5.2/jars by the first run.
# Pointing directly avoids the Ivy round-trip that can cause session drift
# in Jupyter notebooks.
_IVY_DIR = os.path.join(os.path.expanduser("~"), ".ivy2.5.2", "jars")
_DELTA_JARS = [
    os.path.join(_IVY_DIR, "io.delta_delta-spark_4.1_2.13-4.1.0.jar"),
    os.path.join(_IVY_DIR, "io.delta_delta-storage-4.1.0.jar"),
    os.path.join(_IVY_DIR, "io.unitycatalog_unitycatalog-client-0.4.0.jar"),
]
_ALL_JARS = ",".join([_JDBC_JAR] + [j for j in _DELTA_JARS if os.path.exists(j)])


def _build_session() -> SparkSession:
    """Build and return a new SparkSession with Delta + JDBC configured."""
    return (
        SparkSession.builder
        .appName("Medallion_ETL")
        .master("local[*]")
        .config("spark.jars", _ALL_JARS)
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.autoBroadcastJoinThreshold", "104857600")
        .config("spark.sql.broadcastTimeout", "300")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


class SparkConfig:
    def __init__(self):
        self.app_name = "Medallion_ETL"
        self.master   = "local[*]"
        self.postgres_url = (
            f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}"
            f"/{os.getenv('DB_NAME')}?sslmode=require"
        )
        self.postgres_properties = {
            "user":       os.getenv("DB_USER"),
            "password":   os.getenv("DB_PASSWORD"),
            "driver":     "org.postgresql.Driver",
            "ssl":        "true",
            "sslfactory": "org.postgresql.ssl.NonValidatingFactory",
        }

    def get_spark_session(self) -> SparkSession:
        """Return the active SparkSession, creating one if needed."""
        existing = SparkSession.getActiveSession()
        if existing is not None:
            return existing
        return _build_session()

    def get_spark_sql_context(self) -> SparkSession:
        spark = self.get_spark_session()
        spark.sql("SET spark.sql.crossJoin.enabled=true")
        spark.sql("SET spark.sql.adaptive.enabled=true")
        return spark
