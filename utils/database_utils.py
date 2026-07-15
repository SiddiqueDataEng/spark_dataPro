# utils/database_utils.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from config.spark_config import SparkConfig


class DatabaseUtils:
    def __init__(self):
        self.config = SparkConfig()
        self.spark: SparkSession = self.config.get_spark_session()

    # ------------------------------------------------------------------
    # PostgreSQL helpers
    # ------------------------------------------------------------------

    def load_from_postgres(self, table_name: str) -> DataFrame:
        """Load a table from PostgreSQL via JDBC."""
        return (
            self.spark.read
            .jdbc(
                url=self.config.postgres_url,
                table=table_name,
                properties=self.config.postgres_properties,
            )
        )

    def load_all_tables(self) -> dict:
        """Load all tables from PostgreSQL."""
        tables = ["customers", "products", "employees", "stores", "orders", "sales"]
        dfs = {}
        for table in tables:
            try:
                dfs[table] = self.load_from_postgres(table)
                print(f"✅ Loaded {table} - {dfs[table].count()} records")
            except Exception as e:
                print(f"❌ Error loading {table}: {e}")
        return dfs

    def write_to_postgres(self, df: DataFrame, table_name: str, mode: str = "overwrite"):
        """Write a DataFrame back to PostgreSQL."""
        df.write.jdbc(
            url=self.config.postgres_url,
            table=table_name,
            mode=mode,
            properties=self.config.postgres_properties,
        )

    # ------------------------------------------------------------------
    # Delta Lake helpers
    # ------------------------------------------------------------------

    def write_to_delta(self, df: DataFrame, path: str, mode: str = "overwrite"):
        """Write a DataFrame in Delta format."""
        (
            df.write
            .format("delta")
            .mode(mode)
            .option("mergeSchema", "true")
            .save(path)
        )

    def read_delta(self, path: str) -> DataFrame:
        """Read a Delta table from the given path."""
        return self.spark.read.format("delta").load(path)

    def read_bronze(self, table_name: str) -> DataFrame:
        """Convenience: read a table from the Bronze Delta layer."""
        import os
        bronze_path = os.path.join(os.getcwd(), "data", "bronze", table_name)
        return self.read_delta(bronze_path)
