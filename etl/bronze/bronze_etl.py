# etl/bronze/bronze_etl.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, sum
from pyspark.sql.types import *
from utils.database_utils import DatabaseUtils
import os


class BronzeETL:
    """Bronze Layer - Raw Data Ingestion and Validation"""

    def __init__(self):
        self.db_utils = DatabaseUtils()
        self.spark = self.db_utils.spark
        self.bronze_path = os.path.join(os.getcwd(), "data", "bronze")

    # ------------------------------------------------------------------
    # Ingestion helpers
    # ------------------------------------------------------------------

    def _add_audit_columns(self, df: DataFrame) -> DataFrame:
        return (
            df.withColumn("ingestion_timestamp", current_timestamp())
              .withColumn("source_system", lit("postgresql"))
              .withColumn("data_version", lit(1))
        )

    def ingest_customers(self) -> DataFrame:
        """Ingest raw customer data."""
        df = self.db_utils.load_from_postgres("customers")
        df = self._add_audit_columns(df)
        df = df.filter(
            col("customer_id").isNotNull() &
            col("first_name").isNotNull() &
            col("email").isNotNull()
        )
        self.db_utils.write_to_delta(df, f"{self.bronze_path}/customers")
        return df

    def ingest_products(self) -> DataFrame:
        """Ingest raw product data."""
        df = self.db_utils.load_from_postgres("products")
        df = self._add_audit_columns(df)
        df = df.filter(
            col("product_id").isNotNull() &
            col("product_name").isNotNull()
        )
        self.db_utils.write_to_delta(df, f"{self.bronze_path}/products")
        return df

    def ingest_employees(self) -> DataFrame:
        """Ingest raw employee data."""
        df = self.db_utils.load_from_postgres("employees")
        df = self._add_audit_columns(df)
        df = df.filter(col("employee_id").isNotNull())
        self.db_utils.write_to_delta(df, f"{self.bronze_path}/employees")
        return df

    def ingest_stores(self) -> DataFrame:
        """Ingest raw store data."""
        df = self.db_utils.load_from_postgres("stores")
        df = self._add_audit_columns(df)
        df = df.filter(col("store_id").isNotNull())
        self.db_utils.write_to_delta(df, f"{self.bronze_path}/stores")
        return df

    def ingest_orders(self) -> DataFrame:
        """Ingest raw order data."""
        df = self.db_utils.load_from_postgres("orders")
        df = self._add_audit_columns(df)
        df = df.filter(
            col("order_id").isNotNull() &
            col("order_date").isNotNull()
        )
        self.db_utils.write_to_delta(df, f"{self.bronze_path}/orders")
        return df

    def ingest_sales(self) -> DataFrame:
        """Ingest raw sales data.

        Bug fixed: original code had operator-precedence issues mixing
        bitwise & with comparisons. Each condition now wrapped correctly.
        """
        df = self.db_utils.load_from_postgres("sales")
        df = self._add_audit_columns(df)
        df = df.filter(
            col("sale_id").isNotNull() &
            col("order_id").isNotNull() &
            col("product_id").isNotNull() &
            (col("quantity") > 0) &
            (col("unit_price") > 0)
        )
        self.db_utils.write_to_delta(df, f"{self.bronze_path}/sales")
        return df

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def ingest_all(self) -> dict:
        """Ingest all tables into the Bronze layer."""
        print("🔄 Starting Bronze Layer ingestion...")

        dataframes = {
            "customers": self.ingest_customers(),
            "products":  self.ingest_products(),
            "employees": self.ingest_employees(),
            "stores":    self.ingest_stores(),
            "orders":    self.ingest_orders(),
            "sales":     self.ingest_sales(),
        }

        print("✅ Bronze Layer ingestion complete!")
        return dataframes

    def validate_data_quality(self, df: DataFrame, table_name: str) -> dict:
        """Return basic data-quality metrics for a DataFrame."""
        metrics = {
            "total_records": df.count(),
            "null_count": df.select(
                [sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]
            ).collect()[0].asDict(),
            "duplicates": df.groupBy(df.columns).count().filter(col("count") > 1).count(),
            "schema": df.schema.simpleString(),
        }

        print(f"📊 Data Quality Metrics for {table_name}:")
        for key, value in metrics.items():
            print(f"  {key}: {value}")

        return metrics
