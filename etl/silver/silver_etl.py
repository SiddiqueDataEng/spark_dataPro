# etl/silver/silver_etl.py
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, lower, initcap, when, lit, current_timestamp,
    row_number, round, year, month, dayofmonth, quarter, dayofweek,
    weekofyear, create_map,
)
from pyspark.sql.types import *
from pyspark.sql.window import Window
from utils.database_utils import DatabaseUtils
import os


class SilverETL:
    """Silver Layer - Data Cleansing, Normalization, and Deduplication"""

    def __init__(self):
        self.db_utils = DatabaseUtils()
        self.spark = self.db_utils.spark
        self.bronze_path = os.path.join(os.getcwd(), "data", "bronze")
        self.silver_path = os.path.join(os.getcwd(), "data", "silver")

    def read_bronze(self, table_name: str) -> DataFrame:
        """Read data from the Bronze layer."""
        return self.db_utils.read_delta(f"{self.bronze_path}/{table_name}")

    # ------------------------------------------------------------------
    # Per-table cleaning
    # ------------------------------------------------------------------

    def clean_customers(self) -> DataFrame:
        """Deduplicate and clean customer records."""
        df = self.read_bronze("customers")

        # Remove duplicates – keep the lowest customer_id per email
        window = Window.partitionBy("email").orderBy(col("customer_id").asc())
        df = (
            df.withColumn("row_num", row_number().over(window))
              .filter(col("row_num") == 1)
              .drop("row_num")
        )

        # Fix: all withColumn calls are separate (original had unbalanced parens)
        df = df.withColumn("first_name", trim(upper(col("first_name"))))
        df = df.withColumn("last_name",  trim(upper(col("last_name"))))
        df = df.withColumn("email",      lower(trim(col("email"))))
        df = df.withColumn("city",       initcap(trim(col("city"))))
        df = df.withColumn("country",    initcap(trim(col("country"))))

        # Validate gender
        df = df.withColumn(
            "gender",
            when(col("gender").isin("Male", "Female"), col("gender")).otherwise("Unknown"),
        )

        # Email validity flag
        df = df.withColumn(
            "email_valid",
            col("email").rlike(r"^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$"),
        )

        df = df.withColumn("processed_timestamp", current_timestamp())
        self.db_utils.write_to_delta(df, f"{self.silver_path}/customers")
        return df

    def clean_products(self) -> DataFrame:
        """Clean and normalize product records."""
        df = self.read_bronze("products")

        # Fix: create_map expects interleaved key/value literals
        category_map = create_map(
            lit("Electronics"), lit("Electronics"),
            lit("Clothing"),    lit("Apparel"),
            lit("Books"),       lit("Books"),
            lit("Furniture"),   lit("Furniture"),
            lit("Sports"),      lit("Sports"),
            lit("Food"),        lit("Food"),
        )

        df = df.withColumn(
            "category",
            when(col("category").isNotNull(), category_map[col("category")]).otherwise("Unknown"),
        )

        df = df.withColumn(
            "profit_margin",
            round((col("selling_price") - col("cost_price")) / col("selling_price") * 100, 2),
        )
        df = df.withColumn("stock_value", col("stock") * col("cost_price"))
        df = df.withColumn(
            "price_tier",
            when(col("selling_price") < 100,  "Budget")
            .when(col("selling_price") < 300, "Mid-Range")
            .otherwise("Premium"),
        )

        df = df.withColumn("processed_timestamp", current_timestamp())
        self.db_utils.write_to_delta(df, f"{self.silver_path}/products")
        return df

    def clean_employees(self) -> DataFrame:
        """Clean employee data (reads from Bronze if available, else Postgres)."""
        try:
            df = self.read_bronze("employees")
        except Exception:
            df = self.db_utils.load_from_postgres("employees")

        df = df.withColumn(
            "salary_valid",
            when(col("salary").between(20000, 200000), True).otherwise(False),
        )
        df = df.withColumn(
            "salary_tier",
            when(col("salary") < 50000,  "Entry")
            .when(col("salary") < 80000, "Mid")
            .when(col("salary") < 120000, "Senior")
            .otherwise("Executive"),
        )
        df = df.withColumn("processed_timestamp", current_timestamp())
        self.db_utils.write_to_delta(df, f"{self.silver_path}/employees")
        return df

    def clean_stores(self) -> DataFrame:
        """Clean store data (reads from Bronze if available, else Postgres)."""
        try:
            df = self.read_bronze("stores")
        except Exception:
            df = self.db_utils.load_from_postgres("stores")

        df = df.withColumn("city",    initcap(trim(col("city"))))
        df = df.withColumn("country", initcap(trim(col("country"))))

        # Fix: create_map with interleaved key/value literals
        region_map = create_map(
            lit("Usa"),       lit("North America"),
            lit("Canada"),    lit("North America"),
            lit("Uk"),        lit("Europe"),
            lit("Germany"),   lit("Europe"),
            lit("France"),    lit("Europe"),
            lit("Australia"), lit("Oceania"),
            lit("Japan"),     lit("Asia"),
        )

        df = df.withColumn(
            "region",
            when(
                col("country").isin("Usa", "Canada", "Uk", "Germany", "France", "Australia", "Japan"),
                region_map[col("country")],
            ).otherwise("Other"),
        )

        df = df.withColumn("processed_timestamp", current_timestamp())
        self.db_utils.write_to_delta(df, f"{self.silver_path}/stores")
        return df

    def clean_orders(self) -> DataFrame:
        """Clean and enrich order records with date dimensions."""
        df = self.read_bronze("orders")

        valid_statuses = ["Completed", "Pending", "Cancelled", "Shipped"]
        df = df.withColumn(
            "status",
            when(col("status").isin(valid_statuses), col("status")).otherwise("Unknown"),
        )

        df = df.withColumn("order_year",    year(col("order_date")))
        df = df.withColumn("order_month",   month(col("order_date")))
        df = df.withColumn("order_day",     dayofmonth(col("order_date")))
        df = df.withColumn("order_quarter", quarter(col("order_date")))
        df = df.withColumn("order_weekday", dayofweek(col("order_date")))
        df = df.withColumn("order_week",    weekofyear(col("order_date")))

        df = df.withColumn("processed_timestamp", current_timestamp())
        self.db_utils.write_to_delta(df, f"{self.silver_path}/orders")
        return df

    def clean_sales(self) -> DataFrame:
        """Clean and enrich sales records."""
        df = self.read_bronze("sales")

        df = df.withColumn(
            "total_valid",
            col("total") == (col("quantity") * col("unit_price") * (1 - col("discount") / 100)),
        )
        df = df.withColumn("profit_valid", col("profit") >= 0)

        df = df.withColumn(
            "discount_amount",
            col("unit_price") * col("quantity") * (col("discount") / 100),
        )
        df = df.withColumn("revenue_per_unit", col("total") / col("quantity"))
        df = df.withColumn(
            "profit_margin",
            round((col("profit") / col("total")) * 100, 2),
        )
        df = df.withColumn(
            "sales_tier",
            when(col("total") < 100,  "Small")
            .when(col("total") < 500, "Medium")
            .otherwise("Large"),
        )

        df = df.withColumn("processed_timestamp", current_timestamp())
        self.db_utils.write_to_delta(df, f"{self.silver_path}/sales")
        return df

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------

    def process_all(self) -> dict:
        """Process all tables through the Silver layer."""
        print("🔄 Starting Silver Layer processing...")

        dataframes = {
            "customers": self.clean_customers(),
            "products":  self.clean_products(),
            "employees": self.clean_employees(),
            "stores":    self.clean_stores(),
            "orders":    self.clean_orders(),
            "sales":     self.clean_sales(),
        }

        print("✅ Silver Layer processing complete!")
        return dataframes
