# ml/feature_engineering.py
"""
Feature Engineering
===================
Builds a flat feature store from Silver + Gold Delta tables suitable for:
  - Customer churn classification
  - Customer lifetime value (LTV) regression
  - Product demand forecasting
  - Customer segmentation (unsupervised)

Output: pandas DataFrames returned to ml/models.py
"""

from __future__ import annotations

import os
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum, avg, count, max, min, datediff,
    current_date, when, lit, countDistinct,
    stddev, coalesce, round, lag, weekofyear,
    year as spark_year,
)
from pyspark.sql.window import Window


class FeatureEngineering:
    """Build ML feature sets from Delta Lake tables."""

    CHURN_THRESHOLD_DAYS = 90   # inactive > 90 days → churned

    def __init__(self, spark: SparkSession | None = None):
        if spark is None:
            from config.spark_config import SparkConfig
            self.spark = SparkConfig().get_spark_session()
        else:
            self.spark = spark

        self.silver_path = os.path.join(os.getcwd(), "data", "silver")
        self.gold_path   = os.path.join(os.getcwd(), "data", "gold")

    # ------------------------------------------------------------------
    # Loaders
    # ------------------------------------------------------------------

    def _load(self, layer: str, table: str):
        path = os.path.join(os.getcwd(), "data", layer, table)
        return self.spark.read.format("delta").load(path)

    # ------------------------------------------------------------------
    # 1. Customer features (churn + LTV)
    # ------------------------------------------------------------------

    def build_customer_features(self) -> pd.DataFrame:
        """
        One row per customer with behavioural features for churn & LTV models.

        Features
        --------
        recency_days, frequency, monetary, avg_order_value,
        std_order_value, max_order_value, min_order_value,
        distinct_categories, distinct_stores, avg_discount,
        order_span_days, orders_last_30d, orders_last_90d,
        preferred_category (encoded), gender (encoded),
        tenure_days, profit_contribution
        Target columns: is_churned (binary), lifetime_value (continuous)
        """
        customers = self._load("silver", "customers")
        orders    = self._load("silver", "orders")
        sales     = self._load("silver", "sales")
        products  = self._load("silver", "products")

        # Join order + sales + product
        txn = (
            orders
            .join(sales, "order_id")
            .join(products.select("product_id", "category"), "product_id")
        )

        # Recency / frequency / monetary base
        rfm = txn.groupBy("customer_id").agg(
            datediff(current_date(), max("order_date")).alias("recency_days"),
            countDistinct("order_id").alias("frequency"),
            round(sum("total"), 4).alias("monetary"),
            round(avg("total"), 4).alias("avg_order_value"),
            round(stddev("total"), 4).alias("std_order_value"),
            round(max("total"), 4).alias("max_order_value"),
            round(min("total"), 4).alias("min_order_value"),
            round(avg("discount"), 4).alias("avg_discount"),
            round(sum("profit"), 4).alias("profit_contribution"),
            countDistinct("category").alias("distinct_categories"),
            countDistinct("store_id").alias("distinct_stores"),
            datediff(max("order_date"), min("order_date")).alias("order_span_days"),
        )

        # Recent activity windows
        recent_30 = txn.filter(
            datediff(current_date(), col("order_date")) <= 30
        ).groupBy("customer_id").agg(
            countDistinct("order_id").alias("orders_last_30d")
        )
        recent_90 = txn.filter(
            datediff(current_date(), col("order_date")) <= 90
        ).groupBy("customer_id").agg(
            countDistinct("order_id").alias("orders_last_90d")
        )

        # Preferred category (most purchased)
        cat_counts = txn.groupBy("customer_id", "category").agg(
            count("*").alias("cat_count")
        )
        w = Window.partitionBy("customer_id").orderBy(col("cat_count").desc())
        preferred_cat = (
            cat_counts
            .withColumn("rn", col("cat_count") / col("cat_count"))  # placeholder rank
            .groupBy("customer_id")
            .agg(max("category").alias("preferred_category"))
        )

        # Customer demographics
        demo = customers.select(
            "customer_id",
            when(col("gender") == "Male", 1).otherwise(0).alias("gender_male"),
            datediff(current_date(), col("join_date")).alias("tenure_days"),
        )

        # Combine all
        features_spark = (
            rfm
            .join(recent_30, "customer_id", "left")
            .join(recent_90, "customer_id", "left")
            .join(preferred_cat, "customer_id", "left")
            .join(demo, "customer_id", "left")
            .fillna(0)
        )

        # Add target labels
        features_spark = features_spark.withColumn(
            "is_churned",
            when(col("recency_days") > self.CHURN_THRESHOLD_DAYS, 1).otherwise(0),
        ).withColumn(
            "lifetime_value", col("monetary")
        )

        # Encode preferred_category
        categories = ["Electronics", "Apparel", "Books", "Furniture", "Sports", "Food"]
        for cat in categories:
            features_spark = features_spark.withColumn(
                f"cat_{cat.lower()}",
                when(col("preferred_category") == cat, 1).otherwise(0),
            )
        features_spark = features_spark.drop("preferred_category")

        df = features_spark.toPandas()
        df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
        print(f"✅ Customer features: {len(df)} rows × {len(df.columns)} columns")
        return df

    # ------------------------------------------------------------------
    # 2. Product demand features (for forecasting)
    # ------------------------------------------------------------------

    def build_product_features(self) -> pd.DataFrame:
        """
        Weekly demand per product for time-series forecasting.

        Features: product_id, category, price_tier, cost_price,
                  selling_price, profit_margin, stock,
                  week_of_year, year, lag_1w, lag_2w, lag_4w,
                  rolling_mean_4w
        Target: units_sold
        """
        sales    = self._load("silver", "sales")
        orders   = self._load("silver", "orders")
        products = self._load("silver", "products")

        from pyspark.sql.functions import weekofyear, year as yr

        # Weekly units per product
        weekly = (
            sales
            .join(orders.select("order_id", "order_date"), "order_id")
            .join(products.select(
                    col("product_id"),
                    col("category"),
                    col("price_tier"),
                    col("cost_price"),
                    col("selling_price"),
                    col("profit_margin").alias("product_profit_margin"),
                    col("stock"),
                  ), "product_id")
            .groupBy(
                "product_id", "category", "price_tier",
                "cost_price", "selling_price", "product_profit_margin", "stock",
                weekofyear("order_date").alias("week"),
                yr("order_date").alias("year"),
            )
            .agg(sum("quantity").alias("units_sold"))
        )

        # Lag features per product
        w = Window.partitionBy("product_id").orderBy("year", "week")
        weekly = (
            weekly
            .withColumn("lag_1w", lag("units_sold", 1).over(w))
            .withColumn("lag_2w", lag("units_sold", 2).over(w))
            .withColumn("lag_4w", lag("units_sold", 4).over(w))
            .withColumn("rolling_mean_4w",
                avg("units_sold").over(w.rowsBetween(-3, 0)))
            .fillna(0)
        )

        # Encode category
        categories = ["Electronics", "Apparel", "Books", "Furniture", "Sports", "Food"]
        for cat in categories:
            weekly = weekly.withColumn(
                f"cat_{cat.lower()}",
                when(col("category") == cat, 1).otherwise(0),
            )

        # Encode price_tier
        tiers = ["Budget", "Mid-Range", "Premium"]
        for tier in tiers:
            weekly = weekly.withColumn(
                f"tier_{tier.replace('-','_').lower()}",
                when(col("price_tier") == tier, 1).otherwise(0),
            )

        df = weekly.drop("category", "price_tier").toPandas()
        df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
        print(f"✅ Product features: {len(df)} rows × {len(df.columns)} columns")
        return df

    # ------------------------------------------------------------------
    # 3. Store / daily features (for revenue forecasting)
    # ------------------------------------------------------------------

    def build_store_features(self) -> pd.DataFrame:
        """Daily per-store revenue for store-level demand forecasting."""
        daily = self.spark.read.format("delta").load(
            f"{self.gold_path}/daily_sales_summary"
        )

        from pyspark.sql.functions import dayofweek, dayofmonth, month as mth, year as yr

        df_spark = (
            daily
            .withColumn("dow",   dayofweek("sale_date"))
            .withColumn("dom",   dayofmonth("sale_date"))
            .withColumn("mth",   mth("sale_date"))
            .withColumn("yr",    yr("sale_date"))
        )

        # Lag features
        w = Window.partitionBy("store_name").orderBy("sale_date")
        df_spark = (
            df_spark
            .withColumn("revenue_lag1",  lag("total_revenue", 1).over(w))
            .withColumn("revenue_lag7",  lag("total_revenue", 7).over(w))
            .withColumn("revenue_ma7",
                avg("total_revenue").over(w.rowsBetween(-6, 0)))
            .fillna(0)
        )

        df = df_spark.toPandas()
        df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
        print(f"✅ Store features: {len(df)} rows × {len(df.columns)} columns")
        return df

    # ------------------------------------------------------------------
    # Run all
    # ------------------------------------------------------------------

    def build_all(self) -> dict[str, pd.DataFrame]:
        print("\n" + "═" * 60)
        print("  FEATURE ENGINEERING")
        print("═" * 60)
        return {
            "customer": self.build_customer_features(),
            "product":  self.build_product_features(),
            "store":    self.build_store_features(),
        }
