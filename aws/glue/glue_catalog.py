# aws/glue/glue_catalog.py
"""
GlueCatalog
===========
Manages the AWS Glue Data Catalog: databases, crawlers, and table definitions.

Responsibilities:
  - Create three Glue databases (retail_bronze, retail_silver, retail_gold)
  - Register S3 bucket paths as Glue Data Catalog tables
  - Create and run Glue Crawlers that auto-discover schema from Parquet files
  - Define column-level table schemas for the 6 retail tables across 3 layers

Usage:
    from aws.glue.glue_catalog import GlueCatalog
    catalog = GlueCatalog()
    catalog.create_databases()
    catalog.create_crawlers()
    catalog.run_crawlers()          # discovers schema from S3 Parquet
    catalog.create_table_definitions()  # explicit DDL (faster than crawling)
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from aws.config.aws_config import AWSConfig, SOURCE_TABLES

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─────────────────────────────────────────────────────────────────────────────
# Glue column type definitions per table
# (mirrors the Delta Lake schema from etl/silver/silver_etl.py)
# ─────────────────────────────────────────────────────────────────────────────

GLUE_SCHEMAS: dict[str, list[dict]] = {
    "customers": [
        {"Name": "customer_id",   "Type": "int"},
        {"Name": "first_name",    "Type": "string"},
        {"Name": "last_name",     "Type": "string"},
        {"Name": "email",         "Type": "string"},
        {"Name": "phone",         "Type": "string"},
        {"Name": "city",          "Type": "string"},
        {"Name": "country",       "Type": "string"},
        {"Name": "gender",        "Type": "string"},
        {"Name": "join_date",     "Type": "date"},
        {"Name": "updated_at",    "Type": "timestamp"},
    ],
    "products": [
        {"Name": "product_id",    "Type": "int"},
        {"Name": "product_name",  "Type": "string"},
        {"Name": "category",      "Type": "string"},
        {"Name": "selling_price", "Type": "double"},
        {"Name": "cost_price",    "Type": "double"},
        {"Name": "profit_margin", "Type": "double"},
        {"Name": "stock",         "Type": "int"},
        {"Name": "updated_at",    "Type": "timestamp"},
    ],
    "employees": [
        {"Name": "employee_id",   "Type": "int"},
        {"Name": "first_name",    "Type": "string"},
        {"Name": "last_name",     "Type": "string"},
        {"Name": "department",    "Type": "string"},
        {"Name": "salary",        "Type": "double"},
        {"Name": "hire_date",     "Type": "date"},
        {"Name": "store_id",      "Type": "int"},
        {"Name": "updated_at",    "Type": "timestamp"},
    ],
    "stores": [
        {"Name": "store_id",      "Type": "int"},
        {"Name": "store_name",    "Type": "string"},
        {"Name": "city",          "Type": "string"},
        {"Name": "country",       "Type": "string"},
        {"Name": "region",        "Type": "string"},
        {"Name": "updated_at",    "Type": "timestamp"},
    ],
    "orders": [
        {"Name": "order_id",      "Type": "int"},
        {"Name": "customer_id",   "Type": "int"},
        {"Name": "store_id",      "Type": "int"},
        {"Name": "employee_id",   "Type": "int"},
        {"Name": "order_date",    "Type": "date"},
        {"Name": "status",        "Type": "string"},
        {"Name": "discount_pct",  "Type": "double"},
        {"Name": "updated_at",    "Type": "timestamp"},
    ],
    "sales": [
        {"Name": "sale_id",       "Type": "int"},
        {"Name": "order_id",      "Type": "int"},
        {"Name": "product_id",    "Type": "int"},
        {"Name": "quantity",      "Type": "int"},
        {"Name": "unit_price",    "Type": "double"},
        {"Name": "total_revenue", "Type": "double"},
        {"Name": "profit",        "Type": "double"},
        {"Name": "updated_at",    "Type": "timestamp"},
    ],
}


class GlueCatalog:
    """Manages the Glue Data Catalog for the retail data lake."""

    LAYERS = {
        "bronze": "glue_db_raw",
        "silver": "glue_db_clean",
        "gold":   "glue_db_curated",
    }

    def __init__(self, cfg: Optional[AWSConfig] = None) -> None:
        self.cfg   = cfg or AWSConfig()
        self.glue  = self.cfg.glue_client()

    # ──────────────────────────────────────────────────────────────────────────
    # Databases
    # ──────────────────────────────────────────────────────────────────────────

    def create_databases(self) -> None:
        """Create Glue databases for each medallion layer."""
        databases = {
            self.cfg.glue_db_raw:     ("Bronze raw ingestion layer",     self.cfg.bucket_raw),
            self.cfg.glue_db_clean:   ("Silver cleaned / enriched layer", self.cfg.bucket_clean),
            self.cfg.glue_db_curated: ("Gold business aggregations",      self.cfg.bucket_curated),
            self.cfg.glue_db_cdc:     ("CDC changelog tables",            self.cfg.bucket_dms),
        }
        for db_name, (description, location_bucket) in databases.items():
            try:
                self.glue.create_database(
                    DatabaseInput={
                        "Name":        db_name,
                        "Description": description,
                        "LocationUri": f"s3://{location_bucket}/",
                        "Parameters":  {
                            "classification": "parquet",
                            "project":        "retail-medallion",
                        },
                    }
                )
                log.info("Created Glue database: %s", db_name)
            except self.glue.exceptions.AlreadyExistsException:
                log.info("Glue database already exists: %s", db_name)

    # ──────────────────────────────────────────────────────────────────────────
    # Crawlers
    # ──────────────────────────────────────────────────────────────────────────

    def create_crawlers(self) -> None:
        """Create one Glue Crawler per medallion layer."""
        crawler_configs = {
            "retail-bronze-crawler": {
                "db":      self.cfg.glue_db_raw,
                "paths":   [f"s3://{self.cfg.bucket_raw}/bronze/"],
                "desc":    "Crawl raw Parquet files in Bronze S3 layer",
            },
            "retail-silver-crawler": {
                "db":      self.cfg.glue_db_clean,
                "paths":   [f"s3://{self.cfg.bucket_clean}/silver/"],
                "desc":    "Crawl cleaned Parquet files in Silver S3 layer",
            },
            "retail-gold-crawler": {
                "db":      self.cfg.glue_db_curated,
                "paths":   [f"s3://{self.cfg.bucket_curated}/gold/"],
                "desc":    "Crawl aggregated Parquet files in Gold S3 layer",
            },
            "retail-cdc-crawler": {
                "db":      self.cfg.glue_db_cdc,
                "paths":   [f"s3://{self.cfg.bucket_dms}/cdc-events/"],
                "desc":    "Crawl CDC JSONL event files",
            },
        }

        for crawler_name, config in crawler_configs.items():
            targets = {
                "S3Targets": [{"Path": p} for p in config["paths"]]
            }
            try:
                self.glue.create_crawler(
                    Name=crawler_name,
                    Role=self.cfg.glue_role_arn,
                    DatabaseName=config["db"],
                    Description=config["desc"],
                    Targets=targets,
                    SchemaChangePolicy={
                        "UpdateBehavior": "UPDATE_IN_DATABASE",
                        "DeleteBehavior": "LOG",
                    },
                    RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
                    LineageConfiguration={"CrawlerLineageSettings": "ENABLE"},
                    Configuration="""
                    {
                      "Version": 1.0,
                      "CrawlerOutput": {
                        "Partitions": {"AddOrUpdateBehavior": "InheritFromTable"},
                        "Tables": {"AddOrUpdateBehavior": "MergeNewColumns"}
                      },
                      "Grouping": {
                        "TableGroupingPolicy": "CombineCompatibleSchemas"
                      }
                    }""",
                )
                log.info("Created crawler: %s", crawler_name)
            except self.glue.exceptions.AlreadyExistsException:
                log.info("Crawler already exists: %s", crawler_name)

    def run_crawlers(self, wait: bool = True) -> None:
        """Start all retail crawlers and optionally wait for completion."""
        crawlers = [
            "retail-bronze-crawler",
            "retail-silver-crawler",
            "retail-gold-crawler",
            "retail-cdc-crawler",
        ]
        for name in crawlers:
            try:
                self.glue.start_crawler(Name=name)
                log.info("Started crawler: %s", name)
            except self.glue.exceptions.CrawlerRunningException:
                log.info("Crawler already running: %s", name)

        if wait:
            self._wait_for_crawlers(crawlers)

    def _wait_for_crawlers(self, crawler_names: list[str], timeout: int = 600) -> None:
        """Poll until all crawlers reach READY state or timeout."""
        deadline = time.time() + timeout
        remaining = set(crawler_names)

        while remaining and time.time() < deadline:
            for name in list(remaining):
                resp  = self.glue.get_crawler(Name=name)
                state = resp["Crawler"]["State"]
                if state == "READY":
                    log.info("Crawler finished: %s", name)
                    remaining.discard(name)
            if remaining:
                time.sleep(15)

        if remaining:
            log.warning("Timed out waiting for crawlers: %s", remaining)

    # ──────────────────────────────────────────────────────────────────────────
    # Explicit table definitions (faster than crawling; used for known schemas)
    # ──────────────────────────────────────────────────────────────────────────

    def create_table_definitions(self) -> None:
        """Register all 6 retail tables in each of the 3 Glue databases."""
        for layer, db_attr in self.LAYERS.items():
            db_name = getattr(self.cfg, db_attr)
            bucket_attr = {"bronze": "bucket_raw",
                           "silver": "bucket_clean",
                           "gold":   "bucket_curated"}[layer]
            bucket = getattr(self.cfg, bucket_attr)

            for table in SOURCE_TABLES:
                columns = GLUE_SCHEMAS[table]
                s3_location = f"s3://{bucket}/{layer}/{table}/"
                self._create_table(db_name, table, columns, s3_location)

    def _create_table(
        self,
        database: str,
        table: str,
        columns: list[dict],
        s3_location: str,
    ) -> None:
        try:
            self.glue.create_table(
                DatabaseName=database,
                TableInput={
                    "Name":       table,
                    "Description": f"Retail {table} table",
                    "StorageDescriptor": {
                        "Columns":             columns,
                        "Location":            s3_location,
                        "InputFormat":  "org.apache.hadoop.mapred.TextInputFormat",
                        "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                        "SerdeInfo": {
                            "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                            "Parameters": {"serialization.format": "1"},
                        },
                        "Compressed":      True,
                        "NumberOfBuckets": -1,
                        "StoredAsSubDirectories": False,
                    },
                    "PartitionKeys": [],
                    "TableType": "EXTERNAL_TABLE",
                    "Parameters": {
                        "classification":          "parquet",
                        "compressionType":         "snappy",
                        "typeOfData":              "file",
                        "EXTERNAL":                "TRUE",
                        "parquet.compress":        "SNAPPY",
                    },
                },
            )
            log.info("Registered Glue table: %s.%s", database, table)
        except self.glue.exceptions.AlreadyExistsException:
            log.info("Glue table already exists: %s.%s", database, table)

    def list_tables(self, database: str) -> list[str]:
        """Return all table names in a Glue database."""
        paginator = self.glue.get_paginator("get_tables")
        tables = []
        for page in paginator.paginate(DatabaseName=database):
            tables.extend(t["Name"] for t in page["TableList"])
        return tables

    def run(self) -> None:
        """Full catalog setup: databases + tables + crawlers."""
        log.info("=== Glue Data Catalog Setup ===")
        self.create_databases()
        self.create_table_definitions()
        self.create_crawlers()
        log.info("Glue catalog ready. Run crawlers with: catalog.run_crawlers()")
