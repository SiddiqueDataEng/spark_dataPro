# aws/glue/glue_jobs.py
"""
GlueJobs
========
Creates and manages AWS Glue ETL jobs for the Medallion pipeline.

Jobs:
  1. retail_bronze_ingestion   — RDS/PostgreSQL → S3 Bronze (Parquet)
  2. retail_silver_processing  — S3 Bronze → S3 Silver (clean, dedupe, enrich)
  3. retail_gold_aggregation   — S3 Silver → S3 Gold (5 aggregated tables)
  4. retail_cdc_merge          — CDC JSONL → Bronze MERGE (upsert / soft-delete)
  5. retail_scd_processing     — Bronze → Silver SCD-2 dimension versioning

Each job is:
  - PySpark on Glue 4.0 (Spark 3.3 + Delta Lake via connector)
  - Script stored in S3 glue assets bucket
  - G.1X worker (4 vCPU, 16 GB) — scales to G.2X for large loads
  - Continuous CloudWatch logging + job metrics

Usage:
    from aws.glue.glue_jobs import GlueJobs
    jobs = GlueJobs()
    jobs.upload_scripts()   # push PySpark scripts to S3
    jobs.create_jobs()      # register jobs in Glue
    jobs.run_job("retail_bronze_ingestion")
    jobs.wait_for_job("retail_bronze_ingestion", run_id)
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from aws.config.aws_config import AWSConfig
from aws.s3.s3_data_lake import S3DataLake

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─────────────────────────────────────────────────────────────────────────────
# Glue PySpark script bodies
# ─────────────────────────────────────────────────────────────────────────────

def _bronze_script(cfg: AWSConfig) -> str:
    return f'''
# Glue ETL Job: retail_bronze_ingestion
# Reads from RDS PostgreSQL → writes Parquet to S3 Bronze
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import current_timestamp, lit

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc   = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
job  = Job(glue)
job.init(args["JOB_NAME"], args)

TABLES       = {str(cfg.tables)}
JDBC_URL     = "jdbc:postgresql://{{rds_host}}:{cfg.region}/{cfg.account_id}"
S3_BRONZE    = "s3://{cfg.bucket_raw}/bronze/"
CATALOG_DB   = "{cfg.glue_db_raw}"

jdbc_opts = {{
    "driver":   "org.postgresql.Driver",
    "user":     "retail_user",
    "password": "{{rds_password}}",
}}

for table in TABLES:
    df = (spark.read
          .format("jdbc")
          .option("url", JDBC_URL)
          .option("dbtable", f"public.{{table}}")
          .options(**jdbc_opts)
          .load())
    df = df.withColumn("__ingested_at", current_timestamp()) \\
           .withColumn("__source", lit("rds_postgresql"))
    (df.write
       .format("parquet")
       .option("compression", "snappy")
       .mode("overwrite")
       .save(f"{{S3_BRONZE}}{{table}}/"))
    glue.catalog.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={{"path": f"{{S3_BRONZE}}{{table}}/"}},
        format="parquet",
        transformation_ctx=f"{{table}}_ctx"
    )
    print(f"Bronze ingested: {{table}}  rows={{df.count()}}")

job.commit()
'''


def _silver_script(cfg: AWSConfig) -> str:
    return f'''
# Glue ETL Job: retail_silver_processing
# Reads Bronze Parquet → applies cleaning, deduplication, enrichment → Silver
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, current_timestamp, trim, lower, upper, when,
    to_date, coalesce, lit, regexp_replace
)
from pyspark.sql.window import Window
from pyspark.sql import functions as F

args  = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc    = SparkContext()
glue  = GlueContext(sc)
spark = glue.spark_session
job   = Job(glue)
job.init(args["JOB_NAME"], args)

TABLES     = {str(cfg.tables)}
S3_BRONZE  = "s3://{cfg.bucket_raw}/bronze/"
S3_SILVER  = "s3://{cfg.bucket_clean}/silver/"

for table in TABLES:
    df = spark.read.parquet(f"{{S3_BRONZE}}{{table}}/")

    # 1. Drop NULLs in primary key columns
    pk = next((c for c in ["customer_id","product_id","employee_id",
                            "store_id","order_id","sale_id"]
               if c in df.columns), None)
    if pk:
        df = df.dropna(subset=[pk])

    # 2. Deduplicate on primary key — keep latest record
    if pk:
        w  = Window.partitionBy(pk).orderBy(F.desc("__ingested_at"))
        df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    # 3. Standardise string columns
    str_cols = [f.name for f in df.schema.fields
                if str(f.dataType) == "StringType()"]
    for c in str_cols:
        df = df.withColumn(c, trim(col(c)))

    # 4. Customers: normalise email to lowercase
    if "email" in df.columns:
        df = df.withColumn("email", lower(col("email")))

    # 5. Enrich — add load metadata
    df = (df.withColumn("__silver_processed_at", current_timestamp())
            .withColumn("__layer", lit("silver")))

    (df.write
       .format("parquet")
       .option("compression", "snappy")
       .mode("overwrite")
       .save(f"{{S3_SILVER}}{{table}}/"))
    print(f"Silver processed: {{table}}  rows={{df.count()}}")

job.commit()
'''


def _gold_script(cfg: AWSConfig) -> str:
    return f'''
# Glue ETL Job: retail_gold_aggregation
# Reads Silver Parquet → builds 5 Gold aggregation tables
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import (
    col, sum as _sum, avg, count, round as _round,
    date_trunc, current_timestamp, lit
)

args  = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc    = SparkContext()
glue  = GlueContext(sc)
spark = glue.spark_session
job   = Job(glue)
job.init(args["JOB_NAME"], args)

S3_SILVER  = "s3://{cfg.bucket_clean}/silver/"
S3_GOLD    = "s3://{cfg.bucket_curated}/gold/"

# Load Silver tables
customers = spark.read.parquet(f"{{S3_SILVER}}customers/")
products  = spark.read.parquet(f"{{S3_SILVER}}products/")
orders    = spark.read.parquet(f"{{S3_SILVER}}orders/")
sales     = spark.read.parquet(f"{{S3_SILVER}}sales/")
stores    = spark.read.parquet(f"{{S3_SILVER}}stores/")
employees = spark.read.parquet(f"{{S3_SILVER}}employees/")

# 1. Monthly revenue by category
monthly_revenue = (
    sales.join(orders,   "order_id")
         .join(products, "product_id")
         .withColumn("month", date_trunc("month", col("order_date")))
         .groupBy("month", "category")
         .agg(
             _round(_sum("total_revenue"), 2).alias("total_revenue"),
             _round(_sum("profit"), 2).alias("total_profit"),
             count("sale_id").alias("num_sales"),
         )
)
monthly_revenue.write.parquet(f"{{S3_GOLD}}monthly_revenue_by_category/",
                               mode="overwrite")

# 2. Customer lifetime value
customer_ltv = (
    sales.join(orders, "order_id")
         .groupBy("customer_id")
         .agg(
             _round(_sum("total_revenue"), 2).alias("lifetime_value"),
             count("order_id").alias("total_orders"),
             _round(avg("total_revenue"), 2).alias("avg_order_value"),
         )
         .join(customers.select("customer_id","first_name","last_name",
                                "city","country"), "customer_id")
)
customer_ltv.write.parquet(f"{{S3_GOLD}}customer_ltv/", mode="overwrite")

# 3. Product performance
product_perf = (
    sales.join(products, "product_id")
         .groupBy("product_id","product_name","category")
         .agg(
             _round(_sum("total_revenue"), 2).alias("revenue"),
             _round(_sum("profit"), 2).alias("profit"),
             _round(avg("profit") / avg("total_revenue") * 100, 2).alias("margin_pct"),
             count("sale_id").alias("units_sold"),
         )
)
product_perf.write.parquet(f"{{S3_GOLD}}product_performance/", mode="overwrite")

# 4. Store performance
store_perf = (
    sales.join(orders, "order_id")
         .join(stores, "store_id")
         .groupBy("store_id","store_name","city","country")
         .agg(
             _round(_sum("total_revenue"), 2).alias("revenue"),
             count("order_id").alias("num_orders"),
             _round(avg("total_revenue"), 2).alias("avg_order_value"),
         )
)
store_perf.write.parquet(f"{{S3_GOLD}}store_performance/", mode="overwrite")

# 5. Employee performance
emp_perf = (
    sales.join(orders, "order_id")
         .join(employees, "employee_id")
         .groupBy("employee_id","first_name","last_name","department")
         .agg(
             _round(_sum("total_revenue"), 2).alias("revenue_handled"),
             count("order_id").alias("orders_handled"),
             _round(_sum("profit"), 2).alias("profit_generated"),
         )
)
emp_perf.write.parquet(f"{{S3_GOLD}}employee_performance/", mode="overwrite")

print("Gold aggregations complete.")
job.commit()
'''


def _cdc_merge_script(cfg: AWSConfig) -> str:
    return f'''
# Glue ETL Job: retail_cdc_merge
# Reads CDC JSONL events from S3 → applies MERGE to Bronze Parquet tables
import sys
import json
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, current_timestamp, lit
from delta.tables import DeltaTable

args  = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc    = SparkContext()
glue  = GlueContext(sc)
spark = glue.spark_session
job   = Job(glue)
job.init(args["JOB_NAME"], args)

TABLES    = {str(cfg.tables)}
S3_CDC    = "s3://{cfg.bucket_dms}/cdc-events/"
S3_BRONZE = "s3://{cfg.bucket_raw}/bronze/"

PK_MAP = {{
    "customers": "customer_id", "products": "product_id",
    "employees": "employee_id", "stores":   "store_id",
    "orders":    "order_id",    "sales":    "sale_id",
}}

for table in TABLES:
    events_path  = f"{{S3_CDC}}retail.public.{{table}}.jsonl"
    bronze_path  = f"{{S3_BRONZE}}{{table}}/"
    pk           = PK_MAP[table]

    try:
        events = spark.read.json(events_path)
        if events.count() == 0:
            continue
    except Exception:
        continue

    inserts  = events.filter(col("payload.op") == "c").select("payload.after.*")
    updates  = events.filter(col("payload.op") == "u").select("payload.after.*")
    deletes  = events.filter(col("payload.op") == "d").select("payload.before.*")

    bronze_tbl = DeltaTable.forPath(spark, bronze_path)

    # UPSERT inserts + updates
    upserts = inserts.union(updates)
    if upserts.count() > 0:
        (bronze_tbl.alias("tgt")
         .merge(upserts.alias("src"), f"tgt.{{pk}} = src.{{pk}}")
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())

    # Soft-delete
    if deletes.count() > 0:
        delete_ids = [row[pk] for row in deletes.select(pk).collect()]
        (bronze_tbl.update(
            condition=col(pk).isin(delete_ids),
            set={{
                "__cdc_deleted": lit(True),
                "__cdc_op":      lit("DELETE"),
                "__cdc_ts":      current_timestamp(),
            }}
        ))

    print(f"CDC merged: {{table}}  ins={{inserts.count()}} "
          f"upd={{updates.count()}} del={{deletes.count()}}")

job.commit()
'''


# ─────────────────────────────────────────────────────────────────────────────
# GlueJobs class
# ─────────────────────────────────────────────────────────────────────────────

class GlueJobs:
    """Create, manage, and run AWS Glue ETL jobs."""

    JOBS: dict[str, str] = {
        "retail_bronze_ingestion":  "retail_bronze_ingestion.py",
        "retail_silver_processing": "retail_silver_processing.py",
        "retail_gold_aggregation":  "retail_gold_aggregation.py",
        "retail_cdc_merge":         "retail_cdc_merge.py",
    }

    def __init__(self, cfg: Optional[AWSConfig] = None) -> None:
        self.cfg  = cfg or AWSConfig()
        self.glue = self.cfg.glue_client()
        self.lake = S3DataLake(self.cfg)

    def upload_scripts(self) -> dict[str, str]:
        """Upload all Glue PySpark scripts to S3. Returns name → S3 URI map."""
        scripts = {
            "retail_bronze_ingestion.py":  _bronze_script(self.cfg),
            "retail_silver_processing.py": _silver_script(self.cfg),
            "retail_gold_aggregation.py":  _gold_script(self.cfg),
            "retail_cdc_merge.py":         _cdc_merge_script(self.cfg),
        }
        uris = {}
        for filename, body in scripts.items():
            uri = self.lake.put_glue_script(filename, body)
            uris[filename] = uri
        return uris

    def create_jobs(self) -> None:
        """Register all Glue jobs in the AWS account."""
        script_uris = self.upload_scripts()
        glue_bucket = self.cfg.bucket_glue

        job_defaults = {
            "Role":            self.cfg.glue_role_arn,
            "GlueVersion":     "4.0",
            "WorkerType":      "G.1X",
            "NumberOfWorkers": 2,
            "Timeout":         120,            # minutes
            "MaxRetries":      1,
            "ExecutionProperty": {"MaxConcurrentRuns": 1},
            "DefaultArguments": {
                "--enable-metrics":                    "true",
                "--enable-continuous-cloudwatch-log":  "true",
                "--enable-spark-ui":                   "true",
                "--spark-event-logs-path":
                    f"s3://{glue_bucket}/spark-ui-logs/",
                "--enable-job-insights":               "true",
                "--TempDir":
                    f"s3://{glue_bucket}/tmp/",
                "--job-language":                      "python",
                "--extra-jars":
                    f"s3://{glue_bucket}/jars/postgresql-42.7.4.jar",
            },
        }

        job_map = {
            "retail_bronze_ingestion":  "retail_bronze_ingestion.py",
            "retail_silver_processing": "retail_silver_processing.py",
            "retail_gold_aggregation":  "retail_gold_aggregation.py",
            "retail_cdc_merge":         "retail_cdc_merge.py",
        }

        for job_name, script_file in job_map.items():
            command = {
                "Name":           "glueetl",
                "ScriptLocation": script_uris[script_file],
                "PythonVersion":  "3",
            }
            try:
                self.glue.create_job(
                    Name=job_name,
                    Command=command,
                    **job_defaults,
                    Description=f"Retail Medallion ETL: {job_name}",
                    Tags={
                        "Project":     "retail-medallion",
                        "Owner":       "MSiddique",
                        "Environment": self.cfg.env,
                    },
                )
                log.info("Created Glue job: %s", job_name)
            except self.glue.exceptions.AlreadyExistsException:
                log.info("Glue job already exists: %s", job_name)

    def run_job(self, job_name: str, arguments: Optional[dict] = None) -> str:
        """Start a Glue job run. Returns the JobRunId."""
        kwargs: dict = {"JobName": job_name}
        if arguments:
            kwargs["Arguments"] = arguments
        response = self.glue.start_job_run(**kwargs)
        run_id   = response["JobRunId"]
        log.info("Started Glue job %s  run_id=%s", job_name, run_id)
        return run_id

    def wait_for_job(self, job_name: str, run_id: str, timeout: int = 600) -> str:
        """Poll until the job run succeeds or fails. Returns final state."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp  = self.glue.get_job_run(JobName=job_name, RunId=run_id)
            state = resp["JobRun"]["JobRunState"]
            if state in ("SUCCEEDED", "FAILED", "STOPPED", "ERROR", "TIMEOUT"):
                msg = resp["JobRun"].get("ErrorMessage", "")
                log.info("Job %s run %s -> %s  %s", job_name, run_id, state, msg)
                return state
            log.info("Job %s  state=%s  (waiting...)", job_name, state)
            time.sleep(20)
        return "TIMEOUT"

    def run_pipeline(self) -> None:
        """Run the full Medallion pipeline (Bronze → Silver → Gold → CDC)."""
        log.info("=== Glue Medallion Pipeline ===")
        pipeline = [
            "retail_bronze_ingestion",
            "retail_silver_processing",
            "retail_gold_aggregation",
            "retail_cdc_merge",
        ]
        for job_name in pipeline:
            run_id = self.run_job(job_name)
            state  = self.wait_for_job(job_name, run_id)
            if state != "SUCCEEDED":
                log.error("Pipeline aborted at job=%s  state=%s", job_name, state)
                return
        log.info("=== Glue pipeline completed successfully ===")