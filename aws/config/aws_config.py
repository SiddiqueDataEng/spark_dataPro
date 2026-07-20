# aws/config/aws_config.py
"""
AWSConfig
=========
Central configuration for all AWS services used in the retail data platform.

Account:  0218-9160-3670  (MSiddique)
Region:   us-east-1

Services configured:
  - S3 (Data Lake — bronze / silver / gold buckets)
  - AWS Glue (ETL jobs + Data Catalog + crawlers)
  - Amazon Athena (serverless SQL over S3)
  - Amazon RDS for PostgreSQL (source OLTP database)
  - AWS Lake Formation (fine-grained access control)
  - AWS DMS (Database Migration Service — initial load + CDC replication)
  - AWS Step Functions (pipeline orchestration)
  - Amazon MWAA (managed Airflow — optional)

Usage:
    from aws.config.aws_config import AWSConfig
    cfg = AWSConfig()
    s3  = cfg.s3_client()
    glue = cfg.glue_client()
"""
from __future__ import annotations

import csv as _csv
import os
from dataclasses import dataclass, field
from pathlib import Path
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# Load project .env first (DB creds + any AWS_* overrides already set there)
load_dotenv(_PROJECT_ROOT / ".env", override=False)


def _load_aws_credentials() -> None:
    """
    Auto-load AWS credentials from the files in aws/ so boto3 works without
    the AWS CLI or manual ~/.aws/credentials setup.

    Priority:
      1. Already in os.environ  (set by .env or shell)  → do nothing
      2. aws/.env_aws           (key: value lines)
      3. aws/MSiddique10x_credentials.csv  (CSV with header row)

    Injects AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY into os.environ.
    """
    if os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        return   # already configured

    aws_dir = _PROJECT_ROOT / "aws"

    # ── aws/.env_aws ──────────────────────────────────────────────────────────
    env_aws = aws_dir / ".env_aws"
    if env_aws.exists():
        key_id = secret = None
        for raw in env_aws.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if ":" not in line:
                continue
            k, _, v = line.partition(":")
            k = k.strip().lower()
            v = v.strip()
            if k in ("access key", "aws access key id"):
                key_id = v
            elif k in ("secret access key", "aws secret access key"):
                secret = v
        if key_id and secret:
            os.environ["AWS_ACCESS_KEY_ID"]     = key_id
            os.environ["AWS_SECRET_ACCESS_KEY"] = secret
            return

    # ── aws/MSiddique10x_credentials.csv ─────────────────────────────────────
    csv_path = aws_dir / "MSiddique10x_credentials.csv"
    if csv_path.exists():
        rows = list(_csv.DictReader(
            csv_path.read_text(encoding="utf-8").splitlines()
        ))
        if rows:
            row    = {k.strip(): v.strip() for k, v in rows[0].items()}
            key_id = row.get("AWS Access Key ID", "")
            secret = row.get("AWS Secret Access Key", "")
            if key_id and secret:
                os.environ["AWS_ACCESS_KEY_ID"]     = key_id
                os.environ["AWS_SECRET_ACCESS_KEY"] = secret


# Inject credentials before any boto3 client is created
_load_aws_credentials()


def _write_aws_credentials_file() -> None:
    """
    Write ~/.aws/credentials and ~/.aws/config from the injected env vars
    so the AWS CLI, CDK, and any other SDK tool also picks them up.
    Only writes if the file doesn't already have a [default] entry.
    """
    key_id = os.getenv("AWS_ACCESS_KEY_ID", "")
    secret  = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    if not key_id or not secret:
        return

    aws_dir = Path.home() / ".aws"
    aws_dir.mkdir(mode=0o700, exist_ok=True)

    creds_path = aws_dir / "credentials"
    # Only write if file is missing or doesn't yet have our key
    existing = creds_path.read_text(encoding="utf-8") if creds_path.exists() else ""
    if key_id not in existing:
        creds_path.write_text(
            f"[default]\n"
            f"aws_access_key_id     = {key_id}\n"
            f"aws_secret_access_key = {secret}\n",
            encoding="utf-8",
        )
        creds_path.chmod(0o600)

    cfg_path = aws_dir / "config"
    region   = os.getenv("AWS_REGION", "us-east-1")
    if not cfg_path.exists():
        cfg_path.write_text(
            f"[default]\nregion = {region}\noutput = json\n",
            encoding="utf-8",
        )


_write_aws_credentials_file()


# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

ACCOUNT_ID  = "021891603670"           # no hyphens for ARN construction
ACCOUNT_NAME = "MSiddique"
REGION      = os.getenv("AWS_REGION", "us-east-1")
ENV         = os.getenv("AWS_ENV", "dev")          # dev | staging | prod

# S3 bucket names  (account-scoped so globally unique)
S3_BUCKET_RAW     = f"retail-raw-{ACCOUNT_ID}-{REGION}"        # Bronze
S3_BUCKET_CLEAN   = f"retail-clean-{ACCOUNT_ID}-{REGION}"      # Silver
S3_BUCKET_CURATED = f"retail-curated-{ACCOUNT_ID}-{REGION}"    # Gold
S3_BUCKET_ATHENA  = f"retail-athena-results-{ACCOUNT_ID}-{REGION}"
S3_BUCKET_GLUE    = f"retail-glue-assets-{ACCOUNT_ID}-{REGION}"
S3_BUCKET_DMS     = f"retail-dms-{ACCOUNT_ID}-{REGION}"

# Glue Data Catalog
GLUE_DATABASE_RAW     = "retail_bronze"
GLUE_DATABASE_CLEAN   = "retail_silver"
GLUE_DATABASE_CURATED = "retail_gold"
GLUE_DATABASE_CDC     = "retail_cdc"

# Athena
ATHENA_WORKGROUP = "retail_workgroup"
ATHENA_OUTPUT    = f"s3://{S3_BUCKET_ATHENA}/query-results/"

# Lake Formation
LF_DATA_LAKE_ADMIN_ROLE = f"arn:aws:iam::{ACCOUNT_ID}:role/RetailDataLakeAdmin"
LF_ANALYST_ROLE         = f"arn:aws:iam::{ACCOUNT_ID}:role/RetailAnalyst"
LF_ETL_ROLE             = f"arn:aws:iam::{ACCOUNT_ID}:role/RetailGlueETL"

# RDS
RDS_IDENTIFIER   = "retail-postgres-db"
RDS_DB_NAME      = "neondb"
RDS_INSTANCE     = "db.t3.medium"
RDS_PORT         = 5432

# DMS
DMS_REPLICATION_INSTANCE = "retail-dms-instance"
DMS_SOURCE_ENDPOINT_ID   = "retail-pg-source"
DMS_TARGET_ENDPOINT_ID   = "retail-s3-target"

# Step Functions
SFN_STATE_MACHINE_NAME = "RetailMedallionPipeline"

# IAM Role ARNs
IAM_GLUE_ROLE  = f"arn:aws:iam::{ACCOUNT_ID}:role/RetailGlueETLRole"
IAM_LAMBDA_ROLE = f"arn:aws:iam::{ACCOUNT_ID}:role/RetailLambdaRole"
IAM_DMS_ROLE   = f"arn:aws:iam::{ACCOUNT_ID}:role/RetailDMSRole"

# Source tables (mirrors PostgreSQL / Neon schema)
SOURCE_TABLES = ["customers", "products", "employees", "stores", "orders", "sales"]


# ─────────────────────────────────────────────────────────────────────────────
# Config dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class AWSConfig:
    """Holds all AWS connection settings and provides boto3 client factories."""

    account_id:   str = ACCOUNT_ID
    account_name: str = ACCOUNT_NAME
    region:       str = REGION
    env:          str = ENV

    # S3 buckets
    bucket_raw:     str = S3_BUCKET_RAW
    bucket_clean:   str = S3_BUCKET_CLEAN
    bucket_curated: str = S3_BUCKET_CURATED
    bucket_athena:  str = S3_BUCKET_ATHENA
    bucket_glue:    str = S3_BUCKET_GLUE
    bucket_dms:     str = S3_BUCKET_DMS

    # Glue databases
    glue_db_raw:     str = GLUE_DATABASE_RAW
    glue_db_clean:   str = GLUE_DATABASE_CLEAN
    glue_db_curated: str = GLUE_DATABASE_CURATED
    glue_db_cdc:     str = GLUE_DATABASE_CDC

    # IAM roles
    glue_role_arn: str = IAM_GLUE_ROLE

    # Athena
    athena_workgroup: str = ATHENA_WORKGROUP
    athena_output:    str = ATHENA_OUTPUT

    # Source tables
    tables: list = field(default_factory=lambda: list(SOURCE_TABLES))

    # ── boto3 client factories ──────────────────────────────────────────────

    def _session(self):
        """Return a boto3 Session (picks up AWS_* env vars or ~/.aws/credentials)."""
        import boto3
        return boto3.Session(
            region_name=self.region,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        )

    def s3_client(self):
        return self._session().client("s3")

    def test_connection(self) -> bool:
        """
        Verify AWS credentials work by calling STS GetCallerIdentity.
        Returns True on success, raises NoCredentialsError otherwise.
        """
        sts      = self._session().client("sts")
        identity = sts.get_caller_identity()
        print(f"\nAWS connection OK:")
        print(f"  Account: {identity['Account']}")
        print(f"  ARN:     {identity['Arn']}")
        print(f"  UserId:  {identity['UserId']}")
        return True

    def s3_resource(self):
        return self._session().resource("s3")

    def glue_client(self):
        return self._session().client("glue")

    def athena_client(self):
        return self._session().client("athena")

    def rds_client(self):
        return self._session().client("rds")

    def dms_client(self):
        return self._session().client("dms")

    def lakeformation_client(self):
        return self._session().client("lakeformation")

    def stepfunctions_client(self):
        return self._session().client("stepfunctions")

    def iam_client(self):
        return self._session().client("iam")

    def logs_client(self):
        return self._session().client("logs")

    # ── helpers ─────────────────────────────────────────────────────────────

    @property
    def s3_raw_prefix(self) -> str:
        return f"s3://{self.bucket_raw}"

    @property
    def s3_clean_prefix(self) -> str:
        return f"s3://{self.bucket_clean}"

    @property
    def s3_curated_prefix(self) -> str:
        return f"s3://{self.bucket_curated}"

    def table_s3_path(self, layer: str, table: str, fmt: str = "parquet") -> str:
        """Return the S3 URI for a given medallion layer + table."""
        bucket_map = {
            "bronze": self.bucket_raw,
            "silver": self.bucket_clean,
            "gold":   self.bucket_curated,
        }
        bucket = bucket_map[layer]
        return f"s3://{bucket}/{layer}/{table}/"

    def __repr__(self) -> str:
        return (
            f"AWSConfig(account={self.account_id!r}, "
            f"region={self.region!r}, env={self.env!r})"
        )
