# aws/s3/s3_data_lake.py
"""
S3DataLake
==========
Manages the three-tier S3 Data Lake (Bronze / Silver / Gold).

Responsibilities:
  - Create and configure all S3 buckets with versioning, encryption, lifecycle
  - Upload local Delta Lake Parquet files to the correct S3 layer
  - Sync local data/ directory → S3 (Bronze = raw Parquet, Silver/Gold = clean)
  - Partition data by year/month/day (Hive-style) for Athena performance
  - Apply S3 Intelligent-Tiering lifecycle rules (30d → IA, 90d → Glacier)
  - Block all public access on every bucket

Usage:
    from aws.s3.s3_data_lake import S3DataLake
    lake = S3DataLake()
    lake.create_buckets()
    lake.upload_medallion_layer("bronze")
    lake.upload_medallion_layer("silver")
    lake.upload_medallion_layer("gold")
"""
from __future__ import annotations

import gzip
import json
import logging
import os
from pathlib import Path
from typing import Optional

from aws.config.aws_config import AWSConfig, SOURCE_TABLES

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class S3DataLake:
    """Create, configure, and populate the S3-based Medallion Data Lake."""

    LAYER_BUCKETS = {
        "bronze": "bucket_raw",
        "silver": "bucket_clean",
        "gold":   "bucket_curated",
    }

    def __init__(self, cfg: Optional[AWSConfig] = None) -> None:
        self.cfg    = cfg or AWSConfig()
        self.s3     = self.cfg.s3_client()
        self.region = self.cfg.region
        self._project_root = Path(__file__).resolve().parent.parent.parent

    # ──────────────────────────────────────────────────────────────────────────
    # Bucket setup
    # ──────────────────────────────────────────────────────────────────────────

    def create_buckets(self) -> None:
        """Create all data lake + support buckets with security best-practices."""
        buckets = [
            self.cfg.bucket_raw,
            self.cfg.bucket_clean,
            self.cfg.bucket_curated,
            self.cfg.bucket_athena,
            self.cfg.bucket_glue,
            self.cfg.bucket_dms,
        ]
        for bucket in buckets:
            self._create_bucket(bucket)
            self._block_public_access(bucket)
            self._enable_versioning(bucket)
            self._enable_encryption(bucket)
            log.info("Bucket ready: s3://%s", bucket)

        # Lifecycle rules (cost optimisation)
        for bucket in [self.cfg.bucket_raw, self.cfg.bucket_clean,
                       self.cfg.bucket_curated]:
            self._apply_lifecycle(bucket)

        # CORS for Athena query editor (optional but nice)
        self._apply_cors(self.cfg.bucket_athena)

    def _create_bucket(self, bucket: str) -> None:
        try:
            if self.region == "us-east-1":
                self.s3.create_bucket(Bucket=bucket)
            else:
                self.s3.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )
            log.info("Created bucket: %s", bucket)
        except self.s3.exceptions.BucketAlreadyOwnedByYou:
            log.info("Bucket already exists (owned): %s", bucket)
        except self.s3.exceptions.BucketAlreadyExists:
            log.warning("Bucket name taken by another account: %s", bucket)

    def _block_public_access(self, bucket: str) -> None:
        self.s3.put_public_access_block(
            Bucket=bucket,
            PublicAccessBlockConfiguration={
                "BlockPublicAcls":       True,
                "IgnorePublicAcls":      True,
                "BlockPublicPolicy":     True,
                "RestrictPublicBuckets": True,
            },
        )

    def _enable_versioning(self, bucket: str) -> None:
        self.s3.put_bucket_versioning(
            Bucket=bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

    def _enable_encryption(self, bucket: str) -> None:
        """Enable SSE-S3 server-side encryption (free, no KMS cost)."""
        self.s3.put_bucket_encryption(
            Bucket=bucket,
            ServerSideEncryptionConfiguration={
                "Rules": [{
                    "ApplyServerSideEncryptionByDefault": {
                        "SSEAlgorithm": "AES256",
                    },
                    "BucketKeyEnabled": True,
                }]
            },
        )

    def _apply_lifecycle(self, bucket: str) -> None:
        """
        Lifecycle: Parquet files transition through storage tiers.
          Day 0       Standard
          Day 30      Standard-IA
          Day 90      Glacier Instant Retrieval
          Day 365     Glacier Deep Archive
        """
        self.s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration={
                "Rules": [
                    {
                        "ID": "medallion-tiering",
                        "Status": "Enabled",
                        "Filter": {"Prefix": ""},
                        "Transitions": [
                            {"Days": 30,  "StorageClass": "STANDARD_IA"},
                            {"Days": 90,  "StorageClass": "GLACIER_IR"},
                            {"Days": 365, "StorageClass": "DEEP_ARCHIVE"},
                        ],
                        "NoncurrentVersionExpiration": {"NoncurrentDays": 90},
                    }
                ]
            },
        )

    def _apply_cors(self, bucket: str) -> None:
        self.s3.put_bucket_cors(
            Bucket=bucket,
            CORSConfiguration={
                "CORSRules": [{
                    "AllowedHeaders": ["*"],
                    "AllowedMethods": ["GET", "HEAD"],
                    "AllowedOrigins": ["*"],
                    "MaxAgeSeconds": 3000,
                }]
            },
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Upload / sync
    # ──────────────────────────────────────────────────────────────────────────

    def upload_medallion_layer(
        self,
        layer: str,
        tables: Optional[list[str]] = None,
    ) -> None:
        """
        Upload all Parquet files for a medallion layer from local data/ to S3.

        The S3 key structure mirrors Hive partitioning:
          s3://<bucket>/<layer>/<table>/part-*.snappy.parquet

        Delta transaction logs are also uploaded under _delta_log/.
        """
        tables = tables or SOURCE_TABLES
        bucket_attr = self.LAYER_BUCKETS[layer]
        bucket = getattr(self.cfg, bucket_attr)
        local_base = self._project_root / "data" / layer

        if not local_base.exists():
            log.warning("Local layer directory not found: %s", local_base)
            return

        uploaded = 0
        for table in tables:
            table_dir = local_base / table
            if not table_dir.exists():
                log.warning("Table dir missing: %s", table_dir)
                continue

            for local_file in table_dir.rglob("*"):
                if local_file.is_dir():
                    continue
                # Preserve relative path structure in S3 key
                relative = local_file.relative_to(local_base)
                s3_key   = f"{layer}/{relative.as_posix()}"

                self.s3.upload_file(
                    Filename=str(local_file),
                    Bucket=bucket,
                    Key=s3_key,
                    ExtraArgs={"ServerSideEncryption": "AES256"},
                )
                uploaded += 1

        log.info("Uploaded %d files for layer=%s to s3://%s/%s/",
                 uploaded, layer, bucket, layer)

    def upload_cdc_events(self) -> None:
        """Upload CDC JSONL topic files to S3 (feeds DMS / Glue CDC jobs)."""
        bucket  = self.cfg.bucket_dms
        cdc_dir = self._project_root / "cdc" / "cdc_events"

        if not cdc_dir.exists():
            log.warning("CDC events directory not found: %s", cdc_dir)
            return

        uploaded = 0
        for f in cdc_dir.rglob("*.jsonl"):
            s3_key = f"cdc-events/{f.name}"
            self.s3.upload_file(
                Filename=str(f),
                Bucket=bucket,
                Key=s3_key,
                ExtraArgs={"ServerSideEncryption": "AES256"},
            )
            uploaded += 1

        log.info("Uploaded %d CDC event files to s3://%s/cdc-events/",
                 uploaded, bucket)

    def put_glue_script(self, script_name: str, script_content: str) -> str:
        """Upload a Glue ETL script to the Glue assets bucket. Returns S3 URI."""
        bucket  = self.cfg.bucket_glue
        s3_key  = f"scripts/{script_name}"
        self.s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=script_content.encode(),
            ServerSideEncryption="AES256",
        )
        uri = f"s3://{bucket}/{s3_key}"
        log.info("Uploaded Glue script: %s", uri)
        return uri

    # ──────────────────────────────────────────────────────────────────────────
    # Utility
    # ──────────────────────────────────────────────────────────────────────────

    def list_objects(self, layer: str, table: str) -> list[str]:
        """List all S3 keys for a given layer + table prefix."""
        bucket_attr = self.LAYER_BUCKETS.get(layer, "bucket_raw")
        bucket = getattr(self.cfg, bucket_attr)
        prefix = f"{layer}/{table}/"
        paginator = self.s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])
        return keys

    def bucket_stats(self) -> dict[str, dict]:
        """Return object count and total size (MB) per data lake bucket."""
        buckets = {
            "bronze": self.cfg.bucket_raw,
            "silver": self.cfg.bucket_clean,
            "gold":   self.cfg.bucket_curated,
        }
        stats = {}
        for layer, bucket in buckets.items():
            paginator = self.s3.get_paginator("list_objects_v2")
            total_size  = 0
            total_count = 0
            for page in paginator.paginate(Bucket=bucket):
                for obj in page.get("Contents", []):
                    total_size  += obj["Size"]
                    total_count += 1
            stats[layer] = {
                "bucket":     bucket,
                "objects":    total_count,
                "size_mb":    round(total_size / 1_048_576, 2),
            }
        return stats

    def run(self) -> None:
        """Full setup: create buckets + upload all three medallion layers."""
        log.info("=== S3 Data Lake Setup ===")
        self.create_buckets()
        for layer in ("bronze", "silver", "gold"):
            self.upload_medallion_layer(layer)
        self.upload_cdc_events()
        log.info("=== S3 Data Lake Ready ===")
        stats = self.bucket_stats()
        for layer, info in stats.items():
            log.info("  %s: %d objects, %.2f MB  (%s)",
                     layer, info["objects"], info["size_mb"], info["bucket"])
