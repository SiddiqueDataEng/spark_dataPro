# aws/iam/iam_roles.py
"""
IAMRoles
=========
Creates all IAM roles and policies required by the retail data platform.

Roles:
  RetailDataLakeAdmin     — full lake access (Lake Formation admin)
  RetailGlueETLRole       — Glue jobs: S3 read/write + Glue catalog + LF
  RetailAnalyst           — Athena SELECT on Gold (no PII)
  RetailAuditor           — Bronze read-only (compliance / audit)
  RetailDMSRole           — DMS write to S3 buckets
  RetailStepFunctionsRole — Step Functions: invoke Glue + Crawlers

Usage:
    from aws.iam.iam_roles import IAMRoles
    roles = IAMRoles()
    roles.create_all_roles()
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from aws.config.aws_config import (
    AWSConfig, ACCOUNT_ID, REGION,
    S3_BUCKET_RAW, S3_BUCKET_CLEAN, S3_BUCKET_CURATED,
    S3_BUCKET_ATHENA, S3_BUCKET_GLUE, S3_BUCKET_DMS,
    GLUE_DATABASE_RAW, GLUE_DATABASE_CLEAN, GLUE_DATABASE_CURATED,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _trust(service: str) -> dict:
    """Build a simple trust policy for an AWS service principal."""
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": f"{service}.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }


def _account_trust() -> dict:
    """Trust policy for IAM users / roles within the same account."""
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT_ID}:root"},
            "Action": "sts:AssumeRole",
        }],
    }


class IAMRoles:
    """Provision IAM roles and inline policies for the retail data platform."""

    def __init__(self, cfg: Optional[AWSConfig] = None) -> None:
        self.cfg = cfg or AWSConfig()
        self.iam = self.cfg.iam_client()

    # ──────────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _create_role(self, name: str, trust: dict, description: str) -> str:
        """Create a role (idempotent). Returns role ARN."""
        try:
            resp = self.iam.create_role(
                RoleName=name,
                AssumeRolePolicyDocument=json.dumps(trust),
                Description=description,
                Tags=[
                    {"Key": "Project",     "Value": "retail-medallion"},
                    {"Key": "Owner",       "Value": "MSiddique"},
                    {"Key": "Environment", "Value": self.cfg.env},
                ],
            )
            arn = resp["Role"]["Arn"]
            log.info("Created role: %s  arn=%s", name, arn)
            return arn
        except self.iam.exceptions.EntityAlreadyExistsException:
            arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{name}"
            log.info("Role already exists: %s", name)
            return arn

    def _attach_managed(self, role: str, policy_arn: str) -> None:
        try:
            self.iam.attach_role_policy(RoleName=role, PolicyArn=policy_arn)
        except self.iam.exceptions.PolicyNotAttachableException:
            pass

    def _put_inline(self, role: str, policy_name: str, policy: dict) -> None:
        self.iam.put_role_policy(
            RoleName=role,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(policy),
        )

    # ──────────────────────────────────────────────────────────────────────────
    # Individual role creators
    # ──────────────────────────────────────────────────────────────────────────

    def create_glue_etl_role(self) -> str:
        """RetailGlueETLRole — full S3 + Glue + Lake Formation for ETL jobs."""
        role_name = "RetailGlueETLRole"
        self._create_role(role_name, _trust("glue"),
                          "Glue ETL role for retail medallion pipeline")

        # AWS-managed policies
        self._attach_managed(role_name, "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole")
        self._attach_managed(role_name, "arn:aws:iam::aws:policy/CloudWatchFullAccess")

        # S3 access to all data lake buckets
        buckets = [S3_BUCKET_RAW, S3_BUCKET_CLEAN, S3_BUCKET_CURATED,
                   S3_BUCKET_GLUE, S3_BUCKET_DMS]
        s3_arns = [f"arn:aws:s3:::{b}" for b in buckets] + \
                  [f"arn:aws:s3:::{b}/*" for b in buckets]

        self._put_inline(role_name, "RetailS3Access", {
            "Version": "2012-10-17",
            "Statement": [
                {"Effect": "Allow", "Action": ["s3:*"], "Resource": s3_arns},
            ],
        })

        # Lake Formation data access
        self._put_inline(role_name, "RetailLakeFormationAccess", {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["lakeformation:GetDataAccess",
                           "lakeformation:GrantPermissions",
                           "lakeformation:ListPermissions"],
                "Resource": "*",
            }],
        })
        return f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"

    def create_analyst_role(self) -> str:
        """RetailAnalyst — Athena + read-only Gold S3 (no PII columns via LF)."""
        role_name = "RetailAnalyst"
        self._create_role(role_name, _account_trust(),
                          "Analyst role - SELECT Gold tables, no PII columns")

        self._put_inline(role_name, "RetailAnalystPolicy", {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                        "athena:GetWorkGroup",
                        "athena:ListWorkGroups",
                    ],
                    "Resource": [
                        f"arn:aws:athena:{REGION}:{ACCOUNT_ID}:workgroup/*"
                    ],
                },
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:GetDatabase", "glue:GetTable", "glue:GetTables",
                        "glue:GetPartition", "glue:GetPartitions",
                        "glue:BatchGetPartition",
                    ],
                    "Resource": [
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/{GLUE_DATABASE_CURATED}",
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/{GLUE_DATABASE_CURATED}/*",
                    ],
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:ListBucket"],
                    "Resource": [
                        f"arn:aws:s3:::{S3_BUCKET_CURATED}",
                        f"arn:aws:s3:::{S3_BUCKET_CURATED}/*",
                        f"arn:aws:s3:::{S3_BUCKET_ATHENA}",
                        f"arn:aws:s3:::{S3_BUCKET_ATHENA}/*",
                    ],
                },
                {
                    "Effect": "Allow",
                    "Action": ["s3:PutObject"],
                    "Resource": [f"arn:aws:s3:::{S3_BUCKET_ATHENA}/*"],
                },
                {
                    "Effect": "Allow",
                    "Action": ["lakeformation:GetDataAccess"],
                    "Resource": "*",
                },
            ],
        })
        return f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"

    def create_dms_role(self) -> str:
        """RetailDMSRole — DMS write access to S3 target buckets."""
        role_name = "RetailDMSRole"
        self._create_role(role_name, _trust("dms"),
                          "DMS replication role for S3 target")
        self._attach_managed(
            role_name,
            "arn:aws:iam::aws:policy/service-role/AmazonDMSRedshiftS3Role",
        )
        self._put_inline(role_name, "RetailDMSS3Policy", {
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket",
                           "s3:DeleteObject"],
                "Resource": [
                    f"arn:aws:s3:::{S3_BUCKET_DMS}",
                    f"arn:aws:s3:::{S3_BUCKET_DMS}/*",
                    f"arn:aws:s3:::{S3_BUCKET_RAW}",
                    f"arn:aws:s3:::{S3_BUCKET_RAW}/*",
                ],
            }],
        })
        return f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"

    def create_step_functions_role(self) -> str:
        """RetailStepFunctionsRole — start Glue jobs + crawlers from SFN."""
        role_name = "RetailStepFunctionsRole"
        self._create_role(role_name, _trust("states"),
                          "Step Functions role for retail pipeline orchestration")
        self._put_inline(role_name, "RetailSFNPolicy", {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "glue:StartJobRun", "glue:GetJobRun",
                        "glue:BatchStopJobRun",
                    ],
                    "Resource": [
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:job/retail_*"
                    ],
                },
                {
                    "Effect": "Allow",
                    "Action": ["glue:StartCrawler", "glue:GetCrawler"],
                    "Resource": [
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:crawler/retail-*"
                    ],
                },
                {
                    "Effect": "Allow",
                    "Action": ["logs:CreateLogGroup", "logs:CreateLogDelivery",
                               "logs:PutLogEvents", "logs:GetLogDelivery",
                               "logs:DescribeLogGroups"],
                    "Resource": "*",
                },
                {
                    "Effect": "Allow",
                    "Action": ["xray:PutTraceSegments", "xray:PutTelemetryRecords"],
                    "Resource": "*",
                },
            ],
        })
        return f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"

    def create_admin_role(self) -> str:
        """RetailDataLakeAdmin — full admin over the data lake."""
        role_name = "RetailDataLakeAdmin"
        self._create_role(role_name, _account_trust(),
                          "Data Lake admin - full access")
        self._attach_managed(role_name, "arn:aws:iam::aws:policy/AdministratorAccess")
        return f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"

    def create_auditor_role(self) -> str:
        """RetailAuditor — read-only access to Bronze CDC tables."""
        role_name = "RetailAuditor"
        self._create_role(role_name, _account_trust(),
                          "Auditor role - read-only Bronze CDC")
        self._put_inline(role_name, "RetailAuditorPolicy", {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": ["s3:GetObject", "s3:ListBucket"],
                    "Resource": [
                        f"arn:aws:s3:::{S3_BUCKET_RAW}",
                        f"arn:aws:s3:::{S3_BUCKET_RAW}/*",
                    ],
                },
                {
                    "Effect": "Allow",
                    "Action": ["glue:GetDatabase", "glue:GetTable", "glue:GetTables"],
                    "Resource": [
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:catalog",
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:database/{GLUE_DATABASE_RAW}",
                        f"arn:aws:glue:{REGION}:{ACCOUNT_ID}:table/{GLUE_DATABASE_RAW}/*",
                    ],
                },
                {
                    "Effect": "Allow",
                    "Action": ["lakeformation:GetDataAccess"],
                    "Resource": "*",
                },
            ],
        })
        return f"arn:aws:iam::{ACCOUNT_ID}:role/{role_name}"

    # ──────────────────────────────────────────────────────────────────────────
    # Create all roles
    # ──────────────────────────────────────────────────────────────────────────

    def create_all_roles(self) -> dict[str, str]:
        """Create all platform IAM roles. Returns name → ARN map."""
        log.info("=== Creating IAM Roles ===")
        roles = {
            "RetailGlueETLRole":       self.create_glue_etl_role(),
            "RetailAnalyst":           self.create_analyst_role(),
            "RetailDMSRole":           self.create_dms_role(),
            "RetailStepFunctionsRole": self.create_step_functions_role(),
            "RetailDataLakeAdmin":     self.create_admin_role(),
            "RetailAuditor":           self.create_auditor_role(),
        }
        for name, arn in roles.items():
            log.info("  %-30s %s", name, arn)
        log.info("=== IAM Roles complete ===")
        return roles
