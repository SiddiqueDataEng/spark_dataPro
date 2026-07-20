# aws/lake_formation/lake_formation_governance.py
"""
LakeFormationGovernance
========================
Fine-grained access control for the S3 Data Lake using AWS Lake Formation.

Lake Formation sits on top of Glue Data Catalog and S3 to provide:
  - Database-level permissions (CREATE TABLE, ALTER, DROP)
  - Table-level permissions (SELECT, INSERT, DELETE, DESCRIBE)
  - Column-level security (COLUMN PERMISSIONS — hide PII columns from analysts)
  - Row-level filtering (coming: Lake Formation row filters)
  - Data location registration (tells LF which S3 paths are part of the lake)

Roles defined:
  ┌──────────────────────┬─────────────────────────────────────────────────┐
  │ Role                 │ Permissions                                     │
  ├──────────────────────┼─────────────────────────────────────────────────┤
  │ RetailDataLakeAdmin  │ Admin — all databases, all tables               │
  │ RetailGlueETL        │ SELECT/INSERT all tables + S3 full access       │
  │ RetailAnalyst        │ SELECT all Gold tables, NO PII columns          │
  │ RetailAuditor        │ SELECT Bronze CDC audit tables (read-only)      │
  └──────────────────────┴─────────────────────────────────────────────────┘

PII columns excluded from RetailAnalyst role:
  customers: email, phone
  employees: salary

Usage:
    from aws.lake_formation.lake_formation_governance import LakeFormationGovernance
    lf = LakeFormationGovernance()
    lf.register_data_lake_locations()
    lf.grant_admin_permissions()
    lf.grant_etl_permissions()
    lf.grant_analyst_permissions()   # column-level PII redaction
    lf.run()                         # all of the above
"""
from __future__ import annotations

import logging
from typing import Optional

from aws.config.aws_config import (
    AWSConfig, SOURCE_TABLES,
    LF_DATA_LAKE_ADMIN_ROLE, LF_ANALYST_ROLE, LF_ETL_ROLE,
    ACCOUNT_ID,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# PII columns that analysts must NOT see
PII_COLUMNS: dict[str, list[str]] = {
    "customers": ["email", "phone"],
    "employees": ["salary"],
}


class LakeFormationGovernance:
    """Govern the data lake using AWS Lake Formation permissions."""

    AUDITOR_ROLE = f"arn:aws:iam::{ACCOUNT_ID}:role/RetailAuditor"

    def __init__(self, cfg: Optional[AWSConfig] = None) -> None:
        self.cfg = cfg or AWSConfig()
        self.lf  = self.cfg.lakeformation_client()

    # ──────────────────────────────────────────────────────────────────────────
    # Data location registration
    # ──────────────────────────────────────────────────────────────────────────

    def register_data_lake_locations(self) -> None:
        """Register all S3 data lake buckets with Lake Formation."""
        locations = [
            f"s3://{self.cfg.bucket_raw}/",
            f"s3://{self.cfg.bucket_clean}/",
            f"s3://{self.cfg.bucket_curated}/",
            f"s3://{self.cfg.bucket_dms}/",
        ]
        for loc in locations:
            try:
                self.lf.register_resource(
                    ResourceArn=f"arn:aws:s3:::{loc.replace('s3://', '').rstrip('/')}",
                    UseServiceLinkedRole=True,
                )
                log.info("Registered data lake location: %s", loc)
            except self.lf.exceptions.AlreadyExistsException:
                log.info("Location already registered: %s", loc)

    def set_data_lake_admin(self) -> None:
        """Set the Data Lake Admin in Lake Formation settings."""
        self.lf.put_data_lake_settings(
            DataLakeSettings={
                "DataLakeAdmins": [{"DataLakePrincipalIdentifier": LF_DATA_LAKE_ADMIN_ROLE}],
                "CreateDatabaseDefaultPermissions": [],
                "CreateTableDefaultPermissions":   [],
            }
        )
        log.info("Set Data Lake Admin: %s", LF_DATA_LAKE_ADMIN_ROLE)

    # ──────────────────────────────────────────────────────────────────────────
    # Admin permissions
    # ──────────────────────────────────────────────────────────────────────────

    def grant_admin_permissions(self) -> None:
        """Grant full admin permissions on all databases to the admin role."""
        for db in [self.cfg.glue_db_raw, self.cfg.glue_db_clean,
                   self.cfg.glue_db_curated, self.cfg.glue_db_cdc]:
            self._grant(
                principal=LF_DATA_LAKE_ADMIN_ROLE,
                resource={"Database": {"Name": db}},
                permissions=["ALL"],
                grantable=["ALL"],
            )
            # Wildcard table grant
            self._grant(
                principal=LF_DATA_LAKE_ADMIN_ROLE,
                resource={"Table": {"DatabaseName": db, "TableWildcard": {}}},
                permissions=["ALL"],
                grantable=["ALL"],
            )
        log.info("Admin permissions granted to: %s", LF_DATA_LAKE_ADMIN_ROLE)

    # ──────────────────────────────────────────────────────────────────────────
    # ETL role permissions (Glue jobs)
    # ──────────────────────────────────────────────────────────────────────────

    def grant_etl_permissions(self) -> None:
        """Grant SELECT + INSERT + ALTER on all tables to the Glue ETL role."""
        etl_perms = ["SELECT", "INSERT", "DELETE", "ALTER", "DESCRIBE"]
        for db in [self.cfg.glue_db_raw, self.cfg.glue_db_clean,
                   self.cfg.glue_db_curated, self.cfg.glue_db_cdc]:
            # Database-level: CREATE TABLE
            self._grant(
                principal=LF_ETL_ROLE,
                resource={"Database": {"Name": db}},
                permissions=["CREATE_TABLE", "ALTER", "DESCRIBE"],
            )
            # Table-level: all tables in db
            self._grant(
                principal=LF_ETL_ROLE,
                resource={"Table": {"DatabaseName": db, "TableWildcard": {}}},
                permissions=etl_perms,
            )
        log.info("ETL permissions granted to: %s", LF_ETL_ROLE)

    # ──────────────────────────────────────────────────────────────────────────
    # Analyst role permissions (Gold only, no PII)
    # ──────────────────────────────────────────────────────────────────────────

    def grant_analyst_permissions(self) -> None:
        """
        Grant SELECT on Gold tables to the Analyst role.
        PII columns (email, phone, salary) are excluded via column permissions.
        """
        from aws.glue.glue_catalog import GLUE_SCHEMAS

        # Database describe
        self._grant(
            principal=LF_ANALYST_ROLE,
            resource={"Database": {"Name": self.cfg.glue_db_curated}},
            permissions=["DESCRIBE"],
        )

        for table in SOURCE_TABLES:
            all_cols = [c["Name"] for c in GLUE_SCHEMAS[table]]
            pii_cols = set(PII_COLUMNS.get(table, []))
            allowed  = [c for c in all_cols if c not in pii_cols]

            if pii_cols:
                # Column-level permission (exclude PII)
                self._grant(
                    principal=LF_ANALYST_ROLE,
                    resource={
                        "TableWithColumns": {
                            "DatabaseName": self.cfg.glue_db_curated,
                            "Name":         table,
                            "ColumnNames":  allowed,
                        }
                    },
                    permissions=["SELECT"],
                )
                log.info(
                    "Analyst column grant on %s.%s  (excluded: %s)",
                    self.cfg.glue_db_curated, table, pii_cols,
                )
            else:
                # Full table access (no PII in this table)
                self._grant(
                    principal=LF_ANALYST_ROLE,
                    resource={
                        "Table": {
                            "DatabaseName": self.cfg.glue_db_curated,
                            "Name":         table,
                        }
                    },
                    permissions=["SELECT", "DESCRIBE"],
                )
                log.info(
                    "Analyst full grant on %s.%s",
                    self.cfg.glue_db_curated, table,
                )

    # ──────────────────────────────────────────────────────────────────────────
    # Auditor role permissions (Bronze CDC read-only)
    # ──────────────────────────────────────────────────────────────────────────

    def grant_auditor_permissions(self) -> None:
        """Grant SELECT on Bronze CDC tables to the Auditor role."""
        self._grant(
            principal=self.AUDITOR_ROLE,
            resource={"Database": {"Name": self.cfg.glue_db_raw}},
            permissions=["DESCRIBE"],
        )
        self._grant(
            principal=self.AUDITOR_ROLE,
            resource={"Table": {"DatabaseName": self.cfg.glue_db_raw,
                                "TableWildcard": {}}},
            permissions=["SELECT", "DESCRIBE"],
        )
        log.info("Auditor READ-ONLY permissions granted on Bronze CDC tables.")

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helper
    # ──────────────────────────────────────────────────────────────────────────

    def _grant(
        self,
        principal:   str,
        resource:    dict,
        permissions: list[str],
        grantable:   Optional[list[str]] = None,
    ) -> None:
        kwargs: dict = {
            "Principal":   {"DataLakePrincipalIdentifier": principal},
            "Resource":    resource,
            "Permissions": permissions,
        }
        if grantable:
            kwargs["PermissionsWithGrantOption"] = grantable
        try:
            self.lf.grant_permissions(**kwargs)
        except self.lf.exceptions.AlreadyExistsException:
            pass  # idempotent
        except Exception as exc:
            log.warning("Grant failed for %s on %s: %s", principal, resource, exc)

    def list_permissions(self, database: Optional[str] = None) -> list[dict]:
        """List all Lake Formation permissions (optionally filtered by database)."""
        kwargs: dict = {}
        if database:
            kwargs["Resource"] = {"Database": {"Name": database}}
        resp = self.lf.list_permissions(**kwargs)
        return resp.get("PrincipalResourcePermissions", [])

    # ──────────────────────────────────────────────────────────────────────────
    # Full setup
    # ──────────────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Complete Lake Formation governance setup."""
        log.info("=== Lake Formation Governance Setup ===")
        self.register_data_lake_locations()
        self.set_data_lake_admin()
        self.grant_admin_permissions()
        self.grant_etl_permissions()
        self.grant_analyst_permissions()
        self.grant_auditor_permissions()
        log.info("=== Lake Formation governance applied ===")
