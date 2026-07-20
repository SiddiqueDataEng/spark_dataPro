# aws/dms/dms_replication.py
"""
DMSReplication
==============
AWS Database Migration Service — replicates data FROM RDS PostgreSQL
TO S3 (Bronze layer) using both full-load and ongoing CDC replication.

Architecture:
  RDS PostgreSQL (source)
       │
       │  DMS Replication Instance
       │  ├── Full Load Task    → writes Parquet/CSV to S3 Bronze
       │  └── CDC Task          → streams WAL changes to S3 CDC prefix
       ↓
  S3 Bronze bucket
  ├── bronze/<table>/*.parquet    ← Full load output
  └── cdc-dms/<table>/*.csv      ← CDC change records (INSERT/UPDATE/DELETE)

DMS CDC format written to S3:
  Each change record contains Op column: I=INSERT, U=UPDATE, D=DELETE

After DMS delivers to S3:
  → Glue ETL job (retail_cdc_merge) reads the S3 CDC files
  → Applies MERGE to Bronze Delta tables

Usage:
    from aws.dms.dms_replication import DMSReplication
    dms = DMSReplication()
    dms.create_replication_instance()
    dms.wait_for_instance()
    dms.create_endpoints()
    dms.test_connections()
    dms.create_full_load_task()
    dms.start_task("full-load")
    dms.create_cdc_task()
    dms.start_task("cdc")
"""
from __future__ import annotations

import logging
import os
import time
from typing import Optional

from aws.config.aws_config import (
    AWSConfig, ACCOUNT_ID,
    DMS_REPLICATION_INSTANCE, DMS_SOURCE_ENDPOINT_ID, DMS_TARGET_ENDPOINT_ID,
)

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


class DMSReplication:
    """Create and manage AWS DMS replication tasks."""

    FULL_LOAD_TASK = "retail-full-load-task"
    CDC_TASK       = "retail-cdc-task"

    def __init__(self, cfg: Optional[AWSConfig] = None) -> None:
        self.cfg = cfg or AWSConfig()
        self.dms = self.cfg.dms_client()

    # ──────────────────────────────────────────────────────────────────────────
    # Replication Instance
    # ──────────────────────────────────────────────────────────────────────────

    def create_replication_instance(self) -> dict:
        """Provision the DMS replication instance (takes ~5 min)."""
        try:
            resp = self.dms.create_replication_instance(
                ReplicationInstanceIdentifier=DMS_REPLICATION_INSTANCE,
                ReplicationInstanceClass="dms.t3.medium",
                AllocatedStorage=50,
                EngineVersion="3.5.3",
                PubliclyAccessible=False,
                MultiAZ=False,
                AutoMinorVersionUpgrade=True,
                Tags=[
                    {"Key": "Project", "Value": "retail-medallion"},
                    {"Key": "Owner",   "Value": "MSiddique"},
                ],
            )
            log.info("Created DMS replication instance: %s", DMS_REPLICATION_INSTANCE)
            return resp["ReplicationInstance"]
        except self.dms.exceptions.ResourceAlreadyExistsFault:
            log.info("DMS instance already exists: %s", DMS_REPLICATION_INSTANCE)
            return self._describe_instance()

    def wait_for_instance(self, timeout: int = 600) -> bool:
        """Wait until the replication instance is available."""
        log.info("Waiting for DMS instance to become available...")
        deadline = time.time() + timeout
        while time.time() < deadline:
            inst   = self._describe_instance()
            status = inst.get("ReplicationInstanceStatus", "")
            if status == "available":
                log.info("DMS instance available.")
                return True
            log.info("DMS instance status: %s", status)
            time.sleep(30)
        return False

    def _describe_instance(self) -> dict:
        resp = self.dms.describe_replication_instances(
            Filters=[{"Name": "replication-instance-id",
                       "Values": [DMS_REPLICATION_INSTANCE]}]
        )
        instances = resp.get("ReplicationInstances", [])
        return instances[0] if instances else {}

    @property
    def _instance_arn(self) -> str:
        return self._describe_instance().get("ReplicationInstanceArn", "")

    # ──────────────────────────────────────────────────────────────────────────
    # Endpoints
    # ──────────────────────────────────────────────────────────────────────────

    def create_endpoints(
        self,
        rds_host:     Optional[str] = None,
        rds_password: Optional[str] = None,
    ) -> None:
        """Create source (RDS PostgreSQL) and target (S3) endpoints."""
        rds_host = rds_host or os.getenv("RDS_HOST", "")
        rds_pwd  = rds_password or os.getenv("RDS_PASSWORD", "RetailAdmin#2024!")

        # ── Source endpoint: RDS PostgreSQL ───────────────────────────────────
        try:
            self.dms.create_endpoint(
                EndpointIdentifier=DMS_SOURCE_ENDPOINT_ID,
                EndpointType="source",
                EngineName="postgres",
                Username="retail_admin",
                Password=rds_pwd,
                ServerName=rds_host,
                Port=5432,
                DatabaseName="retaildb",
                SslMode="require",
                PostgreSQLSettings={
                    "CaptureDdls":              False,
                    "MaxFileSize":              32768,
                    "DatabaseName":             "retaildb",
                    "DdlArtifactsSchema":       "public",
                    "ExecuteTimeout":           60,
                    "FailTasksOnLobTruncation": False,
                    "SlotName":                 "retail_dms_slot",
                    "PluginName":               "pglogical",
                },
                Tags=[{"Key": "Project", "Value": "retail-medallion"}],
            )
            log.info("Created DMS source endpoint: %s", DMS_SOURCE_ENDPOINT_ID)
        except self.dms.exceptions.ResourceAlreadyExistsFault:
            log.info("DMS source endpoint already exists.")

        # ── Target endpoint: S3 ───────────────────────────────────────────────
        s3_role = f"arn:aws:iam::{ACCOUNT_ID}:role/RetailDMSRole"
        try:
            self.dms.create_endpoint(
                EndpointIdentifier=DMS_TARGET_ENDPOINT_ID,
                EndpointType="target",
                EngineName="s3",
                S3Settings={
                    "ServiceAccessRoleArn":     s3_role,
                    "BucketName":               self.cfg.bucket_dms,
                    "BucketFolder":             "dms-full-load",
                    "CompressionType":          "GZIP",
                    "DataFormat":               "parquet",
                    "ParquetVersion":           "parquet-2-0",
                    "EnableStatistics":         True,
                    "IncludeOpForFullLoad":     True,
                    "CdcInsertsAndUpdates":     True,
                    "CdcInsertsOnly":           False,
                    "PreserveTransactions":     False,
                    "CdcPath":                  "cdc-dms",
                    "TimestampColumnName":       "__dms_timestamp",
                    "DatePartitionEnabled":     True,
                    "DatePartitionSequence":    "YYYYMMDD",
                    "DatePartitionDelimiter":   "NONE",
                    "UseTaskStartTimeForFullLoadTimestamp": True,
                },
                Tags=[{"Key": "Project", "Value": "retail-medallion"}],
            )
            log.info("Created DMS target endpoint: %s", DMS_TARGET_ENDPOINT_ID)
        except self.dms.exceptions.ResourceAlreadyExistsFault:
            log.info("DMS target endpoint already exists.")

    def test_connections(self) -> None:
        """Test both endpoints can connect via the replication instance."""
        instance_arn = self._instance_arn
        if not instance_arn:
            log.error("No replication instance ARN found.")
            return

        for ep_id in [DMS_SOURCE_ENDPOINT_ID, DMS_TARGET_ENDPOINT_ID]:
            # Resolve endpoint ARN
            resp = self.dms.describe_endpoints(
                Filters=[{"Name": "endpoint-id", "Values": [ep_id]}]
            )
            endpoints = resp.get("Endpoints", [])
            if not endpoints:
                log.warning("Endpoint not found: %s", ep_id)
                continue
            ep_arn = endpoints[0]["EndpointArn"]
            self.dms.test_connection(
                ReplicationInstanceArn=instance_arn,
                EndpointArn=ep_arn,
            )
            log.info("Connection test initiated for endpoint: %s", ep_id)

    # ──────────────────────────────────────────────────────────────────────────
    # Replication tasks
    # ──────────────────────────────────────────────────────────────────────────

    def _table_mappings(self) -> str:
        """Return DMS table mapping JSON for all 6 retail tables."""
        import json
        rules = []
        for i, table in enumerate(self.cfg.tables, start=1):
            rules.append({
                "rule-type": "selection",
                "rule-id":   str(i),
                "rule-name": f"include-{table}",
                "object-locator": {
                    "schema-name": "public",
                    "table-name":  table,
                },
                "rule-action": "include",
            })
        return json.dumps({"rules": rules})

    def _get_endpoint_arn(self, ep_id: str) -> str:
        resp = self.dms.describe_endpoints(
            Filters=[{"Name": "endpoint-id", "Values": [ep_id]}]
        )
        return resp["Endpoints"][0]["EndpointArn"]

    def create_full_load_task(self) -> dict:
        """Create a Full Load DMS task (initial one-time data load)."""
        try:
            resp = self.dms.create_replication_task(
                ReplicationTaskIdentifier=self.FULL_LOAD_TASK,
                SourceEndpointArn=self._get_endpoint_arn(DMS_SOURCE_ENDPOINT_ID),
                TargetEndpointArn=self._get_endpoint_arn(DMS_TARGET_ENDPOINT_ID),
                ReplicationInstanceArn=self._instance_arn,
                MigrationType="full-load",
                TableMappings=self._table_mappings(),
                ReplicationTaskSettings="""{
                    "TargetMetadata": {"TargetSchema": "", "SupportLobs": true,
                                       "FullLobMode": false, "LobChunkSize": 64},
                    "FullLoadSettings": {
                        "TargetTablePrepMode": "DROP_AND_CREATE",
                        "CreatePkAfterFullLoad": false,
                        "StopTaskCachedChangesApplied": false,
                        "StopTaskCachedChangesNotApplied": false,
                        "MaxFullLoadSubTasks": 8,
                        "TransactionConsistencyTimeout": 600,
                        "CommitRate": 50000
                    },
                    "Logging": {
                        "EnableLogging": true,
                        "LogComponents": [
                            {"Id": "SOURCE_UNLOAD", "Severity": "LOGGER_SEVERITY_DEFAULT"},
                            {"Id": "TARGET_LOAD",   "Severity": "LOGGER_SEVERITY_DEFAULT"}
                        ]
                    }
                }""",
                Tags=[{"Key": "Project", "Value": "retail-medallion"}],
            )
            log.info("Created Full Load DMS task: %s", self.FULL_LOAD_TASK)
            return resp["ReplicationTask"]
        except self.dms.exceptions.ResourceAlreadyExistsFault:
            log.info("Full Load DMS task already exists.")
            return {}

    def create_cdc_task(self) -> dict:
        """Create a CDC-only DMS task (ongoing replication after full load)."""
        try:
            resp = self.dms.create_replication_task(
                ReplicationTaskIdentifier=self.CDC_TASK,
                SourceEndpointArn=self._get_endpoint_arn(DMS_SOURCE_ENDPOINT_ID),
                TargetEndpointArn=self._get_endpoint_arn(DMS_TARGET_ENDPOINT_ID),
                ReplicationInstanceArn=self._instance_arn,
                MigrationType="cdc",
                TableMappings=self._table_mappings(),
                ReplicationTaskSettings="""{
                    "TargetMetadata": {"TargetSchema": "", "SupportLobs": true},
                    "FullLoadSettings": {"TargetTablePrepMode": "DO_NOTHING"},
                    "Logging": {
                        "EnableLogging": true,
                        "LogComponents": [
                            {"Id": "SOURCE_CDC",       "Severity": "LOGGER_SEVERITY_DEFAULT"},
                            {"Id": "TARGET_APPLY",     "Severity": "LOGGER_SEVERITY_DEFAULT"},
                            {"Id": "TASK_MANAGER",     "Severity": "LOGGER_SEVERITY_DEFAULT"}
                        ]
                    },
                    "ControlTablesSettings": {
                        "historyTimeslotInMinutes": 5,
                        "StatusTableEnabled": true,
                        "historyTableEnabled": true
                    }
                }""",
                Tags=[{"Key": "Project", "Value": "retail-medallion"}],
            )
            log.info("Created CDC DMS task: %s", self.CDC_TASK)
            return resp["ReplicationTask"]
        except self.dms.exceptions.ResourceAlreadyExistsFault:
            log.info("CDC DMS task already exists.")
            return {}

    def start_task(self, task_type: str = "full-load") -> None:
        """Start a replication task. task_type: 'full-load' or 'cdc'."""
        task_id = self.FULL_LOAD_TASK if task_type == "full-load" else self.CDC_TASK
        # Resolve task ARN
        resp = self.dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-id", "Values": [task_id]}]
        )
        tasks = resp.get("ReplicationTasks", [])
        if not tasks:
            log.error("Task not found: %s", task_id)
            return
        task_arn  = tasks[0]["ReplicationTaskArn"]
        start_type = "start-replication" if task_type == "full-load" else "resume-processing"
        self.dms.start_replication_task(
            ReplicationTaskArn=task_arn,
            StartReplicationTaskType=start_type,
        )
        log.info("Started DMS task: %s  type=%s", task_id, start_type)

    def task_status(self) -> dict[str, str]:
        """Return status of all retail DMS tasks."""
        resp   = self.dms.describe_replication_tasks(
            Filters=[{"Name": "replication-task-id",
                       "Values": [self.FULL_LOAD_TASK, self.CDC_TASK]}]
        )
        return {
            t["ReplicationTaskIdentifier"]: t["Status"]
            for t in resp.get("ReplicationTasks", [])
        }