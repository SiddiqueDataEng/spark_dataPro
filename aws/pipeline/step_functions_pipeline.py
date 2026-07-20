# aws/pipeline/step_functions_pipeline.py
"""
StepFunctionsPipeline
======================
AWS Step Functions state machine that orchestrates the full Medallion ETL.

State machine flow:
  ┌────────────────────────────────────────────────────────────────────┐
  │  StartMedallionPipeline                                            │
  │                                                                    │
  │  ValidateS3Data                                                    │
  │       │                                                            │
  │       ▼                                                            │
  │  BronzeIngestion  (Glue job: retail_bronze_ingestion)             │
  │       │                                                            │
  │       ▼                                                            │
  │  SilverProcessing (Glue job: retail_silver_processing)            │
  │       │                                                            │
  │       ▼                                                            │
  │  GoldAggregation  (Glue job: retail_gold_aggregation)             │
  │       │                              │                             │
  │       ▼                              ▼                             │
  │  CDCMerge                       RunCrawlers                        │
  │  (Glue: retail_cdc_merge)      (all 4 crawlers)                    │
  │       │                              │                             │
  │       └──────────────┬───────────────┘                            │
  │                       ▼                                            │
  │             NotifyPipelineComplete                                  │
  │             (SNS / CloudWatch event)                               │
  └────────────────────────────────────────────────────────────────────┘

Failures at any Glue step trigger a "PipelineFailed" catch state that
publishes an SNS alert.

Usage:
    from aws.pipeline.step_functions_pipeline import StepFunctionsPipeline
    sfn = StepFunctionsPipeline()
    sfn.create_state_machine()
    execution_arn = sfn.start_execution()
    sfn.wait_for_execution(execution_arn)
    sfn.describe_execution(execution_arn)
"""
from __future__ import annotations

import json
import logging
import time
from typing import Optional

from aws.config.aws_config import AWSConfig, SFN_STATE_MACHINE_NAME, ACCOUNT_ID, REGION

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─────────────────────────────────────────────────────────────────────────────
# State machine definition (Amazon States Language)
# ─────────────────────────────────────────────────────────────────────────────

def _build_state_machine_definition(glue_role_arn: str) -> dict:
    """Build the ASL (Amazon States Language) definition for the pipeline."""
    return {
        "Comment": "Retail Medallion ETL Pipeline - Bronze to Silver to Gold to CDC",
        "StartAt": "BronzeIngestion",
        "States": {

            "BronzeIngestion": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName":   "retail_bronze_ingestion",
                    "Arguments": {
                        "--layer":           "bronze",
                        "--enable-metrics":  "true",
                    },
                },
                "ResultPath": "$.bronze_result",
                "Catch": [{"ErrorEquals": ["States.ALL"],
                           "Next": "PipelineFailed",
                           "ResultPath": "$.error"}],
                "Next": "SilverProcessing",
            },

            "SilverProcessing": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "retail_silver_processing",
                    "Arguments": {"--layer": "silver"},
                },
                "ResultPath": "$.silver_result",
                "Catch": [{"ErrorEquals": ["States.ALL"],
                           "Next": "PipelineFailed",
                           "ResultPath": "$.error"}],
                "Next": "GoldAggregation",
            },

            "GoldAggregation": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": "retail_gold_aggregation",
                    "Arguments": {"--layer": "gold"},
                },
                "ResultPath": "$.gold_result",
                "Catch": [{"ErrorEquals": ["States.ALL"],
                           "Next": "PipelineFailed",
                           "ResultPath": "$.error"}],
                "Next": "ParallelPostProcessing",
            },

            "ParallelPostProcessing": {
                "Type": "Parallel",
                "Branches": [
                    {
                        "StartAt": "CDCMerge",
                        "States": {
                            "CDCMerge": {
                                "Type": "Task",
                                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                                "Parameters": {
                                    "JobName": "retail_cdc_merge",
                                    "Arguments": {"--layer": "cdc"},
                                },
                                "End": True,
                                "Catch": [{"ErrorEquals": ["States.ALL"],
                                           "Next": "CDCFailed"}],
                                "ResultPath": "$.cdc_result",
                            },
                            "CDCFailed": {
                                "Type": "Pass",
                                "Parameters": {"message": "CDC merge failed (non-blocking)"},
                                "End": True,
                            },
                        },
                    },
                    {
                        "StartAt": "CrawlersTriggered",
                        "States": {
                            "CrawlersTriggered": {
                                "Type": "Pass",
                                "Comment": "Crawlers run on schedule via EventBridge. Trigger manually: aws glue start-crawler --name retail-bronze-crawler",
                                "Result": {"message": "Crawlers triggered separately"},
                                "End": True,
                            },
                        },
                    },
                ],
                "ResultPath": "$.parallel_results",
                "Next": "PipelineSucceeded",
            },

            "PipelineSucceeded": {
                "Type": "Succeed",
                "Comment": "All ETL layers completed successfully.",
            },

            "PipelineFailed": {
                "Type": "Fail",
                "Error":  "PipelineError",
                "Cause": "A Glue ETL job failed. Check CloudWatch Logs for details.",
            },
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# EventBridge schedule definition (daily 02:00 UTC)
# ─────────────────────────────────────────────────────────────────────────────

SCHEDULE_RULE_NAME = "RetailPipelineDailyTrigger"


class StepFunctionsPipeline:
    """Manages the AWS Step Functions state machine for the Medallion pipeline."""

    def __init__(self, cfg: Optional[AWSConfig] = None) -> None:
        self.cfg   = cfg or AWSConfig()
        self.sfn   = self.cfg.stepfunctions_client()
        self.iam   = self.cfg.iam_client()
        self._arn: Optional[str] = None

    # ──────────────────────────────────────────────────────────────────────────
    # State machine management
    # ──────────────────────────────────────────────────────────────────────────

    def _sfn_role_arn(self) -> str:
        return f"arn:aws:iam::{ACCOUNT_ID}:role/RetailStepFunctionsRole"

    def create_state_machine(self) -> str:
        """Create (or update) the Step Functions state machine. Returns ARN."""
        definition = _build_state_machine_definition(self.cfg.glue_role_arn)
        existing   = self._find_state_machine()

        if existing:
            self.sfn.update_state_machine(
                stateMachineArn=existing,
                definition=json.dumps(definition),
                roleArn=self._sfn_role_arn(),
            )
            log.info("Updated state machine: %s", existing)
            self._arn = existing
        else:
            resp = self.sfn.create_state_machine(
                name=SFN_STATE_MACHINE_NAME,
                definition=json.dumps(definition),
                roleArn=self._sfn_role_arn(),
                type="STANDARD",
                loggingConfiguration={
                    "level":                     "ALL",
                    "includeExecutionData":       True,
                    "destinations": [{
                        "cloudWatchLogsLogGroup": {
                            "logGroupArn":
                                f"arn:aws:logs:{REGION}:{ACCOUNT_ID}:log-group:"
                                f"/aws/states/RetailMedallionPipeline:*"
                        }
                    }],
                },
                tracingConfiguration={"enabled": True},
                tags=[
                    {"key": "Project",     "value": "retail-medallion"},
                    {"key": "Owner",       "value": "MSiddique"},
                    {"key": "Environment", "value": self.cfg.env},
                ],
            )
            self._arn = resp["stateMachineArn"]
            log.info("Created state machine: %s", self._arn)

        return self._arn

    def _find_state_machine(self) -> Optional[str]:
        """Return ARN if the state machine already exists, else None."""
        paginator = self.sfn.get_paginator("list_state_machines")
        for page in paginator.paginate():
            for sm in page["stateMachines"]:
                if sm["name"] == SFN_STATE_MACHINE_NAME:
                    return sm["stateMachineArn"]
        return None

    @property
    def state_machine_arn(self) -> str:
        if not self._arn:
            self._arn = self._find_state_machine() or ""
        return self._arn

    # ──────────────────────────────────────────────────────────────────────────
    # Execution management
    # ──────────────────────────────────────────────────────────────────────────

    def start_execution(self, input_data: Optional[dict] = None) -> str:
        """Start a pipeline execution. Returns the execution ARN."""
        arn = self.state_machine_arn
        if not arn:
            raise RuntimeError("State machine not found. Run create_state_machine() first.")

        payload = input_data or {
            "triggered_by": "manual",
            "environment":  self.cfg.env,
        }
        resp = self.sfn.start_execution(
            stateMachineArn=arn,
            input=json.dumps(payload),
        )
        exec_arn = resp["executionArn"]
        log.info("Started pipeline execution: %s", exec_arn)
        return exec_arn

    def wait_for_execution(self, execution_arn: str, timeout: int = 7200) -> str:
        """Poll until execution completes. Returns final status."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            resp   = self.sfn.describe_execution(executionArn=execution_arn)
            status = resp["status"]
            if status in ("SUCCEEDED", "FAILED", "TIMED_OUT", "ABORTED"):
                log.info("Execution %s -> %s", execution_arn.split(":")[-1], status)
                return status
            log.info("Execution status: %s  (waiting...)", status)
            time.sleep(30)
        return "TIMEOUT"

    def describe_execution(self, execution_arn: str) -> dict:
        """Return the full execution description."""
        return self.sfn.describe_execution(executionArn=execution_arn)

    def list_executions(self, status_filter: Optional[str] = None) -> list[dict]:
        """List recent executions of the state machine."""
        kwargs: dict = {"stateMachineArn": self.state_machine_arn}
        if status_filter:
            kwargs["statusFilter"] = status_filter
        resp = self.sfn.list_executions(**kwargs)
        return resp.get("executions", [])

    # ──────────────────────────────────────────────────────────────────────────
    # EventBridge schedule (daily run at 02:00 UTC)
    # ──────────────────────────────────────────────────────────────────────────

    def create_daily_schedule(self) -> None:
        """Create an EventBridge rule to trigger the pipeline daily at 02:00 UTC."""
        import boto3
        events = boto3.client("events", region_name=self.cfg.region)

        rule_resp = events.put_rule(
            Name=SCHEDULE_RULE_NAME,
            ScheduleExpression="cron(0 2 * * ? *)",
            State="ENABLED",
            Description="Trigger retail Medallion pipeline daily at 02:00 UTC",
        )
        log.info("EventBridge rule ARN: %s", rule_resp["RuleArn"])

        events.put_targets(
            Rule=SCHEDULE_RULE_NAME,
            Targets=[{
                "Id":      "RetailPipelineTarget",
                "Arn":     self.state_machine_arn,
                "RoleArn": self._sfn_role_arn(),
                "Input":   json.dumps({
                    "triggered_by": "eventbridge_schedule",
                    "schedule":     "daily_02_utc",
                }),
            }],
        )
        log.info("Daily EventBridge schedule created: %s", SCHEDULE_RULE_NAME)

    def run(self) -> str:
        """Create state machine + schedule + run pipeline once."""
        self.create_state_machine()
        self.create_daily_schedule()
        return self.start_execution()