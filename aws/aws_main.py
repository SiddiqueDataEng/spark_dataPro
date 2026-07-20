#!/usr/bin/env python3
# aws/aws_main.py
"""
AWS Data Engineering Platform — Main Entrypoint
================================================
Complete provisioning and operation of the retail AWS data platform.

Account: 021891603670  (MSiddique)
Region:  us-east-1

Services:
  S3            — Bronze / Silver / Gold data lake
  AWS Glue      — ETL jobs + Data Catalog + Crawlers
  Amazon Athena — Serverless SQL analytics (16 queries)
  Amazon RDS    — PostgreSQL 15 source database
  AWS DMS       — Full-load + CDC replication to S3
  Lake Formation— Column-level security + data governance
  Step Functions— Pipeline orchestration (daily schedule)
  IAM           — Roles and policies for all services

Usage:
    # Full infrastructure setup (first time)
    python -m aws.aws_main --action setup

    # Upload local Delta Lake data to S3
    python -m aws.aws_main --action sync-s3

    # Run ETL pipeline via Step Functions
    python -m aws.aws_main --action run-pipeline

    # Run Athena analytics
    python -m aws.aws_main --action athena

    # Individual services
    python -m aws.aws_main --action iam
    python -m aws.aws_main --action s3
    python -m aws.aws_main --action glue
    python -m aws.aws_main --action rds
    python -m aws.aws_main --action dms
    python -m aws.aws_main --action lakeformation
    python -m aws.aws_main --action stepfunctions

    # CDK (Infrastructure-as-Code deploy)
    python -m aws.aws_main --action cdk-synth
    python -m aws.aws_main --action cdk-deploy
"""
from __future__ import annotations

import argparse
import logging
import sys

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)


def _header() -> None:
    print("""
╔══════════════════════════════════════════════════════════════════╗
║         Retail AWS Data Engineering Platform                     ║
║         Account: 021891603670  (MSiddique)                       ║
║         Region:  us-east-1                                       ║
║                                                                  ║
║  S3 · Glue · Athena · RDS · DMS · Lake Formation · Step Fns     ║
╚══════════════════════════════════════════════════════════════════╝
""")


def action_setup(args) -> None:
    """Full infrastructure provisioning (first-time setup)."""
    from aws.config.aws_config import AWSConfig
    cfg = AWSConfig()

    log.info("Step 1/7 - IAM Roles")
    from aws.iam.iam_roles import IAMRoles
    IAMRoles(cfg).create_all_roles()

    log.info("Step 2/7 - S3 Data Lake buckets")
    from aws.s3.s3_data_lake import S3DataLake
    S3DataLake(cfg).create_buckets()

    log.info("Step 3/7 - Glue Data Catalog")
    from aws.glue.glue_catalog import GlueCatalog
    GlueCatalog(cfg).run()

    log.info("Step 4/7 - Glue ETL Jobs")
    from aws.glue.glue_jobs import GlueJobs
    GlueJobs(cfg).create_jobs()

    log.info("Step 5/7 - Athena Workgroup")
    from aws.athena.athena_analytics import AthenaAnalytics
    AthenaAnalytics(cfg).setup_workgroup()

    log.info("Step 6/7 - Lake Formation Governance")
    from aws.lake_formation.lake_formation_governance import LakeFormationGovernance
    LakeFormationGovernance(cfg).run()

    log.info("Step 7/7 - Step Functions State Machine")
    from aws.pipeline.step_functions_pipeline import StepFunctionsPipeline
    StepFunctionsPipeline(cfg).create_state_machine()

    log.info("=== AWS platform setup complete ===")
    log.info("Next steps:")
    log.info("  1. Provision RDS: python -m aws.aws_main --action rds")
    log.info("  2. Set up DMS:    python -m aws.aws_main --action dms")
    log.info("  3. Sync S3 data:  python -m aws.aws_main --action sync-s3")
    log.info("  4. Run pipeline:  python -m aws.aws_main --action run-pipeline")


def action_sync_s3(args) -> None:
    """Upload local Delta Lake Parquet files to S3."""
    from aws.s3.s3_data_lake import S3DataLake
    lake = S3DataLake()
    lake.run()


def action_iam(args) -> None:
    from aws.iam.iam_roles import IAMRoles
    roles = IAMRoles().create_all_roles()
    for name, arn in roles.items():
        print(f"  {name:<35} {arn}")


def action_s3(args) -> None:
    from aws.s3.s3_data_lake import S3DataLake
    S3DataLake().create_buckets()


def action_glue(args) -> None:
    from aws.glue.glue_catalog import GlueCatalog
    from aws.glue.glue_jobs import GlueJobs
    GlueCatalog().run()
    GlueJobs().create_jobs()


def action_rds(args) -> None:
    from aws.rds.rds_postgres import RDSPostgres
    db = RDSPostgres()
    db.create_parameter_group()
    db.create_instance()
    if db.wait_for_available():
        db.create_schema()
        log.info("RDS endpoint: %s", db.endpoint)


def action_dms(args) -> None:
    from aws.dms.dms_replication import DMSReplication
    rep = DMSReplication()
    rep.create_replication_instance()
    rep.wait_for_instance()
    rep.create_endpoints()
    rep.test_connections()
    rep.create_full_load_task()
    rep.create_cdc_task()
    log.info("DMS tasks ready. Start with:")
    log.info("  rep.start_task('full-load')  - initial data load")
    log.info("  rep.start_task('cdc')        - ongoing CDC")


def action_lakeformation(args) -> None:
    from aws.lake_formation.lake_formation_governance import LakeFormationGovernance
    LakeFormationGovernance().run()


def action_athena(args) -> None:
    from aws.athena.athena_analytics import AthenaAnalytics
    ath = AthenaAnalytics()
    ath.setup_workgroup()

    # Run all 16 analyses
    results = ath.run_all(save_csv=True)
    print(f"\n{'-'*60}")
    print(f"{'Query':<30} {'Rows':>6}")
    print(f"{'-'*60}")
    for name, df in results.items():
        print(f"  {name:<28} {len(df):>6}")
    print(f"{'-'*60}")
    print(f"Results saved to results/athena/")


def action_stepfunctions(args) -> None:
    from aws.pipeline.step_functions_pipeline import StepFunctionsPipeline
    sfn = StepFunctionsPipeline()
    sfn.create_state_machine()
    sfn.create_daily_schedule()
    log.info("State machine and daily schedule created.")


def action_run_pipeline(args) -> None:
    from aws.pipeline.step_functions_pipeline import StepFunctionsPipeline
    sfn = StepFunctionsPipeline()
    exec_arn = sfn.start_execution()
    log.info("Pipeline started: %s", exec_arn)

    final_status = sfn.wait_for_execution(exec_arn)
    log.info("Pipeline final status: %s", final_status)

    if final_status != "SUCCEEDED":
        log.error("Pipeline failed. Check CloudWatch Logs for details.")
        sys.exit(1)


def action_glue_crawlers(args) -> None:
    from aws.glue.glue_catalog import GlueCatalog
    GlueCatalog().run_crawlers(wait=True)


def action_cdk_synth(args) -> None:
    from aws.cdk.retail_data_platform_stack import main
    main()


def action_test(args) -> None:
    """Test AWS credentials and connectivity."""
    from aws.config.aws_config import AWSConfig
    try:
        cfg = AWSConfig()
        cfg.test_connection()
        print(f"\n  Region:  {cfg.region}")
        print(f"  Env:     {cfg.env}")
        print(f"\n  Bronze bucket:  {cfg.bucket_raw}")
        print(f"  Silver bucket:  {cfg.bucket_clean}")
        print(f"  Gold bucket:    {cfg.bucket_curated}")
    except Exception as exc:
        if "NoCredentialsError" in type(exc).__name__ or "NoCredentials" in str(exc):
            print("""
ERROR: No AWS credentials found.

You have two options to fix this:

Option 1 — Interactive setup (recommended):
    python aws/setup_credentials.py

Option 2 — Manual setup:
    1. Go to: https://021891603670.signin.aws.amazon.com/console
    2. Sign in as MSiddique10x (password in aws/.env_aws)
    3. Click username (top-right) -> Security credentials
    4. Access keys -> Create access key -> CLI type
    5. Add to your .env file:
         AWS_ACCESS_KEY_ID=AKIA...
         AWS_SECRET_ACCESS_KEY=<your-secret>
    6. OR run: aws configure  (after installing AWS CLI)

Install AWS CLI:
    https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html
""")
        else:
            print(f"ERROR: {exc}")
        sys.exit(1)


def action_status(args) -> None:
    """Print quick status of all deployed resources."""
    from aws.config.aws_config import AWSConfig
    cfg = AWSConfig()

    print("\n=== AWS Retail Data Platform Status ===")
    print(f"Account: {cfg.account_id}  ({cfg.account_name})")
    print(f"Region:  {cfg.region}")
    print()

    # S3 bucket stats
    try:
        from aws.s3.s3_data_lake import S3DataLake
        stats = S3DataLake(cfg).bucket_stats()
        print("S3 Data Lake:")
        for layer, info in stats.items():
            print(f"  {layer:<8} {info['objects']:>5} objects  {info['size_mb']:>8.2f} MB  "
                  f"({info['bucket']})")
    except Exception as exc:
        print(f"  S3 status unavailable: {exc}")

    # DMS task status
    try:
        from aws.dms.dms_replication import DMSReplication
        task_status = DMSReplication(cfg).task_status()
        print("\nDMS Tasks:")
        for task, status in task_status.items():
            print(f"  {task:<35} {status}")
    except Exception as exc:
        print(f"  DMS status unavailable: {exc}")

    # Step Functions
    try:
        from aws.pipeline.step_functions_pipeline import StepFunctionsPipeline
        sfn = StepFunctionsPipeline(cfg)
        execs = sfn.list_executions()[:3]
        print("\nRecent Pipeline Executions:")
        for ex in execs:
            print(f"  {ex.get('status','?'):<12}  {ex.get('startDate','?')}")
    except Exception as exc:
        print(f"  Step Functions status unavailable: {exc}")

    print()


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

ACTIONS = {
    "test":           action_test,
    "setup":          action_setup,
    "sync-s3":        action_sync_s3,
    "iam":            action_iam,
    "s3":             action_s3,
    "glue":           action_glue,
    "rds":            action_rds,
    "dms":            action_dms,
    "lakeformation":  action_lakeformation,
    "athena":         action_athena,
    "stepfunctions":  action_stepfunctions,
    "run-pipeline":   action_run_pipeline,
    "crawlers":       action_glue_crawlers,
    "cdk-synth":      action_cdk_synth,
    "status":         action_status,
}


def main() -> None:
    _header()

    parser = argparse.ArgumentParser(
        prog="python -m aws.aws_main",
        description="AWS Retail Data Engineering Platform CLI",
    )
    parser.add_argument(
        "--action",
        choices=list(ACTIONS.keys()),
        required=True,
        help="Action to perform",
    )
    args = parser.parse_args()

    log.info("Action: %s", args.action)
    try:
        ACTIONS[args.action](args)
    except Exception as exc:
        exc_name = type(exc).__name__
        msg = str(exc)
        if exc_name == "NoCredentialsError" or "Unable to locate credentials" in msg:
            print("""
ERROR: AWS credentials not configured.
Run:  python aws/setup_credentials.py
""")
            sys.exit(1)
        if "AccessDenied" in msg or "not authorized to perform" in msg:
            # Extract the denied action from the error message
            action_hint = ""
            if "iam:CreateRole" in msg:
                action_hint = "iam:CreateRole"
            elif "s3:" in msg:
                action_hint = "S3"
            elif "glue:" in msg:
                action_hint = "Glue"
            elif "athena:" in msg:
                action_hint = "Athena"
            elif "rds:" in msg:
                action_hint = "RDS"

            print(f"""
ERROR: Permission denied{f' ({action_hint})' if action_hint else ''}.

The IAM user MSiddique10x needs policies attached before running this.

Quick fix — run this to see the exact console steps:
    python aws/grant_permissions.py

Or attach AdministratorAccess directly:
  1. https://021891603670.signin.aws.amazon.com/console
  2. IAM -> Users -> MSiddique10x -> Add permissions
  3. Attach: AdministratorAccess
""")
            sys.exit(1)
        raise


if __name__ == "__main__":
    main()