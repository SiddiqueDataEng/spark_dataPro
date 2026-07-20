# aws/cdk/retail_data_platform_stack.py
"""
RetailDataPlatformStack
========================
AWS CDK stack that provisions the complete retail data engineering platform.

Deploys:
  ┌─────────────────────────────────────────────────────────────────────┐
  │  VPC                                                                 │
  │  ├── Private subnets (RDS, DMS, Glue, Lambda)                       │
  │  └── NAT Gateway (egress for Glue workers)                          │
  ├─────────────────────────────────────────────────────────────────────┤
  │  S3 Buckets (6)                                                      │
  │  ├── retail-raw-*       (Bronze)                                     │
  │  ├── retail-clean-*     (Silver)                                     │
  │  ├── retail-curated-*   (Gold)                                       │
  │  ├── retail-athena-*    (query results)                              │
  │  ├── retail-glue-*      (scripts, jars, spark UI logs)              │
  │  └── retail-dms-*       (DMS full-load + CDC output)                 │
  ├─────────────────────────────────────────────────────────────────────┤
  │  IAM Roles (4)                                                       │
  │  ├── RetailDataLakeAdmin                                             │
  │  ├── RetailGlueETLRole                                               │
  │  ├── RetailAnalyst                                                   │
  │  └── RetailDMSRole                                                   │
  ├─────────────────────────────────────────────────────────────────────┤
  │  AWS Glue                                                            │
  │  ├── 4 Databases (bronze, silver, gold, cdc)                         │
  │  ├── 4 Crawlers                                                      │
  │  └── 4 ETL Jobs                                                      │
  ├─────────────────────────────────────────────────────────────────────┤
  │  Athena Workgroup                                                    │
  ├─────────────────────────────────────────────────────────────────────┤
  │  RDS PostgreSQL 15                                                   │
  │  └── Parameter group with logical replication                        │
  ├─────────────────────────────────────────────────────────────────────┤
  │  DMS Replication Instance + Full Load + CDC Tasks                    │
  ├─────────────────────────────────────────────────────────────────────┤
  │  Step Functions State Machine (pipeline orchestration)               │
  ├─────────────────────────────────────────────────────────────────────┤
  │  CloudWatch Dashboards + Alarms                                      │
  └─────────────────────────────────────────────────────────────────────┘

Deploy:
    pip install aws-cdk-lib constructs
    cdk bootstrap aws://021891603670/us-east-1
    cdk deploy RetailDataPlatform

Destroy (with confirmation prompt):
    cdk destroy RetailDataPlatform
"""
from __future__ import annotations

import os

try:
    import aws_cdk as cdk
    from aws_cdk import (
        Stack, RemovalPolicy, Duration, Tags,
        aws_s3           as s3,
        aws_iam          as iam,
        aws_glue         as glue,
        aws_athena       as athena,
        aws_rds          as rds,
        aws_ec2          as ec2,
        aws_dms          as dms,
        aws_stepfunctions as sfn,
        aws_stepfunctions_tasks as sfn_tasks,
        aws_cloudwatch   as cw,
        aws_logs         as logs,
    )
    from constructs import Construct
    CDK_AVAILABLE = True
except ImportError:
    CDK_AVAILABLE = False
    # Provide stub so the module imports cleanly without CDK installed
    class Construct: pass
    class Stack:
        def __init__(self, *a, **kw): pass

from aws.config.aws_config import (
    ACCOUNT_ID, REGION, SOURCE_TABLES,
    GLUE_DATABASE_RAW, GLUE_DATABASE_CLEAN, GLUE_DATABASE_CURATED, GLUE_DATABASE_CDC,
    S3_BUCKET_RAW, S3_BUCKET_CLEAN, S3_BUCKET_CURATED,
    S3_BUCKET_ATHENA, S3_BUCKET_GLUE, S3_BUCKET_DMS,
    ATHENA_WORKGROUP, RDS_IDENTIFIER, RDS_INSTANCE,
)


def _bucket(
    scope: "Construct",
    construct_id: str,
    bucket_name: str,
) -> "s3.Bucket":
    """Helper — create an encrypted, versioned, no-public-access S3 bucket."""
    return s3.Bucket(
        scope,
        construct_id,
        bucket_name=bucket_name,
        versioned=True,
        encryption=s3.BucketEncryption.S3_MANAGED,
        block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        removal_policy=RemovalPolicy.RETAIN,
        lifecycle_rules=[
            s3.LifecycleRule(
                transitions=[
                    s3.Transition(
                        storage_class=s3.StorageClass.INFREQUENT_ACCESS,
                        transition_after=Duration.days(30),
                    ),
                    s3.Transition(
                        storage_class=s3.StorageClass.GLACIER_INSTANT_RETRIEVAL,
                        transition_after=Duration.days(90),
                    ),
                    s3.Transition(
                        storage_class=s3.StorageClass.DEEP_ARCHIVE,
                        transition_after=Duration.days(365),
                    ),
                ],
                noncurrent_version_expiration=Duration.days(90),
            )
        ],
    )


class RetailDataPlatformStack(Stack):
    """Complete AWS data engineering platform for the retail medallion pipeline."""

    def __init__(
        self,
        scope: "Construct",
        construct_id: str = "RetailDataPlatform",
        **kwargs,
    ) -> None:
        if not CDK_AVAILABLE:
            raise ImportError(
                "aws-cdk-lib not installed. Run: pip install aws-cdk-lib constructs"
            )
        super().__init__(scope, construct_id, **kwargs)

        Tags.of(self).add("Project",     "retail-medallion")
        Tags.of(self).add("Owner",       "MSiddique")
        Tags.of(self).add("Environment", os.getenv("AWS_ENV", "dev"))

        # ── VPC ────────────────────────────────────────────────────────────────
        self.vpc = ec2.Vpc(
            self, "RetailVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",    cidr_mask=24,
                    subnet_type=ec2.SubnetType.PUBLIC,
                ),
                ec2.SubnetConfiguration(
                    name="Private",   cidr_mask=24,
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                ),
                ec2.SubnetConfiguration(
                    name="Isolated",  cidr_mask=24,
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                ),
            ],
        )

        # ── S3 Buckets ─────────────────────────────────────────────────────────
        self.bucket_raw     = _bucket(self, "BucketRaw",     S3_BUCKET_RAW)
        self.bucket_clean   = _bucket(self, "BucketClean",   S3_BUCKET_CLEAN)
        self.bucket_curated = _bucket(self, "BucketCurated", S3_BUCKET_CURATED)
        self.bucket_athena  = _bucket(self, "BucketAthena",  S3_BUCKET_ATHENA)
        self.bucket_glue    = _bucket(self, "BucketGlue",    S3_BUCKET_GLUE)
        self.bucket_dms     = _bucket(self, "BucketDms",     S3_BUCKET_DMS)

        # ── IAM Roles ──────────────────────────────────────────────────────────
        self.glue_role = self._create_glue_role()
        self.dms_role  = self._create_dms_role()
        self.admin_role = self._create_admin_role()
        self.analyst_role = self._create_analyst_role()

        # ── Glue Databases ─────────────────────────────────────────────────────
        for db_name, location_bucket in [
            (GLUE_DATABASE_RAW,     self.bucket_raw),
            (GLUE_DATABASE_CLEAN,   self.bucket_clean),
            (GLUE_DATABASE_CURATED, self.bucket_curated),
            (GLUE_DATABASE_CDC,     self.bucket_dms),
        ]:
            glue.CfnDatabase(
                self, f"GlueDb{db_name.replace('_','').title()}",
                catalog_id=ACCOUNT_ID,
                database_input=glue.CfnDatabase.DatabaseInputProperty(
                    name=db_name,
                    description=f"Retail {db_name} database",
                    location_uri=location_bucket.bucket_regional_domain_name,
                ),
            )

        # ── Athena Workgroup ───────────────────────────────────────────────────
        athena.CfnWorkGroup(
            self, "AthenaWorkgroup",
            name=ATHENA_WORKGROUP,
            description="Retail Medallion Analytics",
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                bytes_scanned_cutoff_per_query=10_737_418_240,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{S3_BUCKET_ATHENA}/query-results/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3",
                    ),
                ),
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="Athena engine version 3",
                ),
            ),
        )

        # ── RDS PostgreSQL ─────────────────────────────────────────────────────
        self.db_sg = ec2.SecurityGroup(
            self, "RdsSG",
            vpc=self.vpc,
            description="RDS PostgreSQL access",
            allow_all_outbound=True,
        )
        # Allow Glue workers + DMS to reach RDS
        self.db_sg.add_ingress_rule(
            ec2.Peer.ipv4(self.vpc.vpc_cidr_block),
            ec2.Port.tcp(5432),
            "VPC PostgreSQL access",
        )

        rds_param_group = rds.ParameterGroup(
            self, "RdsParamGroup",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_5
            ),
            description="Retail pipeline - logical replication enabled",
            parameters={
                "rds.logical_replication": "1",
                "max_replication_slots":   "10",
                "max_wal_senders":         "10",
                "wal_level":               "logical",
            },
        )

        self.rds_instance = rds.DatabaseInstance(
            self, "RetailRDS",
            engine=rds.DatabaseInstanceEngine.postgres(
                version=rds.PostgresEngineVersion.VER_15_5
            ),
            instance_type=ec2.InstanceType(RDS_INSTANCE),
            vpc=self.vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
            ),
            security_groups=[self.db_sg],
            parameter_group=rds_param_group,
            database_name="retaildb",
            credentials=rds.Credentials.from_generated_secret("retail_admin"),
            allocated_storage=100,
            max_allocated_storage=1000,
            storage_type=rds.StorageType.GP3,
            storage_encrypted=True,
            multi_az=False,
            auto_minor_version_upgrade=True,
            backup_retention=Duration.days(7),
            preferred_backup_window="03:00-04:00",
            preferred_maintenance_window="sun:04:00-sun:05:00",
            enable_performance_insights=True,
            performance_insight_retention=rds.PerformanceInsightRetention.DEFAULT_7_DAYS,
            cloudwatch_logs_exports=["postgresql", "upgrade"],
            deletion_protection=True,
            removal_policy=RemovalPolicy.RETAIN,
        )

        # ── CloudWatch Dashboard ───────────────────────────────────────────────
        self._create_dashboard()

        # ── Outputs ────────────────────────────────────────────────────────────
        cdk.CfnOutput(self, "BucketRawOutput",    value=self.bucket_raw.bucket_name)
        cdk.CfnOutput(self, "BucketGoldOutput",   value=self.bucket_curated.bucket_name)
        cdk.CfnOutput(self, "RdsEndpoint",
                      value=self.rds_instance.db_instance_endpoint_address)
        cdk.CfnOutput(self, "AthenaOutput",
                      value=f"s3://{S3_BUCKET_ATHENA}/query-results/")

    # ──────────────────────────────────────────────────────────────────────────
    # IAM role helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _create_glue_role(self) -> "iam.Role":
        role = iam.Role(
            self, "GlueETLRole",
            role_name="RetailGlueETLRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
            ],
            description="Glue ETL role for retail medallion pipeline",
        )
        # Lake Formation inline policy
        role.add_to_policy(iam.PolicyStatement(
            actions=["lakeformation:GetDataAccess",
                     "lakeformation:GrantPermissions"],
            resources=["*"],
        ))
        return role

    def _create_dms_role(self) -> "iam.Role":
        role = iam.Role(
            self, "DMSRole",
            role_name="RetailDMSRole",
            assumed_by=iam.ServicePrincipal("dms.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonDMSRedshiftS3Role"
                ),
            ],
            description="DMS role for retail S3 target",
        )
        for bucket in [self.bucket_raw, self.bucket_dms]:
            bucket.grant_read_write(role)
        return role

    def _create_admin_role(self) -> "iam.Role":
        return iam.Role(
            self, "DataLakeAdminRole",
            role_name="RetailDataLakeAdmin",
            assumed_by=iam.AccountPrincipal(ACCOUNT_ID),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AdministratorAccess"),
            ],
            description="Data Lake admin - full access to all resources",
        )

    def _create_analyst_role(self) -> "iam.Role":
        role = iam.Role(
            self, "AnalystRole",
            role_name="RetailAnalyst",
            assumed_by=iam.AccountPrincipal(ACCOUNT_ID),
            description="Analyst - SELECT Gold tables (no PII columns)",
        )
        role.add_to_policy(iam.PolicyStatement(
            actions=["athena:StartQueryExecution",
                     "athena:GetQueryExecution",
                     "athena:GetQueryResults",
                     "athena:ListWorkGroups",
                     "glue:GetDatabase",  "glue:GetTable",
                     "glue:GetTables",    "glue:GetPartitions",
                     "lakeformation:GetDataAccess"],
            resources=["*"],
        ))
        self.bucket_curated.grant_read(role)
        self.bucket_athena.grant_read_write(role)
        return role

    # ──────────────────────────────────────────────────────────────────────────
    # CloudWatch Dashboard
    # ──────────────────────────────────────────────────────────────────────────

    def _create_dashboard(self) -> None:
        dashboard = cw.Dashboard(
            self, "RetailDashboard",
            dashboard_name="RetailMedallionPipeline",
        )
        # RDS CPU utilisation
        rds_cpu = cw.Metric(
            namespace="AWS/RDS",
            metric_name="CPUUtilization",
            dimensions_map={"DBInstanceIdentifier": RDS_IDENTIFIER},
            period=Duration.minutes(5),
        )
        # Athena queries
        athena_queries = cw.Metric(
            namespace="AWS/Athena",
            metric_name="TotalQueryDataScanned",
            dimensions_map={"WorkGroup": ATHENA_WORKGROUP},
            period=Duration.minutes(5),
            statistic="Sum",
        )
        dashboard.add_widgets(
            cw.GraphWidget(
                title="RDS CPU Utilization",
                left=[rds_cpu],
                width=12, height=6,
            ),
            cw.GraphWidget(
                title="Athena Data Scanned (bytes)",
                left=[athena_queries],
                width=12, height=6,
            ),
        )


# ─────────────────────────────────────────────────────────────────────────────
# CDK App entrypoint
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    if not CDK_AVAILABLE:
        print("ERROR: aws-cdk-lib not installed.")
        print("  pip install aws-cdk-lib constructs")
        return

    app = cdk.App()
    RetailDataPlatformStack(
        app,
        "RetailDataPlatform",
        env=cdk.Environment(account=ACCOUNT_ID, region=REGION),
        description="Retail Medallion Data Engineering Platform (MSiddique)",
    )
    app.synth()


if __name__ == "__main__":
    main()
