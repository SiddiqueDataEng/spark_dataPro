# aws/__init__.py
"""
AWS Data Engineering Integration
=================================
Complete AWS data platform layer for the Retail Medallion Pipeline.

Components:
  config/          - AWS credentials, S3/Glue/Athena/RDS/Lake Formation config
  s3/              - S3 bronze/silver/gold sync, lifecycle, partitioning
  glue/            - Glue ETL jobs (Spark-based), crawlers, Data Catalog
  athena/          - Athena SQL analytics (mirrors Spark SQL layer)
  rds/             - RDS PostgreSQL: schema, migration, DMS replication
  lake_formation/  - Data Lake permissions, governance, column-level security
  cdk/             - Infrastructure-as-Code (CDK stacks)
  pipeline/        - End-to-end AWS orchestration (Step Functions / MWAA)

Account: 0218-9160-3670  (MSiddique)
Region:  us-east-1
"""
