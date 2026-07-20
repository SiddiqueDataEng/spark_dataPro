# aws/rds/rds_postgres.py
"""
RDSPostgres
===========
Manages an Amazon RDS for PostgreSQL instance — the source OLTP database
that replaces the Neon cloud PostgreSQL for the AWS-native architecture.

Responsibilities:
  - Provision an RDS PostgreSQL 15 instance (Multi-AZ optional)
  - Create the retail database schema (6 tables mirroring Neon)
  - Migrate data FROM Neon/local PostgreSQL INTO RDS via psycopg2
  - Configure parameter group for logical replication (needed by DMS)
  - Enable Enhanced Monitoring + Performance Insights
  - Create a DMS-compatible read replica for CDC replication

Usage:
    from aws.rds.rds_postgres import RDSPostgres
    rds = RDSPostgres()
    rds.create_parameter_group()     # enable logical replication
    rds.create_instance()            # provisions RDS (takes ~5-10 min)
    rds.wait_for_available()
    rds.create_schema()              # CREATE TABLE statements
    rds.migrate_from_source()        # copy rows from Neon → RDS
"""
from __future__ import annotations

import logging
import os
import time
from typing import Optional

from aws.config.aws_config import AWSConfig, RDS_IDENTIFIER, RDS_INSTANCE, RDS_PORT

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─────────────────────────────────────────────────────────────────────────────
# DDL — retail schema
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA_SQL = """
-- Retail OLTP schema for RDS PostgreSQL
-- Mirrors the Neon schema used in the original Medallion pipeline

CREATE TABLE IF NOT EXISTS customers (
    customer_id  SERIAL PRIMARY KEY,
    first_name   VARCHAR(100)        NOT NULL,
    last_name    VARCHAR(100)        NOT NULL,
    email        VARCHAR(255) UNIQUE NOT NULL,
    phone        VARCHAR(50),
    city         VARCHAR(100),
    country      VARCHAR(100),
    gender       VARCHAR(20),
    join_date    DATE,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
    product_id    SERIAL PRIMARY KEY,
    product_name  VARCHAR(255)  NOT NULL,
    category      VARCHAR(100),
    selling_price NUMERIC(10,2) NOT NULL,
    cost_price    NUMERIC(10,2) NOT NULL,
    profit_margin NUMERIC(5,2)  GENERATED ALWAYS AS
                    (ROUND((selling_price - cost_price) /
                           NULLIF(selling_price, 0) * 100, 2)) STORED,
    stock         INTEGER DEFAULT 0,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stores (
    store_id    SERIAL PRIMARY KEY,
    store_name  VARCHAR(255) NOT NULL,
    city        VARCHAR(100),
    country     VARCHAR(100),
    region      VARCHAR(100),
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS employees (
    employee_id  SERIAL PRIMARY KEY,
    first_name   VARCHAR(100) NOT NULL,
    last_name    VARCHAR(100) NOT NULL,
    department   VARCHAR(100),
    salary       NUMERIC(10,2),
    hire_date    DATE,
    store_id     INTEGER REFERENCES stores(store_id),
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    order_id     SERIAL PRIMARY KEY,
    customer_id  INTEGER REFERENCES customers(customer_id),
    store_id     INTEGER REFERENCES stores(store_id),
    employee_id  INTEGER REFERENCES employees(employee_id),
    order_date   DATE NOT NULL,
    status       VARCHAR(50) DEFAULT 'completed',
    discount_pct NUMERIC(5,4) DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sales (
    sale_id       SERIAL PRIMARY KEY,
    order_id      INTEGER REFERENCES orders(order_id),
    product_id    INTEGER REFERENCES products(product_id),
    quantity      INTEGER NOT NULL,
    unit_price    NUMERIC(10,2) NOT NULL,
    total_revenue NUMERIC(12,2) GENERATED ALWAYS AS
                    (ROUND(quantity * unit_price, 2)) STORED,
    profit        NUMERIC(12,2),
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for join performance
CREATE INDEX IF NOT EXISTS idx_orders_customer  ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_store     ON orders(store_id);
CREATE INDEX IF NOT EXISTS idx_orders_date      ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_sales_order      ON sales(order_id);
CREATE INDEX IF NOT EXISTS idx_sales_product    ON sales(product_id);

-- updated_at trigger (required for Timestamp-based CDC)
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        'customers','products','stores','employees','orders','sales'
    ]
    LOOP
        EXECUTE format(
            'DROP TRIGGER IF EXISTS trg_updated_at ON %I;
             CREATE TRIGGER trg_updated_at
             BEFORE UPDATE ON %I
             FOR EACH ROW EXECUTE FUNCTION update_updated_at();', t, t);
    END LOOP;
END $$;
"""


class RDSPostgres:
    """Manage the Amazon RDS PostgreSQL instance for the retail pipeline."""

    PARAM_GROUP_NAME  = "retail-pg15-logical"
    SUBNET_GROUP_NAME = "retail-rds-subnet-group"
    DB_USER           = "retail_admin"

    def __init__(self, cfg: Optional[AWSConfig] = None) -> None:
        self.cfg  = cfg or AWSConfig()
        self.rds  = self.cfg.rds_client()

    # ──────────────────────────────────────────────────────────────────────────
    # Parameter group  (logical replication for DMS CDC)
    # ──────────────────────────────────────────────────────────────────────────

    def create_parameter_group(self) -> None:
        """
        Create an RDS parameter group with logical replication enabled.
        This is required for AWS DMS log-based CDC to work.
        """
        try:
            self.rds.create_db_parameter_group(
                DBParameterGroupName=   self.PARAM_GROUP_NAME,
                DBParameterGroupFamily= "postgres15",
                Description=            "Retail pipeline: logical replication enabled",
                Tags=[
                    {"Key": "Project", "Value": "retail-medallion"},
                    {"Key": "Owner",   "Value": "MSiddique"},
                ],
            )
            log.info("Created parameter group: %s", self.PARAM_GROUP_NAME)
        except self.rds.exceptions.DBParameterGroupAlreadyExistsFault:
            log.info("Parameter group already exists: %s", self.PARAM_GROUP_NAME)

        # Enable logical replication
        self.rds.modify_db_parameter_group(
            DBParameterGroupName=self.PARAM_GROUP_NAME,
            Parameters=[
                {
                    "ParameterName":  "rds.logical_replication",
                    "ParameterValue": "1",
                    "ApplyMethod":    "pending-reboot",
                },
                {
                    "ParameterName":  "max_replication_slots",
                    "ParameterValue": "10",
                    "ApplyMethod":    "pending-reboot",
                },
                {
                    "ParameterName":  "max_wal_senders",
                    "ParameterValue": "10",
                    "ApplyMethod":    "pending-reboot",
                },
                {
                    "ParameterName":  "wal_level",
                    "ParameterValue": "logical",
                    "ApplyMethod":    "pending-reboot",
                },
            ],
        )
        log.info("Logical replication parameters applied.")

    # ──────────────────────────────────────────────────────────────────────────
    # Instance provisioning
    # ──────────────────────────────────────────────────────────────────────────

    def create_instance(
        self,
        db_password: Optional[str] = None,
        multi_az:    bool = False,
    ) -> dict:
        """
        Provision an RDS PostgreSQL 15 instance.

        Args:
            db_password: Master password. Falls back to env var RDS_PASSWORD.
            multi_az:    Enable Multi-AZ for HA (increases cost).
        """
        password = db_password or os.getenv("RDS_PASSWORD", "RetailAdmin#2024!")

        try:
            resp = self.rds.create_db_instance(
                DBInstanceIdentifier=   RDS_IDENTIFIER,
                DBInstanceClass=        RDS_INSTANCE,
                Engine=                 "postgres",
                EngineVersion=          "15.5",
                MasterUsername=         self.DB_USER,
                MasterUserPassword=     password,
                DBName=                 "retaildb",
                AllocatedStorage=       100,             # GB
                MaxAllocatedStorage=    1000,            # auto-scaling ceiling
                StorageType=            "gp3",
                StorageEncrypted=       True,
                MultiAZ=                multi_az,
                AutoMinorVersionUpgrade=True,
                BackupRetentionPeriod=  7,               # days
                PreferredBackupWindow=  "03:00-04:00",
                PreferredMaintenanceWindow="sun:04:00-sun:05:00",
                DBParameterGroupName=   self.PARAM_GROUP_NAME,
                EnablePerformanceInsights=True,
                PerformanceInsightsRetentionPeriod=7,
                MonitoringInterval=     60,              # Enhanced Monitoring (seconds)
                EnableCloudwatchLogsExports=["postgresql", "upgrade"],
                DeletionProtection=     True,
                Tags=[
                    {"Key": "Project",     "Value": "retail-medallion"},
                    {"Key": "Owner",       "Value": "MSiddique"},
                    {"Key": "Environment", "Value": self.cfg.env},
                ],
            )
            log.info("RDS instance creation initiated: %s", RDS_IDENTIFIER)
            return resp["DBInstance"]
        except self.rds.exceptions.DBInstanceAlreadyExistsFault:
            log.info("RDS instance already exists: %s", RDS_IDENTIFIER)
            return self.describe()

    def wait_for_available(self, timeout: int = 900) -> bool:
        """Wait until the RDS instance reaches 'available' status."""
        log.info("Waiting for RDS instance to become available...")
        waiter = self.rds.get_waiter("db_instance_available")
        try:
            waiter.wait(
                DBInstanceIdentifier=RDS_IDENTIFIER,
                WaiterConfig={"Delay": 30, "MaxAttempts": timeout // 30},
            )
            log.info("RDS instance is available: %s", RDS_IDENTIFIER)
            return True
        except Exception as exc:
            log.error("RDS wait failed: %s", exc)
            return False

    def describe(self) -> dict:
        """Return the RDS instance description dict."""
        resp = self.rds.describe_db_instances(
            DBInstanceIdentifier=RDS_IDENTIFIER
        )
        return resp["DBInstances"][0]

    @property
    def endpoint(self) -> Optional[str]:
        """Return the RDS endpoint hostname (None if not yet available)."""
        inst = self.describe()
        ep   = inst.get("Endpoint")
        return ep["Address"] if ep else None

    # ──────────────────────────────────────────────────────────────────────────
    # Schema + data migration
    # ──────────────────────────────────────────────────────────────────────────

    def create_schema(self, db_password: Optional[str] = None) -> None:
        """Execute the retail DDL against the RDS instance."""
        try:
            import psycopg as pg          # psycopg v3
        except ImportError:
            import psycopg2 as pg         # psycopg2 fallback

        host     = self.endpoint
        password = db_password or os.getenv("RDS_PASSWORD", "RetailAdmin#2024!")

        if not host:
            raise RuntimeError("RDS instance endpoint not available yet.")

        conn = pg.connect(
            host=host, port=RDS_PORT, dbname="retaildb",
            user=self.DB_USER, password=password, sslmode="require",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        conn.close()
        log.info("Retail schema created on RDS: %s", host)

    def migrate_from_source(
        self,
        source_conn_str: Optional[str] = None,
        rds_password:    Optional[str] = None,
    ) -> None:
        """
        Copy all rows from the source PostgreSQL (Neon) into RDS.

        Uses psycopg copy_expert for bulk transfer via intermediate CSV.
        Migration order respects FK dependencies:
          stores → customers → products → employees → orders → sales
        """
        try:
            import psycopg as pg
        except ImportError:
            import psycopg2 as pg

        import io

        src_str = source_conn_str or os.getenv("DATABASE_URL")
        rds_pwd = rds_password or os.getenv("RDS_PASSWORD", "RetailAdmin#2024!")
        rds_host = self.endpoint

        if not src_str or not rds_host:
            raise RuntimeError(
                "SOURCE DATABASE_URL and RDS endpoint are required."
            )

        src_conn = pg.connect(src_str, sslmode="require")
        rds_conn = pg.connect(
            host=rds_host, port=RDS_PORT, dbname="retaildb",
            user=self.DB_USER, password=rds_pwd, sslmode="require",
        )

        tables = ["stores", "customers", "products", "employees", "orders", "sales"]

        for table in tables:
            # Extract from source
            buf = io.StringIO()
            with src_conn.cursor() as cur:
                cur.copy_expert(
                    f"COPY {table} TO STDOUT WITH (FORMAT CSV, HEADER TRUE)", buf
                )
            buf.seek(0)

            # Load into RDS
            with rds_conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE")
                cur.copy_expert(
                    f"COPY {table} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", buf
                )
            rds_conn.commit()
            log.info("Migrated table: %s", table)

        src_conn.close()
        rds_conn.close()
        log.info("Migration from source to RDS complete.")

    # ──────────────────────────────────────────────────────────────────────────
    # Snapshot / backup helpers
    # ──────────────────────────────────────────────────────────────────────────

    def create_snapshot(self, snapshot_id: Optional[str] = None) -> str:
        """Create a manual DB snapshot. Returns the snapshot identifier."""
        snap_id = snapshot_id or f"{RDS_IDENTIFIER}-manual-{int(time.time())}"
        self.rds.create_db_snapshot(
            DBSnapshotIdentifier=snap_id,
            DBInstanceIdentifier=RDS_IDENTIFIER,
            Tags=[{"Key": "Project", "Value": "retail-medallion"}],
        )
        log.info("Snapshot initiated: %s", snap_id)
        return snap_id