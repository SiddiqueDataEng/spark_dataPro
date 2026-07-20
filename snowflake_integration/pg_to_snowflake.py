# snowflake/pg_to_snowflake.py
"""
PostgreSQL → Snowflake ETL Pipeline
====================================
Reads every retail table from the Neon PostgreSQL source using psycopg2,
then writes it to Snowflake using the snowflake-connector-python bulk-insert
(write_pandas) approach.

Tables migrated
---------------
  customers  products  employees  stores  orders  sales

Usage (standalone)
------------------
    python -m snowflake_integration.pg_to_snowflake                   # all tables
    python -m snowflake_integration.pg_to_snowflake --tables customers,products
    python -m snowflake_integration.pg_to_snowflake --create-schema   # DDL only, no data

Requirements
------------
    pip install psycopg2-binary pandas snowflake-connector-python[pandas]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

# Make project root importable when executed directly
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

load_dotenv()

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source table definitions
# ---------------------------------------------------------------------------

# Map table name → primary key (used for MERGE / ordering)
TABLES: dict[str, str] = {
    "customers": "customer_id",
    "products":  "product_id",
    "employees": "employee_id",
    "stores":    "store_id",
    "orders":    "order_id",
    "sales":     "sale_id",
}

# Snowflake DDL for each table — mirrors the PostgreSQL schema.
# All column names are upper-cased to match Snowflake conventions.
SNOWFLAKE_DDL: dict[str, str] = {
    "customers": """
        CREATE TABLE IF NOT EXISTS CUSTOMERS (
            CUSTOMER_ID   INTEGER      PRIMARY KEY,
            FIRST_NAME    VARCHAR(100),
            LAST_NAME     VARCHAR(100),
            EMAIL         VARCHAR(200),
            PHONE         VARCHAR(50),
            ADDRESS       VARCHAR(300),
            CITY          VARCHAR(100),
            COUNTRY       VARCHAR(100),
            GENDER        VARCHAR(20),
            JOIN_DATE     DATE,
            UPDATED_AT    TIMESTAMP_NTZ
        )
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS PRODUCTS (
            PRODUCT_ID    INTEGER      PRIMARY KEY,
            PRODUCT_NAME  VARCHAR(200),
            CATEGORY      VARCHAR(100),
            COST_PRICE    FLOAT,
            SELLING_PRICE FLOAT,
            STOCK         INTEGER,
            UPDATED_AT    TIMESTAMP_NTZ
        )
    """,
    "employees": """
        CREATE TABLE IF NOT EXISTS EMPLOYEES (
            EMPLOYEE_ID   INTEGER      PRIMARY KEY,
            EMPLOYEE_NAME VARCHAR(200),
            DEPARTMENT    VARCHAR(100),
            SALARY        FLOAT,
            HIRE_DATE     DATE,
            STORE_ID      INTEGER,
            UPDATED_AT    TIMESTAMP_NTZ
        )
    """,
    "stores": """
        CREATE TABLE IF NOT EXISTS STORES (
            STORE_ID      INTEGER      PRIMARY KEY,
            STORE_NAME    VARCHAR(200),
            CITY          VARCHAR(100),
            COUNTRY       VARCHAR(100),
            REGION        VARCHAR(100),
            UPDATED_AT    TIMESTAMP_NTZ
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS ORDERS (
            ORDER_ID      INTEGER      PRIMARY KEY,
            CUSTOMER_ID   INTEGER,
            EMPLOYEE_ID   INTEGER,
            STORE_ID      INTEGER,
            ORDER_DATE    DATE,
            STATUS        VARCHAR(50),
            UPDATED_AT    TIMESTAMP_NTZ
        )
    """,
    "sales": """
        CREATE TABLE IF NOT EXISTS SALES (
            SALE_ID       INTEGER      PRIMARY KEY,
            ORDER_ID      INTEGER,
            PRODUCT_ID    INTEGER,
            QUANTITY      INTEGER,
            UNIT_PRICE    FLOAT,
            DISCOUNT      FLOAT,
            TOTAL         FLOAT,
            PROFIT        FLOAT,
            UPDATED_AT    TIMESTAMP_NTZ
        )
    """,
}


# ---------------------------------------------------------------------------
# PostgreSQL helpers
# ---------------------------------------------------------------------------

def _pg_connection() -> psycopg2.extensions.connection:
    """Open a PostgreSQL connection using .env credentials."""
    return psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        sslmode=os.getenv("DB_SSLMODE", "require"),
        connect_timeout=30,
    )


def read_postgres_table(table: str) -> pd.DataFrame:
    """
    Read an entire PostgreSQL table into a pandas DataFrame.

    Column names are upper-cased to match Snowflake conventions so that
    write_pandas does not need a column-name mapping.
    """
    log.info("Reading PostgreSQL table: %s", table)
    conn = _pg_connection()
    try:
        cur = conn.cursor()
        cur.execute(f'SELECT * FROM public."{table}"')  # noqa: S608
        columns = [desc[0].upper() for desc in cur.description]
        rows = cur.fetchall()
        df = pd.DataFrame(rows, columns=columns)
    finally:
        conn.close()
    log.info("  → %d rows, %d columns", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Snowflake helpers
# ---------------------------------------------------------------------------

def _sf_connection() -> snowflake.connector.SnowflakeConnection:
    """Open a Snowflake connection using .env credentials."""
    from config.snowflake_config import SnowflakeConfig
    return SnowflakeConfig().get_connection()


def ensure_schema(conn: snowflake.connector.SnowflakeConnection,
                  database: str,
                  schema: str) -> None:
    """Create the target database and schema if they do not already exist."""
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {database}")
    cur.execute(f"USE DATABASE {database}")
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    cur.execute(f"USE SCHEMA {schema}")
    cur.close()
    log.info("Schema ready: %s.%s", database, schema)


def create_tables(conn: snowflake.connector.SnowflakeConnection) -> None:
    """Run DDL for all tables (CREATE TABLE IF NOT EXISTS)."""
    cur = conn.cursor()
    for table, ddl in SNOWFLAKE_DDL.items():
        log.info("Creating table if not exists: %s", table.upper())
        cur.execute(ddl)
    cur.close()
    log.info("All DDL complete.")


def load_table(
    conn: snowflake.connector.SnowflakeConnection,
    table: str,
    df: pd.DataFrame,
    mode: str = "overwrite",
) -> None:
    """
    Load a pandas DataFrame into a Snowflake table.

    mode='overwrite'  → TRUNCATE then INSERT  (full refresh)
    mode='append'     → INSERT only            (incremental)
    """
    sf_table = table.upper()
    cur = conn.cursor()

    if mode == "overwrite":
        log.info("Truncating %s before reload …", sf_table)
        cur.execute(f"TRUNCATE TABLE IF EXISTS {sf_table}")
    cur.close()

    success, nchunks, nrows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=sf_table,
        auto_create_table=False,   # we created the tables with explicit DDL
        overwrite=False,           # truncate handled above
    )

    if success:
        log.info("  ✅ Loaded %d rows into %s (%d chunks)", nrows, sf_table, nchunks)
    else:
        raise RuntimeError(f"write_pandas failed for table {sf_table}")


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class PgToSnowflake:
    """
    Orchestrates the full PostgreSQL → Snowflake migration for the retail schema.

    Quick start:
        pipeline = PgToSnowflake()
        pipeline.run()                        # all tables, overwrite
        pipeline.run(tables=["customers"])    # single table
        pipeline.run(mode="append")           # incremental
    """

    def __init__(self) -> None:
        from config.snowflake_config import SnowflakeConfig
        self._sf_cfg = SnowflakeConfig()

    def run(
        self,
        tables: Optional[list[str]] = None,
        mode: str = "overwrite",
        create_schema: bool = False,
    ) -> dict[str, int]:
        """
        Execute the ETL pipeline.

        Parameters
        ----------
        tables : list[str] | None
            Subset of tables to migrate. None = all tables.
        mode : str
            'overwrite' (default) truncates the Snowflake table before loading.
            'append' adds rows without removing existing ones.
        create_schema : bool
            If True, also create the database/schema/tables in Snowflake.
            Safe to run on an already-initialised warehouse.

        Returns
        -------
        dict mapping table name → row count loaded.
        """
        targets = tables if tables else list(TABLES.keys())
        results: dict[str, int] = {}

        log.info("Opening Snowflake connection …")
        conn = _sf_connection()

        try:
            db  = self._sf_cfg.database
            sch = self._sf_cfg.schema

            ensure_schema(conn, db, sch)

            if create_schema:
                create_tables(conn)

            for table in targets:
                if table not in TABLES:
                    log.warning("Unknown table '%s' — skipping.", table)
                    continue

                log.info("─" * 50)
                log.info("Migrating: %s", table)

                # 1. Read from PostgreSQL
                df = read_postgres_table(table)

                # 2. Write to Snowflake
                load_table(conn, table, df, mode=mode)

                results[table] = len(df)

        finally:
            conn.close()
            log.info("Snowflake connection closed.")

        log.info("=" * 50)
        log.info("Migration complete.  Tables loaded: %d", len(results))
        for t, n in results.items():
            log.info("  %-15s %d rows", t, n)

        return results

    # ------------------------------------------------------------------
    # Convenience: run a quick connectivity test
    # ------------------------------------------------------------------

    def test_connection(self) -> None:
        """Verify both PostgreSQL and Snowflake are reachable."""
        print("Testing PostgreSQL connection …")
        with _pg_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT VERSION()")
            ver = cur.fetchone()[0]
            print(f"  ✅ PostgreSQL: {ver[:60]}")

        print("Testing Snowflake connection …")
        with _sf_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT CURRENT_VERSION(), CURRENT_WAREHOUSE(), CURRENT_DATABASE()")
            row = cur.fetchone()
            print(f"  ✅ Snowflake version : {row[0]}")
            print(f"     Warehouse        : {row[1]}")
            print(f"     Database         : {row[2]}")


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="PostgreSQL → Snowflake ETL for the retail schema."
    )
    p.add_argument(
        "--tables",
        default="",
        help="Comma-separated list of tables to migrate (default: all).",
    )
    p.add_argument(
        "--mode",
        choices=["overwrite", "append"],
        default="overwrite",
        help="Load mode: overwrite (default) truncates before insert; append adds rows.",
    )
    p.add_argument(
        "--create-schema",
        action="store_true",
        help="Create Snowflake database / schema / tables before loading.",
    )
    p.add_argument(
        "--test",
        action="store_true",
        help="Only test connectivity to both databases, then exit.",
    )
    return p.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _parse_args()
    pipeline = PgToSnowflake()

    if args.test:
        pipeline.test_connection()
        sys.exit(0)

    tables = [t.strip() for t in args.tables.split(",") if t.strip()] or None
    pipeline.run(
        tables=tables,
        mode=args.mode,
        create_schema=args.create_schema,
    )
