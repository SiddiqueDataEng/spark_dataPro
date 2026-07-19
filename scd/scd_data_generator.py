"""
SCD Data Generator
==================
Generates realistic dimension changes to drive all SCD demonstrations.

Each dimension table gets a dedicated generator that produces a mix of:
  * New members  (INSERT) — brand new dimension rows
  * Attribute changes (UPDATE) — changes to tracked attributes
  * Stable data   (unchanged rows, to prove no false positives)

The generator writes directly to PostgreSQL so every SCD strategy
can pull fresh changes on each demo run.

Design
------
  * Pure psycopg2 — no Spark dependency at change-generation time
  * One generator class per dimension table (Customers, Products, Employees,
    Stores) plus a generic BatchGenerator that drives all of them
  * Change log is persisted to  scd/scd_events/scd_changes.jsonl  so the
    SCD pipeline can replay or audit what changed
"""

from __future__ import annotations

import json
import logging
import os
import random
from datetime import date, datetime, timedelta
from pathlib import Path

import psycopg2
from faker import Faker
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)
fake = Faker()

# ── Output ───────────────────────────────────────────────────────────────────
SCD_EVENTS_DIR  = Path(__file__).resolve().parent / "scd_events"
SCD_EVENTS_DIR.mkdir(parents=True, exist_ok=True)
SCD_CHANGES_LOG = SCD_EVENTS_DIR / "scd_changes.jsonl"

# ── Domain data ───────────────────────────────────────────────────────────────
CATEGORIES   = ["Electronics", "Clothing", "Books", "Furniture", "Sports", "Food"]
DEPARTMENTS  = ["Sales", "IT", "Finance", "HR", "Marketing", "Operations"]
ORDER_STATUS = ["Completed", "Pending", "Cancelled", "Returned"]

PRICE_TIERS  = {
    "Electronics": (200.0, 1500.0),
    "Clothing":    (15.0,  200.0),
    "Books":       (8.0,   80.0),
    "Furniture":   (100.0, 2000.0),
    "Sports":      (20.0,  500.0),
    "Food":        (5.0,   50.0),
}

SALARY_BANDS = {
    "Sales":      (35_000, 90_000),
    "IT":         (55_000, 140_000),
    "Finance":    (50_000, 120_000),
    "HR":         (40_000, 85_000),
    "Marketing":  (45_000, 100_000),
    "Operations": (38_000, 95_000),
}


# ─────────────────────────────────────────────────────────────────────────────
# DB helpers
# ─────────────────────────────────────────────────────────────────────────────

def _connect():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        port=int(os.getenv("DB_PORT", 5432)),
        sslmode="require",
    )


def _max_id(cur, table: str, pk: str) -> int:
    cur.execute(f"SELECT COALESCE(MAX({pk}), 0) FROM {table}")
    return cur.fetchone()[0]


def _rand_id(cur, table: str, pk: str) -> int | None:
    cur.execute(f"SELECT {pk} FROM {table} ORDER BY RANDOM() LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None


def _fetch_row(cur, table: str, pk: str, pk_val: int) -> dict:
    cur.execute(f"SELECT * FROM {table} WHERE {pk} = %s", (pk_val,))
    row = cur.fetchone()
    if not row:
        return {}
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, [str(v) if v is not None else None for v in row]))


# ─────────────────────────────────────────────────────────────────────────────
# Per-dimension change generators
# ─────────────────────────────────────────────────────────────────────────────

class CustomerChangeGenerator:
    """
    Generates realistic customer dimension changes.

    Tracked attributes (SCD columns)
    ---------------------------------
    * city, country  — customer relocates
    * email          — customer updates contact
    * gender         — correction / update
    """

    TABLE = "customers"
    PK    = "customer_id"

    # Attributes we track for SCD purposes
    SCD_ATTRS = ["city", "country", "email"]

    def insert(self, cur) -> dict | None:
        row = (
            fake.first_name(), fake.last_name(),
            random.choice(["Male", "Female"]),
            fake.unique.email(), fake.city(), fake.country(),
            fake.date_between("-3y", "today").isoformat(),
        )
        cur.execute(
            "INSERT INTO customers "
            "(first_name,last_name,gender,email,city,country,join_date) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING customer_id",
            row,
        )
        new_id = cur.fetchone()[0]
        return {
            "table": self.TABLE, "op": "INSERT", "pk": new_id,
            "before": None,
            "after": {
                "customer_id": new_id,
                "first_name": row[0], "last_name": row[1],
                "gender": row[2], "email": row[3],
                "city": row[4], "country": row[5],
                "join_date": str(row[6]),
            },
            "changed_attrs": list(self.SCD_ATTRS),
        }

    def update(self, cur) -> dict | None:
        pk = _rand_id(cur, self.TABLE, self.PK)
        if not pk:
            return None
        before = _fetch_row(cur, self.TABLE, self.PK, pk)

        # Pick 1-2 SCD attributes to change
        attrs = random.sample(self.SCD_ATTRS, k=random.randint(1, 2))
        updates = {}
        for attr in attrs:
            if attr == "city":
                updates[attr] = fake.city()
            elif attr == "country":
                updates[attr] = fake.country()
            elif attr == "email":
                updates[attr] = fake.unique.email()

        set_clause = ", ".join(f"{k}=%s" for k in updates)
        cur.execute(
            f"UPDATE customers SET {set_clause} WHERE customer_id=%s",
            list(updates.values()) + [pk],
        )
        after = {**before, **{k: str(v) for k, v in updates.items()}}
        return {
            "table": self.TABLE, "op": "UPDATE", "pk": pk,
            "before": before, "after": after,
            "changed_attrs": attrs,
        }


class ProductChangeGenerator:
    """
    Generates product dimension changes.

    Tracked attributes
    ------------------
    * selling_price  — price adjustments (common, low-cardinality SCD)
    * category       — reclassification
    * stock          — inventory adjustment (often excluded from SCD)
    """

    TABLE    = "products"
    PK       = "product_id"
    SCD_ATTRS = ["selling_price", "category"]

    def insert(self, cur) -> dict | None:
        category = random.choice(CATEGORIES)
        lo, hi   = PRICE_TIERS[category]
        cost     = round(random.uniform(lo * 0.4, hi * 0.6), 2)
        price    = round(cost * random.uniform(1.3, 2.5), 2)
        stock    = random.randint(0, 500)
        name     = f"{fake.word().title()} {fake.word().title()}"
        cur.execute(
            "INSERT INTO products "
            "(product_name,category,cost_price,selling_price,stock) "
            "VALUES (%s,%s,%s,%s,%s) RETURNING product_id",
            (name, category, cost, price, stock),
        )
        new_id = cur.fetchone()[0]
        return {
            "table": self.TABLE, "op": "INSERT", "pk": new_id,
            "before": None,
            "after": {
                "product_id": new_id, "product_name": name,
                "category": category, "cost_price": str(cost),
                "selling_price": str(price), "stock": str(stock),
            },
            "changed_attrs": list(self.SCD_ATTRS),
        }

    def update(self, cur) -> dict | None:
        pk = _rand_id(cur, self.TABLE, self.PK)
        if not pk:
            return None
        before = _fetch_row(cur, self.TABLE, self.PK, pk)

        attrs = random.sample(self.SCD_ATTRS, k=random.randint(1, 2))
        updates = {}
        for attr in attrs:
            if attr == "selling_price":
                # Price change ±5-30%
                current = float(before.get("selling_price") or 100)
                delta   = random.uniform(-0.30, 0.30)
                updates[attr] = round(max(1.0, current * (1 + delta)), 2)
            elif attr == "category":
                new_cat = random.choice([c for c in CATEGORIES
                                         if c != before.get("category")])
                updates[attr] = new_cat

        set_clause = ", ".join(f"{k}=%s" for k in updates)
        cur.execute(
            f"UPDATE products SET {set_clause} WHERE product_id=%s",
            list(updates.values()) + [pk],
        )
        after = {**before, **{k: str(v) for k, v in updates.items()}}
        return {
            "table": self.TABLE, "op": "UPDATE", "pk": pk,
            "before": before, "after": after,
            "changed_attrs": attrs,
        }


class EmployeeChangeGenerator:
    """
    Generates employee dimension changes.

    Tracked attributes
    ------------------
    * department  — internal transfers
    * salary      — pay reviews
    """

    TABLE     = "employees"
    PK        = "employee_id"
    SCD_ATTRS = ["department", "salary"]

    def insert(self, cur) -> dict | None:
        dept   = random.choice(DEPARTMENTS)
        lo, hi = SALARY_BANDS[dept]
        salary = random.randint(lo, hi)
        name   = fake.name()
        cur.execute(
            "INSERT INTO employees (employee_name,department,salary) "
            "VALUES (%s,%s,%s) RETURNING employee_id",
            (name, dept, salary),
        )
        new_id = cur.fetchone()[0]
        return {
            "table": self.TABLE, "op": "INSERT", "pk": new_id,
            "before": None,
            "after": {
                "employee_id": new_id, "employee_name": name,
                "department": dept, "salary": str(salary),
            },
            "changed_attrs": list(self.SCD_ATTRS),
        }

    def update(self, cur) -> dict | None:
        pk = _rand_id(cur, self.TABLE, self.PK)
        if not pk:
            return None
        before = _fetch_row(cur, self.TABLE, self.PK, pk)

        attrs = random.sample(self.SCD_ATTRS, k=random.randint(1, 2))
        updates: dict = {}
        for attr in attrs:
            if attr == "department":
                new_dept = random.choice(
                    [d for d in DEPARTMENTS if d != before.get("department")]
                )
                updates[attr] = new_dept
            elif attr == "salary":
                current = int(float(before.get("salary") or 50000))
                # Annual raise 3-15% or reduction (rare)
                pct = random.uniform(0.03, 0.15) * random.choice([1, 1, 1, -1])
                updates[attr] = max(25_000, round(current * (1 + pct)))

        set_clause = ", ".join(f"{k}=%s" for k in updates)
        cur.execute(
            f"UPDATE employees SET {set_clause} WHERE employee_id=%s",
            list(updates.values()) + [pk],
        )
        after = {**before, **{k: str(v) for k, v in updates.items()}}
        return {
            "table": self.TABLE, "op": "UPDATE", "pk": pk,
            "before": before, "after": after,
            "changed_attrs": attrs,
        }


class StoreChangeGenerator:
    """
    Generates store dimension changes.

    Tracked attributes
    ------------------
    * city, country  — store relocation / re-assignment
    * store_name     — rebranding
    """

    TABLE     = "stores"
    PK        = "store_id"
    SCD_ATTRS = ["city", "country", "store_name"]

    def insert(self, cur) -> dict | None:
        max_id = _max_id(cur, self.TABLE, self.PK)
        name   = f"Store {max_id + 1} — {fake.city()}"
        city   = fake.city()
        country = fake.country()
        cur.execute(
            "INSERT INTO stores (store_name,city,country) "
            "VALUES (%s,%s,%s) RETURNING store_id",
            (name, city, country),
        )
        new_id = cur.fetchone()[0]
        return {
            "table": self.TABLE, "op": "INSERT", "pk": new_id,
            "before": None,
            "after": {
                "store_id": new_id, "store_name": name,
                "city": city, "country": country,
            },
            "changed_attrs": list(self.SCD_ATTRS),
        }

    def update(self, cur) -> dict | None:
        pk = _rand_id(cur, self.TABLE, self.PK)
        if not pk:
            return None
        before = _fetch_row(cur, self.TABLE, self.PK, pk)

        attrs = random.sample(self.SCD_ATTRS, k=random.randint(1, 2))
        updates: dict = {}
        for attr in attrs:
            if attr == "city":
                updates[attr] = fake.city()
            elif attr == "country":
                updates[attr] = fake.country()
            elif attr == "store_name":
                updates[attr] = f"Store {pk} — {fake.city()}"

        set_clause = ", ".join(f"{k}=%s" for k in updates)
        cur.execute(
            f"UPDATE stores SET {set_clause} WHERE store_id=%s",
            list(updates.values()) + [pk],
        )
        after = {**before, **{k: str(v) for k, v in updates.items()}}
        return {
            "table": self.TABLE, "op": "UPDATE", "pk": pk,
            "before": before, "after": after,
            "changed_attrs": attrs,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Batch generator — drives all dimension generators
# ─────────────────────────────────────────────────────────────────────────────

class SCDDataGenerator:
    """
    Generates a batch of dimension changes across all tracked tables
    and persists them to  scd/scd_events/scd_changes.jsonl.

    Parameters
    ----------
    n            : total number of change events to generate
    insert_ratio : fraction of events that are INSERTs (default 0.25)
    table_filter : restrict to a single table name (optional)
    append_log   : append to existing log file (default True)
    """

    # Distribution across dimensions
    TABLE_WEIGHTS = {
        "customers": 0.35,
        "products":  0.25,
        "employees": 0.25,
        "stores":    0.15,
    }

    GENERATORS = {
        "customers": CustomerChangeGenerator(),
        "products":  ProductChangeGenerator(),
        "employees": EmployeeChangeGenerator(),
        "stores":    StoreChangeGenerator(),
    }

    def __init__(
        self,
        n: int = 50,
        insert_ratio: float = 0.25,
        table_filter: str | None = None,
        append_log: bool = True,
    ):
        self.n            = n
        self.insert_ratio = insert_ratio
        self.table_filter = table_filter
        self._log_mode    = "a" if append_log else "w"
        self._conn        = _connect()

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()

    # ── Public API ────────────────────────────────────────────────────────────

    def generate(self) -> list[dict]:
        """
        Generate *n* dimension changes, write to JSONL log, return event list.
        """
        tables  = [self.table_filter] if self.table_filter else list(self.TABLE_WEIGHTS.keys())
        weights = [self.TABLE_WEIGHTS[t] for t in tables]

        events: list[dict] = []
        with open(SCD_CHANGES_LOG, self._log_mode, encoding="utf-8") as fh:
            with self._conn.cursor() as cur:
                for _ in range(self.n):
                    table = random.choices(tables, weights=weights, k=1)[0]
                    gen   = self.GENERATORS[table]

                    op = "INSERT" if random.random() < self.insert_ratio else "UPDATE"
                    try:
                        if op == "INSERT":
                            event = gen.insert(cur)
                        else:
                            event = gen.update(cur)

                        if not event:
                            continue

                        # Wrap with SCD envelope
                        envelope = self._envelope(event)
                        self._conn.commit()

                        events.append(envelope)
                        fh.write(json.dumps(envelope, default=str) + "\n")
                        fh.flush()

                    except Exception as exc:
                        self._conn.rollback()
                        log.warning("SCD change failed (%s %s): %s", op, table, exc)

        print(f"✅ SCDDataGenerator: {len(events)} events → {SCD_CHANGES_LOG}")
        self._print_summary(events)
        return events

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _envelope(event: dict) -> dict:
        return {
            **event,
            "generated_at": datetime.utcnow().isoformat(),
            "effective_date": date.today().isoformat(),
        }

    @staticmethod
    def _print_summary(events: list[dict]):
        from collections import Counter
        counts: Counter = Counter(
            (e["table"], e["op"]) for e in events
        )
        print("\n  SCD Change Summary:")
        print(f"  {'Table':<15}  {'Op':<8}  {'Count':>6}")
        print("  " + "─" * 35)
        for (tbl, op), cnt in sorted(counts.items()):
            sym = "➕" if op == "INSERT" else "✏️ "
            print(f"  {tbl:<15}  {sym} {op:<8}  {cnt:>6}")
        print()

    def load_events(self) -> list[dict]:
        """Re-load all events from the JSONL log file."""
        if not SCD_CHANGES_LOG.exists():
            return []
        events = []
        with open(SCD_CHANGES_LOG, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    try:
                        events.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
        return events

    def clear_log(self):
        """Truncate the change log (useful before a fresh demo run)."""
        SCD_CHANGES_LOG.write_text("")
        print("🗑️  SCD change log cleared.")


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Generate SCD dimension changes")
    parser.add_argument("--changes", type=int, default=30)
    parser.add_argument("--table", type=str, default=None)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    gen = SCDDataGenerator(
        n=args.changes,
        table_filter=args.table,
        append_log=not args.overwrite,
    )
    try:
        gen.generate()
    finally:
        gen.close()
