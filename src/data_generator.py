"""
data_generator.py
-----------------
Generates a complete, referentially-consistent synthetic banking dataset
from scratch — no database connection required.

Tables produced (in dependency order) — columns match database_schema.cql exactly:
    branch      → no deps
    customer    → no deps
    employee    → manager_id self-references (10 % are managers)
    product     → no deps
    account     → customer_id, branch_id
    transaction → account_id, product_id, approved_by_emp_id

Usage examples
--------------
    # Print counts only (dry run):
    python data_generator.py

    # Custom sizes:
    python data_generator.py --branches 20 --customers 500 --employees 80 \
        --products 30 --accounts 2000 --transactions 10000

    # Write JSON files to ./output/:
    python data_generator.py --out-dir ./output

    # Write CSV files to ./output/:
    python data_generator.py --out-dir ./output --format csv

    # Write to AstraDB (reads ASTRA_* env vars / .env):
    python data_generator.py --astra

    # Combine — write files AND load into Astra:
    python data_generator.py --out-dir ./output --astra
"""

from __future__ import annotations

import argparse
import csv
import datetime
import json
import math
import random
import uuid
from dataclasses import dataclass, asdict
from pathlib import Path

from cassandra.cluster import BatchStatement
from cassandra.query import BatchType

import astra_client

# ─────────────────────────────── domain dictionaries ───────────────────────────

SEGMENTS = ["Retail", "HNI", "Corporate"]
SEGMENT_WEIGHTS = [0.7, 0.2, 0.1]

CITIES = ["Mumbai", "Delhi", "Bengaluru", "Chennai", "Pune", "Hyderabad"]
CITY_WEIGHTS = [30, 15, 10, 10, 20, 15]
REGIONS = ["North", "South", "East", "West", "Central"]

CHANNELS = ["Digital", "Branch", "POS"]
CHANNEL_WEIGHTS = [0.6, 0.3, 0.1]

TXN_TYPES = ["Deposit", "Withdrawal", "Transfer", "POS", "Fee", "EMI"]

EMPLOYEE_ROLES = [
    "Manager",
    "Assistant Manager",
    "Teller",
    "Loan Officer",
    "Customer Service",
]

PRODUCT_CATEGORIES = [
    "Savings",
    "Current",
    "Loan",
    "Credit Card",
    "Investment",
    "Insurance",
]

_ACCOUNT_TYPES = ["Savings", "Current", "Loan", "Credit Card", "Investment", "Insurance"]
_ACCOUNT_STATUSES = ["Active", "Inactive", "Frozen"]
_ACCOUNT_STATUS_WEIGHTS = [0.8, 0.15, 0.05]

_TXN_STATUSES = ["approved", "pending", "declined"]
_TXN_STATUS_WEIGHTS = [0.70, 0.20, 0.10]

_CURRENCIES = ["USD", "EUR", "GBP", "INR", "AUD"]
_CURRENCY_WEIGHTS = [0.50, 0.15, 0.10, 0.20, 0.05]

# ──────────────────────────────── dataclasses ───────────────────────────────────

@dataclass
class Branch:
    branch_id: str
    branch_name: str
    city: str
    region: str


@dataclass
class Customer:
    customer_id: str
    name: str
    dob: str           # date  (ISO-8601 date string)
    customer_segment: str
    pan_hash: str
    city: str
    created_at: str    # timestamp (ISO-8601 datetime string)


@dataclass
class Employee:
    emp_id: str
    name: str
    role: str
    branch_id: str
    manager_id: str | None   # None only for the root manager
    hire_date: str     # date (ISO-8601 date string)


@dataclass
class Product:
    product_id: str
    product_name: str
    product_category: str


@dataclass
class Account:
    account_id: str
    customer_id: str
    account_type: str
    status: str
    branch_id: str
    opened_at: str     # timestamp (ISO-8601 datetime string)
    closed_at: str | None


@dataclass
class Transaction:
    txn_id: str
    account_id: str
    txn_type: str
    amount: float
    currency: str
    txn_time: str
    channel: str
    status: str
    product_id: str
    approved_by_emp_id: str | None


# ──────────────────────────────── helpers ───────────────────────────────────────

def _uid() -> str:
    return str(uuid.uuid4())


def _date_in_past(years_back: int = 5) -> str:
    days = random.randint(0, years_back * 365)
    d = datetime.date.today() - datetime.timedelta(days=days)
    return d.isoformat()


def _dt_in_past(days_back: int = 365) -> str:
    seconds = random.randint(0, days_back * 86_400)
    dt = datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(seconds=seconds)
    return dt.isoformat()


def _sample_amount(mu: float = 4.5, sigma: float = 1.5,
                   lo: float = 1.0, hi: float = 100_000.0) -> float:
    raw = random.lognormvariate(mu, sigma)
    return round(max(lo, min(raw, hi)), 2)


def _pan_hash(seq: int) -> str:
    """Deterministic fake PAN hash — not a real PAN, just a plausible hex digest."""
    import hashlib
    return hashlib.sha256(f"PAN-{seq}".encode()).hexdigest()[:16].upper()


_FIRST_NAMES = [
    "Aarav", "Aisha", "Arjun", "Deepa", "Farhan", "Gita", "Harish",
    "Isha", "Jayesh", "Kavya", "Lakshmi", "Mohan", "Neha", "Omar",
    "Priya", "Rahul", "Sana", "Tarun", "Uma", "Vijay",
]
_LAST_NAMES = [
    "Sharma", "Patel", "Iyer", "Khan", "Mehta", "Reddy", "Singh",
    "Joshi", "Nair", "Verma", "Gupta", "Das", "Rao", "Pillai", "Malhotra",
]


def _full_name() -> str:
    return f"{random.choice(_FIRST_NAMES)} {random.choice(_LAST_NAMES)}"


# ──────────────────────────────── generators ────────────────────────────────────

def generate_branches(n: int) -> list[Branch]:
    branches = []
    for i in range(n):
        city = random.choices(CITIES, weights=CITY_WEIGHTS, k=1)[0]
        region = random.choice(REGIONS)
        branches.append(Branch(
            branch_id=_uid(),
            branch_name=f"{city} Branch {i + 1}",
            city=city,
            region=region,
        ))
    return branches


def generate_customers(n: int) -> list[Customer]:
    customers = []
    for i in range(n):
        city = random.choices(CITIES, weights=CITY_WEIGHTS, k=1)[0]
        customers.append(Customer(
            customer_id=_uid(),
            name=_full_name(),
            dob=_date_in_past(years_back=60),
            customer_segment=random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0],
            pan_hash=_pan_hash(i),
            city=city,
            created_at=_dt_in_past(days_back=365 * 7),
        ))
    return customers


def generate_employees(n: int, branches: list[Branch]) -> list[Employee]:
    """
    Exactly 10% of employees (rounded up, minimum 1) are managers.
    Every non-manager is assigned a manager_id pointing to a real manager.
    The first manager created is the root of the hierarchy (manager_id = None).
    Subsequent managers are assigned a manager from the already-created managers.
    """
    n_managers = max(1, math.ceil(n * 0.10))
    employees: list[Employee] = []
    manager_ids: list[str] = []

    # Pass 1: create all managers first so non-managers always have a valid ref
    for i in range(n_managers):
        emp_id = _uid()
        mgr_id = None if i == 0 else random.choice(manager_ids)
        employees.append(Employee(
            emp_id=emp_id,
            name=_full_name(),
            role="Manager",
            branch_id=random.choice(branches).branch_id,
            manager_id=mgr_id,
            hire_date=_date_in_past(years_back=15),
        ))
        manager_ids.append(emp_id)

    # Pass 2: remaining staff — each gets a random manager from the manager pool
    non_manager_roles = [r for r in EMPLOYEE_ROLES if r != "Manager"]
    for i in range(n - n_managers):
        employees.append(Employee(
            emp_id=_uid(),
            name=_full_name(),
            role=random.choice(non_manager_roles),
            branch_id=random.choice(branches).branch_id,
            manager_id=random.choice(manager_ids),
            hire_date=_date_in_past(years_back=10),
        ))
    return employees


def generate_products(n: int) -> list[Product]:
    products = []
    for i in range(n):
        category = PRODUCT_CATEGORIES[i % len(PRODUCT_CATEGORIES)]
        products.append(Product(
            product_id=_uid(),
            product_name=f"{category} Plan {i + 1}",
            product_category=category,
        ))
    return products


def generate_accounts(
    n: int,
    customers: list[Customer],
    branches: list[Branch],
) -> list[Account]:
    branch_ids = [b.branch_id for b in branches]
    accounts = []
    for _ in range(n):
        status = random.choices(_ACCOUNT_STATUSES, weights=_ACCOUNT_STATUS_WEIGHTS, k=1)[0]
        opened_at = _dt_in_past(days_back=365 * 10)
        # Only closed/inactive accounts have a closed_at timestamp
        closed_at = _dt_in_past(days_back=365 * 2) if status == "Inactive" else None
        accounts.append(Account(
            account_id=_uid(),
            customer_id=random.choice(customers).customer_id,
            account_type=random.choice(_ACCOUNT_TYPES),
            status=status,
            branch_id=random.choice(branch_ids),
            opened_at=opened_at,
            closed_at=closed_at,
        ))
    return accounts


def generate_transactions(
    n: int,
    accounts: list[Account],
    employees: list[Employee],
    products: list[Product],
) -> list[Transaction]:
    product_ids = [p.product_id for p in products]
    emp_ids = [e.emp_id for e in employees]
    transactions = []
    for _ in range(n):
        account = random.choice(accounts)
        status = random.choices(_TXN_STATUSES, weights=_TXN_STATUS_WEIGHTS, k=1)[0]
        approved_by = random.choice(emp_ids) if status == "approved" else None
        transactions.append(Transaction(
            txn_id=_uid(),
            account_id=account.account_id,
            txn_type=random.choice(TXN_TYPES),
            amount=_sample_amount(),
            #currency=random.choices(_CURRENCIES, weights=_CURRENCY_WEIGHTS, k=1)[0],
            currency="INR",
            txn_time=_dt_in_past(days_back=365),
            channel=random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0],
            status=status,
            product_id=random.choice(product_ids),
            approved_by_emp_id=approved_by,
        ))
    return transactions


# ──────────────────────────────── dataset builder ───────────────────────────────

def build_dataset(
    n_branches: int = 10,
    n_customers: int = 200,
    n_employees: int = 50,
    n_products: int = 12,
    n_accounts: int = 500,
    n_transactions: int = 5000,
    skip_transactions: bool = True,
) -> dict[str, list]:
    """
    Generate all tables in dependency order.
    Returns a dict mapping table name → list of dataclass instances.
    Pass skip_transactions=False (or use --transactions N on the CLI) to include
    the transaction table.
    """
    #print("Generating branches     …", end="  ", flush=True)
    branches = generate_branches(n_branches)
    #print(f"{len(branches):,} records")

    #print("Generating customers    …", end="  ", flush=True)
    customers = generate_customers(n_customers)
    #print(f"{len(customers):,} records")

    #print("Generating employees    …", end="  ", flush=True)
    employees = generate_employees(n_employees, branches)
    n_mgr = sum(1 for e in employees if e.role == "Manager")
    #print(f"{len(employees):,} records  ({n_mgr} managers, {len(employees) - n_mgr} staff)")

    #print("Generating products     …", end="  ", flush=True)
    products = generate_products(n_products)
    #print(f"{len(products):,} records")

    #print("Generating accounts     …", end="  ", flush=True)
    accounts = generate_accounts(n_accounts, customers, branches)
    #print(f"{len(accounts):,} records")

    dataset = {
        "branches": branches,
        "customers": customers,
        "employees": employees,
        "products": products,
        "accounts": accounts,
    }

    if not skip_transactions:
        #print("Generating transactions …", end="  ", flush=True)
        transactions = generate_transactions(n_transactions, accounts, employees, products)
        #print(f"{len(transactions):,} records")
        dataset["transactions"] = transactions
    #else:
    #    print("Skipping transactions  (add --transactions to include them)")

    return dataset


# ──────────────────────────────── I/O helpers ───────────────────────────────────

def _rows(records: list) -> list[dict]:
    return [asdict(r) if hasattr(r, "__dataclass_fields__") else r for r in records]


def write_json(dataset: dict[str, list], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for table, records in dataset.items():
        path = out_dir / f"{table}.json"
        with path.open("w", encoding="utf-8") as fh:
            json.dump(_rows(records), fh, indent=2, default=str)
        print(f"  Wrote {path}  ({len(records):,} rows)")


def write_csv(dataset: dict[str, list], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    for table, records in dataset.items():
        rows = _rows(records)
        if not rows:
            continue
        path = out_dir / f"{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"  Wrote {path}  ({len(records):,} rows)")




# ──────────────────────────────── AstraDB writer ────────────────────────────────

_SCHEMA_FILE = Path(__file__).parent / "database_schema.cql"


def _load_schema_stmts() -> list[str]:
    """Parse database_schema.cql and return each DDL statement as a string."""
    raw = _SCHEMA_FILE.read_text(encoding="utf-8")
    # Strip line comments, split on statement terminator
    lines = [line.split("--")[0] for line in raw.splitlines()]
    stmts = [s.strip() for s in "\n".join(lines).split(";")]
    return [s for s in stmts if s]


_CREATE_STMTS = _load_schema_stmts()

# INSERT CQL for each table (positional ? placeholders match _rows() field order)
_INSERT_CQL: dict[str, str] = {
    "branches":     "INSERT INTO branch     (branch_id, branch_name, city, region) VALUES (?, ?, ?, ?)",
    "customers":    "INSERT INTO customer   (customer_id, name, dob, customer_segment, pan_hash, city, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
    "employees":    "INSERT INTO employee   (emp_id, name, role, branch_id, manager_id, hire_date) VALUES (?, ?, ?, ?, ?, ?)",
    "products":     "INSERT INTO product    (product_id, product_name, product_category) VALUES (?, ?, ?)",
    "accounts":     "INSERT INTO account    (account_id, customer_id, account_type, status, branch_id, opened_at, closed_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
    "transactions": "INSERT INTO transaction (txn_id, account_id, txn_type, amount, currency, txn_time, channel, status, product_id, approved_by_emp_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
}

# Field order must match the INSERT placeholders above
_FIELD_ORDER: dict[str, list[str]] = {
    "branches":     ["branch_id", "branch_name", "city", "region"],
    "customers":    ["customer_id", "name", "dob", "customer_segment", "pan_hash", "city", "created_at"],
    "employees":    ["emp_id", "name", "role", "branch_id", "manager_id", "hire_date"],
    "products":     ["product_id", "product_name", "product_category"],
    "accounts":     ["account_id", "customer_id", "account_type", "status", "branch_id", "opened_at", "closed_at"],
    "transactions": ["txn_id", "account_id", "txn_type", "amount", "currency", "txn_time", "channel", "status", "product_id", "approved_by_emp_id"],
}

_BATCH_SIZE = 50  # Astra recommends small batches (≤ 50 statements)


def _to_uuid(value: str | None):
    """Convert a UUID string (or None) to a uuid.UUID object for the driver."""
    if value is None:
        return None
    return uuid.UUID(value)


def _coerce_row(table: str, row: dict) -> tuple:
    """
    Return a tuple of values in _FIELD_ORDER order, with types coerced so the
    Cassandra driver can bind them:
      • UUID columns → uuid.UUID
      • date/timestamp columns → datetime.date / datetime.datetime
      • everything else → str / float / int as-is
    """
    _UUID_COLS = {
        "branch_id", "customer_id", "emp_id", "manager_id",
        "account_id", "product_id", "txn_id", "approved_by_emp_id",
    }
    _DATE_COLS = {"dob", "hire_date"}
    _TS_COLS   = {"created_at", "opened_at", "closed_at", "txn_time"}

    values = []
    for field in _FIELD_ORDER[table]:
        v = row[field]
        if field in _UUID_COLS:
            v = _to_uuid(v)
        elif field in _DATE_COLS and v is not None:
            v = datetime.date.fromisoformat(v)
        elif field in _TS_COLS and v is not None:
            v = datetime.datetime.fromisoformat(v)
        values.append(v)
    return tuple(values)


def write_astra(dataset: dict[str, list]) -> None:
    """
    Write every table in *dataset* to AstraDB.
    Tables are created (IF NOT EXISTS) before any inserts are attempted.
    Rows are inserted in batches of _BATCH_SIZE.
    """
    print("Connecting to AstraDB …")
    session = astra_client.connect()

    # ── DDL ──────────────────────────────────────────────────────────────────
    print("Creating tables (IF NOT EXISTS) …")
    for stmt in _CREATE_STMTS:
        session.execute(stmt)

    # ── DML ──────────────────────────────────────────────────────────────────
    for table_key, records in dataset.items():
        cql = _INSERT_CQL[table_key]
        prepared = session.prepare(cql)
        rows = _rows(records)
        total = len(rows)

        print(f"  Inserting {total:,} rows into {table_key} …", end="  ", flush=True)

        inserted = 0
        for chunk_start in range(0, total, _BATCH_SIZE):
            chunk = rows[chunk_start : chunk_start + _BATCH_SIZE]
            batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            for row in chunk:
                batch.add(prepared, _coerce_row(table_key, row))
            session.execute(batch)
            inserted += len(chunk)

        print(f"{inserted:,} rows inserted")

    session.shutdown()
    print("AstraDB write complete.")


# ──────────────────────────────── CLI ───────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a referentially-consistent synthetic banking dataset.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--branches",     type=int,  default=10,   help="Number of branches")
    parser.add_argument("--customers",    type=int,  default=200,  help="Number of customers")
    parser.add_argument("--employees",    type=int,  default=50,   help="Number of employees (10%% will be managers)")
    parser.add_argument("--products",     type=int,  default=12,   help="Number of products")
    parser.add_argument("--accounts",     type=int,  default=500,  help="Number of accounts")
    parser.add_argument("--transactions", type=int,  default=None, help="Generate N transactions (omit to skip)")
    parser.add_argument("--out-dir",      type=Path, default=None, help="Directory to write output files (omit to skip writing)")
    parser.add_argument("--format",       choices=["json", "csv"], default="json", help="Output file format")
    parser.add_argument("--seed",         type=int,  default=None, help="Random seed for reproducibility")
    parser.add_argument("--astra",        action="store_true",     help="Write dataset to AstraDB (requires ASTRA_* env vars)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.seed is not None:
        random.seed(args.seed)
        print(f"Random seed: {args.seed}")

    skip_txn = args.transactions is None
    txn_note = "skipped" if skip_txn else str(args.transactions)
    print(
        f"\nDataset sizes requested: "
        f"{args.branches} branches, "
        f"{args.customers} customers, "
        f"{args.employees} employees, "
        f"{args.products} products, "
        f"{args.accounts} accounts, "
        f"transactions: {txn_note}\n"
    )

    dataset = build_dataset(
        n_branches=args.branches,
        n_customers=args.customers,
        n_employees=args.employees,
        n_products=args.products,
        n_accounts=args.accounts,
        n_transactions=args.transactions or 5000,
        skip_transactions=skip_txn,
    )

    print()

    if args.out_dir:
        print(f"Writing {args.format.upper()} files to {args.out_dir} …")
        if args.format == "csv":
            write_csv(dataset, args.out_dir)
        else:
            write_json(dataset, args.out_dir)
    else:
        print("No --out-dir specified; skipping file output.")

    if args.astra:
        print()
        write_astra(dataset)
    else:
        print("No --astra flag; skipping AstraDB write.")
