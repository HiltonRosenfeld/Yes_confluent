# Connects to Astra DB and loads reference data
# into the same in-memory dataclass structures used by data_generator.py.

import datetime

from cassandra.util import Date

import astra_client
from data_generator import Account, Branch, Customer, Employee, Product


def get_session():
    return astra_client.connect()


def _iso_date(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, Date):
        return str(value)
    return value.isoformat()


def _iso_datetime(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value.isoformat()
    return datetime.datetime.combine(value, datetime.time.min, tzinfo=datetime.timezone.utc).isoformat()


def load_reference_data(session):
    branches = [
        Branch(
            branch_id=str(row.branch_id),
            branch_name=row.branch_name,
            city=row.city,
            region=row.region,
        )
        for row in session.execute("SELECT branch_id, branch_name, city, region FROM branch")
    ]
    customers = [
        Customer(
            customer_id=str(row.customer_id),
            name=row.name,
            dob=_iso_date(row.dob),
            customer_segment=row.customer_segment,
            pan_hash=row.pan_hash,
            city=row.city,
            created_at=_iso_datetime(row.created_at),
        )
        for row in session.execute(
            "SELECT customer_id, name, dob, customer_segment, pan_hash, city, created_at FROM customer"
        )
    ]
    employees = [
        Employee(
            emp_id=str(row.emp_id),
            name=row.name,
            role=row.role,
            branch_id=str(row.branch_id),
            manager_id=str(row.manager_id) if row.manager_id is not None else None,
            hire_date=_iso_date(row.hire_date),
        )
        for row in session.execute(
            "SELECT emp_id, name, role, branch_id, manager_id, hire_date FROM employee"
        )
    ]
    products = [
        Product(
            product_id=str(row.product_id),
            product_name=row.product_name,
            product_category=row.product_category,
        )
        for row in session.execute("SELECT product_id, product_name, product_category FROM product")
    ]
    accounts = [
        Account(
            account_id=str(row.account_id),
            customer_id=str(row.customer_id),
            account_type=row.account_type,
            status=row.status,
            branch_id=str(row.branch_id),
            opened_at=_iso_datetime(row.opened_at),
            closed_at=_iso_datetime(row.closed_at),
        )
        for row in session.execute(
            "SELECT account_id, customer_id, account_type, status, branch_id, opened_at, closed_at FROM account"
        )
    ]
    return {
        "branches": branches,
        "customers": customers,
        "employees": employees,
        "products": products,
        "accounts": accounts,
    }
