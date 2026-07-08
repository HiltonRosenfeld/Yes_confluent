-- ============================================================================
-- ODS (Operational Data Store) Keyspace Schema
-- ============================================================================
-- This keyspace contains base/reference tables for the banking system
-- Tables: customer, account, transaction, employee, branch, product


CREATE TABLE IF NOT EXISTS customer (
    customer_id UUID PRIMARY KEY,
    name text,
    dob date,
    customer_segment text,
    pan_hash text,
    city text,
    created_at timestamp
);

CREATE TABLE IF NOT EXISTS account (
    account_id UUID PRIMARY KEY,
    customer_id UUID,
    account_type text,
    status text,
    branch_id UUID,
    opened_at timestamp,
    closed_at timestamp
);


CREATE TABLE IF NOT EXISTS transaction (
    txn_id UUID PRIMARY KEY,
    account_id UUID,
    txn_type text,
    amount double,
    currency text,
    txn_time timestamp,
    channel text,
    status text,
    product_id UUID,
    approved_by_emp_id UUID
);


CREATE TABLE IF NOT EXISTS employee (
    emp_id UUID PRIMARY KEY,
    name text,
    role text,
    branch_id UUID,
    manager_id UUID,
    hire_date date
);
CREATE CUSTOM INDEX IF NOT EXISTS employee_mngr_idx ON employee (manager_id) USING 'StorageAttachedIndex';

CREATE TABLE IF NOT EXISTS branch (
    branch_id UUID PRIMARY KEY,
    branch_name text,
    city text,
    region text
);

CREATE TABLE IF NOT EXISTS product (
    product_id UUID PRIMARY KEY,
    product_name text,
    product_category text
);
