-- 1 - UPDATED
CREATE TABLE IF NOT EXISTS transactions_by_account (
    account_id UUID,
    txn_day text,    -- YYYYMMDD
    txn_timestamp timestamp,
    txn_id UUID,
    currency text,
    txn_type text,
    channel text,
    status text,
    product_id UUID,
    approved_by_emp_id UUID,
    amount double,
    PRIMARY KEY ((account_id), txn_timestamp)
) WITH CLUSTERING ORDER BY (txn_timestamp DESC);
CREATE CUSTOM INDEX txn_day_idx ON transactions_by_account (txn_day) USING 'StorageAttachedIndex';




-- 2
CREATE TABLE IF NOT EXISTS high_value_transaction_hourly (
    txn_minute text,             -- YYYYMMDDHHmm
    txn_time timestamp,
    txn_id UUID,
    account_id UUID,
    currency text,
    amount double,
    channel text,
    status text,
    PRIMARY KEY ((txn_minute), txn_time, txn_id)
) WITH CLUSTERING ORDER BY (txn_time DESC, txn_id ASC);

-- 3
CREATE TABLE IF NOT EXISTS high_value_transaction_by_city (
    bucket_hour text,        -- YYYYMMDDHH
    city text,
    account_id UUID,
    txn_id UUID,
    amount double,
    PRIMARY KEY ((bucket_hour), city, txn_id)
) WITH CLUSTERING ORDER BY (city ASC, txn_id ASC);


-- 4
CREATE TABLE IF NOT EXISTS withdrawal_transaction_by_employee (
    employee_id UUID,
    quarter text,               -- YYYYQx (e.g., 2025Q1)
    txn_time timestamp,
    txn_id UUID,
    account_id UUID,
    currency text,
    amount double,
    branch_name text,
    PRIMARY KEY ((employee_id, quarter), txn_time, txn_id)
) WITH CLUSTERING ORDER BY (txn_time DESC, txn_id ASC);


-- 5.1
CREATE TABLE IF NOT EXISTS customer_account_count (
    customer_id UUID PRIMARY KEY,
    active_account_count counter
);

-- 5.2
CREATE TABLE IF NOT EXISTS customer_quarterly_txn_total (
    customer_id UUID,
    quarter text,            -- e.g., 2025-Q1
    amount double,
    PRIMARY KEY (customer_id, quarter)
);

--5
CREATE TABLE IF NOT EXISTS high_value_customer_quarterly (
    quarter text,
    customer_id UUID,
    amount double,
    PRIMARY KEY ((quarter), customer_id)
);

--6
CREATE TABLE IF NOT EXISTS branch_daily_rollup (
    branch_id UUID,
    txn_year text,
    txn_date date,
    total_amount double,
    count_txn bigint,
    PRIMARY KEY ((branch_id, txn_year), txn_date)
) WITH CLUSTERING ORDER BY (txn_date ASC);

