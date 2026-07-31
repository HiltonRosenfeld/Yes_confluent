-- Q1 — Transactions by account

CREATE MATERIALIZED TABLE analytics_transactions_by_account AS
SELECT
    account_id,
    -- Derive YYYYMMDD UTC bucket for SAI-indexed date range scans in AstraDB
    DATE_FORMAT(txn_time, 'yyyyMMdd')                       AS txn_day,
    DATE_FORMAT(txn_time, 'yyyy-MM-dd''T''HH:mm:ss.SSSSSS') AS txn_timestamp,
    txn_id,
    currency,
    txn_type,
    channel,
    status,
    product_id,
    approved_by_emp_id,
    amount
FROM `banking.transactions`;