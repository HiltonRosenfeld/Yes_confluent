-- =============================================================================
-- Confluent Cloud Flink — Output Materialized Table Definitions
-- =============================================================================
-- Confluent Cloud Flink uses CREATE MATERIALIZED TABLE ... AS ... SELECT ...
-- for Materialized Tables. This syntax:
--   • auto-provisions the backing Kafka topic
--   • registers the schema in Schema Registry
--   • makes the table queryable and writable by INSERT INTO ... SELECT
--
-- Column names and types match the AstraDB analytics tables in analytics_schema.cql
-- field-for-field so the Cassandra Sink Connector needs no transformation.
--
-- Type mapping:
--   Cassandra UUID      → Flink STRING
--   Cassandra timestamp → Flink TIMESTAMP_LTZ(3)
--   Cassandra text      → Flink STRING
--   Cassandra double    → Flink DOUBLE
--   Cassandra bigint    → Flink BIGINT
--   Cassandra date      → Flink DATE
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Q1 — Transactions by account
--    AstraDB target : transactions_by_account
--    Write mode     : append
--    Partition key  : account_id  (matches AstraDB partition key)
-- ---------------------------------------------------------------------------
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


-- ---------------------------------------------------------------------------
-- Q2 — High-value transactions > 10,000 (hourly)
--    AstraDB target : high_value_transaction_hourly
--    Write mode     : append
--    Partition key  : txn_minute  (matches AstraDB partition key)
-- Only transactions with amount > 10,000 are forwarded.
-- Partitioned by txn_minute (YYYYMMDDHHmm) so AstraDB can efficiently scan
-- any 60-minute window by querying up to 60 consecutive partition keys.
--
-- UTC bucket derivation:
--   txn_minute = DATE_FORMAT(txn_time, 'yyyyMMddHHmm', '+00:00')
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED TABLE analytics_high_value_hourly AS
SELECT
    -- Derive YYYYMMDDHHmm UTC bucket (one partition per minute in AstraDB)
    DATE_FORMAT(txn_time, 'yyyyMMddHHmm')  AS txn_minute,
    txn_time,
    txn_id,
    account_id,
    currency,
    amount,
    channel,
    status
FROM `banking.transactions`
WHERE amount > 10000;


-- ---------------------------------------------------------------------------
-- Q3 — High-value transactions > 50,000 by city
--    AstraDB target : high_value_transaction_by_city
--    Write mode     : append
--    Partition key  : bucket_hour  (matches AstraDB partition key)
-- Filter  : amount > 50,000
-- Joins   : banking_transactions → dim_account (to get branch_id)
--                                → dim_branch  (to get city)
-- Bucket  : bucket_hour = YYYYMMDDHH UTC
--
-- AstraDB layout: partition key bucket_hour, clustering by city then txn_id.
-- Dashboards scan up to 24 partition keys (one per hour) to cover 24 hours.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED TABLE analytics_high_value_by_city AS
SELECT
    DATE_FORMAT(t.txn_time, 'yyyyMMddHH') AS bucket_hour,
    b.city,
    t.account_id,
    t.txn_id,
    t.amount
FROM `banking.transactions` AS t
LEFT JOIN `banking.dimensions.account` AS a
    ON t.account_id = a.account_id
LEFT JOIN `banking.dimensions.branch` AS b
    ON a.branch_id = b.branch_id
WHERE t.amount > 50000;



-- ---------------------------------------------------------------------------
-- Q4 — Withdrawal transactions by employee
--    AstraDB target : withdrawal_transaction_by_employee
--    Write mode     : append
--    Partition key  : employee_id  (matches AstraDB partition key)
-- Filter  : txn_type = 'Withdrawal' AND approved_by_emp_id IS NOT NULL
--           (IS NOT NULL guard is required because approved_by_emp_id is a
--            nullable field in the Avro schema; joining on a null key would
--            silently drop the row from a LEFT JOIN but could cause incorrect
--            results with an INNER JOIN — the explicit guard makes intent clear)
-- Joins   : banking_transactions → dim_employee (to get branch_id)
--                                → dim_branch   (to get branch_name)
-- Bucket  : quarter = YYYYQn UTC (e.g. 2025Q1)
--           YEAR() and QUARTER() operate on the TIMESTAMP_LTZ value in UTC.
--
-- AstraDB layout: compound partition key (employee_id, quarter); dashboards
-- query by employee_id + quarter and can filter by manager_id at read time.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED TABLE analytics_withdrawal_by_employee AS
SELECT
    e.emp_id AS employee_id,
    -- Derive YYYYQn quarter string (no hyphen) in UTC
    CONCAT(
        CAST(YEAR(t.txn_time)    AS STRING),
        'Q',
        CAST(QUARTER(t.txn_time) AS STRING)
    )        AS quarter,
    t.txn_time,
    t.txn_id,
    t.account_id,
    t.currency,
    t.amount,
    b.branch_name
FROM `banking.transactions` AS t
JOIN `banking.dimensions.employee` AS e
    ON t.approved_by_emp_id = e.emp_id
JOIN `banking.dimensions.branch` AS b
    ON e.branch_id = b.branch_id
WHERE t.txn_type = 'withdrawal'
  AND t.approved_by_emp_id IS NOT NULL;


-- ---------------------------------------------------------------------------
-- Q5a — Customer quarterly transaction total (rolling SUM)
--    AstraDB target : customer_quarterly_txn_total
--    Write mode     : upsert  (continuous GROUP BY emits retract+upsert changelog)
--    Partition key  : customer_id
-- Joins banking_transactions with dim_account to resolve account_id → customer_id.
-- Groups by (customer_id, quarter) with a continuous (non-windowed) SUM.
-- Confluent Cloud Flink checkpoints this GROUP BY state automatically; the
-- running total survives restarts and resumes from the last checkpoint value.
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED TABLE analytics_customer_quarterly_txn AS
SELECT
    a.customer_id,
    -- Derive YYYYQn quarter string in UTC (e.g. 2025Q1)
    CONCAT(
        CAST(YEAR(t.txn_time)    AS STRING),
        'Q',
        CAST(QUARTER(t.txn_time) AS STRING)
    )                  AS quarter,
    SUM(t.amount)      AS amount
FROM `banking.transactions` AS t
JOIN `banking.dimensions.account` AS a
    ON t.account_id = a.account_id
GROUP BY
    a.customer_id,
    CONCAT(
        CAST(YEAR(t.txn_time)    AS STRING),
        'Q',
        CAST(QUARTER(t.txn_time) AS STRING)
    );


-- ---------------------------------------------------------------------------
-- Q5b — High-value customers quarterly
--    AstraDB target : high_value_customer_quarterly
--    Write mode     : upsert  (derived from Q5a upsert stream)
--    Partition key  : quarter  (matches AstraDB partition key)
-- Source    : analytics_customer_quarterly_txn  (Q5a Materialized Table)
-- Join      : active_counts CTE derived from dim_account_active_events
--             dim_account_active_events carries one event per active account
--             (seeded by dimension_loader.py seed_active_account_counts()).
--             COUNT(*) GROUP BY customer_id gives the current active account
--             count for each customer entirely within Flink state.
--
-- Filters applied here (not deferred to dashboard):
--   1. q.amount > 1,000,000  — quarterly cumulative total threshold
--   2. ac.active_account_count > 5  — minimum active account count threshold
--
-- The output upsert stream is gated: only customers meeting both criteria
-- appear in analytics_high_value_customer_qtrly, and rows are retracted when
-- a customer's running total falls back below the threshold (e.g. after a
-- late correction event).
-- ---------------------------------------------------------------------------
INSERT INTO analytics_high_value_customer_qtrly
WITH active_counts AS (
    -- Derive active account count per customer from the seed event stream.
    -- Each row in dim_account_active_events represents one active account.
    SELECT
        customer_id,
        COUNT(*) AS active_account_count
    FROM dim_account_active_events
    GROUP BY customer_id
)
SELECT
    q.quarter,
    q.customer_id,
    q.amount
FROM analytics_customer_quarterly_txn AS q
JOIN active_counts AS ac
    ON q.customer_id = ac.customer_id
WHERE q.amount              > 1000000
  AND ac.active_account_count > 5;



-- ---------------------------------------------------------------------------
-- Q6 — Branch daily rollup
--    AstraDB target : branch_daily_rollup
--    Write mode     : upsert  (continuous GROUP BY emits retract+upsert changelog)
--    Partition key  : branch_id  (matches first component of AstraDB partition key)
-- Joins  : banking_transactions → dim_account (to get branch_id)
--                               → dim_branch  (to confirm branch exists)
-- Groups : (branch_id, txn_year, txn_date)
-- Output : total_amount DOUBLE, count_txn BIGINT — rolling daily aggregates
-- ---------------------------------------------------------------------------
CREATE MATERIALIZED TABLE analytics_branch_daily_rollup AS
SELECT
    b.branch_id,
    -- Derive YYYY year string in UTC (partition component in AstraDB)
    COALESCE(DATE_FORMAT(t.txn_time, 'yyyy'),'1970')     AS txn_year,
    -- Derive DATE in UTC (clustering column in AstraDB, ordered ASC)
    CAST(DATE_FORMAT(t.txn_time, 'yyyy-MM-dd') AS DATE)  AS txn_date,
    SUM(t.amount)                                        AS total_amount,
    COUNT(*)                                             AS count_txn
FROM `banking.transactions` AS t
JOIN `banking.dimensions.account` AS a
    ON t.account_id = a.account_id
JOIN `banking.dimensions.branch` AS b
    ON a.branch_id = b.branch_id
GROUP BY
    b.branch_id,
    DATE_FORMAT(t.txn_time, 'yyyy'),
    CAST(DATE_FORMAT(t.txn_time, 'yyyy-MM-dd') AS DATE);