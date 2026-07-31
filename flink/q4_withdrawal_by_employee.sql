-- Q4 — Withdrawal transactions by employee
-- List withdrawal transactions approved by manager_id = ??? during a quarter
--
-- Filter  : txn_type = 'Withdrawal' AND approved_by_emp_id IS NOT NULL
--           (IS NOT NULL guard is required because approved_by_emp_id is a
--            nullable field in the Avro schema; joining on a null key would
--            silently drop the row from a LEFT JOIN but could cause incorrect
--            results with an INNER JOIN — the explicit guard makes intent clear)
-- Joins   : banking.transactions → banking.dimensions.employee (to get branch_id)
--                                → banking.dimensions.branch   (to get branch_name)
--                                → dim_branch   (to get branch_name)
-- Bucket  : quarter = YYYYQn UTC (e.g. 2025Q1)
--           YEAR() and QUARTER() operate on the TIMESTAMP_LTZ value in UTC.

CREATE MATERIALIZED TABLE analytics_withdrawal_transaction_by_employee AS
SELECT
    e.emp_id AS employee_id,
    -- Derive YYYYQn quarter string in UTC (e.g. 2025Q1)
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
WHERE t.txn_type = 'Withdrawal'
  AND t.approved_by_emp_id IS NOT NULL;