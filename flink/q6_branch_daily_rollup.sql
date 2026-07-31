-- Q6 — Branch daily rollup
--
-- Joins  : banking.transactions → banking.dimensions.account (to get branch_id)
-- Groups : (branch_id, txn_year, txn_date)
-- Filter  : status = 'approved'

CREATE MATERIALIZED TABLE analytics_branch_daily_rollup AS

SELECT
    a.branch_id,
    CAST(EXTRACT(YEAR FROM t.txn_time) AS STRING) AS txn_year,
    CAST(t.txn_time AS DATE) AS txn_date,
    SUM(t.amount) AS total_amount,
    COUNT(*) AS count_txn
FROM `banking.transactions` t
JOIN `banking.dimensions.account` a
  ON t.account_id = a.account_id
WHERE t.status = 'approved'
GROUP BY
    a.branch_id,
    CAST(EXTRACT(YEAR FROM t.txn_time) AS STRING),
    CAST(t.txn_time AS DATE);