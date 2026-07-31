-- Q3 — High-value transactions > 50,000 by city
-- Count of high-value transactions (>50,000) in last 24 hours grouped by city.

-- Filter  : amount > 50,000
-- Joins   : banking.transactions → banking.dimensions.account (to get branch_id)
--                                → banking.dimensions.branch  (to get city)
-- Bucket  : bucket_hour = YYYYMMDDHH UTC


CREATE MATERIALIZED TABLE analytics_high_value_transaction_by_city AS
SELECT 
  window_start,
  window_end,
  b.city,
  COUNT(t.txn_id) AS txn_count
FROM TABLE(
    TUMBLE(TABLE `banking.transactions`, DESCRIPTOR(txn_time), INTERVAL '1' HOUR)
) AS t
JOIN `banking.dimensions.account` AS a
  ON t.account_id = a.account_id
JOIN `banking.dimensions.branch` AS b
  ON a.branch_id = b.branch_id
WHERE t.amount > 50000 
GROUP BY 
    window_start, 
    window_end, 
    b.city;