-- Q2 — High-value transactions > 10,000 (hourly)
-- transactions where amount > 10000 in the last 60 minutes (across all accounts).
--
-- Only transactions with amount > 10,000 are forwarded.
-- Partitioned by txn_minute (YYYYMMDDHHmm) so AstraDB can efficiently scan
-- any 60-minute window by querying up to 60 consecutive partition keys.


CREATE MATERIALIZED TABLE analytics_high_value_transaction_hourly AS
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