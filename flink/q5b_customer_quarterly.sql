-- Q5b — Customer quarterly transaction total (rolling SUM)
--
-- Joins banking.transactions with banking.dimensions.account to resolve account_id → customer_id.
-- Groups by (customer_id, quarter) with a continuous SUM.

CREATE MATERIALIZED TABLE analytics_customer_quarterly_txn_total AS
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
