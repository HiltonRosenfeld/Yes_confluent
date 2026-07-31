-- Q5a — Active account count by customer

CREATE MATERIALIZED TABLE analytics_customer_account_count AS
SELECT 
    customer_id, 
    COUNT(*) AS account_count
FROM `banking.dimensions.account`
WHERE status = 'Active'
GROUP BY customer_id;