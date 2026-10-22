-- =============================================================================
-- Layer: Mart
-- Table: mart_top_customers
-- Rule:  SQL only: business-ready table answering Business Question 5:
--        Top 10 customers by spend, with loyalty tier (no raw email or phone shown).
-- =============================================================================
CREATE OR REPLACE TABLE mart_top_customers AS
WITH customer_spend AS (
    SELECT 
        c.customer_id,
        c.full_name,
        c.masked_email,
        c.masked_phone,
        c.loyalty_tier,
        c.city,
        COUNT(o.order_id) AS total_orders,
        COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders,
        ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.net_revenue ELSE 0 END), 2) AS total_spend
    FROM stg_customers c
    JOIN stg_orders o ON c.customer_id = o.customer_id
    WHERE o.dq_status = 'PASSED' AND c.dq_status = 'PASSED'
    GROUP BY c.customer_id, c.full_name, c.masked_email, c.masked_phone, c.loyalty_tier, c.city
)
SELECT 
    DENSE_RANK() OVER (ORDER BY total_spend DESC) AS spend_rank,
    customer_id,
    full_name,
    masked_email,
    masked_phone,
    loyalty_tier,
    city,
    total_orders,
    delivered_orders,
    total_spend
FROM customer_spend
WHERE total_spend > 0
ORDER BY total_spend DESC
LIMIT 10;
