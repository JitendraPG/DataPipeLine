-- =============================================================================
-- Layer: Mart
-- Table: mart_daily_store_kpis
-- Rule:  SQL only: business-ready table answering Business Question 1:
--        Daily net revenue by zone and by store (delivered orders only, after discount).
-- =============================================================================
CREATE OR REPLACE TABLE mart_daily_store_kpis AS
SELECT
    o.order_date,
    COALESCE(s.zone, 'Unknown') AS zone,
    s.store_id,
    s.store_name,
    COUNT(o.order_id) AS total_orders,
    COUNT(CASE WHEN o.order_status = 'DELIVERED' THEN 1 END) AS delivered_orders,
    ROUND(SUM(CASE WHEN o.order_status = 'DELIVERED' THEN o.net_revenue ELSE 0 END), 2) AS daily_net_revenue,
    ROUND(AVG(CASE WHEN o.order_status = 'DELIVERED' THEN o.delivery_minutes ELSE NULL END), 2) AS avg_delivery_minutes
FROM stg_orders o
JOIN stg_stores s ON o.store_id = s.store_id
WHERE o.dq_status = 'PASSED'
GROUP BY o.order_date, COALESCE(s.zone, 'Unknown'), s.store_id, s.store_name
ORDER BY o.order_date, COALESCE(s.zone, 'Unknown'), s.store_name;
