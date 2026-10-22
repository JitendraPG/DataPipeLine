-- =============================================================================
-- Layer: Mart
-- Table: mart_category_margin
-- Rule:  SQL only: business-ready table answering Business Question 2:
--        Revenue and gross margin by product category (margin = revenue - quantity * cost_price).
-- =============================================================================
CREATE OR REPLACE TABLE mart_category_margin AS
SELECT
    COALESCE(p.category, 'Uncategorized') AS category,
    SUM(o.quantity) AS total_quantity_sold,
    ROUND(SUM(o.net_revenue), 2) AS total_revenue,
    ROUND(SUM(o.quantity * p.cost_price), 2) AS total_cost,
    ROUND(SUM(o.net_revenue - o.quantity * p.cost_price), 2) AS total_gross_margin,
    ROUND(SUM(o.net_revenue - o.quantity * p.cost_price) * 100.0 / NULLIF(SUM(o.net_revenue), 0), 2) AS margin_pct
FROM stg_orders o
LEFT JOIN stg_products p ON o.product_id = p.product_id
WHERE o.dq_status = 'PASSED' AND o.order_status = 'DELIVERED'
GROUP BY COALESCE(p.category, 'Uncategorized')
ORDER BY total_revenue DESC;
