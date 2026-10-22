-- =============================================================================
-- Layer: Staging
-- Table: stg_orders
-- Rule:  SQL only: cast types, standardise, dedupe, apply quality rules across
--        all 6 dimensions, derive net_revenue and within_15_min, add dq_status
--        and failure_reason.
-- =============================================================================
CREATE OR REPLACE TABLE stg_orders AS
WITH order_counts AS (
    SELECT order_id, COUNT(*) AS ord_count
    FROM raw_orders
    GROUP BY order_id
),
valid_cust AS (
    SELECT DISTINCT customer_id FROM raw_customers WHERE customer_id IS NOT NULL AND TRIM(customer_id) != ''
),
valid_prod AS (
    SELECT DISTINCT product_id FROM raw_products WHERE product_id IS NOT NULL AND TRIM(product_id) != ''
),
valid_store AS (
    SELECT DISTINCT store_id FROM raw_stores WHERE store_id IS NOT NULL AND TRIM(store_id) != ''
),
evaluated AS (
    SELECT 
        TRIM(o.order_id) AS order_id,
        TRY_CAST(TRIM(o.order_ts) AS TIMESTAMP) AS order_ts,
        STRFTIME(TRY_CAST(TRIM(o.order_ts) AS TIMESTAMP), '%Y-%m-%d') AS order_date,
        TRIM(o.customer_id) AS customer_id,
        TRIM(o.store_id) AS store_id,
        TRIM(o.product_id) AS product_id,
        TRY_CAST(o.quantity AS BIGINT) AS quantity,
        TRY_CAST(o.unit_price AS DOUBLE) AS unit_price,
        TRY_CAST(o.discount_pct AS DOUBLE) AS discount_pct,
        TRY_CAST(o.order_amount AS DOUBLE) AS order_amount,
        ROUND(TRY_CAST(o.order_amount AS DOUBLE), 2) AS net_revenue,
        o.payment_mode AS payment_mode_raw,
        CASE 
            WHEN UPPER(TRIM(o.payment_mode)) = 'UPI' THEN 'UPI'
            WHEN UPPER(TRIM(o.payment_mode)) IN ('CARD', 'CREDIT CARD', 'DEBIT CARD') THEN 'Card'
            WHEN UPPER(TRIM(o.payment_mode)) IN ('CASH ON DELIVERY', 'COD', 'CASHH', 'CASH') THEN 'Cash on Delivery'
            WHEN UPPER(TRIM(o.payment_mode)) = 'WALLET' THEN 'Wallet'
            ELSE TRIM(o.payment_mode)
        END AS payment_mode,
        o.order_status AS order_status_raw,
        UPPER(TRIM(o.order_status)) AS order_status,
        TRIM(o.channel) AS channel,
        TRY_CAST(o.delivery_minutes AS DOUBLE) AS delivery_minutes,
        (UPPER(TRIM(o.order_status)) = 'DELIVERED' AND TRY_CAST(o.delivery_minutes AS DOUBLE) <= 15) AS within_15_min,
        o._loaded_at,
        o._source_file,
        o._run_id,
        oc.ord_count,
        ROW_NUMBER() OVER (PARTITION BY o.order_id ORDER BY o._loaded_at ASC) AS rn,
        ARRAY_FILTER(ARRAY[
            CASE WHEN o.customer_id IS NULL OR TRIM(o.customer_id) = '' THEN 'Completeness: missing customer_id' END,
            CASE WHEN UPPER(TRIM(COALESCE(o.order_status, ''))) = 'DELIVERED' AND (o.delivery_minutes IS NULL OR TRIM(o.delivery_minutes) = '') THEN 'Completeness: missing delivery_minutes for DELIVERED order' END,
            CASE WHEN oc.ord_count > 1 THEN 'Uniqueness: duplicate order_id (' || COALESCE(o.order_id, 'NULL') || ')' END,
            CASE WHEN o.order_status NOT IN ('DELIVERED', 'CANCELLED', 'RETURNED') THEN 'Validity: invalid order_status (' || COALESCE(o.order_status, 'NULL') || ')' END,
            CASE WHEN o.payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'Validity: invalid payment_mode (' || COALESCE(o.payment_mode, 'NULL') || ')' END,
            CASE WHEN o.order_ts IS NULL OR TRIM(o.order_ts) = '' OR TRY_CAST(o.order_ts AS TIMESTAMP) IS NULL THEN 'Validity: unparseable order_ts (' || COALESCE(o.order_ts, 'NULL') || ')' END,
            CASE WHEN TRY_CAST(o.discount_pct AS DOUBLE) IS NULL OR TRY_CAST(o.discount_pct AS DOUBLE) < 0 OR TRY_CAST(o.discount_pct AS DOUBLE) > 100 THEN 'Validity: discount_pct out of range 0-100 (' || COALESCE(o.discount_pct, 'NULL') || ')' END,
            CASE WHEN TRY_CAST(o.quantity AS BIGINT) IS NULL OR TRY_CAST(o.quantity AS BIGINT) <= 0 THEN 'Accuracy: quantity must be > 0 (' || COALESCE(o.quantity, 'NULL') || ')' END,
            CASE WHEN TRY_CAST(o.quantity AS BIGINT) IS NOT NULL AND TRY_CAST(o.unit_price AS DOUBLE) IS NOT NULL AND TRY_CAST(o.discount_pct AS DOUBLE) IS NOT NULL AND TRY_CAST(o.order_amount AS DOUBLE) IS NOT NULL AND ABS(TRY_CAST(o.order_amount AS DOUBLE) - ROUND(TRY_CAST(o.quantity AS BIGINT) * TRY_CAST(o.unit_price AS DOUBLE) * (1.0 - TRY_CAST(o.discount_pct AS DOUBLE) / 100.0), 2)) > 0.05 THEN 'Accuracy: order_amount formula mismatch (expected ' || CAST(ROUND(TRY_CAST(o.quantity AS BIGINT) * TRY_CAST(o.unit_price AS DOUBLE) * (1.0 - TRY_CAST(o.discount_pct AS DOUBLE) / 100.0), 2) AS VARCHAR) || ', got ' || COALESCE(o.order_amount, 'NULL') || ')' END,
            CASE WHEN UPPER(TRIM(COALESCE(o.order_status, ''))) = 'DELIVERED' AND TRY_CAST(o.delivery_minutes AS DOUBLE) > 120 THEN 'Accuracy: delivery_minutes exceeds 120 (' || COALESCE(o.delivery_minutes, 'NULL') || ')' END,
            CASE WHEN o.store_id IS NULL OR o.store_id NOT IN (SELECT store_id FROM valid_store) THEN 'Consistency: store_id not found in store master (' || COALESCE(o.store_id, 'NULL') || ')' END,
            CASE WHEN o.product_id IS NULL OR o.product_id NOT IN (SELECT product_id FROM valid_prod) THEN 'Consistency: product_id not found in product master (' || COALESCE(o.product_id, 'NULL') || ')' END,
            CASE WHEN o.customer_id IS NOT NULL AND TRIM(o.customer_id) != '' AND o.customer_id NOT IN (SELECT customer_id FROM valid_cust) THEN 'Consistency: customer_id not found in customer master (' || o.customer_id || ')' END,
            CASE WHEN TRY_CAST(o.order_ts AS TIMESTAMP) > TIMESTAMP '2026-10-20 23:59:59' THEN 'Timeliness: future-dated order_ts (' || COALESCE(o.order_ts, 'NULL') || ')' END
        ], x -> x IS NOT NULL) AS reasons_arr
    FROM raw_orders o
    LEFT JOIN order_counts oc ON o.order_id = oc.order_id
)
SELECT 
    order_id,
    order_ts,
    order_date,
    customer_id,
    store_id,
    product_id,
    quantity,
    unit_price,
    discount_pct,
    order_amount,
    net_revenue,
    payment_mode_raw,
    payment_mode,
    order_status_raw,
    order_status,
    channel,
    delivery_minutes,
    within_15_min,
    CASE 
        WHEN LENGTH(ARRAY_TO_STRING(reasons_arr, '; ')) = 0 THEN 'PASSED'
        ELSE 'REJECTED'
    END AS dq_status,
    NULLIF(ARRAY_TO_STRING(reasons_arr, '; '), '') AS failure_reason,
    _loaded_at,
    _source_file,
    _run_id
FROM evaluated
WHERE rn = 1;
