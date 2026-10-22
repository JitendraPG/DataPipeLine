-- =============================================================================
-- Layer: Staging
-- Table: stg_payments
-- Rule:  SQL only: cast types, standardise, dedupe, apply quality rules,
--        add dq_status and failure_reason.
-- =============================================================================
CREATE OR REPLACE TABLE stg_payments AS
WITH counts AS (
    SELECT payment_id, COUNT(*) AS pay_count
    FROM raw_payments
    GROUP BY payment_id
),
distinct_orders AS (
    SELECT order_id, TRY_CAST(order_amount AS DOUBLE) AS order_amount
    FROM raw_orders
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at) = 1
),
ranked AS (
    SELECT 
        TRIM(p.payment_id) AS payment_id,
        TRIM(p.order_id) AS order_id,
        TRY_CAST(TRIM(p.payment_ts) AS TIMESTAMP) AS payment_ts,
        STRFTIME(TRY_CAST(TRIM(p.payment_ts) AS TIMESTAMP), '%Y-%m-%d') AS payment_date,
        p.payment_mode AS payment_mode_raw,
        CASE 
            WHEN UPPER(TRIM(p.payment_mode)) = 'UPI' THEN 'UPI'
            WHEN UPPER(TRIM(p.payment_mode)) IN ('CARD', 'CREDIT CARD', 'DEBIT CARD') THEN 'Card'
            WHEN UPPER(TRIM(p.payment_mode)) IN ('CASH ON DELIVERY', 'COD', 'CASHH', 'CASH') THEN 'Cash on Delivery'
            WHEN UPPER(TRIM(p.payment_mode)) = 'WALLET' THEN 'Wallet'
            ELSE TRIM(p.payment_mode)
        END AS payment_mode,
        TRY_CAST(p.amount_paid AS DOUBLE) AS amount_paid,
        p.payment_status AS payment_status_raw,
        UPPER(TRIM(p.payment_status)) AS payment_status,
        TRIM(p.gateway_ref) AS gateway_ref,
        p._loaded_at,
        p._source_file,
        p._run_id,
        cnt.pay_count,
        ROW_NUMBER() OVER (PARTITION BY p.payment_id ORDER BY p._loaded_at ASC) AS rn,
        ARRAY_FILTER(ARRAY[
            CASE WHEN cnt.pay_count > 1 THEN 'Uniqueness: duplicate payment_id (' || COALESCE(p.payment_id, 'NULL') || ')' END,
            CASE WHEN p.payment_status NOT IN ('SUCCESS', 'FAILED', 'REFUNDED') THEN 'Validity: invalid payment_status (' || COALESCE(p.payment_status, 'NULL') || ')' END,
            CASE WHEN p.payment_mode NOT IN ('UPI', 'Card', 'Cash on Delivery', 'Wallet') THEN 'Validity: invalid payment_mode (' || COALESCE(p.payment_mode, 'NULL') || ')' END,
            CASE WHEN TRY_CAST(p.amount_paid AS DOUBLE) IS NULL OR TRY_CAST(p.amount_paid AS DOUBLE) <= 0 THEN 'Accuracy: amount_paid must be > 0 (' || COALESCE(p.amount_paid, 'NULL') || ')' END,
            CASE WHEN p.order_id IS NULL OR p.order_id NOT IN (SELECT order_id FROM distinct_orders) THEN 'Consistency: order_id not found in orders master (' || COALESCE(p.order_id, 'NULL') || ')' END,
            CASE WHEN p.payment_status = 'SUCCESS' AND p.order_id IN (SELECT order_id FROM distinct_orders) AND o.order_amount IS NOT NULL AND ABS(TRY_CAST(p.amount_paid AS DOUBLE) - o.order_amount) > 0.05 THEN 'Consistency: amount_paid (' || p.amount_paid || ') does not match order_amount (' || CAST(o.order_amount AS VARCHAR) || ')' END,
            CASE WHEN p.payment_ts IS NULL OR TRIM(p.payment_ts) > '2026-10-20 23:59:59' THEN 'Timeliness: future payment_ts (' || COALESCE(p.payment_ts, 'NULL') || ')' END
        ], x -> x IS NOT NULL) AS reasons_arr
    FROM raw_payments p
    LEFT JOIN counts cnt ON p.payment_id = cnt.payment_id
    LEFT JOIN distinct_orders o ON p.order_id = o.order_id
)
SELECT 
    payment_id,
    order_id,
    payment_ts,
    payment_date,
    payment_mode_raw,
    payment_mode,
    amount_paid,
    payment_status_raw,
    payment_status,
    gateway_ref,
    CASE 
        WHEN LENGTH(ARRAY_TO_STRING(reasons_arr, '; ')) = 0 THEN 'PASSED'
        ELSE 'REJECTED'
    END AS dq_status,
    NULLIF(ARRAY_TO_STRING(reasons_arr, '; '), '') AS failure_reason,
    _loaded_at,
    _source_file,
    _run_id
FROM ranked
WHERE rn = 1;
