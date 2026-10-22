-- =============================================================================
-- Layer: Mart
-- Table: mart_payment_reconciliation
-- Rule:  SQL only: business-ready table answering Business Question 4 & Criteria 7:
--        Revenue by payment mode, and total amount that doesn't reconcile with gateway.
--        Lists every order/payment mismatch with reasons.
-- =============================================================================
CREATE OR REPLACE TABLE mart_payment_reconciliation AS
WITH clean_orders AS (
    SELECT order_id, order_status, order_amount, payment_mode
    FROM stg_orders
    WHERE dq_status = 'PASSED'
),
payments_std AS (
    SELECT payment_id, order_id, amount_paid, payment_status, payment_mode
    FROM stg_payments
    WHERE dq_status = 'PASSED'
),
joined AS (
    SELECT 
        o.order_id AS o_order_id,
        p.order_id AS p_order_id,
        COALESCE(o.order_id, p.order_id) AS order_id,
        p.payment_id,
        o.order_status,
        p.payment_status,
        o.order_amount,
        p.amount_paid,
        ROUND(COALESCE(o.order_amount, 0.0) - COALESCE(p.amount_paid, 0.0), 2) AS discrepancy_amount,
        o.payment_mode AS order_payment_mode,
        p.payment_mode AS gateway_payment_mode
    FROM clean_orders o
    FULL OUTER JOIN payments_std p ON o.order_id = p.order_id
)
SELECT 
    order_id,
    payment_id,
    order_status,
    payment_status,
    order_amount,
    amount_paid,
    discrepancy_amount,
    order_payment_mode,
    gateway_payment_mode,
    CASE 
        WHEN o_order_id IS NOT NULL AND payment_id IS NULL THEN 'MISSING_PAYMENT'
        WHEN o_order_id IS NULL AND payment_id IS NOT NULL THEN 'ORPHAN_PAYMENT'
        WHEN (order_status = 'DELIVERED' AND payment_status != 'SUCCESS') OR (ABS(discrepancy_amount) > 0.05) THEN 'MISMATCH'
        ELSE 'RECONCILED'
    END AS reconciliation_status,
    CASE 
        WHEN o_order_id IS NOT NULL AND payment_id IS NULL THEN 'Order with status ' || COALESCE(order_status, 'UNKNOWN') || ' has no payment record in gateway'
        WHEN o_order_id IS NULL AND payment_id IS NOT NULL THEN 'Payment record has no matching clean order (order may have been quarantined or invalid)'
        WHEN order_status = 'DELIVERED' AND payment_status = 'FAILED' AND ABS(discrepancy_amount) > 0.05 THEN 'Delivered order has gateway status FAILED; Amount mismatch: order ₹' || CAST(order_amount AS VARCHAR) || ' vs paid ₹' || CAST(amount_paid AS VARCHAR)
        WHEN order_status = 'DELIVERED' AND payment_status = 'FAILED' THEN 'Delivered order has gateway status FAILED'
        WHEN ABS(discrepancy_amount) > 0.05 THEN 'Amount mismatch: order ₹' || CAST(order_amount AS VARCHAR) || ' vs paid ₹' || CAST(amount_paid AS VARCHAR)
        ELSE 'Reconciled successfully'
    END AS mismatch_reason,
    CURRENT_TIMESTAMP::VARCHAR AS reconciled_at
FROM joined
WHERE 
    (o_order_id IS NOT NULL AND payment_id IS NULL)
    OR (o_order_id IS NULL AND payment_id IS NOT NULL)
    OR (order_status = 'DELIVERED' AND payment_status != 'SUCCESS')
    OR (ABS(discrepancy_amount) > 0.05);
