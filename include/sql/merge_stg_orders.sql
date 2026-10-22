-- =============================================================================
-- Incremental Merge: orders_2026-10-01_increment.csv -> stg_orders
-- Rule:  MERGE / upsert on order_id, so the 10 status corrections overwrite
--        the old rows and the 150 new orders are added.
-- =============================================================================
MERGE INTO stg_orders AS t
USING (
    SELECT 
        TRIM(order_id) AS order_id,
        TRY_CAST(TRIM(order_ts) AS TIMESTAMP) AS order_ts,
        STRFTIME(TRY_CAST(TRIM(order_ts) AS TIMESTAMP), '%Y-%m-%d') AS order_date,
        TRIM(customer_id) AS customer_id,
        TRIM(store_id) AS store_id,
        TRIM(product_id) AS product_id,
        TRY_CAST(quantity AS BIGINT) AS quantity,
        TRY_CAST(unit_price AS DOUBLE) AS unit_price,
        TRY_CAST(discount_pct AS DOUBLE) AS discount_pct,
        TRY_CAST(order_amount AS DOUBLE) AS order_amount,
        ROUND(TRY_CAST(order_amount AS DOUBLE), 2) AS net_revenue,
        payment_mode AS payment_mode_raw,
        CASE 
            WHEN UPPER(TRIM(payment_mode)) = 'UPI' THEN 'UPI'
            WHEN UPPER(TRIM(payment_mode)) IN ('CARD', 'CREDIT CARD', 'DEBIT CARD') THEN 'Card'
            WHEN UPPER(TRIM(payment_mode)) IN ('CASH ON DELIVERY', 'COD', 'CASHH', 'CASH') THEN 'Cash on Delivery'
            WHEN UPPER(TRIM(payment_mode)) = 'WALLET' THEN 'Wallet'
            ELSE TRIM(payment_mode)
        END AS payment_mode,
        order_status AS order_status_raw,
        UPPER(TRIM(order_status)) AS order_status,
        TRIM(channel) AS channel,
        TRY_CAST(delivery_minutes AS DOUBLE) AS delivery_minutes,
        (UPPER(TRIM(order_status)) = 'DELIVERED' AND TRY_CAST(delivery_minutes AS DOUBLE) <= 15) AS within_15_min,
        'PASSED' AS dq_status,
        NULL::VARCHAR AS failure_reason,
        CURRENT_TIMESTAMP::VARCHAR AS _loaded_at,
        'orders_2026-10-01_increment.csv' AS _source_file,
        '__RUN_ID__' AS _run_id
    FROM read_csv('__CSV_PATH__', all_varchar=true)
) AS s
ON t.order_id = s.order_id
WHEN MATCHED THEN UPDATE SET
    order_ts = s.order_ts,
    order_date = s.order_date,
    customer_id = s.customer_id,
    store_id = s.store_id,
    product_id = s.product_id,
    quantity = s.quantity,
    unit_price = s.unit_price,
    discount_pct = s.discount_pct,
    order_amount = s.order_amount,
    net_revenue = s.net_revenue,
    payment_mode_raw = s.payment_mode_raw,
    payment_mode = s.payment_mode,
    order_status_raw = s.order_status_raw,
    order_status = s.order_status,
    channel = s.channel,
    delivery_minutes = s.delivery_minutes,
    within_15_min = s.within_15_min,
    dq_status = s.dq_status,
    failure_reason = s.failure_reason,
    _loaded_at = s._loaded_at,
    _source_file = s._source_file,
    _run_id = s._run_id
WHEN NOT MATCHED THEN INSERT (
    order_id, order_ts, order_date, customer_id, store_id, product_id,
    quantity, unit_price, discount_pct, order_amount, net_revenue,
    payment_mode_raw, payment_mode, order_status_raw, order_status,
    channel, delivery_minutes, within_15_min, dq_status, failure_reason,
    _loaded_at, _source_file, _run_id
) VALUES (
    s.order_id, s.order_ts, s.order_date, s.customer_id, s.store_id, s.product_id,
    s.quantity, s.unit_price, s.discount_pct, s.order_amount, s.net_revenue,
    s.payment_mode_raw, s.payment_mode, s.order_status_raw, s.order_status,
    s.channel, s.delivery_minutes, s.within_15_min, s.dq_status, s.failure_reason,
    s._loaded_at, s._source_file, s._run_id
);
