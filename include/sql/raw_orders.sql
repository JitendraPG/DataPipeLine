-- Layer: Raw Table: raw_orders
CREATE TABLE IF NOT EXISTS raw_orders AS
SELECT *,
    CURRENT_TIMESTAMP::VARCHAR AS _loaded_at,
    'orders.csv' AS _source_file,
    '__RUN_ID__' AS _run_id
FROM read_csv('__CSV_PATH__', all_varchar=true);
