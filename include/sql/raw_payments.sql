-- Layer: Raw Table: raw_payments
CREATE TABLE IF NOT EXISTS raw_payments AS
SELECT *,
    CURRENT_TIMESTAMP::VARCHAR AS _loaded_at,
    'payments.csv' AS _source_file,
    '__RUN_ID__' AS _run_id
FROM read_csv('__CSV_PATH__', all_varchar=true);
