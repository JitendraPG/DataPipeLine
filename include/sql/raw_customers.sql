-- Layer: Raw Table: raw_customers
CREATE TABLE IF NOT EXISTS raw_customers AS
SELECT *,
    CURRENT_TIMESTAMP::VARCHAR AS _loaded_at,
    'customers.csv' AS _source_file,
    '__RUN_ID__' AS _run_id
FROM read_csv('__CSV_PATH__', all_varchar=true);
