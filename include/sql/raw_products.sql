-- Layer: Raw Table: raw_products
CREATE TABLE IF NOT EXISTS raw_products AS
SELECT *,
    CURRENT_TIMESTAMP::VARCHAR AS _loaded_at,
    'products.csv' AS _source_file,
    '__RUN_ID__' AS _run_id
FROM read_csv('__CSV_PATH__', all_varchar=true);
