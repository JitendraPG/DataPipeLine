-- Layer: Raw Table: raw_stores
CREATE TABLE IF NOT EXISTS raw_stores AS
SELECT *,
    CURRENT_TIMESTAMP::VARCHAR AS _loaded_at,
    'stores.csv' AS _source_file,
    '__RUN_ID__' AS _run_id
FROM read_csv('__CSV_PATH__', all_varchar=true);
