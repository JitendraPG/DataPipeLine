-- =============================================================================
-- Layer: Staging
-- Table: stg_stores
-- Rule:  SQL only: cast types, standardise, dedupe, apply quality rules,
--        add dq_status and failure_reason.
-- =============================================================================
CREATE OR REPLACE TABLE stg_stores AS
WITH counts AS (
    SELECT store_id, COUNT(*) AS store_count
    FROM raw_stores
    GROUP BY store_id
),
ranked AS (
    SELECT 
        TRIM(s.store_id) AS store_id,
        TRIM(s.store_name) AS store_name,
        CASE 
            WHEN s.zone IS NOT NULL 
            THEN UPPER(SUBSTR(TRIM(s.zone), 1, 1)) || LOWER(SUBSTR(TRIM(s.zone), 2)) 
        END AS zone,
        TRIM(s.area) AS area,
        TRIM(s.pincode) AS pincode,
        TRIM(s.open_date) AS open_date,
        TRIM(s.manager_name) AS manager_name,
        TRY_CAST(s.capacity_orders_per_day AS BIGINT) AS capacity_orders_per_day,
        s._loaded_at,
        s._source_file,
        s._run_id,
        cnt.store_count,
        ROW_NUMBER() OVER (PARTITION BY s.store_id ORDER BY s._loaded_at ASC) AS rn,
        ARRAY_FILTER(ARRAY[
            CASE WHEN s.store_id IS NULL OR TRIM(s.store_id) = '' THEN 'Completeness: missing store_id' END,
            CASE WHEN s.store_name IS NULL OR TRIM(s.store_name) = '' THEN 'Completeness: missing store_name' END,
            CASE WHEN s.zone IS NULL OR TRIM(s.zone) = '' THEN 'Completeness: zone is null or empty' END,
            CASE WHEN cnt.store_count > 1 THEN 'Uniqueness: duplicate store_id (' || COALESCE(s.store_id, 'NULL') || ')' END,
            CASE WHEN TRY_CAST(s.capacity_orders_per_day AS BIGINT) IS NULL OR TRY_CAST(s.capacity_orders_per_day AS BIGINT) <= 0 THEN 'Accuracy: capacity_orders_per_day must be > 0 (' || COALESCE(s.capacity_orders_per_day, 'NULL') || ')' END,
            CASE WHEN s.pincode IS NULL OR NOT regexp_matches(TRIM(s.pincode), '^[0-9]{6}$') THEN 'Validity: invalid pincode format (' || COALESCE(s.pincode, 'NULL') || ')' END,
            CASE WHEN s.open_date IS NULL OR TRIM(s.open_date) > '2026-10-20' THEN 'Timeliness: future open_date (' || COALESCE(s.open_date, 'NULL') || ')' END
        ], x -> x IS NOT NULL) AS reasons_arr
    FROM raw_stores s
    LEFT JOIN counts cnt ON s.store_id = cnt.store_id
)
SELECT 
    store_id,
    store_name,
    zone,
    area,
    pincode,
    open_date,
    manager_name,
    capacity_orders_per_day,
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
