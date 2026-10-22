-- =============================================================================
-- Layer: Staging
-- Table: stg_products
-- Rule:  SQL only: cast types, standardise, dedupe, apply quality rules,
--        add dq_status and failure_reason.
-- =============================================================================
CREATE OR REPLACE TABLE stg_products AS
WITH counts AS (
    SELECT product_id, COUNT(*) AS prod_count
    FROM raw_products
    GROUP BY product_id
),
ranked AS (
    SELECT 
        TRIM(p.product_id) AS product_id,
        TRIM(p.product_name) AS product_name,
        TRIM(p.category) AS category,
        TRIM(p.sub_category) AS sub_category,
        TRIM(p.brand) AS brand,
        TRY_CAST(p.mrp AS DOUBLE) AS mrp,
        TRY_CAST(p.cost_price AS DOUBLE) AS cost_price,
        TRIM(p.unit) AS unit,
        UPPER(TRIM(COALESCE(p.is_perishable, 'FALSE'))) = 'TRUE' AS is_perishable,
        TRIM(p.launch_date) AS launch_date,
        p._loaded_at,
        p._source_file,
        p._run_id,
        cnt.prod_count,
        ROW_NUMBER() OVER (PARTITION BY p.product_id ORDER BY p._loaded_at ASC) AS rn,
        ARRAY_FILTER(ARRAY[
            CASE WHEN p.product_id IS NULL OR TRIM(p.product_id) = '' THEN 'Completeness: missing product_id' END,
            CASE WHEN p.product_name IS NULL OR TRIM(p.product_name) = '' THEN 'Completeness: missing product_name' END,
            CASE WHEN p.category IS NULL OR TRIM(p.category) = '' THEN 'Completeness: category is null or empty' END,
            CASE WHEN cnt.prod_count > 1 THEN 'Uniqueness: duplicate product_id (' || COALESCE(p.product_id, 'NULL') || ')' END,
            CASE WHEN TRY_CAST(p.mrp AS DOUBLE) IS NULL OR TRY_CAST(p.mrp AS DOUBLE) <= 0 THEN 'Accuracy: mrp must be > 0 (' || COALESCE(p.mrp, 'NULL') || ')' END,
            CASE WHEN TRY_CAST(p.cost_price AS DOUBLE) IS NULL OR TRY_CAST(p.cost_price AS DOUBLE) <= 0 THEN 'Accuracy: cost_price must be > 0 (' || COALESCE(p.cost_price, 'NULL') || ')' END,
            CASE WHEN TRY_CAST(p.cost_price AS DOUBLE) IS NOT NULL AND TRY_CAST(p.mrp AS DOUBLE) IS NOT NULL AND TRY_CAST(p.cost_price AS DOUBLE) > TRY_CAST(p.mrp AS DOUBLE) THEN 'Accuracy: cost_price (' || p.cost_price || ') exceeds mrp (' || p.mrp || ')' END,
            CASE WHEN p.launch_date IS NULL OR TRIM(p.launch_date) > '2026-10-20' THEN 'Timeliness: future launch_date (' || COALESCE(p.launch_date, 'NULL') || ')' END
        ], x -> x IS NOT NULL) AS reasons_arr
    FROM raw_products p
    LEFT JOIN counts cnt ON p.product_id = cnt.product_id
)
SELECT 
    product_id,
    product_name,
    category,
    sub_category,
    brand,
    mrp,
    cost_price,
    unit,
    is_perishable,
    launch_date,
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
