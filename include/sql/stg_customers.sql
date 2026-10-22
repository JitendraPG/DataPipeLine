-- =============================================================================
-- Layer: Staging
-- Table: stg_customers
-- Rule:  SQL only: cast types, standardise, dedupe, mask PII, apply quality rules,
--        add dq_status and failure_reason.
-- =============================================================================
CREATE OR REPLACE TABLE stg_customers AS
WITH counts AS (
    SELECT customer_id, COUNT(*) AS cust_count
    FROM raw_customers
    GROUP BY customer_id
),
ranked AS (
    SELECT 
        TRIM(c.customer_id) AS customer_id,
        TRIM(c.full_name) AS full_name,
        c.email AS raw_email,
        c.phone AS raw_phone,
        TRIM(c.city) AS city,
        TRIM(c.area) AS area,
        TRIM(c.pincode) AS pincode,
        TRIM(c.signup_date) AS signup_date,
        CASE 
            WHEN c.loyalty_tier IS NOT NULL 
            THEN UPPER(SUBSTR(TRIM(c.loyalty_tier), 1, 1)) || LOWER(SUBSTR(TRIM(c.loyalty_tier), 2)) 
        END AS loyalty_tier,
        UPPER(TRIM(COALESCE(c.is_active, 'TRUE'))) = 'TRUE' AS is_active,
        c._loaded_at,
        c._source_file,
        c._run_id,
        cnt.cust_count,
        ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY c._loaded_at ASC) AS rn,
        ARRAY_FILTER(ARRAY[
            CASE WHEN c.customer_id IS NULL OR TRIM(c.customer_id) = '' THEN 'Completeness: missing customer_id' END,
            CASE WHEN c.city IS NULL OR TRIM(c.city) = '' THEN 'Completeness: city is null or empty' END,
            CASE WHEN cnt.cust_count > 1 THEN 'Uniqueness: duplicate customer_id (' || COALESCE(c.customer_id, 'NULL') || ')' END,
            CASE WHEN c.pincode IS NULL OR NOT regexp_matches(TRIM(c.pincode), '^[0-9]{6}$') THEN 'Validity: invalid pincode format (' || COALESCE(c.pincode, 'NULL') || ')' END,
            CASE WHEN c.signup_date IS NULL OR TRIM(c.signup_date) > '2026-10-20' THEN 'Timeliness: future signup_date (' || COALESCE(c.signup_date, 'NULL') || ')' END
        ], x -> x IS NOT NULL) AS reasons_arr
    FROM raw_customers c
    LEFT JOIN counts cnt ON c.customer_id = cnt.customer_id
)
SELECT 
    customer_id,
    full_name,
    CASE 
        WHEN raw_email IS NULL OR TRIM(raw_email) = '' THEN ''
        WHEN raw_email NOT LIKE '%@%' THEN '***@***'
        ELSE SUBSTR(SPLIT_PART(raw_email, '@', 1), 1, CASE WHEN LENGTH(SPLIT_PART(raw_email, '@', 1)) >= 2 THEN 2 ELSE 1 END) || '***@' || LOWER(SPLIT_PART(raw_email, '@', 2))
    END AS masked_email,
    CASE
        WHEN raw_phone IS NULL OR LENGTH(TRIM(raw_phone)) != 10 THEN '**********'
        ELSE SUBSTR(TRIM(raw_phone), 1, 2) || '******' || SUBSTR(TRIM(raw_phone), 9, 2)
    END AS masked_phone,
    city,
    area,
    pincode,
    signup_date,
    loyalty_tier,
    is_active,
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
