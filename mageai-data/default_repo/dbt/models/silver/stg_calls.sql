SELECT
    id                          AS call_id,
    user_id,
    CAST(call_date AS DATE)     AS call_date,
    CEIL(duration)              AS duration_minutes  -- redondear hacia arriba
FROM {{ source('megaline_raw', 'megaline_calls') }}
WHERE duration IS NOT NULL