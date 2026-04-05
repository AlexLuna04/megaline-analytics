SELECT
    id                              AS session_id,
    user_id,
    CAST(session_date AS DATE)      AS session_date,
    COALESCE(mb_used, 0)            AS mb_used
FROM {{ source('megaline_raw', 'megaline_internet') }}