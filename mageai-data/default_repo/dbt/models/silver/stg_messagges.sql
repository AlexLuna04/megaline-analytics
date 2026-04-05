SELECT
    id                              AS message_id,
    user_id,
    CAST(message_date AS DATE)      AS message_date
FROM {{ source('megaline_raw', 'megaline_messages') }}