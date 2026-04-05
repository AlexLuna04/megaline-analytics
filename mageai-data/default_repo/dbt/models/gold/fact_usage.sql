
SELECT
    c.user_id,
    c.call_date                             AS event_date,
    u.plan,
    p.usd_monthly_pay,
    p.minutes_included,
    p.messages_included,
    p.mb_per_month_included,
    p.usd_per_minute,
    p.usd_per_message,
    p.usd_per_gb,
    c.duration_minutes                      AS total_minutes,
    m.total_messages,
    i.total_mb

FROM {{ ref('stg_calls') }}             AS c

INNER JOIN {{ source('megaline_raw', 'megaline_users') }} AS u
    ON c.user_id = u.user_id

INNER JOIN {{ source('megaline_raw', 'megaline_plans') }} AS p
    ON u.plan = p.plan_name

LEFT JOIN (
    SELECT
        user_id,
        DATE_TRUNC('month', message_date)   AS month,
        COUNT(*)                            AS total_messages
    FROM {{ ref('stg_messages') }}
    GROUP BY 1, 2
) AS m
    ON c.user_id = m.user_id
    AND DATE_TRUNC('month', c.call_date) = m.month

LEFT JOIN (
    SELECT
        user_id,
        DATE_TRUNC('month', session_date)   AS month,
        SUM(mb_used)                        AS total_mb
    FROM {{ ref('stg_internet') }}
    GROUP BY 1, 2
) AS i
    ON c.user_id = i.user_id
    AND DATE_TRUNC('month', c.call_date) = i.month