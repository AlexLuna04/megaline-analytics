CREATE TABLE gold.fact_usage (
    user_id         INTEGER         NOT NULL,
    event_date      DATE            NOT NULL,
    plan            VARCHAR(50),
    usd_monthly_pay NUMERIC(10,2),
    minutes_included INTEGER,
    messages_included INTEGER,
    mb_per_month_included INTEGER,
    usd_per_minute  NUMERIC(6,4),
    usd_per_message NUMERIC(6,4),
    usd_per_gb      NUMERIC(6,4),
    total_minutes   INTEGER,
    total_messages  INTEGER,
    total_mb        NUMERIC(12,2)
) PARTITION BY RANGE (event_date);


CREATE TABLE gold.fact_usage_2025_01
    PARTITION OF gold.fact_usage
    FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');


CREATE TABLE gold.fact_usage_2025_02
    PARTITION OF gold.fact_usage
    FOR VALUES FROM ('2025-02-01') TO ('2025-03-01');