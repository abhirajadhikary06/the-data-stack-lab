CREATE OR REPLACE TABLE fact_telecom_usage AS
SELECT
    -- Keys
    t.usage_id,
    t.customer_id,
    t.tower_id,
    n.network_key,                          -- FK to dim_network

    -- Session timestamps
    CAST(t.session_start AS TIMESTAMP)      AS session_start,
    CAST(t.session_end   AS TIMESTAMP)      AS session_end,

    -- Metrics
    t.data_used_mb,
    t.session_duration_minutes,
    t.signal_strength,
    t.upload_speed_mbps,
    t.download_speed_mbps,

    -- Quality buckets
    t.data_usage_level,
    t.signal_strength_quality,
    t.upload_speed_quality,
    t.download_speed_quality,

    -- Network context
    t.network_type,

    -- Time intelligence
    t.signup_year,
    t.signup_month,
    t.signup_week

FROM transformed_telecom_usage t
LEFT JOIN dim_network n
    ON  t.network_type  = n.network_type
    AND t.tower_id      = n.tower_id
    AND t.source_system = n.source_system
ORDER BY t.usage_id;
