CREATE OR REPLACE TABLE gold_usage_kpis AS
SELECT
    -- Grouping dimensions
    f.signup_year,
    f.signup_month,
    c.region,
    c.plan_type,

    -- Volume KPIs
    COUNT(f.usage_id)                               AS total_sessions,
    COUNT(DISTINCT f.customer_id)                   AS active_users,

    -- Data consumption KPIs
    ROUND(SUM(f.data_used_mb), 2)                   AS total_data_consumed_mb,
    ROUND(AVG(f.data_used_mb), 2)                   AS avg_data_usage_mb,

    -- Performance KPIs
    ROUND(AVG(f.session_duration_minutes), 2)       AS avg_session_duration,
    ROUND(AVG(f.download_speed_mbps), 2)            AS avg_download_speed,
    ROUND(AVG(f.upload_speed_mbps), 2)              AS avg_upload_speed,

    -- Signal quality distribution
    ROUND(AVG(f.signal_strength), 2)                AS avg_signal_strength,

    -- Usage level breakdown
    COUNT(CASE WHEN f.data_usage_level = 'LOW'    THEN 1 END)  AS low_usage_sessions,
    COUNT(CASE WHEN f.data_usage_level = 'MEDIUM' THEN 1 END)  AS medium_usage_sessions,
    COUNT(CASE WHEN f.data_usage_level = 'HIGH'   THEN 1 END)  AS high_usage_sessions,

    -- Network type breakdown
    COUNT(CASE WHEN f.network_type = '5G'  THEN 1 END)         AS sessions_5g,
    COUNT(CASE WHEN f.network_type = '4G'  THEN 1 END)         AS sessions_4g,
    COUNT(CASE WHEN f.network_type = 'LTE' THEN 1 END)         AS sessions_lte

FROM fact_telecom_usage f
LEFT JOIN dim_customer c
    ON f.customer_id = c.customer_id
GROUP BY
    f.signup_year,
    f.signup_month,
    c.region,
    c.plan_type
ORDER BY
    f.signup_year,
    f.signup_month,
    c.region,
    c.plan_type;
