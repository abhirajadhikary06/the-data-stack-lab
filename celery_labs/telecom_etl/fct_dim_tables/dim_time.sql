CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    CAST(session_start AS TIMESTAMP)                        AS session_start,
    CAST(session_start AS DATE)                             AS session_date,
    EXTRACT(YEAR  FROM session_start)::INTEGER              AS session_year,
    EXTRACT(MONTH FROM session_start)::INTEGER              AS session_month,
    EXTRACT(WEEK  FROM session_start)::INTEGER              AS session_week,

    -- Weekday name: Monday, Tuesday, etc.
    DAYNAME(CAST(session_start AS DATE))                    AS weekday_name,

    -- Day period based on hour of session_start
    CASE
        WHEN EXTRACT(HOUR FROM session_start) BETWEEN 5  AND 11 THEN 'MORNING'
        WHEN EXTRACT(HOUR FROM session_start) BETWEEN 12 AND 16 THEN 'AFTERNOON'
        WHEN EXTRACT(HOUR FROM session_start) BETWEEN 17 AND 20 THEN 'EVENING'
        ELSE 'NIGHT'
    END                                                     AS day_period

FROM transformed_telecom_usage
ORDER BY session_start;
