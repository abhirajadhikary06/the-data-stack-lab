CREATE OR REPLACE TABLE dim_customer AS
SELECT DISTINCT
    customer_id,
    phone_number,
    region,
    plan_type,
    device_type,
    customer_status,
    CAST(signup_date AS DATE)   AS signup_date
FROM transformed_telecom_usage
ORDER BY customer_id;
