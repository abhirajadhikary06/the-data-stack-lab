CREATE OR REPLACE TABLE dim_network AS
SELECT DISTINCT
    -- Surrogate key for joining with fact table
    ROW_NUMBER() OVER (
        ORDER BY network_type, tower_id, source_system
    )                           AS network_key,
    network_type,
    tower_id,
    source_system
FROM transformed_telecom_usage
ORDER BY network_type, tower_id;
