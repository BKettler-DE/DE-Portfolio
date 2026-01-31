-- Staging model for sensor readings
-- Purpose: Standardize raw sensor data from streaming pipeline

{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    -- Read parquet files directly from data lake
    SELECT * FROM read_parquet('../data_lake/raw/streaming/sensor_readings_[0-9]*.parquet')
),

renamed AS (
    SELECT
        -- TIME DIMENSION
        -- Keep original timestamp and extract useful date parts
        time AS reading_timestamp,
        DATE(time) AS reading_date,
        EXTRACT(HOUR FROM time) AS reading_hour,
        
        -- SENSOR IDENTIFIER
        sensor_id,
        
        -- MEASUREMENTS
        temperature,
        humidity,
        pressure,
        
        -- LOCATION
        location,
        
        -- METADATA COLUMNS
        CURRENT_TIMESTAMP AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM source
)

SELECT * FROM renamed
