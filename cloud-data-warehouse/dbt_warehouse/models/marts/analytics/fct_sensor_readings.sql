-- Fact table: Sensor readings
-- Purpose: Time-series measurements with derived metrics
-- Grain: One row per sensor reading

{{
    config(
        materialized='table'
    )
}}

-- WHY A TABLE?
-- - Large dataset (1000+ rows, grows over time)
-- - Queried frequently for dashboards
-- - Pre-compute categorizations and flags
-- - Fast aggregations (no parquet reading)

WITH sensor_readings AS (
    -- Read from staging view
    SELECT * FROM {{ ref('stg_sensor_readings') }}
),

enriched AS (
    SELECT
        -- TIME DIMENSIONS
        -- Keep all time fields for flexible analysis
        reading_timestamp,
        reading_date,
        reading_hour,
        
        -- SENSOR IDENTIFIER
        -- Foreign key to dim_sensors (if we build it)
        sensor_id,
        
        -- MEASUREMENTS (FACTS)
        -- These are the actual metrics we measure
        temperature,
        humidity,
        pressure,
        
        -- LOCATION
        location,
        
        -- DERIVED METRICS - TEMPERATURE CATEGORIZATION
        -- Business logic: classify temperatures for analysis
        CASE
            WHEN temperature < 0 THEN 'freezing'
            WHEN temperature < 15 THEN 'cold'
            WHEN temperature < 25 THEN 'comfortable'
            WHEN temperature < 35 THEN 'warm'
            ELSE 'hot'
        END AS temperature_category,
        
        -- DERIVED METRICS - HUMIDITY CATEGORIZATION
        CASE
            WHEN humidity < 30 THEN 'dry'
            WHEN humidity < 60 THEN 'comfortable'
            WHEN humidity < 80 THEN 'humid'
            ELSE 'very_humid'
        END AS humidity_category,
        
        -- DATA QUALITY FLAGS
        -- Track completeness for quality monitoring
        CASE
            WHEN temperature IS NULL OR humidity IS NULL OR pressure IS NULL 
            THEN TRUE 
            ELSE FALSE
        END AS has_missing_data,
        
        -- ANOMALY DETECTION (SIMPLE RULE-BASED)
        -- Flag readings that seem suspicious
        CASE
            WHEN temperature < -50 OR temperature > 60 THEN TRUE
            WHEN humidity < 0 OR humidity > 100 THEN TRUE
            WHEN pressure < 900 OR pressure > 1100 THEN TRUE
            ELSE FALSE
        END AS is_anomaly,
        
        -- COMPOSITE FLAG: Is this a "good" reading?
        CASE
            WHEN temperature IS NULL OR humidity IS NULL OR pressure IS NULL THEN FALSE
            WHEN temperature < -50 OR temperature > 60 THEN FALSE
            WHEN humidity < 0 OR humidity > 100 THEN FALSE
            WHEN pressure < 900 OR pressure > 1100 THEN FALSE
            ELSE TRUE
        END AS is_valid_reading,
        
        -- METADATA
        dbt_updated_at,
        dbt_invocation_id
        
    FROM sensor_readings
)

SELECT * FROM enriched


-- =============================================================================
-- UNDERSTANDING FACT TABLES
-- =============================================================================
--
-- Fact Table Characteristics:
-- ✓ Contains MEASUREMENTS (temperature, humidity, pressure)
-- ✓ Contains METRICS (counts, sums, averages)
-- ✓ High ROW COUNT (thousands to millions)
-- ✓ Has FOREIGN KEYS to dimensions (sensor_id)
-- ✓ Has TIME dimension (reading_timestamp)
--
-- Dimension Table Characteristics (for comparison):
-- ✓ Contains ATTRIBUTES (product_name, category)
-- ✓ Contains DESCRIPTIONS (text fields)
-- ✓ Low ROW COUNT (hundreds to thousands)
-- ✓ Has PRIMARY KEY (product_key)
-- ✓ Changes slowly
--
-- =============================================================================
-- GRAIN DEFINITION
-- =============================================================================
--
-- Grain: "One row per sensor reading"
--
-- This means:
-- - Each sensor reading gets exactly one row
-- - sensor_id + reading_timestamp should be unique
-- - No aggregation (that comes in aggregate tables)
--
-- Example data:
-- sensor_id | reading_timestamp    | temperature | humidity
-- sensor_01 | 2026-01-28 10:00:00 | 22.5        | 45.2
-- sensor_01 | 2026-01-28 10:01:00 | 22.6        | 45.3
-- sensor_02 | 2026-01-28 10:00:00 | 21.8        | 50.1
--
-- =============================================================================
-- DERIVED METRICS - WHY ADD THEM?
-- =============================================================================
--
-- Instead of calculating in every query:
-- SELECT 
--     CASE WHEN temperature < 0 THEN 'freezing'
--          WHEN temperature < 15 THEN 'cold'
--          ... END as temp_category
-- FROM sensor_readings
--
-- Pre-compute once in the table:
-- SELECT temperature_category
-- FROM fct_sensor_readings
--
-- Benefits:
-- - Faster dashboard queries
-- - Consistent logic (defined once)
-- - Easy filtering (WHERE temperature_category = 'hot')
-- - Simpler downstream SQL
--
-- =============================================================================
-- ANOMALY DETECTION
-- =============================================================================
--
-- is_anomaly flag catches suspicious readings:
--
-- Temperature: < -50°C or > 60°C
-- - Normal range for most sensors
-- - Outside this = sensor error or extreme conditions
--
-- Humidity: < 0% or > 100%
-- - Physically impossible
-- - Indicates sensor malfunction
--
-- Pressure: < 900 or > 1100 hPa
-- - Extreme but possible
-- - Might indicate sensor issues
--
-- Use cases:
-- - Data quality monitoring
-- - Alerting dashboards
-- - Filter out bad data: WHERE is_anomaly = FALSE
-- =============================================================================
-- STAR SCHEMA PATTERN
-- =============================================================================
--
-- This fact table is the CENTER of a star:
--
--        dim_sensors
--             |
--             |
--  dim_date - fct_sensor_readings - dim_location
--             |
--             |
--        dim_time
--
-- Join pattern:
-- SELECT 
--     d.date,
--     s.sensor_type,
--     l.location_name,
--     AVG(f.temperature) as avg_temp
-- FROM fct_sensor_readings f
-- JOIN dim_date d ON f.reading_date = d.date
-- JOIN dim_sensors s ON f.sensor_id = s.sensor_id
-- JOIN dim_location l ON f.location = l.location_code
-- GROUP BY 1, 2, 3;
--
-- For now, we only have the fact table.
-- Dimension tables are optional but make queries cleaner!
--
-- =============================================================================