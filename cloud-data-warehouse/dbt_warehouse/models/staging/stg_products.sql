-- Staging model for products
-- Purpose: Standardize raw product data from batch pipeline

{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    -- Read parquet files directly from data lake
    -- Using read_parquet() instead of source() due to dbt-duckdb limitations
    SELECT * FROM read_parquet('../data_lake/raw/batch/clean_products_*.parquet')
),

renamed AS (
    SELECT
        -- PRIMARY KEY
        product_id,
        
        -- ATTRIBUTES
        name AS product_name,
        price,
        stock AS stock_quantity,
        source AS source_system,
        category,
        
        -- METADATA COLUMNS
        loaded_at AS source_loaded_at,
        CURRENT_TIMESTAMP AS dbt_updated_at,
        '{{ invocation_id }}' AS dbt_invocation_id
        
    FROM source
)

SELECT * FROM renamed