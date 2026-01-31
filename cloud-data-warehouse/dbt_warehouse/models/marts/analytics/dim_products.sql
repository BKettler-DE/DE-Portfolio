-- Dimension table: Products
-- Purpose: Product master data with business classifications

{{
    config(
        materialized='table'
    )
}}

-- WHY A TABLE (not a view)?
-- - Queried frequently by dashboards
-- - Pre-compute categorizations (price tier, stock status)
-- - Fast performance (no parquet reading)

WITH products AS (
    -- Read from staging view
    SELECT * FROM {{ ref('stg_products') }}
),

enriched AS (
    SELECT
        -- SURROGATE KEY
        -- Generate a unique hash from product_id
        -- This is the PRIMARY KEY for this dimension
        {{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key,
        
        -- NATURAL KEY
        -- Keep the original ID for reference
        product_id,
        
        -- ATTRIBUTES
        product_name,
        price,
        stock_quantity,
        source_system,
        category,
        
        -- DERIVED ATTRIBUTES - BUSINESS LOGIC
        -- Price categorization for analysis
        CASE
            WHEN price < 50 THEN 'budget'
            WHEN price < 200 THEN 'mid-range'
            WHEN price < 500 THEN 'premium'
            ELSE 'luxury'
        END AS price_tier,
        
        -- Stock status for inventory alerts
        CASE
            WHEN stock_quantity = 0 THEN 'out_of_stock'
            WHEN stock_quantity < 10 THEN 'low_stock'
            WHEN stock_quantity < 50 THEN 'normal_stock'
            ELSE 'high_stock'
        END AS stock_status,
        
        -- Is product available?
        CASE
            WHEN stock_quantity > 0 THEN TRUE
            ELSE FALSE
        END AS is_available,
        
        -- METADATA
        source_loaded_at,
        dbt_updated_at
        
    FROM products
)

SELECT * FROM enriched


-- =============================================================================
-- UNDERSTANDING SURROGATE KEYS
-- =============================================================================
--
-- Natural Key: product_id (e.g., "P4109")
-- - Comes from source system
-- - Might change if systems merge
-- - Might have duplicates across sources
--
-- Surrogate Key: product_key (e.g., "abc123def456...")
-- - Generated hash of product_id
-- - Stable and unique
-- - Works even if product_id changes
--
-- Example:
-- {{ dbt_utils.generate_surrogate_key(['product_id']) }}
-- 
-- Generates: MD5 hash like "5f2b8c3a1e9d4f7b..."
--
-- Why use it?
-- - Slowly Changing Dimensions (track history)
-- - Join stability (hash doesn't change)
-- - Multi-source consolidation
--
-- =============================================================================
-- BUSINESS LOGIC - PRICE TIERS
-- =============================================================================
--
-- Why categorize prices?
-- - Enables "avg revenue by tier" analysis
-- - Segmentation for marketing
-- - Easier filtering in dashboards
--
-- Example queries this enables:
--
-- SELECT price_tier, COUNT(*), AVG(stock_quantity)
-- FROM marts.dim_products
-- GROUP BY price_tier;
--
-- SELECT * FROM marts.dim_products
-- WHERE price_tier = 'luxury' AND is_available = TRUE;
--
-- =============================================================================
-- SLOWLY CHANGING DIMENSIONS (SCD)
-- =============================================================================
--
-- This is currently a Type 1 SCD (overwrite):
-- - Each product_id has ONE row
-- - Updates overwrite old values
-- - No history tracking
--
-- To track history (Type 2 SCD), you'd add:
-- - valid_from, valid_to dates
-- - is_current flag
-- - Version number
--
-- Example Type 2 row evolution:
-- product_id | price | valid_from | valid_to   | is_current
-- P001       | 99.99 | 2025-01-01 | 2026-01-15 | FALSE
-- P001       | 89.99 | 2026-01-16 | 9999-12-31 | TRUE
--
-- For this project, Type 1 is fine!
--
-- =============================================================================
-- MATERIALIZATION: TABLE VS VIEW
-- =============================================================================
--
-- This model: materialized='table'
-- - Runs: CREATE TABLE marts.dim_products AS (SQL)
-- - Stores data physically
-- - Fast queries (no computation needed)
-- - Takes storage space
-- - Perfect for marts layer
--
-- Compare to staging: materialized='view'
-- - Saves SQL definition
-- - Computes on-demand
-- - Always fresh
-- - Perfect for staging layer
--
-- =============================================================================