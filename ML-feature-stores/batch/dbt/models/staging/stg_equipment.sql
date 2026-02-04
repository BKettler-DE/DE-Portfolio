-- Standardize equipment data
SELECT
    equipment_id,
    equipment_type,
    location,
    installation_date,
    manufacturer,
    model,
    rated_capacity,
    last_maintenance_date,
    
    -- Computed fields
    CURRENT_DATE - installation_date AS age_days,
    
    -- Categorize equipment age
    CASE
        WHEN CURRENT_DATE - installation_date < 365 THEN 'new'
        WHEN CURRENT_DATE - installation_date < 1095 THEN 'established'
        ELSE 'aging'
    END AS age_category,
    
    -- Metadata
    created_at,
    updated_at
    
FROM {{ source('ml_source', 'equipment') }}
WHERE installation_date IS NOT NULL