-- Batch equipment features for ML training and serving
WITH equipment_base AS (
    SELECT * FROM {{ ref('stg_equipment') }}
),

maintenance_stats AS (
    SELECT
        equipment_id,
        
        -- Last maintenance date
        MAX(CASE WHEN event_type = 'maintenance' THEN maintenance_date END) AS last_maintenance_date,
        
        -- Maintenance counts (last 30 days)
        COUNT(CASE 
            WHEN event_type = 'maintenance' 
            AND maintenance_date >= CURRENT_DATE - {{ var('lookback_days_maintenance') }}
            THEN 1 
        END) AS maintenance_count_30d,
        
        -- Failure counts and metrics (last 90 days)
        COUNT(CASE 
            WHEN event_type = 'failure'
            AND maintenance_date >= CURRENT_DATE - {{ var('lookback_days_failures') }}
            THEN 1 
        END) AS failure_count_90d,
        
        AVG(CASE 
            WHEN event_type = 'failure'
            AND maintenance_date >= CURRENT_DATE - {{ var('lookback_days_failures') }}
            THEN downtime_hours 
        END) AS avg_downtime_hours_90d,
        
        SUM(CASE 
            WHEN event_type = 'failure'
            AND maintenance_date >= CURRENT_DATE - {{ var('lookback_days_failures') }}
            THEN maintenance_cost 
        END) AS total_repair_cost_90d,
        
        -- Weighted failure severity score
        AVG(CASE 
            WHEN event_type = 'failure'
            AND maintenance_date >= CURRENT_DATE - {{ var('lookback_days_failures') }}
            THEN severity_weight 
        END) AS avg_severity_score_90d
        
    FROM {{ ref('stg_maintenance_events') }}
    GROUP BY equipment_id
),

equipment_type_risk AS (
    -- Calculate type-level failure rates
    SELECT
        e.equipment_type,
        COUNT(DISTINCT m.equipment_id) AS equipment_count,
        COUNT(CASE WHEN m.event_type = 'failure' THEN 1 END) AS total_failures,
        COUNT(CASE WHEN m.event_type = 'failure' THEN 1 END)::float / 
            NULLIF(COUNT(DISTINCT m.equipment_id), 0) AS failures_per_equipment
    FROM equipment_base e
    LEFT JOIN {{ ref('stg_maintenance_events') }} m 
        ON e.equipment_id = m.equipment_id
        AND m.maintenance_date >= CURRENT_DATE - 365
    GROUP BY e.equipment_type
)

SELECT
    -- Primary key
    e.equipment_id,
    
    -- Equipment characteristics
    e.equipment_type,
    e.location,
    e.manufacturer,
    e.model,
    e.age_category,
    
    -- Age features
    e.age_days AS equipment_age_days,
    e.age_days * 24 AS total_operating_hours,  -- Simplified assumption
    
    -- Maintenance features
    COALESCE(
        CURRENT_DATE - m.last_maintenance_date,
        e.age_days  -- If never maintained, use equipment age
    ) AS days_since_maintenance,
    COALESCE(m.maintenance_count_30d, 0) AS maintenance_count_30d,
    
    -- Failure features
    COALESCE(m.failure_count_90d, 0) AS failure_count_90d,
    COALESCE(m.avg_downtime_hours_90d, 0) AS avg_downtime_hours_90d,
    COALESCE(m.total_repair_cost_90d, 0) AS total_repair_cost_90d,
    COALESCE(m.avg_severity_score_90d, 0) AS avg_severity_score_90d,
    
    -- Risk features
    COALESCE(r.failures_per_equipment, 0) AS equipment_type_risk_score,
    
    -- Create risk tier
    CASE
        WHEN COALESCE(m.failure_count_90d, 0) >= 2 THEN 'high_risk'
        WHEN COALESCE(m.failure_count_90d, 0) = 1 THEN 'medium_risk'
        WHEN e.age_days > 1095 THEN 'aging_risk'
        ELSE 'low_risk'
    END AS risk_tier,
    
    -- Metadata
    CURRENT_DATE AS feature_date,
    CURRENT_TIMESTAMP AS feature_timestamp
    
FROM equipment_base e
LEFT JOIN maintenance_stats m ON e.equipment_id = m.equipment_id
LEFT JOIN equipment_type_risk r ON e.equipment_type = r.equipment_type