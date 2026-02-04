-- Combine maintenance and failure events into unified timeline
WITH maintenance AS (
    SELECT
        equipment_id,
        maintenance_date,
        maintenance_type,
        description,
        cost AS maintenance_cost,
        NULL AS failure_type,
        NULL AS severity,
        NULL AS downtime_hours,
        NULL AS severity_weight,
        'maintenance' AS event_type
    FROM {{ source('ml_source', 'maintenance_history') }}
),

failures AS (
    SELECT
        equipment_id,
        failure_date::date AS maintenance_date,
        'Failure' AS maintenance_type,
        root_cause AS description,
        repair_cost AS maintenance_cost,
        failure_type,
        severity,
        downtime_hours,
        
        -- Severity weight for risk scoring
        CASE severity
            WHEN 'Low' THEN 1
            WHEN 'Medium' THEN 2
            WHEN 'High' THEN 3
            WHEN 'Critical' THEN 4
            ELSE 1
        END AS severity_weight,
        'failure' AS event_type
        
    FROM {{ source('ml_source', 'failure_history') }}
)

SELECT * FROM maintenance
UNION ALL
SELECT * FROM failures