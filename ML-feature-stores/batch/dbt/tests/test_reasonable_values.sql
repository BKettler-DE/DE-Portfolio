SELECT *
FROM {{ ref('batch_equipment_features') }}
WHERE 
    -- Equipment age shouldn't exceed 50 years
    equipment_age_days > 18250  
    -- Days since maintenance shouldn't exceed equipment age
    OR days_since_maintenance > equipment_age_days
    -- Risk score shouldn't exceed 100 failures per equipment
    OR equipment_type_risk_score > 100
    -- Maintenance count shouldn't exceed 100 in 30 days (unrealistic)
    OR maintenance_count_30d > 100