SELECT 
    e.equipment_id
FROM {{ source('ml_source', 'equipment') }} e
LEFT JOIN {{ ref('batch_equipment_features') }} f 
    ON e.equipment_id = f.equipment_id
WHERE f.equipment_id IS NULL