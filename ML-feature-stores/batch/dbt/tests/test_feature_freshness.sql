SELECT *
FROM {{ ref('batch_equipment_features') }}
WHERE feature_date != CURRENT_DATE