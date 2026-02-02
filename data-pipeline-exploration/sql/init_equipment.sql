-- Equipment and maintenance tables for ML features
-- These will coexist with your existing pipeline tables

CREATE TABLE IF NOT EXISTS equipment (
    equipment_id VARCHAR(50) PRIMARY KEY,
    equipment_type VARCHAR(100) NOT NULL,
    location VARCHAR(100),
    installation_date DATE NOT NULL,
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    rated_capacity INTEGER,
    last_maintenance_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sensors (
    sensor_id VARCHAR(50) PRIMARY KEY,
    equipment_id VARCHAR(50) REFERENCES equipment(equipment_id),
    sensor_type VARCHAR(50) NOT NULL,
    measurement_unit VARCHAR(20),
    normal_range_min DECIMAL(10,2),
    normal_range_max DECIMAL(10,2),
    critical_threshold DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS maintenance_history (
    maintenance_id SERIAL PRIMARY KEY,
    equipment_id VARCHAR(50) REFERENCES equipment(equipment_id),
    maintenance_date DATE NOT NULL,
    maintenance_type VARCHAR(50),
    description TEXT,
    cost DECIMAL(10,2),
    performed_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS failure_history (
    failure_id SERIAL PRIMARY KEY,
    equipment_id VARCHAR(50) REFERENCES equipment(equipment_id),
    failure_date TIMESTAMP NOT NULL,
    failure_type VARCHAR(100),
    severity VARCHAR(20),
    downtime_hours DECIMAL(6,2),
    repair_cost DECIMAL(10,2),
    root_cause TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_equipment_type ON equipment(equipment_type);
CREATE INDEX IF NOT EXISTS idx_sensors_equipment ON sensors(equipment_id);
CREATE INDEX IF NOT EXISTS idx_maintenance_equipment ON maintenance_history(equipment_id);
CREATE INDEX IF NOT EXISTS idx_failure_equipment ON failure_history(equipment_id);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO pipeline_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO pipeline_user;