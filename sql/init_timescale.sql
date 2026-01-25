-- This runs automatically when TimescaleDB container starts

-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create table for sensor readings
CREATE TABLE sensor_readings (
    time TIMESTAMPTZ NOT NULL,
    sensor_id VARCHAR(50) NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2),
    pressure DECIMAL(6,2),
    location VARCHAR(100)
);

-- Convert to hypertable (TimescaleDB magic!)
SELECT create_hypertable('sensor_readings', 'time');

-- Create table for invalid sensor data
CREATE TABLE sensor_readings_invalid (
    time TIMESTAMPTZ NOT NULL,
    sensor_id VARCHAR(50),
    raw_data JSONB,
    issues TEXT
);

SELECT create_hypertable('sensor_readings_invalid', 'time');

-- Insert sample sensor data (last 2 hours)
INSERT INTO sensor_readings (time, sensor_id, temperature, humidity, pressure, location)
SELECT 
    NOW() - (random() * INTERVAL '2 hours'),
    'sensor_' || LPAD((random() * 3)::int::text, 3, '0'),
    20 + (random() * 10)::numeric(5,2),
    40 + (random() * 30)::numeric(5,2),
    1000 + (random() * 20)::numeric(6,2),
    CASE (random() * 2)::int 
        WHEN 0 THEN 'warehouse_a'
        WHEN 1 THEN 'warehouse_b'
        ELSE 'warehouse_c'
    END
FROM generate_series(1, 1000);

-- Create indexes for common queries
CREATE INDEX idx_sensor_readings_sensor_time ON sensor_readings (sensor_id, time DESC);

COMMENT ON TABLE sensor_readings IS 'Valid time-series sensor data';
COMMENT ON TABLE sensor_readings_invalid IS 'Invalid sensor readings with validation issues';