"""
ML Feature Store - Sample Data Loader
Loads equipment, sensor, maintenance, and failure data for ML features
Works with your existing PostgreSQL and TimescaleDB setup
"""

import psycopg2
from datetime import datetime, timedelta
import random

# Use your existing database credentials
POSTGRES_CONN = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'pipeline_db',  # Your existing database
    'user': 'pipeline_user',
    'password': 'pipeline_pass'
}

TIMESCALE_CONN = {
    'host': 'localhost',
    'port': 5433,
    'dbname': 'timeseries_db',  # Your existing TimescaleDB
    'user': 'timeseries_user',
    'password': 'timeseries_pass'
}

# Sample data - Equipment inventory
EQUIPMENT_DATA = [
    ('PUMP-001', 'Centrifugal Pump', 'Building A', '2023-01-15', 'Acme Pumps', 'CP-500', 500, '2025-12-01'),
    ('PUMP-002', 'Centrifugal Pump', 'Building A', '2023-03-20', 'Acme Pumps', 'CP-500', 500, '2025-11-15'),
    ('PUMP-003', 'Centrifugal Pump', 'Building B', '2022-06-10', 'FlowTech', 'FT-750', 750, '2025-10-20'),
    ('COMPRESSOR-001', 'Air Compressor', 'Building C', '2021-09-05', 'AirMax', 'AM-1000', 1000, '2025-12-10'),
    ('COMPRESSOR-002', 'Air Compressor', 'Building C', '2022-11-12', 'AirMax', 'AM-1000', 1000, '2025-09-15'),
    ('MOTOR-001', 'Electric Motor', 'Building A', '2020-04-18', 'MotorCorp', 'MC-250', 250, '2025-11-01'),
    ('MOTOR-002', 'Electric Motor', 'Building B', '2020-08-22', 'MotorCorp', 'MC-250', 250, '2025-12-05'),
    ('MOTOR-003', 'Electric Motor', 'Building D', '2023-02-14', 'MotorCorp', 'MC-500', 500, '2025-10-10'),
    ('HVAC-001', 'HVAC System', 'Building A', '2019-12-01', 'ClimateControl', 'CC-2000', 2000, '2025-11-20'),
    ('HVAC-002', 'HVAC System', 'Building B', '2019-12-01', 'ClimateControl', 'CC-2000', 2000, '2025-12-15'),
]

# Sensor to equipment mapping
SENSOR_DATA = [
    ('SENSOR-001', 'PUMP-001', 'Temperature', 'Celsius', 60, 80, 90),
    ('SENSOR-002', 'PUMP-002', 'Temperature', 'Celsius', 60, 80, 90),
    ('SENSOR-003', 'PUMP-003', 'Temperature', 'Celsius', 60, 80, 90),
    ('SENSOR-004', 'COMPRESSOR-001', 'Temperature', 'Celsius', 70, 90, 100),
    ('SENSOR-005', 'COMPRESSOR-002', 'Temperature', 'Celsius', 70, 90, 100),
    ('SENSOR-006', 'MOTOR-001', 'Temperature', 'Celsius', 65, 85, 95),
    ('SENSOR-007', 'MOTOR-002', 'Temperature', 'Celsius', 65, 85, 95),
    ('SENSOR-008', 'MOTOR-003', 'Temperature', 'Celsius', 65, 85, 95),
    ('SENSOR-009', 'HVAC-001', 'Temperature', 'Celsius', 55, 75, 85),
    ('SENSOR-010', 'HVAC-002', 'Temperature', 'Celsius', 55, 75, 85),
]


def load_equipment_data():
    """Load equipment and sensor data into PostgreSQL"""
    print("\n" + "="*70)
    print("LOADING EQUIPMENT DATA INTO POSTGRESQL")
    print("="*70)
    
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    
    try:
        # Load equipment data
        print("\n1. Loading equipment records...")
        for equip in EQUIPMENT_DATA:
            cur.execute("""
                INSERT INTO equipment (
                    equipment_id, equipment_type, location, installation_date,
                    manufacturer, model, rated_capacity, last_maintenance_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (equipment_id) DO NOTHING
            """, equip)
        
        print(f"   ✅ Loaded {len(EQUIPMENT_DATA)} equipment records")
        
        # Load sensor data
        print("\n2. Loading sensor mappings...")
        for sensor in SENSOR_DATA:
            cur.execute("""
                INSERT INTO sensors (
                    sensor_id, equipment_id, sensor_type, measurement_unit,
                    normal_range_min, normal_range_max, critical_threshold
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sensor_id) DO NOTHING
            """, sensor)
        
        print(f"   ✅ Loaded {len(SENSOR_DATA)} sensor records")
        
        # Generate sample maintenance history
        print("\n3. Generating maintenance history...")
        maintenance_types = ['Routine', 'Preventive', 'Corrective', 'Emergency']
        maintenance_count = 0
        
        for equipment_id, *_ in EQUIPMENT_DATA:
            # Generate 3-10 random maintenance records per equipment
            num_records = random.randint(3, 10)
            for _ in range(num_records):
                days_ago = random.randint(30, 365)
                maintenance_date = datetime.now() - timedelta(days=days_ago)
                
                cur.execute("""
                    INSERT INTO maintenance_history (
                        equipment_id, maintenance_date, maintenance_type,
                        description, cost, performed_by
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    equipment_id,
                    maintenance_date.date(),
                    random.choice(maintenance_types),
                    f"Maintenance performed on {equipment_id}",
                    round(random.uniform(100, 5000), 2),
                    f"Technician {random.randint(1, 10)}"
                ))
                maintenance_count += 1
        
        print(f"   ✅ Generated {maintenance_count} maintenance records")
        
        # Generate sample failure history (labels for ML training)
        print("\n4. Generating failure history (ML training labels)...")
        failure_types = ['Overheating', 'Mechanical Wear', 'Electrical', 'Seal Failure']
        severities = ['Low', 'Medium', 'High', 'Critical']
        failure_count = 0
        
        for equipment_id, *_ in EQUIPMENT_DATA:
            # 30% chance of having failures
            if random.random() < 0.3:
                num_failures = random.randint(1, 3)
                for _ in range(num_failures):
                    days_ago = random.randint(10, 180)
                    failure_date = datetime.now() - timedelta(days=days_ago)
                    
                    cur.execute("""
                        INSERT INTO failure_history (
                            equipment_id, failure_date, failure_type, severity,
                            downtime_hours, repair_cost, root_cause
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        equipment_id,
                        failure_date,
                        random.choice(failure_types),
                        random.choice(severities),
                        round(random.uniform(1, 48), 2),
                        round(random.uniform(500, 15000), 2),
                        "Investigation pending"
                    ))
                    failure_count += 1
        
        print(f"   ✅ Generated {failure_count} failure records")
        print(f"   💡 These failures are your ML training labels!")
        
        conn.commit()
        print("\n✅ PostgreSQL data loaded successfully")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error loading data: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def load_sensor_events():
    """Load historical sensor events into TimescaleDB"""
    print("\n" + "="*70)
    print("LOADING SENSOR EVENTS INTO TIMESCALEDB")
    print("="*70)
    
    conn = psycopg2.connect(**TIMESCALE_CONN)
    cur = conn.cursor()
    
    try:
        # Create sensor_events table if it doesn't exist
        print("\n1. Creating sensor_events table...")
        
        # First, drop the table if it exists (to start fresh)
        cur.execute("DROP TABLE IF EXISTS sensor_events CASCADE;")
        
        # Create table WITHOUT primary key (TimescaleDB handles uniqueness differently)
        cur.execute("""
            CREATE TABLE sensor_events (
                event_id VARCHAR(100) NOT NULL,
                sensor_id VARCHAR(50) NOT NULL,
                timestamp TIMESTAMPTZ NOT NULL,
                temperature DECIMAL(6,2),
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            );
        """)
        print("   ✅ Created sensor_events table")
        
        # Convert to hypertable BEFORE adding indexes
        cur.execute("""
            SELECT create_hypertable('sensor_events', 'timestamp',
                chunk_time_interval => INTERVAL '1 day'
            );
        """)
        print("   ✅ Created TimescaleDB hypertable")
        
        # Now add indexes (after hypertable conversion)
        cur.execute("""
            CREATE INDEX idx_sensor_events_sensor_time 
                ON sensor_events (sensor_id, timestamp DESC);
        """)
        
        cur.execute("""
            CREATE INDEX idx_sensor_events_event_id 
                ON sensor_events (event_id, timestamp);
        """)
        print("   ✅ Created indexes")
        
        conn.commit()
        
        # Generate historical sensor events (last 7 days)
        print("\n2. Generating sensor events (this may take 15-30 seconds)...")
        
        start_date = datetime.now() - timedelta(days=7)
        
        batch = []
        total_events = 0
        
        for sensor_id, _, sensor_type, _, min_temp, max_temp, _ in SENSOR_DATA:
            base_temp = (min_temp + max_temp) / 2
            
            # Generate 200 events per sensor (spread over 7 days)
            for i in range(200):
                # Random time within the 7-day window
                minutes_offset = random.randint(0, 7 * 24 * 60)
                timestamp = start_date + timedelta(minutes=minutes_offset)
                
                # Add daily cycle (higher during day, lower at night)
                hour_factor = 1.0 + 0.2 * abs(12 - timestamp.hour) / 12
                
                # Add random noise
                temperature = base_temp * hour_factor + random.gauss(0, 3)
                
                # Occasional anomalies (5% chance - these help test anomaly detection)
                if random.random() < 0.05:
                    temperature += random.uniform(10, 20)
                
                event_id = f"{sensor_id}_{timestamp.strftime('%Y%m%d%H%M%S')}_{i}"
                batch.append((event_id, sensor_id, timestamp, round(temperature, 2)))
                
                # Insert in batches of 500 for performance
                if len(batch) >= 500:
                    cur.executemany("""
                        INSERT INTO sensor_events (
                            event_id, sensor_id, timestamp, temperature
                        ) VALUES (%s, %s, %s, %s)
                    """, batch)
                    total_events += len(batch)
                    batch = []
                    print(f"   Loaded {total_events} events...", end='\r')
        
        # Insert remaining events
        if batch:
            cur.executemany("""
                INSERT INTO sensor_events (
                    event_id, sensor_id, timestamp, temperature
                ) VALUES (%s, %s, %s, %s)
            """, batch)
            total_events += len(batch)
        
        conn.commit()
        print(f"\n   ✅ Loaded {total_events} sensor events")
        
        # Show data range
        cur.execute("SELECT MIN(timestamp), MAX(timestamp) FROM sensor_events")
        min_ts, max_ts = cur.fetchone()
        print(f"   ✅ Event date range: {min_ts} to {max_ts}")
        
        print("\n✅ TimescaleDB data loaded successfully")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error loading sensor events: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def verify_data():
    """Verify data was loaded correctly"""
    print("\n" + "="*70)
    print("VERIFYING DATA")
    print("="*70)
    
    # Check PostgreSQL
    print("\nPostgreSQL (pipeline_db):")
    conn = psycopg2.connect(**POSTGRES_CONN)
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM equipment")
    equipment_count = cur.fetchone()[0]
    print(f"  ✅ Equipment records: {equipment_count}")
    
    cur.execute("SELECT COUNT(*) FROM sensors")
    sensor_count = cur.fetchone()[0]
    print(f"  ✅ Sensor records: {sensor_count}")
    
    cur.execute("SELECT COUNT(*) FROM maintenance_history")
    maintenance_count = cur.fetchone()[0]
    print(f"  ✅ Maintenance records: {maintenance_count}")
    
    cur.execute("SELECT COUNT(*) FROM failure_history")
    failure_count = cur.fetchone()[0]
    print(f"  ✅ Failure records: {failure_count} (ML labels)")
    
    # Show some sample data
    print("\n  Sample Equipment:")
    cur.execute("SELECT equipment_id, equipment_type, location FROM equipment LIMIT 3")
    for row in cur.fetchall():
        print(f"    - {row[0]}: {row[1]} in {row[2]}")
    
    cur.close()
    conn.close()
    
    # Check TimescaleDB
    print("\nTimescaleDB (timeseries_db):")
    conn = psycopg2.connect(**TIMESCALE_CONN)
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) FROM sensor_events")
    events_count = cur.fetchone()[0]
    print(f"  ✅ Sensor events: {events_count}")
    
    # Show recent sensor activity
    print("\n  Recent sensor readings:")
    cur.execute("""
        SELECT sensor_id, 
               COUNT(*) as reading_count,
               ROUND(AVG(temperature), 2) as avg_temp,
               MAX(timestamp) as last_reading
        FROM sensor_events
        GROUP BY sensor_id
        ORDER BY sensor_id
        LIMIT 5
    """)
    for row in cur.fetchall():
        print(f"    - {row[0]}: {row[1]} readings, avg temp {row[2]}°C, last: {row[3]}")
    
    cur.close()
    conn.close()
    
    print("\n" + "="*70)
    print("✅ LAYER 2: DATA LAYER COMPLETE!")
    print("="*70)
    print("\nYou now have:")
    print("  • Equipment inventory in PostgreSQL")
    print("  • Sensor mappings connecting sensors to equipment")
    print("  • Maintenance history (for feature engineering)")
    print("  • Failure history (ML training labels)")
    print("  • Historical sensor events in TimescaleDB")
    print("\n" + "="*70)


if __name__ == '__main__':
    print("\n" + "="*70)
    print("ML FEATURE STORE - SAMPLE DATA LOADER")
    print("="*70)
    print("\nThis will populate your existing databases with ML training data:")
    print("  • PostgreSQL: Equipment, sensors, maintenance, failures")
    print("  • TimescaleDB: Historical sensor temperature readings")
    print("\n⏱️  Estimated time: 30-60 seconds")
    print("="*70)
    
    try:
        load_equipment_data()
        load_sensor_events()
        verify_data()
        
    except Exception as e:
        print(f"\n❌ Failed to load data: {e}")
        print("\nTroubleshooting:")
        print("  1. Check Docker containers are running: docker ps")
        print("  2. Check PostgreSQL is accessible: docker exec my_postgres psql -U pipeline_user -d pipeline_db -c 'SELECT 1'")
        print("  3. Check TimescaleDB is accessible: docker exec my_timescaledb psql -U timeseries_user -d timeseries_db -c 'SELECT 1'")
        exit(1)