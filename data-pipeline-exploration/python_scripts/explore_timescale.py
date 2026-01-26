"""
Explore TimescaleDB - Time-Series Database
Run: python python_scripts/explore_timescale.py
"""

import psycopg2
from tabulate import tabulate
from datetime import datetime

def connect_timescale():
    """Connect to TimescaleDB"""
    return psycopg2.connect(
        host="localhost",
        port=5433,  # Note: Different port!
        database="timeseries_db",
        user="timeseries_user",
        password="timeseries_pass"
    )

def show_hypertable_info(conn):
    """Show TimescaleDB-specific information"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("TIMESCALEDB HYPERTABLE INFO")
    print("="*60)
    
    # Check if TimescaleDB extension is enabled
    cursor.execute("SELECT extname, extversion FROM pg_extension WHERE extname = 'timescaledb';")
    result = cursor.fetchone()
    print(f"\n✅ TimescaleDB Extension: {result[0]} v{result[1]}")
    
    # Show hypertable details (compatible with newer versions)
    cursor.execute("""
        SELECT hypertable_schema,
               hypertable_name, 
               num_chunks
        FROM timescaledb_information.hypertables;
    """)
    
    rows = cursor.fetchall()
    print("\n📊 Hypertables:")
    print(tabulate(rows, 
                   headers=['Schema', 'Table', 'Chunks'],
                   tablefmt='grid'))
    
    # Get size information separately (works across versions)
    cursor.execute("""
        SELECT 
            schemaname || '.' || tablename as table_name,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as total_size
        FROM timescaledb_information.hypertables h
        JOIN pg_tables t ON h.hypertable_name = t.tablename;
    """)
    
    rows = cursor.fetchall()
    if rows:
        print("\n💾 Table Sizes:")
        print(tabulate(rows, 
                       headers=['Table', 'Total Size'],
                       tablefmt='grid'))
    
    cursor.close()

def explore_recent_readings(conn):
    """Look at recent sensor data"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("RECENT SENSOR READINGS (Last 10)")
    print("="*60)
    
    cursor.execute("""
        SELECT 
            time,
            sensor_id,
            temperature,
            humidity,
            pressure,
            location
        FROM sensor_readings
        ORDER BY time DESC
        LIMIT 10;
    """)
    
    rows = cursor.fetchall()
    
    # Format timestamps nicely
    formatted_rows = []
    for row in rows:
        formatted_rows.append([
            row[0].strftime('%Y-%m-%d %H:%M:%S'),
            row[1], 
            f"{row[2]}°C",
            f"{row[3]}%",
            f"{row[4]} hPa",
            row[5]
        ])
    
    print(tabulate(formatted_rows,
                   headers=['Time', 'Sensor', 'Temp', 'Humidity', 'Pressure', 'Location'],
                   tablefmt='grid'))
    
    cursor.close()

def time_series_analytics(conn):
    """Run time-series specific queries"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("TIME-SERIES ANALYTICS")
    print("="*60)
    
    # Average readings per sensor (last hour)
    cursor.execute("""
        SELECT 
            sensor_id,
            COUNT(*) as reading_count,
            ROUND(AVG(temperature)::numeric, 2) as avg_temp,
            ROUND(MIN(temperature)::numeric, 2) as min_temp,
            ROUND(MAX(temperature)::numeric, 2) as max_temp
        FROM sensor_readings
        WHERE time > NOW() - INTERVAL '1 hour'
        GROUP BY sensor_id
        ORDER BY sensor_id;
    """)
    
    rows = cursor.fetchall()
    print("\n🌡️  Temperature Stats (Last Hour):")
    print(tabulate(rows,
                   headers=['Sensor', 'Readings', 'Avg Temp', 'Min Temp', 'Max Temp'],
                   tablefmt='grid'))
    
    # Time bucket aggregation (5-minute windows)
    cursor.execute("""
        SELECT 
            time_bucket('5 minutes', time) AS five_min_bucket,
            sensor_id,
            ROUND(AVG(temperature)::numeric, 2) as avg_temp,
            COUNT(*) as reading_count
        FROM sensor_readings
        WHERE time > NOW() - INTERVAL '30 minutes'
        GROUP BY five_min_bucket, sensor_id
        ORDER BY five_min_bucket DESC, sensor_id
        LIMIT 15;
    """)
    
    rows = cursor.fetchall()
    formatted_rows = [[row[0].strftime('%H:%M:%S'), row[1], row[2], row[3]] for row in rows]
    
    print("\n📊 5-Minute Aggregations (Last 30 min):")
    print(tabulate(formatted_rows,
                   headers=['Time Bucket', 'Sensor', 'Avg Temp', 'Count'],
                   tablefmt='grid'))
    
    cursor.close()

def show_data_distribution(conn):
    """Show how data is distributed across time"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("DATA DISTRIBUTION OVER TIME")
    print("="*60)
    
    cursor.execute("""
        SELECT 
            date_trunc('hour', time) as hour,
            COUNT(*) as reading_count
        FROM sensor_readings
        GROUP BY hour
        ORDER BY hour DESC;
    """)
    
    rows = cursor.fetchall()
    formatted_rows = [[row[0].strftime('%Y-%m-%d %H:00'), row[1]] for row in rows]
    
    print(tabulate(formatted_rows,
                   headers=['Hour', 'Reading Count'],
                   tablefmt='grid'))
    
    cursor.close()

def compare_locations(conn):
    """Compare sensor readings across locations"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("COMPARISON BY LOCATION")
    print("="*60)
    
    cursor.execute("""
        SELECT 
            location,
            COUNT(DISTINCT sensor_id) as sensor_count,
            COUNT(*) as total_readings,
            ROUND(AVG(temperature)::numeric, 2) as avg_temp,
            ROUND(AVG(humidity)::numeric, 2) as avg_humidity
        FROM sensor_readings
        GROUP BY location
        ORDER BY location;
    """)
    
    rows = cursor.fetchall()
    print(tabulate(rows,
                   headers=['Location', 'Sensors', 'Readings', 'Avg Temp', 'Avg Humidity'],
                   tablefmt='grid'))
    
    cursor.close()

def interactive_query(conn):
    """Let user run custom SQL"""
    cursor = conn.cursor()
    
    print("\n" + "="*60)
    print("INTERACTIVE SQL (Type 'exit' to quit)")
    print("="*60)
    
    while True:
        query = input("\nSQL> ").strip()
        
        if query.lower() == 'exit':
            break
        
        try:
            cursor.execute(query)
            
            # If SELECT query, show results
            if query.lower().startswith('select'):
                rows = cursor.fetchall()
                if cursor.description:
                    headers = [desc[0] for desc in cursor.description]
                    print(tabulate(rows, headers=headers, tablefmt='grid'))
            else:
                conn.commit()
                print(f"✅ Query executed successfully!")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            conn.rollback()
    
    cursor.close()

if __name__ == "__main__":
    conn = connect_timescale()
    
    try:
        show_hypertable_info(conn)
        explore_recent_readings(conn)
        time_series_analytics(conn)
        show_data_distribution(conn)
        compare_locations(conn)

        print("\n✨ Try running your own queries!")
        print("Example: SELECT * FROM sensor_readings WHERE sensor_id = 'sensor_001' LIMIT 5;")
        
        interactive_query(conn)
    
        
    finally:
        conn.close()
        print("\n👋 Connection closed!")