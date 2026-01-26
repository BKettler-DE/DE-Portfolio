"""
Stream Validator - Kafka Consumer
Consumes sensor readings from Kafka, validates them, and saves to TimescaleDB

Usage:
    python stream_validator.py
"""

import json
import psycopg2
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError

class StreamValidator:
    """Real-time validation of streaming sensor data"""
    
    def __init__(self, 
                 kafka_bootstrap_servers='localhost:9092',
                 db_host='localhost',
                 db_port=5433):
        """Initialize Kafka consumer and database connection"""
        
        # Connect to Kafka
        print("Connecting to Kafka...")
        try:
            self.consumer = KafkaConsumer(
                'sensor_readings_raw',
                bootstrap_servers=[kafka_bootstrap_servers],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',  # Start from beginning
                enable_auto_commit=True,
                group_id='sensor_validator_group',
                consumer_timeout_ms=10000  # Exit if no messages for 10 seconds
            )
            print("✅ Connected to Kafka consumer")
        except KafkaError as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            print("\nTroubleshooting:")
            print("1. Is Kafka running? Check: docker-compose ps")
            print("2. Has the producer sent data? Run sensor_simulator.py first")
            raise
        
        # Connect to TimescaleDB
        print("Connecting to TimescaleDB...")
        try:
            self.conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database="timeseries_db",
                user="timeseries_user",
                password="timeseries_pass"
            )
            self.conn.autocommit = True
            print("✅ Connected to TimescaleDB")
        except Exception as e:
            print(f"❌ Failed to connect to TimescaleDB: {e}")
            print("\nTroubleshooting:")
            print("1. Is TimescaleDB running? Check: docker-compose ps")
            print("2. Check logs: docker logs my_timescaledb")
            raise
        
        # Deduplication tracking (in-memory)
        self.seen_readings = {}
        self.max_seen_cache = 10000
        
        # Statistics
        self.stats = {
            'processed': 0,
            'valid': 0,
            'duplicates': 0,
            'missing_fields': 0,
            'out_of_range': 0,
            'late_data': 0,
            'other_errors': 0
        }
    
    def is_duplicate(self, reading):
        """Check if reading is a duplicate using sensor_id + timestamp"""
        
        sensor_id = reading.get('sensor_id')
        timestamp = reading.get('timestamp')
        
        if not sensor_id or not timestamp:
            return False
        
        key = f"{sensor_id}_{timestamp}"
        
        if key in self.seen_readings:
            return True
        
        # Mark as seen
        self.seen_readings[key] = True
        
        # Prevent memory overflow
        if len(self.seen_readings) > self.max_seen_cache:
            keys_to_remove = list(self.seen_readings.keys())[:self.max_seen_cache // 2]
            for k in keys_to_remove:
                del self.seen_readings[k]
        
        return False
    
    def validate_reading(self, reading):
        """
        Validate a sensor reading
        
        Returns:
            (is_valid: bool, issues: list of strings)
        """
        issues = []
        
        # Check required fields
        required_fields = ['sensor_id', 'timestamp', 'temperature', 'humidity', 'pressure']
        for field in required_fields:
            if field not in reading:
                issues.append(f"Missing required field: {field}")
        
        if issues:
            self.stats['missing_fields'] += 1
            return False, issues
        
        # Validate timestamp and check if late
        try:
            ts = datetime.fromisoformat(reading['timestamp'])
            age = datetime.now() - ts
            
            if age > timedelta(minutes=30):
                issues.append(f"Late data: {age.total_seconds()/60:.1f} minutes old")
                self.stats['late_data'] += 1
                
        except (ValueError, TypeError) as e:
            issues.append(f"Invalid timestamp format: {reading['timestamp']}")
            self.stats['other_errors'] += 1
            return False, issues
        
        # Validate temperature range (-40 to 85°C)
        temp = reading.get('temperature')
        if not isinstance(temp, (int, float)) or not (-40 <= temp <= 85):
            issues.append(f"Temperature out of range: {temp}°C (expected -40 to 85)")
            self.stats['out_of_range'] += 1
            return False, issues
        
        # Validate humidity range (0-100%)
        humidity = reading.get('humidity')
        if not isinstance(humidity, (int, float)) or not (0 <= humidity <= 100):
            issues.append(f"Humidity out of range: {humidity}% (expected 0-100)")
            self.stats['out_of_range'] += 1
            return False, issues
        
        # Validate pressure range
        pressure = reading.get('pressure')
        if not isinstance(pressure, (int, float)) or not (900 <= pressure <= 1100):
            issues.append(f"Pressure out of range: {pressure} hPa (expected 900-1100)")
            self.stats['out_of_range'] += 1
            return False, issues
        
        return len(issues) == 0, issues
    
    def save_to_database(self, reading, is_valid, issues=None):
        """Save reading to appropriate table"""
        cursor = self.conn.cursor()
        
        try:
            if is_valid:
                # Save to valid readings table
                cursor.execute("""
                    INSERT INTO sensor_readings 
                    (time, sensor_id, temperature, humidity, pressure, location)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    reading['timestamp'],
                    reading['sensor_id'],
                    reading['temperature'],
                    reading['humidity'],
                    reading['pressure'],
                    reading.get('location', 'unknown')
                ))
                
            else:
                # Save to invalid readings table
                cursor.execute("""
                    INSERT INTO sensor_readings_invalid 
                    (time, sensor_id, raw_data, issues)
                    VALUES (%s, %s, %s, %s)
                """, (
                    reading.get('timestamp', datetime.now()),
                    reading.get('sensor_id', 'unknown'),
                    json.dumps(reading),
                    ', '.join(issues) if issues else 'Unknown error'
                ))
            
            cursor.close()
            
        except Exception as e:
            print(f"  ❌ Database error: {e}")
            self.stats['other_errors'] += 1
            cursor.close()
    
    def process_stream(self, max_messages=None):
        """
        Main processing loop
        
        Args:
            max_messages: Stop after processing this many (None = infinite)
        """
        print("\n" + "="*60)
        print("STREAM VALIDATOR - STARTING")
        print("="*60)
        print("Consuming from topic: sensor_readings_raw")
        print("Validating and saving to TimescaleDB")
        print("\n🟢 Processing messages... (Press Ctrl+C to stop)\n")
        
        messages_found = False
        
        try:
            for message in self.consumer:
                messages_found = True
                reading = message.value
                self.stats['processed'] += 1
                
                # Check for duplicates
                if self.is_duplicate(reading):
                    self.stats['duplicates'] += 1
                    if self.stats['duplicates'] % 10 == 1:
                        print(f"  🟡 Duplicate detected: {reading.get('sensor_id')} at {reading.get('timestamp')}")
                    continue
                
                # Validate
                is_valid, issues = self.validate_reading(reading)
                
                if is_valid:
                    self.stats['valid'] += 1
                else:
                    print(f"  🔴 Invalid: {', '.join(issues)}")
                
                # Save to database
                self.save_to_database(reading, is_valid, issues)
                
                # Print progress every 50 messages
                if self.stats['processed'] % 50 == 0:
                    self._print_progress()
                
                # Stop if reached max
                if max_messages and self.stats['processed'] >= max_messages:
                    print(f"\n✅ Reached max messages ({max_messages})")
                    break
            
            # If we exit the loop and found no messages
            if not messages_found:
                print("\n⚠️  No messages found in Kafka topic 'sensor_readings_raw'")
                print("\nPossible reasons:")
                print("1. Producer hasn't sent data yet - Run sensor_simulator.py first")
                print("2. Messages were already consumed by this consumer group")
                print("3. Topic doesn't exist yet - Kafka will create it when producer sends data")
                
        except KeyboardInterrupt:
            print("\n\n⏸️  Stopped by user")
        except Exception as e:
            print(f"\n❌ Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self._print_final_stats()
            self.cleanup()
    
    def _print_progress(self):
        """Print current processing statistics"""
        total = self.stats['processed']
        valid = self.stats['valid']
        invalid = total - valid - self.stats['duplicates']
        
        print(f"\n📊 Processed: {total} | Valid: {valid} | Invalid: {invalid} | Duplicates: {self.stats['duplicates']}")
    
    def _print_final_stats(self):
        """Print final statistics"""
        print("\n" + "="*60)
        print("VALIDATION STATISTICS")
        print("="*60)
        print(f"📊 Total processed: {self.stats['processed']}")
        print(f"✅ Valid: {self.stats['valid']}")
        print(f"❌ Invalid: {self.stats['processed'] - self.stats['valid'] - self.stats['duplicates']}")
        print(f"\nBreakdown:")
        print(f"  🟡 Duplicates: {self.stats['duplicates']}")
        print(f"  🔴 Missing fields: {self.stats['missing_fields']}")
        print(f"  🔴 Out of range: {self.stats['out_of_range']}")
        print(f"  🟡 Late data: {self.stats['late_data']}")
        print(f"  ❌ Other errors: {self.stats['other_errors']}")
        
        if self.stats['processed'] > 0:
            valid_pct = (self.stats['valid'] / self.stats['processed']) * 100
            print(f"\n📈 Data quality: {valid_pct:.1f}% valid")
        
        print("="*60)
    
    def cleanup(self):
        """Close connections"""
        self.consumer.close()
        self.conn.close()
        print("\n👋 Validator stopped\n")


if __name__ == "__main__":
    print("Starting Stream Validator (Kafka Consumer)")
    print("This will consume and validate sensor readings from Kafka")
    print("Press Ctrl+C to stop\n")
    
    validator = StreamValidator()
    validator.process_stream()
        