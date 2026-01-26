"""
IoT Sensor Simulator - Kafka Producer
Generates sensor readings and sends them to Kafka

Usage:
    python sensor_simulator.py
"""

import random
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

class SensorSimulator:
    """Generate realistic (and sometimes problematic) IoT sensor data"""
    
    def __init__(self, kafka_bootstrap_servers='localhost:9092'):
        """Initialize Kafka producer"""
        print("Connecting to Kafka...")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[kafka_bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3
            )
            print("✅ Connected to Kafka broker at localhost:9092")
        except KafkaError as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            print("\nTroubleshooting:")
            print("1. Is Kafka running? Check: docker-compose ps")
            print("2. Is Kafka healthy? Check: docker logs my_kafka")
            print("3. Wait 30 seconds after starting Docker and try again")
            raise
        
        # Three sensors in different locations
        self.sensors = [
            {'id': 'sensor_001', 'location': 'warehouse_a'},
            {'id': 'sensor_002', 'location': 'warehouse_b'},
            {'id': 'sensor_003', 'location': 'warehouse_c'}
        ]
        
        # Statistics tracking
        self.stats = {
            'total_sent': 0,
            'duplicates_sent': 0,
            'invalid_sent': 0,
            'late_data_sent': 0
        }
    
    def generate_reading(self, sensor):
        """Generate a single sensor reading (sometimes messy!)"""
        
        # Base reading (valid)
        reading = {
            'sensor_id': sensor['id'],
            'location': sensor['location'],
            'timestamp': datetime.now().isoformat(),
            'temperature': round(random.uniform(18.0, 28.0), 2),
            'humidity': round(random.uniform(35.0, 65.0), 2),
            'pressure': round(random.uniform(995.0, 1015.0), 2)
        }
        
        # Introduce data quality issues (8% of time)
        issue_type = random.random()
        
        if issue_type < 0.02:  # 2% missing timestamp
            del reading['timestamp']
            self.stats['invalid_sent'] += 1
            print(f"  🔴 Sending reading WITHOUT timestamp from {sensor['id']}")
            
        elif issue_type < 0.05:  # 3% out-of-range temperature
            reading['temperature'] = round(random.uniform(-50, 100), 2)
            self.stats['invalid_sent'] += 1
            print(f"  🔴 Sending INVALID temperature: {reading['temperature']}°C")
            
        elif issue_type < 0.07:  # 2% late data (old timestamp)
            minutes_late = random.randint(10, 60)
            old_time = datetime.now() - timedelta(minutes=minutes_late)
            reading['timestamp'] = old_time.isoformat()
            self.stats['late_data_sent'] += 1
            print(f"  🟡 Sending LATE data ({minutes_late} min old)")
            
        elif issue_type < 0.08:  # 1% missing sensor_id
            del reading['sensor_id']
            self.stats['invalid_sent'] += 1
            print(f"  🔴 Sending reading WITHOUT sensor_id")
        
        return reading
    
    def send_reading(self, reading, duplicate=False):
        """Send reading to Kafka"""
        
        # Use sensor_id as key for partitioning
        key = reading.get('sensor_id', 'unknown')
        
        try:
            future = self.producer.send(
                'sensor_readings_raw',
                key=key,
                value=reading
            )
            
            # Wait for send to complete
            result = future.get(timeout=10)
            
            self.stats['total_sent'] += 1
            
            if not duplicate:
                # Print every 50th normal reading
                if self.stats['total_sent'] % 50 == 0:
                    print(f"  ✅ Sent {self.stats['total_sent']} readings...")
            
        except KafkaError as e:
            print(f"  ❌ Failed to send: {e}")
    
    def run(self, duration_seconds=60, readings_per_second=10):
        """
        Run the simulator for specified duration
        
        Args:
            duration_seconds: How long to run (default 60 seconds)
            readings_per_second: Rate of data generation (default 10/sec)
        """
        print("\n" + "="*60)
        print("IOT SENSOR SIMULATOR - STARTING")
        print("="*60)
        print(f"Duration: {duration_seconds} seconds")
        print(f"Rate: {readings_per_second} readings/second")
        print(f"Sensors: {len(self.sensors)}")
        print(f"Target messages: ~{duration_seconds * readings_per_second}")
        print("\n🟢 Generating data... (Press Ctrl+C to stop early)\n")
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        sleep_interval = 1.0 / readings_per_second
        
        try:
            while time.time() < end_time:
                # Pick a random sensor
                sensor = random.choice(self.sensors)
                
                # Generate reading
                reading = self.generate_reading(sensor)
                
                # Send to Kafka
                self.send_reading(reading)
                
                # Occasionally send a duplicate (1% chance)
                if random.random() < 0.01:
                    print(f"  🟡 Sending DUPLICATE from {sensor['id']}")
                    self.send_reading(reading, duplicate=True)
                    self.stats['duplicates_sent'] += 1
                
                # Wait before next reading
                time.sleep(sleep_interval)
                
        except KeyboardInterrupt:
            print("\n\n⏸️  Stopped by user")
        
        # Print final statistics
        elapsed = time.time() - start_time
        self._print_stats(elapsed)
        
        # Clean up
        self.producer.flush()
        self.producer.close()
        print("\n👋 Simulator stopped\n")
    
    def _print_stats(self, elapsed):
        """Print final statistics"""
        print("\n" + "="*60)
        print("SIMULATION STATISTICS")
        print("="*60)
        print(f"⏱️  Duration: {elapsed:.1f} seconds")
        print(f"📊 Total sent: {self.stats['total_sent']}")
        print(f"📈 Rate: {self.stats['total_sent']/elapsed:.1f} msg/sec")
        print(f"\nData Quality Issues Generated:")
        print(f"  🟡 Duplicates: {self.stats['duplicates_sent']}")
        print(f"  🔴 Invalid: {self.stats['invalid_sent']}")
        print(f"  🟡 Late data: {self.stats['late_data_sent']}")
        clean = self.stats['total_sent'] - self.stats['duplicates_sent'] - self.stats['invalid_sent']
        print(f"  ✅ Clean: {clean}")
        print("="*60)


if __name__ == "__main__":
    # Run the simulator
    print("Starting IoT Sensor Simulator (Kafka Producer)")
    print("This will generate sensor readings for 5 minutes")
    print("You can stop anytime with Ctrl+C\n")
    
    simulator = SensorSimulator()
    
    # Run for 5 minutes (300 seconds) at 10 readings/second
    # This will generate ~3000 messages
    simulator.run(duration_seconds=300, readings_per_second=10)
