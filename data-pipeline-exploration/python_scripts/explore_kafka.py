"""
Explore Kafka - Understanding Topics, Messages, and Offsets

Run: python python_scripts/explore_kafka.py
"""

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
from datetime import datetime

def explore_kafka_basics():
    """Learn Kafka fundamentals through examples"""
    
    print("\n" + "="*60)
    print("EXPLORING KAFKA")
    print("="*60)
    
    # Step 1: Connect to Kafka Admin (to manage topics)
    print("\n📋 Step 1: Connecting to Kafka Admin...")
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    
    # Step 2: List existing topics
    print("\n📋 Step 2: Listing existing topics...")
    topics = admin.list_topics()
    print(f"Current topics: {topics}")
    
    # Step 3: Create a test topic (if it doesn't exist)
    print("\n📋 Step 3: Creating test topic...")
    test_topic = NewTopic(
        name='test_topic',
        num_partitions=3,
        replication_factor=1
    )
    
    try:
        admin.create_topics([test_topic])
        print("✅ Created topic 'test_topic' with 3 partitions")
    except TopicAlreadyExistsError:
        print("ℹ️  Topic 'test_topic' already exists")
    
    # Step 4: Produce some test messages
    print("\n📋 Step 4: Sending test messages...")
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8') if k else None
    )
    
    # Send 10 messages with different keys
    for i in range(10):
        message = {
            'id': i,
            'timestamp': datetime.now().isoformat(),
            'data': f'Test message {i}'
        }
        
        # Use key to control which partition
        key = f'key_{i % 3}'  # 3 different keys for 3 partitions
        
        future = producer.send('test_topic', key=key, value=message)
        result = future.get(timeout=10)
        
        print(f"  ✅ Sent message {i} with key '{key}' to partition {result.partition}")
    
    producer.flush()
    producer.close()
    
    # Step 5: Consume messages
    print("\n📋 Step 5: Reading messages back...")
    consumer = KafkaConsumer(
        'test_topic',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',  # Read from beginning
        group_id='test_consumer_group',
        consumer_timeout_ms=5000  # Stop after 5 seconds of no data
    )
    
    print("\nMessages from Kafka:")
    print("-" * 60)
    
    message_count = 0
    for message in consumer:
        message_count += 1
        print(f"Partition: {message.partition} | Offset: {message.offset} | Key: {message.key.decode()}")
        print(f"Value: {message.value}")
        print("-" * 60)
    
    print(f"\n✅ Read {message_count} messages")
    
    consumer.close()
    admin.close()
    
    # Explain what we saw
    print("\n" + "="*60)
    print("WHAT WE LEARNED")
    print("="*60)
    print("""
1. TOPICS: Named channels for messages (like 'test_topic')
   - Think of them as different mailboxes

2. PARTITIONS: Topics are split into partitions for parallelism
   - Our topic has 3 partitions (0, 1, 2)
   - Messages with same KEY always go to same partition
   - This guarantees order for related messages

3. KEYS: Determine which partition a message goes to
   - 'key_0', 'key_1', 'key_2' were distributed across partitions
   - No key = random partition

4. OFFSETS: Position of message in partition
   - Like line numbers in a file
   - Consumer tracks which offset it last read
   - Can replay by resetting offset

5. CONSUMER GROUPS: Multiple consumers working together
   - All consumers in a group share the work
   - Each partition assigned to one consumer in the group
    """)


def explore_sensor_topic():
    """Explore the actual sensor_readings_raw topic"""
    
    print("\n" + "="*60)
    print("EXPLORING SENSOR READINGS TOPIC")
    print("="*60)
    
    # Connect as admin
    admin = KafkaAdminClient(bootstrap_servers='localhost:9092')
    
    # Check if topic exists
    topics = admin.list_topics()
    
    if 'sensor_readings_raw' not in topics:
        print("\n⚠️  Topic 'sensor_readings_raw' doesn't exist yet")
        print("Run the sensor simulator first:")
        print("  python streaming_pipeline/sensor_simulator.py")
        admin.close()
        return
    
    # Get topic details
    print("\n📊 Topic Information:")
    print("-" * 60)
    
    # Create consumer to peek at messages
    consumer = KafkaConsumer(
        'sensor_readings_raw',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        consumer_timeout_ms=5000,
        group_id='exploration_group'
    )
    
    # Read first 10 messages
    print("\n📬 Recent Messages (first 10):")
    print("-" * 60)
    
    count = 0
    for message in consumer:
        count += 1
        if count > 10:
            break
        
        reading = message.value
        print(f"\nMessage {count}:")
        print(f"  Partition: {message.partition}")
        print(f"  Offset: {message.offset}")
        print(f"  Key: {message.key.decode() if message.key else 'None'}")
        print(f"  Sensor: {reading.get('sensor_id')}")
        print(f"  Temp: {reading.get('temperature')}°C")
        print(f"  Time: {reading.get('timestamp')}")
    
    if count == 0:
        print("No messages in topic yet. Run the simulator!")
    else:
        print(f"\n✅ Showed {count} messages")
    
    consumer.close()
    admin.close()


def main():
    """Main exploration menu"""
    
    print("\n" + "="*60)
    print("KAFKA EXPLORATION TOOL")
    print("="*60)
    print("\nWhat would you like to explore?")
    print("1. Kafka Basics (create topic, send/receive messages)")
    print("2. Sensor Readings Topic (view actual streaming data)")
    print("3. Both")
    
    choice = input("\nEnter choice (1/2/3): ").strip()
    
    if choice == '1':
        explore_kafka_basics()
    elif choice == '2':
        explore_sensor_topic()
    elif choice == '3':
        explore_kafka_basics()
        time.sleep(2)
        explore_sensor_topic()
    else:
        print("Invalid choice")

if __name__ == "__main__":
    main()