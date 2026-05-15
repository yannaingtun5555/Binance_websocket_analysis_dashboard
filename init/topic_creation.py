from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import os
import time
import sys

# Configuration
BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')

# Topic configurations for your three tables
TOPICS_CONFIG = {
    'crypto.trades': {
        'num_partitions': 3,      # 3 partitions for parallel processing of different symbols
        'replication_factor': 1,
        'description': 'Raw trade data from Binance WebSocket'
    },
    'crypto.klines': {
        'num_partitions': 3,      # 3 partitions for different symbol intervals
        'replication_factor': 1,
        'description': 'OHLCV kline/candlestick data'
    },
    'crypto.ticker': {
        'num_partitions': 3,      # 3 partitions for 24hr ticker data
        'replication_factor': 1,
        'description': '24hr ticker statistics'
    }
}

def wait_for_kafka(max_retries=30, delay=2):
    """Wait for Kafka to be ready"""
    print(f"Waiting for Kafka at {BOOTSTRAP_SERVERS}...")
    
    for attempt in range(max_retries):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                client_id='topic_creator'
            )
            # Try to list topics to check if Kafka is ready
            admin_client.list_topics()
            print("✓ Kafka is ready!")
            return admin_client
        except NoBrokersAvailable:
            print(f"⏳ Kafka not ready yet (attempt {attempt + 1}/{max_retries})...")
            time.sleep(delay)
        except Exception as e:
            print(f"⚠️ Error connecting to Kafka: {e}")
            time.sleep(delay)
    
    print("❌ Failed to connect to Kafka after maximum retries")
    sys.exit(1)

def create_topic(admin_client, topic_name, num_partitions, replication_factor):
    """Create a single topic if it doesn't exist"""
    try:
        # Check if topic exists
        existing_topics = admin_client.list_topics()
        
        if topic_name in existing_topics:
            print(f"  ℹ️ Topic '{topic_name}' already exists")
            return True
        
        # Create new topic
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        
        admin_client.create_topics([new_topic])
        print(f"  ✓ Topic '{topic_name}' created successfully!")
        print(f"    - Partitions: {num_partitions}")
        print(f"    - Replication Factor: {replication_factor}")
        return True
        
    except TopicAlreadyExistsError:
        print(f"  ℹ️ Topic '{topic_name}' already exists")
        return True
    except Exception as e:
        print(f"  ❌ Error creating topic '{topic_name}': {e}")
        return False

def describe_topic(admin_client, topic_name):
    """Get and display detailed information about a topic"""
    try:
        topic_metadata = admin_client.describe_topics([topic_name])
        if topic_metadata:
            print(f"\n📊 Topic Details for '{topic_name}':")
            for topic in topic_metadata:
                topic_id = topic.get('topic') or topic.get('name') or topic_name
                partitions = topic.get('partitions', [])
                print(f"  - Topic: {topic_id}")
                print(f"  - Partitions: {len(partitions)}")
                print(f"  - Is Internal: {topic.get('is_internal', False)}")
                for partition in partitions[:2]:  # Show first 2 partitions
                    print(
                        f"    Partition {partition.get('partition')}: "
                        f"leader={partition.get('leader')}, "
                        f"replicas={len(partition.get('replicas', []))}"
                    )
    except Exception as e:
        print(f"  ⚠️ Could not describe topic '{topic_name}': {e}")

def main():
    print("=" * 60)
    print("       Kafka Topic Creator for Crypto Pipeline")
    print("=" * 60)
    print(f"Bootstrap Server: {BOOTSTRAP_SERVERS}")
    print(f"Topics to create: {len(TOPICS_CONFIG)}")
    print()
    
    # Display topic configuration
    print("📋 Topic Configuration:")
    for topic_name, config in TOPICS_CONFIG.items():
        print(f"  - {topic_name}")
        print(f"      Partitions: {config['num_partitions']}")
        print(f"      Replication: {config['replication_factor']}")
        print(f"      Description: {config['description']}")
    print()
    
    # Wait for Kafka to be ready
    admin_client = wait_for_kafka()
    
    # Create all topics
    print("\n🔧 Creating/Validating topics...")
    success_count = 0
    
    for topic_name, config in TOPICS_CONFIG.items():
        print(f"\n📝 Processing topic: {topic_name}")
        success = create_topic(
            admin_client,
            topic_name,
            config['num_partitions'],
            config['replication_factor']
        )
        if success:
            success_count += 1
            # Optionally describe the topic after creation
            describe_topic(admin_client, topic_name)
    
    # Close client
    admin_client.close()
    
    # Summary
    print("\n" + "=" * 60)
    print("                 OPERATION SUMMARY")
    print("=" * 60)
    print(f"✓ Topics successfully processed: {success_count}/{len(TOPICS_CONFIG)}")
    
    if success_count == len(TOPICS_CONFIG):
        print("\n✅ All topics are ready for use!")
        print("\nNext steps:")
        print("  1. Run your Kafka producer to ingest Binance data")
        print("  2. Run your Kafka consumer to write to ClickHouse")
        print("  3. Launch your Streamlit dashboard")
        sys.exit(0)
    else:
        print(f"\n⚠️ Some topics could not be created ({len(TOPICS_CONFIG) - success_count} failed)")
        print("Check the errors above and verify Kafka configuration")
        sys.exit(1)

if __name__ == "__main__":
    main()
