from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import time
import sys

# Configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC_NAME = 'crypto_stream'
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1

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
            print("Kafka is ready!")
            return admin_client
        except NoBrokersAvailable:
            print(f"Kafka not ready yet (attempt {attempt + 1}/{max_retries})...")
            time.sleep(delay)
        except Exception as e:
            print(f"Error connecting to Kafka: {e}")
            time.sleep(delay)
    
    print("Failed to connect to Kafka after maximum retries")
    sys.exit(1)

def create_topic(admin_client):
    """Create topic if it doesn't exist"""
    try:
        # Check if topic exists
        existing_topics = admin_client.list_topics()
        
        if TOPIC_NAME in existing_topics:
            print(f"Topic '{TOPIC_NAME}' already exists")
            return True
        
        # Create new topic
        new_topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        )
        
        admin_client.create_topics([new_topic])
        print(f"Topic '{TOPIC_NAME}' created successfully!")
        print(f"  - Partitions: {NUM_PARTITIONS}")
        print(f"  - Replication Factor: {REPLICATION_FACTOR}")
        return True
        
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC_NAME}' already exists")
        return True
    except Exception as e:
        print(f"Error creating topic: {e}")
        return False

def main():
    print(f"=== Kafka Topic Creator ===")
    print(f"Bootstrap Server: {BOOTSTRAP_SERVERS}")
    print(f"Topic Name: {TOPIC_NAME}")
    print(f"Partitions: {NUM_PARTITIONS}")
    print(f"Replication Factor: {REPLICATION_FACTOR}")
    print()
    
    # Wait for Kafka to be ready
    admin_client = wait_for_kafka()
    
    # Create topic
    success = create_topic(admin_client)
    
    # Close client
    admin_client.close()
    
    if success:
        print("\n✓ Topic operation completed successfully!")
        sys.exit(0)
    else:
        print("\n✗ Failed to create topic")
        sys.exit(1)

if __name__ == "__main__":
    main()