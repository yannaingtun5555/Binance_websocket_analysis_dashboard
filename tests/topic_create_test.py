#!/usr/bin/env python
"""Script to create Kafka topics for the crypto pipeline"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable
import time
import sys

def create_topics():
    """Create all required topics if they don't exist"""
    bootstrap_servers = 'localhost:9092'
    max_retries = 10
    
    topics_config = {
        'crypto.trades': {'partitions': 3, 'replication': 1},
        'crypto.klines': {'partitions': 3, 'replication': 1},
        'crypto.ticker': {'partitions': 3, 'replication': 1}
    }
    
    print("🚀 Starting topic creation...")
    
    for attempt in range(max_retries):
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='topic_creator'
            )
            
            existing_topics = set(admin_client.list_topics())
            new_topics = []
            
            for topic_name, config in topics_config.items():
                if topic_name not in existing_topics:
                    new_topics.append(NewTopic(
                        name=topic_name,
                        num_partitions=config['partitions'],
                        replication_factor=config['replication']
                    ))
                    print(f"  ✓ Will create: {topic_name}")
            
            if new_topics:
                admin_client.create_topics(new_topics)
                print(f"\n✅ Created {len(new_topics)} topics")
            else:
                print("\n✅ All topics already exist")
            
            # Verify topics
            final_topics = admin_client.list_topics()
            for topic in topics_config.keys():
                if topic in final_topics:
                    print(f"  ✓ {topic} exists")
                else:
                    print(f"  ✗ {topic} missing")
            
            admin_client.close()
            return True
            
        except NoBrokersAvailable:
            print(f"⏳ Waiting for Kafka (attempt {attempt+1}/{max_retries})...")
            time.sleep(3)
        except Exception as e:
            print(f"❌ Error: {e}")
            time.sleep(3)
    
    print("❌ Failed to connect to Kafka")
    return False

if __name__ == "__main__":
    success = create_topics()
    sys.exit(0 if success else 1)