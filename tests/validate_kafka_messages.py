#!/usr/bin/env python
"""Validate that messages exist in Kafka topics"""
from kafka import KafkaConsumer
import json
import sys

def validate_messages():
    """Check if messages exist in each topic"""
    print("🔍 Validating Kafka Messages")
    print("=" * 40)
    
    topics = ['crypto.trades', 'crypto.klines', 'crypto.ticker']
    all_success = True
    
    for topic in topics:
        print(f"\n📋 Checking topic: {topic}")
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers='localhost:9092',
                auto_offset_reset='earliest',
                max_poll_records=1,
                consumer_timeout_ms=5000
            )
            
            messages = []
            for msg in consumer:
                messages.append(msg)
                break  # Just check first message
            
            if messages:
                print(f"  ✓ Found messages in {topic}")
                msg_value = json.loads(messages[0].value.decode('utf-8'))
                print(f"    Sample: {msg_value}")
            else:
                print(f"  ⚠️ No messages found in {topic}")
                all_success = False
            
            consumer.close()
            
        except Exception as e:
            print(f"  ❌ Error reading {topic}: {e}")
            all_success = False
    
    print("\n" + "=" * 40)
    if all_success:
        print("✅ All topics have messages!")
    else:
        print("⚠️ Some topics have no messages")
    
    return all_success

if __name__ == "__main__":
    success = validate_messages()
    sys.exit(0 if success else 1)