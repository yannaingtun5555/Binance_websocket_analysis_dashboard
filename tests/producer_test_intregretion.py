#!/usr/bin/env python
"""Integration tests for Kafka producer"""
import pytest
import json
from kafka import KafkaProducer, KafkaConsumer
from datetime import datetime
import time

@pytest.fixture
def kafka_producer():
    """Fixture to create Kafka producer"""
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda v: v.encode('utf-8'),
        max_block_ms=5000
    )
    yield producer
    producer.close()

@pytest.fixture
def kafka_consumer():
    """Fixture to create Kafka consumer"""
    consumer = KafkaConsumer(
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    yield consumer
    consumer.close()

def test_kafka_connection():
    """Test Kafka broker connection"""
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092', max_block_ms=5000)
        assert producer.bootstrap_connected()
        producer.close()
        print("✓ Kafka connection successful")
    except Exception as e:
        pytest.fail(f"Kafka connection failed: {e}")

def test_send_trade_message(kafka_producer):
    """Test sending trade message to Kafka"""
    trade_message = {
        'symbol': 'BTCUSDT',
        'price': 50000.00,
        'qty': 0.001,
        'trade_time': datetime.now().isoformat()
    }
    
    future = kafka_producer.send('crypto.trades', key='BTCUSDT', value=trade_message)
    result = future.get(timeout=5)
    
    assert result.topic == 'crypto.trades'
    assert result.partition is not None
    print(f"✓ Trade message sent to partition {result.partition}")

def test_send_kline_message(kafka_producer):
    """Test sending kline message to Kafka"""
    kline_message = {
        'symbol': 'ETHUSDT',
        'open': 3000.00,
        'high': 3100.00,
        'low': 2950.00,
        'close': 3050.00,
        'volume': 1000.5,
        'interval': '1m',
        'start_time': datetime.now().isoformat()
    }
    
    future = kafka_producer.send('crypto.klines', key='ETHUSDT', value=kline_message)
    result = future.get(timeout=5)
    
    assert result.topic == 'crypto.klines'
    print(f"✓ Kline message sent to partition {result.partition}")

def test_send_ticker_message(kafka_producer):
    """Test sending ticker message to Kafka"""
    ticker_message = {
        'symbol': 'BNBUSDT',
        'price': 500.00,
        'change_percent': 2.5,
        'volume': 1000000,
        'event_time': datetime.now().isoformat()
    }
    
    future = kafka_producer.send('crypto.ticker', key='BNBUSDT', value=ticker_message)
    result = future.get(timeout=5)
    
    assert result.topic == 'crypto.ticker'
    print(f"✓ Ticker message sent to partition {result.partition}")

def test_message_ordering_with_same_key(kafka_producer):
    """Test that messages with same key go to same partition"""
    symbol = 'TESTUSDT'
    partitions = set()
    
    for i in range(5):
        message = {
            'symbol': symbol,
            'price': 100.00 + i,
            'qty': 1.0,
            'trade_time': datetime.now().isoformat()
        }
        
        future = kafka_producer.send('crypto.trades', key=symbol, value=message)
        result = future.get(timeout=5)
        partitions.add(result.partition)
    
    # All messages should go to the same partition
    assert len(partitions) == 1
    print(f"✓ Message ordering preserved: all messages went to partition {partitions.pop()}")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])