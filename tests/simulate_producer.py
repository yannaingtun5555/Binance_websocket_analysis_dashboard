#!/usr/bin/env python
"""Simulate producer sending test messages to all topics"""
from kafka import KafkaProducer
import json
import time
from datetime import datetime

def simulate_producer():
    """Send test messages to all three topics"""
    print("=" * 50)
    print("Simulating Kafka Producer")
    print("=" * 50)
    
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8'),
            acks='all'
        )
        
        # Test data for multiple symbols
        symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
        
        for symbol in symbols:
            print(f"\n📤 Sending messages for {symbol}")
            
            # Trade message
            trade_msg = {
                'symbol': symbol,
                'price': 50000.00 if symbol == 'BTCUSDT' else 3000.00,
                'qty': 0.001,
                'trade_time': datetime.now().isoformat()
            }
            future = producer.send('crypto.trades', key=symbol, value=trade_msg)
            result = future.get(timeout=5)
            print(f"  ✓ Trade → partition {result.partition}")
            
            # Kline message
            kline_msg = {
                'symbol': symbol,
                'open': 49900.00,
                'high': 50100.00,
                'low': 49800.00,
                'close': 50000.00,
                'volume': 100.5,
                'interval': '1m',
                'start_time': datetime.now().isoformat()
            }
            future = producer.send('crypto.klines', key=symbol, value=kline_msg)
            result = future.get(timeout=5)
            print(f"  ✓ Kline → partition {result.partition}")
            
            # Ticker message
            ticker_msg = {
                'symbol': symbol,
                'price': 50000.00,
                'change_percent': 2.5,
                'volume': 1000000,
                'event_time': datetime.now().isoformat()
            }
            future = producer.send('crypto.ticker', key=symbol, value=ticker_msg)
            result = future.get(timeout=5)
            print(f"  ✓ Ticker → partition {result.partition}")
            
            time.sleep(0.5)
        
        producer.flush()
        producer.close()
        
        print("\n" + "=" * 50)
        print("✅ All test messages sent successfully!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise

if __name__ == "__main__":
    simulate_producer()