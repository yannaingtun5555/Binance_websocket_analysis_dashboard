#!/usr/bin/env python
"""Test consumer message parsing logic"""
import json
import sys

def parse_trade_message(message):
    """Parse trade message from Kafka"""
    data = json.loads(message) if isinstance(message, str) else message
    return {
        'symbol': data['symbol'],
        'price': float(data['price']),
        'qty': float(data['qty']),
        'trade_time': data['trade_time']
    }

def parse_kline_message(message):
    """Parse kline message from Kafka"""
    data = json.loads(message) if isinstance(message, str) else message
    return {
        'symbol': data['symbol'],
        'open': float(data['open']),
        'high': float(data['high']),
        'low': float(data['low']),
        'close': float(data['close']),
        'volume': float(data['volume']),
        'interval': data['interval'],
        'start_time': data['start_time']
    }

def parse_ticker_message(message):
    """Parse ticker message from Kafka"""
    data = json.loads(message) if isinstance(message, str) else message
    return {
        'symbol': data['symbol'],
        'price': float(data['price']),
        'change_percent': float(data['change_percent']),
        'volume': float(data['volume']),
        'event_time': data['event_time']
    }

def test_parsers():
    """Test all message parsers"""
    print("🧪 Testing Consumer Message Parsers")
    print("=" * 40)
    
    # Test trade parser
    trade_json = '{"symbol": "BTCUSDT", "price": 50000.00, "qty": 0.001, "trade_time": "2024-01-01T00:00:00"}'
    parsed_trade = parse_trade_message(trade_json)
    assert parsed_trade['price'] == 50000.00
    assert parsed_trade['qty'] == 0.001
    assert parsed_trade['symbol'] == 'BTCUSDT'
    print("✓ Trade parser works")
    
    # Test kline parser
    kline_json = '{"symbol": "ETHUSDT", "open": 3000.00, "high": 3100.00, "low": 2950.00, "close": 3050.00, "volume": 1000.5, "interval": "1m", "start_time": "2024-01-01T00:00:00"}'
    parsed_kline = parse_kline_message(kline_json)
    assert parsed_kline['high'] == 3100.00
    assert parsed_kline['low'] == 2950.00
    assert parsed_kline['interval'] == '1m'
    print("✓ Kline parser works")
    
    # Test ticker parser
    ticker_json = '{"symbol": "BNBUSDT", "price": 500.00, "change_percent": 2.5, "volume": 1000000, "event_time": "2024-01-01T00:00:00"}'
    parsed_ticker = parse_ticker_message(ticker_json)
    assert parsed_ticker['change_percent'] == 2.5
    assert parsed_ticker['volume'] == 1000000
    print("✓ Ticker parser works")
    
    # Test edge cases
    print("\n📊 Testing Edge Cases:")
    
    # Large numbers
    large_trade = '{"symbol": "BTCUSDT", "price": 1000000.12345678, "qty": 100.999, "trade_time": "2024-01-01T00:00:00"}'
    parsed = parse_trade_message(large_trade)
    assert parsed['price'] == 1000000.12345678
    assert parsed['qty'] == 100.999
    print("  ✓ Handles large numbers")
    
    # Negative changes
    negative_ticker = '{"symbol": "DOGEUSDT", "price": 0.08, "change_percent": -5.2, "volume": 5000000, "event_time": "2024-01-01T00:00:00"}'
    parsed = parse_ticker_message(negative_ticker)
    assert parsed['change_percent'] == -5.2
    print("  ✓ Handles negative percentages")
    
    # Zero values
    zero_trade = '{"symbol": "TESTUSDT", "price": 0, "qty": 0, "trade_time": "2024-01-01T00:00:00"}'
    parsed = parse_trade_message(zero_trade)
    assert parsed['price'] == 0
    assert parsed['qty'] == 0
    print("  ✓ Handles zero values")
    
    print("\n" + "=" * 40)
    print("✅ All parser tests passed!")
    return True

if __name__ == "__main__":
    success = test_parsers()
    sys.exit(0 if success else 1)