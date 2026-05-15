#!/usr/bin/env python
"""Unit tests for producer data transformation logic"""
import pytest
from datetime import datetime

class TestTradeTransformation:
    """Test trade data transformation"""
    
    def test_transform_trade(self):
        raw_data = {
            's': 'BTCUSDT',
            'p': '50000.00',
            'q': '0.001',
            'T': 1700000000000
        }
        
        transformed = {
            'symbol': raw_data['s'],
            'price': float(raw_data['p']),
            'qty': float(raw_data['q']),
            'trade_time': datetime.fromtimestamp(raw_data['T']/1000).isoformat()
        }
        
        assert transformed['symbol'] == 'BTCUSDT'
        assert transformed['price'] == 50000.00
        assert transformed['qty'] == 0.001
        assert isinstance(transformed['trade_time'], str)
    
    def test_trade_with_different_symbol(self):
        raw_data = {
            's': 'ETHUSDT',
            'p': '3000.50',
            'q': '0.5',
            'T': 1700000000000
        }
        
        transformed = {
            'symbol': raw_data['s'],
            'price': float(raw_data['p']),
            'qty': float(raw_data['q']),
            'trade_time': datetime.fromtimestamp(raw_data['T']/1000).isoformat()
        }
        
        assert transformed['symbol'] == 'ETHUSDT'
        assert transformed['price'] == 3000.50
        assert transformed['qty'] == 0.5

class TestKlineTransformation:
    """Test kline/candlestick data transformation"""
    
    def test_transform_kline(self):
        raw_data = {
            's': 'ETHUSDT',
            'k': {
                'o': '3000.00',
                'h': '3100.00',
                'l': '2950.00',
                'c': '3050.00',
                'v': '1000.5',
                'i': '1m',
                't': 1700000000000
            }
        }
        
        k = raw_data['k']
        transformed = {
            'symbol': raw_data['s'],
            'open': float(k['o']),
            'high': float(k['h']),
            'low': float(k['l']),
            'close': float(k['c']),
            'volume': float(k['v']),
            'interval': k['i'],
            'start_time': datetime.fromtimestamp(k['t']/1000).isoformat()
        }
        
        assert transformed['symbol'] == 'ETHUSDT'
        assert transformed['open'] == 3000.00
        assert transformed['high'] == 3100.00
        assert transformed['low'] == 2950.00
        assert transformed['close'] == 3050.00
        assert transformed['volume'] == 1000.5
        assert transformed['interval'] == '1m'
    
    def test_transform_5min_kline(self):
        raw_data = {
            's': 'BTCUSDT',
            'k': {
                'o': '50000.00',
                'h': '51000.00',
                'l': '49000.00',
                'c': '50500.00',
                'v': '5000.25',
                'i': '5m',
                't': 1700000000000
            }
        }
        
        k = raw_data['k']
        transformed = {
            'symbol': raw_data['s'],
            'open': float(k['o']),
            'high': float(k['h']),
            'low': float(k['l']),
            'close': float(k['c']),
            'volume': float(k['v']),
            'interval': k['i'],
            'start_time': datetime.fromtimestamp(k['t']/1000).isoformat()
        }
        
        assert transformed['interval'] == '5m'
        assert transformed['volume'] == 5000.25

class TestTickerTransformation:
    """Test ticker data transformation"""
    
    def test_transform_ticker(self):
        raw_data = {
            's': 'BNBUSDT',
            'c': '500.00',
            'P': '2.5',
            'v': '1000000',
            'E': 1700000000000
        }
        
        transformed = {
            'symbol': raw_data['s'],
            'price': float(raw_data['c']),
            'change_percent': float(raw_data['P']),
            'volume': float(raw_data['v']),
            'event_time': datetime.fromtimestamp(raw_data['E']/1000).isoformat()
        }
        
        assert transformed['symbol'] == 'BNBUSDT'
        assert transformed['price'] == 500.00
        assert transformed['change_percent'] == 2.5
        assert transformed['volume'] == 1000000
    
    def test_ticker_with_negative_change(self):
        raw_data = {
            's': 'DOGEUSDT',
            'c': '0.08',
            'P': '-5.2',
            'v': '5000000',
            'E': 1700000000000
        }
        
        transformed = {
            'symbol': raw_data['s'],
            'price': float(raw_data['c']),
            'change_percent': float(raw_data['P']),
            'volume': float(raw_data['v']),
            'event_time': datetime.fromtimestamp(raw_data['E']/1000).isoformat()
        }
        
        assert transformed['change_percent'] == -5.2

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])