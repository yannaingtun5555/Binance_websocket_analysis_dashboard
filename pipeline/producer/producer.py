# producer.py - Single producer for all topics
import asyncio
import json
import websockets
from kafka import KafkaProducer
from datetime import datetime

class UnifiedCryptoProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda v: v.encode('utf-8'),
            compression_type='gzip'
        )
    
    async def stream_binance(self):
        # Single WebSocket connection for all data types
        stream_url = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@kline_1m/btcusdt@ticker"
        
        async with websockets.connect(stream_url) as websocket:
            while True:
                message = await websocket.recv()
                data = json.loads(message)
                stream_name = data['stream']
                
                # Route to appropriate topic
                if '@trade' in stream_name:
                    trade_data = self.transform_trade(data['data'])
                    self.producer.send('crypto.trades', key=trade_data['symbol'], value=trade_data)
                    
                elif '@kline' in stream_name:
                    kline_data = self.transform_kline(data['data'])
                    self.producer.send('crypto.klines', key=kline_data['symbol'], value=kline_data)
                    
                elif '@ticker' in stream_name:
                    ticker_data = self.transform_ticker(data['data'])
                    self.producer.send('crypto.ticker', key=ticker_data['symbol'], value=ticker_data)
    
    def transform_trade(self, data):
        return {
            'symbol': data['s'],
            'price': float(data['p']),
            'qty': float(data['q']),
            'trade_time': datetime.fromtimestamp(data['T']/1000).isoformat()
        }
    
    def transform_kline(self, data):
        k = data['k']
        return {
            'symbol': data['s'],
            'open': float(k['o']),
            'high': float(k['h']),
            'low': float(k['l']),
            'close': float(k['c']),
            'volume': float(k['v']),
            'interval': k['i'],
            'start_time': datetime.fromtimestamp(k['t']/1000).isoformat()
        }
    
    def transform_ticker(self, data):
        return {
            'symbol': data['s'],
            'price': float(data['c']),
            'change_percent': float(data['P']),
            'volume': float(data['v']),
            'event_time': datetime.fromtimestamp(data['E']/1000).isoformat()
        }

# Run single producer
producer = UnifiedCryptoProducer()
asyncio.run(producer.stream_binance())