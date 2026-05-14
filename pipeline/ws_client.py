import websocket
import json

# Coins to stream

COINS = [
    "btcusdt",
    "ethusdt",
    "bnbusdt",
    "solusdt",
    "adausdt"
]

streams = "/".join([f"{c}@trade" for c in COINS])
url = f"wss://stream.binance.com:9443/stream?streams={streams}"

# Handle incoming data

def on_message(ws, message):
    data = json.loads(message)

    stream = data["stream"]
    trade = data["data"]

    # Extract ONLY data
    result = {
        "symbol": trade["s"],
        "price": float(trade["p"]),
        "qty": float(trade["q"]),
        "timestamp": trade["T"]
    }

    # JUST OUTPUT (no DB here)
    print(result)


def on_error(ws, error):
    print("Error:", error)


def on_close(ws, close_status_code, close_msg):
    print("Closed connection")


def on_open(ws):
    print("Connected to Binance WebSocket")

# Start WebSocket

ws = websocket.WebSocketApp(
    url,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.on_open = on_open
ws.run_forever()