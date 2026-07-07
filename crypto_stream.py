import websocket
import json
from datetime import datetime

# Coinbase public WebSocket feed (no API key needed)
WS_URL = "wss://ws-feed.exchange.coinbase.com"

# Which crypto products to watch
PRODUCTS = ["BTC-USD", "ETH-USD"]


def on_open(ws):
    """Runs when the connection opens — we subscribe to the trade feed."""
    print("✅ Connected to Coinbase. Subscribing to live trades...\n")
    subscribe_message = {
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": ["matches"]   # 'matches' = completed trades
    }
    ws.send(json.dumps(subscribe_message))


def on_message(ws, message):
    """Runs every time a new message arrives from Coinbase."""
    data = json.loads(message)

    # 'match' messages are actual completed trades
    if data.get("type") == "match":
        product = data.get("product_id")
        price = data.get("price")
        size = data.get("size")
        side = data.get("side")
        time = data.get("time", "")

        print(f"[{time}] {product} | {side.upper():4} | price: ${price} | size: {size}")


def on_error(ws, error):
    print(f"❌ Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("\n🔌 Connection closed.")


if __name__ == "__main__":
    print("Connecting to live crypto trade stream...")
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()