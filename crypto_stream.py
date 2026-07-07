import websocket
import json
from datetime import datetime

# Coinbase public WebSocket feed (no API key needed)
WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCTS = ["BTC-USD", "ETH-USD"]

# Counter to track how many trades we've processed
trade_count = 0


def process_trade(trade: dict):
    """
    Central place where each trade is handled.
    Right now it prints. LATER: this is where we'll send to Kinesis
    (just one line changes — everything else stays the same).
    """
    global trade_count
    trade_count += 1

    # Print a status line every 50 trades so we don't flood the screen
    if trade_count % 50 == 0:
        print(f"  ...processed {trade_count} trades so far "
              f"(latest: {trade['product']} ${trade['price']})")


def on_open(ws):
    print("✅ Connected to Coinbase. Subscribing to live trades...\n")
    subscribe_message = {
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": ["matches"]
    }
    ws.send(json.dumps(subscribe_message))


def on_message(ws, message):
    data = json.loads(message)

    if data.get("type") == "match":
        # Build a clean, structured record — this is the shape we'll send to Kinesis
        trade = {
            "trade_id": data.get("trade_id"),
            "product": data.get("product_id"),
            "side": data.get("side"),
            "price": data.get("price"),
            "size": data.get("size"),
            "timestamp": data.get("time"),
        }
        process_trade(trade)


def on_error(ws, error):
    if error:
        print(f"❌ Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print(f"\n🔌 Connection closed. Total trades processed: {trade_count}")


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