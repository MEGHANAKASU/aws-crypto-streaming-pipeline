import websocket
import json
import boto3
from datetime import datetime

# ─── Configuration ───────────────────────────────────────────────
WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCTS = ["BTC-USD", "ETH-USD"]

AWS_REGION = "us-east-1"
STREAM_NAME = "crypto-trades-stream"   # must match the Kinesis stream we'll create

# ─── Kinesis client ──────────────────────────────────────────────
# boto3 automatically uses the AWS credentials configured on your machine
kinesis = boto3.client("kinesis", region_name=AWS_REGION)

trade_count = 0


def send_to_kinesis(trade: dict):
    """Send one trade record into the Kinesis stream."""
    kinesis.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(trade),            # the record, as JSON bytes
        PartitionKey=trade["product"]      # groups records by product (BTC-USD / ETH-USD)
    )


def process_trade(trade: dict):
    """Handle each trade: send to Kinesis + print occasional status."""
    global trade_count
    trade_count += 1

    send_to_kinesis(trade)

    if trade_count % 50 == 0:
        print(f"  ...sent {trade_count} trades to Kinesis "
              f"(latest: {trade['product']} ${trade['price']})")


def on_open(ws):
    print("✅ Connected to Coinbase. Streaming trades → Kinesis...\n")
    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": ["matches"]
    }))


def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "match":
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
    print(f"\n🔌 Connection closed. Total sent to Kinesis: {trade_count}")


if __name__ == "__main__":
    print("Connecting to live crypto trade stream → Kinesis...")
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()