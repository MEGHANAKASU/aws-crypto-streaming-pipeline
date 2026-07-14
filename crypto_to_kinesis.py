import websocket
import json
import boto3
import atexit

# ─── Configuration ───────────────────────────────────────────────
WS_URL = "wss://ws-feed.exchange.coinbase.com"
PRODUCTS = ["BTC-USD", "ETH-USD"]

AWS_REGION = "us-east-1"
STREAM_NAME = "crypto-trades-stream"

BATCH_SIZE = 100          # Kinesis put_records accepts up to 500 per call

# ─── Kinesis client ──────────────────────────────────────────────
kinesis = boto3.client("kinesis", region_name=AWS_REGION)

trade_count = 0
batch = []


def flush_batch():
    """Send all buffered trades to Kinesis in a single batched call."""
    global batch
    if not batch:
        return
    try:
        kinesis.put_records(
            StreamName=STREAM_NAME,
            Records=[
                {"Data": json.dumps(t), "PartitionKey": t["product"]}
                for t in batch
            ],
        )
    except Exception as e:
        print(f"⚠️  Kinesis send failed: {e}")
    finally:
        batch = []


# Make sure nothing is lost if the script exits unexpectedly
atexit.register(flush_batch)


def process_trade(trade: dict):
    """Buffer each trade; flush to Kinesis once the batch is full."""
    global trade_count
    trade_count += 1
    batch.append(trade)

    if len(batch) >= BATCH_SIZE:
        flush_batch()

    if trade_count % 500 == 0:
        print(f"  ...{trade_count:,} trades sent  "
              f"(latest: {trade['product']} ${trade['price']})")


def on_open(ws):
    print("✅ Connected to Coinbase. Streaming trades → Kinesis...\n")
    ws.send(json.dumps({
        "type": "subscribe",
        "product_ids": PRODUCTS,
        "channels": ["matches"],
    }))


def on_message(ws, message):
    data = json.loads(message)
    if data.get("type") == "match":
        process_trade({
            "trade_id": data.get("trade_id"),
            "product": data.get("product_id"),
            "side": data.get("side"),
            "price": data.get("price"),
            "size": data.get("size"),
            "timestamp": data.get("time"),
        })


def on_error(ws, error):
    if error:
        print(f"⚠️  {error} — reconnecting...")


def on_close(ws, close_status_code, close_msg):
    flush_batch()          # don't lose the partial batch
    print(f"🔌 Connection closed. Total sent: {trade_count:,}")


if __name__ == "__main__":
    print("Connecting to live crypto trade stream → Kinesis...")
    ws = websocket.WebSocketApp(
        WS_URL,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    # reconnect=5 → if Coinbase drops us, wait 5s and reconnect automatically
    ws.run_forever(reconnect=5)