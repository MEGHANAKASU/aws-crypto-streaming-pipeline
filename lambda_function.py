import json
import boto3
import base64
from datetime import datetime

s3 = boto3.client("s3")
BUCKET = "crypto-trades-lake-meghana-2026"


def lambda_handler(event, context):
    """
    Triggered by Kinesis. Receives a batch of trade records,
    then writes them to S3 as a single file, partitioned by date.
    """
    trades = []

    # Each Kinesis record is base64-encoded — decode it back to our trade JSON
    for record in event["Records"]:
        payload = base64.b64decode(record["kinesis"]["data"])
        trade = json.loads(payload)
        trades.append(trade)

    if not trades:
        return {"statusCode": 200, "body": "No trades in batch"}

    # Build a date-partitioned S3 key: year=YYYY/month=MM/day=DD/<timestamp>.json
    now = datetime.utcnow()
    partition = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
    filename = f"{now.strftime('%H%M%S%f')}.json"
    s3_key = f"raw/{partition}/{filename}"

    # Write all trades in this batch as newline-delimited JSON (one trade per line)
    body = "\n".join(json.dumps(t) for t in trades)

    s3.put_object(Bucket=BUCKET, Key=s3_key, Body=body)

    print(f"Wrote {len(trades)} trades to s3://{BUCKET}/{s3_key}")
    return {"statusCode": 200, "body": f"Wrote {len(trades)} trades"}