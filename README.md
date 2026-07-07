# Real-Time Crypto Streaming Pipeline (AWS)

A real-time streaming data pipeline that ingests live cryptocurrency trades and processes them through an AWS-based data stack.

> ⚠️ **Work in progress** — building in the open, one stage at a time.

## Architecture

Live Market Data (WebSocket) → AWS Kinesis → AWS Lambda → Amazon S3 (Data Lake) → dbt → Warehouse → Dashboard

## Current Status

- ✅ Live crypto trade feed (Coinbase WebSocket) streaming in real time
- ⏳ Kinesis ingestion (in progress)
- ⏳ Lambda → S3 data lake
- ⏳ dbt transformations
- ⏳ Warehouse + dashboard

## Tech Stack

- **Language:** Python
- **Streaming:** AWS Kinesis Data Streams
- **Processing:** AWS Lambda
- **Storage:** Amazon S3
- **Transformation:** dbt
- **Warehouse:** DuckDB / Amazon Redshift
- **Data Source:** Coinbase public WebSocket API

## Data Source

Live BTC-USD and ETH-USD trades from the Coinbase exchange WebSocket feed — real market data, no API key required.

## Author

**Meghana Kasu** — Data Engineer
GitHub: [github.com/MEGHANAKASU](https://github.com/MEGHANAKASU)