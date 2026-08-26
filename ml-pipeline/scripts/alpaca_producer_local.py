"""
Alpaca trade producer — runs locally, not on Databricks.

Why local: Databricks serverless compute in this workspace has no general
internet DNS resolution (confirmed — stream.data.alpaca.markets and
paper-api.alpaca.markets both fail to resolve from a Databricks job, the
same restriction that blocked the SEC EDGAR fetch in Stage 3).
alpaca-py's websocket client retries connection failures silently instead
of raising, so a Databricks-hosted version of this producer ran cleanly for
its full timeout window without ever actually connecting or erroring.

Run: infra-deploy/venv/Scripts/python.exe ml-pipeline/scripts/alpaca_producer_local.py [seconds]
Requires the financial-intel-deploy AWS credentials (source
infra-deploy/set-env.ps1 first) and ALPACA_API_KEY/ALPACA_SECRET_KEY set as
local env vars.
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone

import boto3
from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream

AWS_REGION = "us-west-2"
KINESIS_STREAM = "financial-intel-platform-z7oxnm-market-data"
BRONZE_BUCKET = "financial-intel-platform-z7oxnm-bronze"
RAW_ARCHIVE_PREFIX = "raw_archive"
WATCHLIST = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]

RUN_SECONDS = int(sys.argv[1]) if len(sys.argv) > 1 else 180

ALPACA_API_KEY = os.environ["ALPACA_API_KEY"]
ALPACA_SECRET_KEY = os.environ["ALPACA_SECRET_KEY"]

kinesis = boto3.client("kinesis", region_name=AWS_REGION)
s3 = boto3.client("s3", region_name=AWS_REGION)

trade_count = 0
stream = StockDataStream(ALPACA_API_KEY, ALPACA_SECRET_KEY, feed=DataFeed.IEX)


async def on_trade(trade) -> None:
    global trade_count
    try:
        payload = {
            "symbol": trade.symbol,
            "price": float(trade.price),
            "size": int(trade.size),
            "timestamp": trade.timestamp.isoformat(),
            "exchange": trade.exchange,
            "conditions": ",".join(trade.conditions) if trade.conditions else None,
        }
        body = json.dumps(payload).encode("utf-8")

        # Blocking boto3 calls off the event loop, same reasoning as the
        # (now-removed) Databricks version: don't starve the websocket's
        # own message-receiving loop.
        await asyncio.to_thread(
            kinesis.put_record,
            StreamName=KINESIS_STREAM,
            Data=body,
            PartitionKey=payload["symbol"],
        )

        now = datetime.now(timezone.utc)
        key = f"{RAW_ARCHIVE_PREFIX}/{now:%Y/%m/%d/%H}/{payload['symbol']}_{now.timestamp()}.json"
        await asyncio.to_thread(s3.put_object, Bucket=BRONZE_BUCKET, Key=key, Body=body)

        trade_count += 1
        if trade_count % 25 == 0:
            print(f"...{trade_count} trades relayed so far")
    except Exception as e:
        print(f"[warn] on_trade failed for {getattr(trade, 'symbol', '?')}: {e}")


for symbol in WATCHLIST:
    stream.subscribe_trades(on_trade, symbol)


async def run_with_timeout():
    task = asyncio.create_task(stream._run_forever())
    try:
        await asyncio.wait_for(task, timeout=RUN_SECONDS)
    except asyncio.TimeoutError:
        print(f"{RUN_SECONDS}s window elapsed, stopping.")
        await stream.stop_ws()


print(f"Streaming trades for {WATCHLIST} -> Kinesis stream {KINESIS_STREAM} for {RUN_SECONDS}s")
asyncio.run(run_with_timeout())
print(f"TOTAL TRADES RELAYED: {trade_count}")
