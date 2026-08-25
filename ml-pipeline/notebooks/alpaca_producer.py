# Databricks notebook source
# MAGIC %pip install alpaca-py nest_asyncio --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json
from datetime import datetime, timezone

import boto3

AWS_REGION = "us-west-2"
KINESIS_STREAM = "financial-intel-platform-z7oxnm-market-data"
ALPACA_SECRET_ID = "financial-intel-platform-z7oxnm/alpaca_api"
BRONZE_BUCKET = "financial-intel-platform-z7oxnm-bronze"
RAW_ARCHIVE_PREFIX = "raw_archive"
WATCHLIST = ["AAPL", "MSFT", "NVDA", "GOOGL", "AMZN", "META", "TSLA"]

aws_access_key_id = dbutils.secrets.get(scope="aws-boto3-creds", key="aws_access_key_id")
aws_secret_access_key = dbutils.secrets.get(scope="aws-boto3-creds", key="aws_secret_access_key")


def aws_client(service: str):
    return boto3.client(
        service,
        region_name=AWS_REGION,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


kinesis = aws_client("kinesis")
s3 = aws_client("s3")
secretsmanager = aws_client("secretsmanager")

alpaca_creds = json.loads(secretsmanager.get_secret_value(SecretId=ALPACA_SECRET_ID)["SecretString"])
ALPACA_API_KEY = alpaca_creds["api_key"]
ALPACA_SECRET_KEY = alpaca_creds["api_secret"]

# COMMAND ----------
from alpaca.data.enums import DataFeed
from alpaca.data.live import StockDataStream

# IEX feed is what's available on Alpaca's free/paper tier (not full SIP).
# Must be the DataFeed enum, not the string "iex" — alpaca-py's __init__
# accepts the string via its `in (DataFeed.IEX, DataFeed.SIP)` check (a
# str-backed enum compares equal to the raw string) but then crashes calling
# `.value` on it since `feed` itself was never rebound to the enum member.
stream = StockDataStream(ALPACA_API_KEY, ALPACA_SECRET_KEY, feed=DataFeed.IEX)


async def on_trade(trade) -> None:
    payload = {
        "symbol": trade.symbol,
        "price": float(trade.price),
        "size": int(trade.size),
        "timestamp": trade.timestamp.isoformat(),
        "exchange": trade.exchange,
        "conditions": ",".join(trade.conditions) if trade.conditions else None,
    }
    body = json.dumps(payload).encode("utf-8")

    kinesis.put_record(
        StreamName=KINESIS_STREAM,
        Data=body,
        PartitionKey=payload["symbol"],
    )

    # Independent of the Kinesis path — cheap raw replay/audit archive.
    now = datetime.now(timezone.utc)
    key = f"{RAW_ARCHIVE_PREFIX}/{now:%Y/%m/%d/%H}/{payload['symbol']}_{now.timestamp()}.json"
    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=body)


for symbol in WATCHLIST:
    stream.subscribe_trades(on_trade, symbol)

# Databricks notebooks already run inside an active asyncio event loop
# (the kernel/driver's own loop) — alpaca-py's stream.run() calls
# asyncio.run() internally, which raises "cannot be called from a running
# event loop" without this patch. Standard fix for alpaca-py in
# Jupyter/notebook-style environments.
import nest_asyncio
nest_asyncio.apply()

print(f"Streaming trades for {WATCHLIST} -> Kinesis stream {KINESIS_STREAM}")
print("This runs until the job is manually cancelled.")
stream.run()
