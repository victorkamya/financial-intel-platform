# Databricks notebook source
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

import boto3
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

AWS_REGION = "us-west-2"
KINESIS_STREAM = "financial-intel-platform-z7oxnm-market-data"

aws_access_key_id = dbutils.secrets.get(scope="aws-boto3-creds", key="aws_access_key_id")
aws_secret_access_key = dbutils.secrets.get(scope="aws-boto3-creds", key="aws_secret_access_key")

kinesis = boto3.client(
    "kinesis",
    region_name=AWS_REGION,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
)

# COMMAND ----------

@dataclass
class IngestionConfig:
    source_name: str
    target_catalog: str
    target_schema: str
    target_table: str
    partition_cols: list = field(default_factory=list)


class BaseIngester(ABC):
    """Abstract base class for all ingestion patterns."""

    def __init__(self, spark: SparkSession, config: IngestionConfig):
        self.spark = spark
        self.config = config
        self._target_path = f"{config.target_catalog}.{config.target_schema}.{config.target_table}"

    @abstractmethod
    def read(self) -> Optional[DataFrame]:
        """Read from source and return a micro-batch DataFrame, or None if empty."""
        pass

    def _add_metadata_columns(self, df: DataFrame) -> DataFrame:
        return df.withColumns({
            "_ingested_at": F.current_timestamp(),
            "_source": F.lit(self.config.source_name),
            "_batch_id": F.lit(str(int(time.time() * 1000))),
        })

    def write(self, df: DataFrame) -> None:
        df = self._add_metadata_columns(df)
        (df.write
           .format("delta")
           .mode("append")
           .option("mergeSchema", "true")
           .saveAsTable(self._target_path))

    def run(self) -> int:
        df = self.read()
        if df is None:
            return 0
        count = df.count()
        if count > 0:
            self.write(df)
        return count


TRADE_SCHEMA = StructType([
    StructField("symbol", StringType()),
    StructField("price", DoubleType()),
    StructField("size", LongType()),
    StructField("timestamp", StringType()),
    StructField("exchange", StringType()),
    StructField("conditions", StringType()),
])


class KinesisTradeIngester(BaseIngester):
    """Ingest trade data from Kinesis via boto3 polling (get_shard_iterator /
    get_records) rather than spark.readStream.format("kinesis") — the
    Spark-native Kinesis connector needs a classic-compute init-script JAR,
    incompatible with serverless compute. Shard iterators are kept as instance
    state across calls to read() so consecutive micro-batches don't miss
    records in the gap between polls."""

    def __init__(self, spark, config: IngestionConfig, stream_name: str, poll_seconds: int = 30):
        super().__init__(spark, config)
        self.stream_name = stream_name
        self.poll_seconds = poll_seconds
        self._shard_iterators: dict[str, Optional[str]] = {}

    def _ensure_iterators(self) -> None:
        shards = kinesis.list_shards(StreamName=self.stream_name)["Shards"]
        for shard in shards:
            shard_id = shard["ShardId"]
            if shard_id not in self._shard_iterators:
                self._shard_iterators[shard_id] = kinesis.get_shard_iterator(
                    StreamName=self.stream_name,
                    ShardId=shard_id,
                    ShardIteratorType="LATEST",
                )["ShardIterator"]

    def read(self) -> Optional[DataFrame]:
        self._ensure_iterators()
        records = []
        end_time = time.time() + self.poll_seconds

        while time.time() < end_time:
            for shard_id, iterator in list(self._shard_iterators.items()):
                if iterator is None:
                    continue
                resp = kinesis.get_records(ShardIterator=iterator, Limit=1000)
                for rec in resp["Records"]:
                    try:
                        records.append(json.loads(rec["Data"]))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue  # malformed records are routed to the DLQ Lambda separately
                self._shard_iterators[shard_id] = resp.get("NextShardIterator")
            time.sleep(2)

        if not records:
            return None

        df = self.spark.createDataFrame(records, schema=TRADE_SCHEMA)
        return df.withColumn("event_date", F.to_date(F.col("timestamp")))


# COMMAND ----------

config = IngestionConfig(
    source_name="alpaca_kinesis",
    target_catalog="bronze",
    target_schema="market_data",
    target_table="raw_trades",
)

ingester = KinesisTradeIngester(spark, config, KINESIS_STREAM, poll_seconds=30)

print(f"Starting continuous Kinesis -> bronze.market_data.raw_trades consumption "
      f"(poll window {ingester.poll_seconds}s). Cancel the job run to stop.")
while True:
    n = ingester.run()
    if n:
        print(f"Wrote {n} record(s) to bronze.market_data.raw_trades")
