# Databricks notebook source
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# ── SILVER LAYER ──────────────────────────────────────────────────────────────
# Explicitly qualified names (catalog.schema.table) so this one pipeline can
# publish across silver.market_data and gold.market_data — the pipeline's own
# default catalog/target (set at creation) only applies to unqualified names.

@dlt.table(
    name="silver.market_data.silver_trades",
    comment="Cleaned, validated trade records with deduplication",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "pipelines.autoOptimize.zOrderCols": "symbol,exchange",
    },
)
@dlt.expect_all_or_drop({
    "valid_price": "price > 0",
    "valid_size": "size > 0",
    "valid_symbol": "length(symbol) BETWEEN 1 AND 5",
    # Checks trade_ts, not the raw `timestamp` string column — the final
    # .select() below drops `timestamp` in favor of the parsed `trade_ts`,
    # and DLT expectations evaluate against the function's final output.
    "not_null_timestamp": "trade_ts IS NOT NULL",
})
def silver_trades():
    return (
        dlt.read_stream("bronze.market_data.raw_trades")
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("trade_ts", F.to_timestamp(F.col("timestamp")))
        .withColumn("trade_date", F.to_date(F.col("trade_ts")))
        .withColumn("trade_hour", F.hour(F.col("trade_ts")))
        .dropDuplicates(["symbol", "timestamp", "price", "size"])
        .select(
            "symbol", "price", "size", "exchange",
            "trade_ts", "trade_date", "trade_hour",
            "_ingested_at", "_source",
        )
    )


@dlt.table(
    name="silver.market_data.silver_ohlcv",
    comment="1-minute OHLCV bars aggregated from tick data",
)
def silver_ohlcv():
    return (
        dlt.read_stream("silver.market_data.silver_trades")
        .groupBy(
            "symbol",
            F.window("trade_ts", "1 minute").alias("window"),
            "trade_date",
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("size").alias("volume"),
            F.count("*").alias("trade_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


# ── CDC APPLY CHANGES: account positions ────────────────────────────────────
# Source is a synthetic bronze.market_data.position_changes table (Alpaca's
# paper account has no real position history) — seeded once to demonstrate
# the CDC/apply_changes pattern (SCD Type 1 upsert + delete).

dlt.create_streaming_table("silver.market_data.silver_positions_current")

dlt.apply_changes(
    target="silver.market_data.silver_positions_current",
    source="bronze.market_data.position_changes",
    keys=["account_id", "symbol"],
    sequence_by="event_timestamp",
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    column_list=["account_id", "symbol", "quantity", "avg_cost", "event_timestamp"],
)


# ── GOLD LAYER: STAR SCHEMA ──────────────────────────────────────────────────

@dlt.table(
    name="gold.market_data.gold_dim_symbol",
    comment="Symbol dimension — SCD Type 1",
)
def gold_dim_symbol():
    return (
        dlt.read("silver.market_data.silver_trades")
        .select("symbol")
        .distinct()
        .withColumn("symbol_key", F.sha2(F.col("symbol"), 256))
        .withColumn("asset_class", F.lit("equity"))
        .withColumn("_dw_updated_at", F.current_timestamp())
    )


@dlt.table(
    name="gold.market_data.gold_dim_date",
    comment="Date dimension",
)
def gold_dim_date():
    return (
        dlt.read("silver.market_data.silver_trades")
        .select("trade_date")
        .distinct()
        .withColumn("year", F.year("trade_date"))
        .withColumn("month", F.month("trade_date"))
        .withColumn("quarter", F.quarter("trade_date"))
        .withColumn("day_of_week", F.dayofweek("trade_date"))
        .withColumn("is_weekend", F.col("day_of_week").isin(1, 7))
    )


@dlt.table(
    name="gold.market_data.gold_fact_daily_ohlcv",
    comment="Daily OHLCV fact table — Star Schema core fact",
)
@dlt.expect("positive_volume", "volume > 0")
def gold_fact_daily_ohlcv():
    ohlcv = dlt.read("silver.market_data.silver_ohlcv")
    dim_symbol = dlt.read("gold.market_data.gold_dim_symbol")

    return (
        ohlcv
        .groupBy("symbol", "trade_date")
        .agg(
            F.first("open").alias("open"),
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.last("close").alias("close"),
            F.sum("volume").alias("volume"),
            F.sum("trade_count").alias("total_trades"),
        )
        .join(dim_symbol, on="symbol", how="left")
        .withColumn(
            "daily_return_pct",
            (F.col("close") - F.col("open")) / F.col("open") * 100,
        )
        .withColumn("_dw_loaded_at", F.current_timestamp())
    )
