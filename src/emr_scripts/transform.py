import sys
from datetime import datetime, timezone
from pyspark.sql import SparkSession, functions as F


def get_spark() -> SparkSession:
    """Initialize and return a SparkSession."""

    return SparkSession.builder.appName("btc-lakehouse").getOrCreate()


def run_daily_metrics(spark: SparkSession, date: str, bucket: str) -> None:
    """Compute daily metrics for a given date and write to Glue catalog."""

    df = spark.read.parquet(f"s3://{bucket}/raw/transactions/date={date}/")
    txs = df.filter(~F.col("is_coinbase"))

    # Note: 1 BTC = 1e8 satoshis
    daily = txs.groupBy(F.to_date("block_timestamp").alias("date")).agg(
        F.count("txid").alias("total_transactions"),
        F.round(F.sum("output_value"), 8).alias("total_volume_btc"),
        F.round(F.sum("fee"), 8).alias("total_fees_btc"),
        F.round(F.avg("fee") * 1e8, 0).cast("long").alias("avg_fee_sat"),
        F.round(F.percentile_approx("fee", 0.5) * 1e8, 0)
        .cast("long")
        .alias("median_fee_sat"),
    )

    daily.writeTo("glue.btc_lakehouse.daily_metrics").overwritePartitions()


def run_address_stats(spark: SparkSession, date: str, bucket: str) -> None:
    """Compute per-address BTC stats and upsert into the Iceberg table."""

    df = spark.read.parquet(f"s3://{bucket}/raw/transactions/date={date}/")
    txs = df.filter(~F.col("is_coinbase"))

    received = (
        txs.select("block_timestamp", F.explode("outputs").alias("o"))
        .select(
            "block_timestamp",
            F.col("o.address").alias("address"),
            F.col("o.value").alias("value_btc"),
        )
        .filter(F.col("address").isNotNull())
    )

    sent = (
        txs.select("block_timestamp", F.explode("inputs").alias("i"))
        .select(
            "block_timestamp",
            F.col("i.address").alias("address"),
            F.col("i.value").alias("value_btc"),
        )
        .filter(F.col("address").isNotNull())
    )

    received_agg = received.groupBy("address").agg(
        F.round(F.sum("value_btc"), 8).alias("total_received_btc"),
        F.count("*").alias("received_count"),
        F.min("block_timestamp").alias("first_seen"),
        F.max("block_timestamp").alias("last_seen"),
    )

    sent_agg = sent.groupBy("address").agg(
        F.round(F.sum("value_btc"), 8).alias("total_sent_btc"),
        F.count("*").alias("sent_count"),
    )

    delta = (
        received_agg.join(sent_agg, on="address", how="full_outer")
        .withColumn("total_received_btc", F.coalesce("total_received_btc", F.lit(0.0)))
        .withColumn("total_sent_btc", F.coalesce("total_sent_btc", F.lit(0.0)))
        .withColumn(
            "balance_btc",
            F.round(F.col("total_received_btc") - F.col("total_sent_btc"), 8),
        )
        .withColumn(
            "tx_count",
            F.coalesce("received_count", F.lit(0)) + F.coalesce("sent_count", F.lit(0)),
        )
        .drop("received_count", "sent_count")
    )

    delta.createOrReplaceTempView("delta")

    spark.sql("""
        MERGE INTO glue.btc_lakehouse.address_stats t
        USING delta d ON t.address = d.address
        WHEN MATCHED THEN UPDATE SET
            total_received_btc = t.total_received_btc + d.total_received_btc,
            total_sent_btc     = t.total_sent_btc     + d.total_sent_btc,
            balance_btc        = round((t.total_received_btc + d.total_received_btc)
                                     - (t.total_sent_btc + d.total_sent_btc), 8),
            tx_count           = t.tx_count + d.tx_count,
            first_seen         = least(t.first_seen, d.first_seen),
            last_seen          = greatest(t.last_seen, d.last_seen)
        WHEN NOT MATCHED THEN INSERT *
    """)

    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    spark.sql(f"""
        CALL glue.system.expire_snapshots(
            table => 'btc_lakehouse.address_stats',
            older_than => TIMESTAMP '{now_ts}',
            retain_last => 1
        )
    """)


if __name__ == "__main__":
    date = sys.argv[1]
    bucket = sys.argv[2]
    job = sys.argv[3] if len(sys.argv) > 3 else "all"

    spark = get_spark()
    if job in ("all", "daily_metrics"):
        run_daily_metrics(spark, date, bucket)
    if job in ("all", "address_stats"):
        run_address_stats(spark, date, bucket)
    spark.stop()
