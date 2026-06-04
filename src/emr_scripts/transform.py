import sys
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


if __name__ == "__main__":
    date = sys.argv[1]
    bucket = sys.argv[2]

    spark = get_spark()
    run_daily_metrics(spark, date, bucket)
    spark.stop()
