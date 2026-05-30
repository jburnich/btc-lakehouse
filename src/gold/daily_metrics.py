from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

TARGET_PREFIX = "gold/btc/daily_metrics"


def run(spark: SparkSession, partition_path: str, target_bucket: str) -> str:
    """Compute daily metrics and write to S3 as Parquet."""

    df = compute(spark, partition_path)
    output = f"s3a://{target_bucket}/{TARGET_PREFIX}"
    df.write.mode("overwrite").partitionBy("date").parquet(output)

    return output


def compute(spark: SparkSession, partition_path: str) -> DataFrame:
    """Compute daily BTC transaction metrics from a raw partition."""

    df = spark.read.parquet(partition_path)

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

    return daily.orderBy("date")
