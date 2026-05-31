import sys
from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("btc-lakehouse").getOrCreate()


def init_tables(spark: SparkSession, bucket: str) -> None:
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS glue.btc_lakehouse.daily_metrics (
            total_transactions BIGINT,
            total_volume_btc   DOUBLE,
            total_fees_btc     DOUBLE,
            avg_fee_sat        BIGINT,
            median_fee_sat     BIGINT,
            date               DATE
        )
        USING iceberg
        PARTITIONED BY (date)
        LOCATION 's3://{bucket}/gold/daily_metrics'
        TBLPROPERTIES ('format-version' = '2')
    """)


if __name__ == "__main__":
    bucket = sys.argv[1]
    spark = get_spark()
    init_tables(spark, bucket)
    spark.stop()
