import argparse
import os
import sys
from pyspark.errors import AnalysisException
from spark_session import create_session
from gold.daily_metrics import run as run_daily_metrics

RAW_PREFIX = "raw/transactions"


def main():
    parser = argparse.ArgumentParser(description="Compute gold metrics from a raw BTC partition")
    parser.add_argument("date", help="Partition date (YYYY-MM-DD)")
    args = parser.parse_args()
    try:
        bucket = os.environ.get("AWS_BUCKET_NAME")
        if not bucket:
            raise EnvironmentError("AWS_BUCKET_NAME is not set")
        spark = create_session()
        partition_path = f"s3a://{bucket}/{RAW_PREFIX}/date={args.date}/"
        output = run_daily_metrics(spark, partition_path, bucket)
        print(f"daily_metrics written to {output}")
        spark.stop()
    except AnalysisException as e:
        print(f"Error: partition not found — run ingest {args.date} first", file=sys.stderr)
        sys.exit(1)
    except (ValueError, EnvironmentError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
