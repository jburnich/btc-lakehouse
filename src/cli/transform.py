import argparse
import sys
from pathlib import Path

from cli.emr import ICEBERG_CONFIGS, get_env, upload, run_job

JOBS_PREFIX = "jobs"
JOB_SCRIPT = Path(__file__).parents[1] / "emr_scripts" / "transform.py"


def main():
    parser = argparse.ArgumentParser(
        description="Submit BTC transform job to EMR Serverless"
    )
    parser.add_argument("date", help="Partition date (YYYY-MM-DD)")
    parser.add_argument(
        "--job",
        default="all",
        choices=["all", "daily_metrics", "address_stats"],
        help="Which job to run (default: all)",
    )
    args = parser.parse_args()

    bucket, region, app_id, role_arn = get_env()

    try:
        key = f"{JOBS_PREFIX}/transform.py"
        upload(bucket, region, (JOB_SCRIPT, key))

        configs = {
            **ICEBERG_CONFIGS,
            "spark.dynamicAllocation.executorIdleTimeout": "300s",
            "spark.sql.catalog.glue.warehouse": f"s3://{bucket}/gold/",
        }

        state, details = run_job(
            bucket,
            region,
            app_id,
            role_arn,
            entry_point=f"s3://{bucket}/{key}",
            arguments=[args.date, bucket, args.job],
            spark_configs=configs,
            submit_params="--conf spark.sql.parquet.enableVectorizedReader=false",
        )

        if state == "SUCCESS":
            print(f"Transform complete for {args.date}")
        else:
            msg = f"Error: job {state}"
            if details:
                msg += f" — {details}"
            print(msg, file=sys.stderr)
            sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
