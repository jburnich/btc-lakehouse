import argparse
from pathlib import Path

from cli.emr import ICEBERG_CONFIGS, get_env, upload, run_job

JOBS_PREFIX = "jobs"
JOB_SCRIPT = Path(__file__).parents[1] / "emr_scripts" / "sync_tables.py"
CONF_FILE = Path(__file__).parents[1] / "tables.json"


def main():
    parser = argparse.ArgumentParser(description="Synchronize Iceberg tables with the Glue catalog")
    parser.parse_args()

    bucket, region, app_id, role_arn = get_env()

    key = f"{JOBS_PREFIX}/sync_tables.py"
    upload(bucket, region, (JOB_SCRIPT, key), (CONF_FILE, "conf/tables.json"))

    configs = {
        **ICEBERG_CONFIGS,
        "spark.sql.catalog.glue.warehouse": f"s3://{bucket}/gold/",
        # DDL only — no executors needed
        "spark.dynamicAllocation.enabled": "false",
        "spark.executor.instances": "1",
    }

    state, details = run_job(
        bucket, region, app_id, role_arn,
        entry_point=f"s3://{bucket}/{key}",
        arguments=[bucket],
        spark_configs=configs,
    )

    if state == "SUCCESS":
        print("Tables synchronized successfully")
    else:
        raise RuntimeError(f"EMR job {state}: {details}")
