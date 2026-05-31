import argparse
import os
import sys
import time
from pathlib import Path

import boto3

JOBS_PREFIX = "jobs"
JOB_SCRIPT = Path(__file__).parents[1] / "emr_scripts" / "sync_tables.py"
CONF_FILE = Path(__file__).parents[1] / "tables.json"

ICEBERG_CONFIGS = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
    # DDL only — no executors needed
    "spark.dynamicAllocation.enabled": "false",
    "spark.executor.instances": "1",
}


def main():
    parser = argparse.ArgumentParser(
        description="Synchronize Iceberg tables with the Glue catalog"
    )
    parser.parse_args()

    bucket = os.environ.get("AWS_BUCKET_NAME")
    region = os.environ.get("AWS_REGION")
    app_id = os.environ.get("EMR_APPLICATION_ID")
    role_arn = os.environ.get("EMR_EXECUTION_ROLE_ARN")

    if not all([bucket, region, app_id, role_arn]):
        missing = [
            k
            for k, v in {
                "AWS_BUCKET_NAME": bucket,
                "AWS_REGION": region,
                "EMR_APPLICATION_ID": app_id,
                "EMR_EXECUTION_ROLE_ARN": role_arn,
            }.items()
            if not v
        ]
        print(
            f"Error: missing environment variables: {', '.join(missing)}",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        s3 = boto3.client("s3", region_name=region)
        key = f"{JOBS_PREFIX}/sync_tables.py"
        s3.upload_file(str(JOB_SCRIPT), bucket, key)
        s3.upload_file(str(CONF_FILE), bucket, "conf/tables.json")

        configs = {
            **ICEBERG_CONFIGS,
            "spark.sql.catalog.glue.warehouse": f"s3://{bucket}/gold/",
        }
        client = boto3.client("emr-serverless", region_name=region)
        resp = client.start_job_run(
            applicationId=app_id,
            executionRoleArn=role_arn,
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": f"s3://{bucket}/{key}",
                    "entryPointArguments": [bucket],
                }
            },
            configurationOverrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": f"s3://{bucket}/emr-logs/"}
                },
                "applicationConfiguration": [
                    {"classification": "spark-defaults", "properties": configs}
                ],
            },
        )
        job_id = resp["jobRunId"]
        print(f"Job submitted: {job_id}")

        while True:
            job_run = client.get_job_run(applicationId=app_id, jobRunId=job_id)[
                "jobRun"
            ]
            state = job_run["state"]
            if state in ("SUCCESS", "FAILED", "CANCELLED"):
                break
            time.sleep(10)

        if state == "SUCCESS":
            print("Tables synchronized successfully")
        else:
            print(
                f"Error: job {state} — {job_run.get('stateDetails', '')}",
                file=sys.stderr,
            )
            sys.exit(1)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
