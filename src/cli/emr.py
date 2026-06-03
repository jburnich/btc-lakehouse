import os
import sys
import time
import boto3

ICEBERG_CONFIGS = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.glue": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue.io-impl": "org.apache.iceberg.io.ResolvingFileIO",
}


def get_env() -> tuple[str, str, str, str]:
    bucket = os.environ.get("AWS_BUCKET_NAME")
    region = os.environ.get("AWS_REGION_BTC")
    app_id = os.environ.get("EMR_APPLICATION_ID")
    role_arn = os.environ.get("EMR_EXECUTION_ROLE_ARN")

    if not all([bucket, region, app_id, role_arn]):
        missing = [
            k
            for k, v in {
                "AWS_BUCKET_NAME": bucket,
                "AWS_REGION_BTC": region,
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

    return bucket, region, app_id, role_arn


def upload(bucket: str, region: str, *local_to_s3: tuple) -> None:
    s3 = _boto_client("s3", region)
    for local_path, s3_key in local_to_s3:
        s3.upload_file(str(local_path), bucket, s3_key)


def _boto_client(service: str, region: str) -> object:
    return boto3.client(
        service,
        region_name=region,
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )


def run_job(
    bucket: str,
    region: str,
    app_id: str,
    role_arn: str,
    entry_point: str,
    arguments: list[str],
    spark_configs: dict,
    submit_params: str = "",
) -> tuple[str, str]:
    client = _boto_client("emr-serverless", region)

    spark_submit = {
        "entryPoint": entry_point,
        "entryPointArguments": arguments,
    }
    if submit_params:
        spark_submit["sparkSubmitParameters"] = submit_params

    resp = client.start_job_run(
        applicationId=app_id,
        executionRoleArn=role_arn,
        jobDriver={"sparkSubmit": spark_submit},
        configurationOverrides={
            "monitoringConfiguration": {
                "s3MonitoringConfiguration": {"logUri": f"s3://{bucket}/emr-logs/"}
            },
            "applicationConfiguration": [
                {"classification": "spark-defaults", "properties": spark_configs}
            ],
        },
    )

    job_id = resp["jobRunId"]
    print(f"Job submitted: {job_id}")

    poller = _boto_client("emr-serverless", region)
    while True:
        job_run = poller.get_job_run(applicationId=app_id, jobRunId=job_id)["jobRun"]
        state = job_run["state"]
        if state in ("SUCCESS", "FAILED", "CANCELLED"):
            return state, job_run.get("stateDetails", "")
        time.sleep(10)
