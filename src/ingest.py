import os
import boto3
from botocore import UNSIGNED
from botocore.config import Config

SOURCE_BUCKET = "aws-public-blockchain"
SOURCE_PREFIX = "v1.0/btc/transactions"
SOURCE_REGION = "us-east-2"

TARGET_PREFIX = "raw/transactions"


def fetch_partition(date: str) -> str:
    """Upload a daily BTC transaction partition to S3."""

    target_bucket = os.environ.get("AWS_BUCKET_NAME")
    target_region = os.environ.get("AWS_REGION")

    if not target_bucket or not target_region:
        raise EnvironmentError("AWS_BUCKET_NAME and AWS_REGION must be set")

    # UNSIGNED to skip signing on public buckets
    source_s3 = boto3.client(
        "s3", region_name=SOURCE_REGION, config=Config(signature_version=UNSIGNED)
    )
    target_s3 = boto3.client("s3", region_name=target_region)

    prefix = f"{SOURCE_PREFIX}/date={date}/"
    print(f"Listing objects at s3://{SOURCE_BUCKET}/{prefix}")
    response = source_s3.list_objects_v2(Bucket=SOURCE_BUCKET, Prefix=prefix)

    if "Contents" not in response:
        raise ValueError(f"No data found for date={date}")

    files = response["Contents"]
    target_prefix = f"{TARGET_PREFIX}/date={date}"
    print(f"Uploading {len(files)} file(s) to s3://{target_bucket}/{target_prefix}/")

    for i, obj in enumerate(files, 1):
        key = obj["Key"]
        target_key = f"{target_prefix}/{key.split('/')[-1]}"
        body = source_s3.get_object(Bucket=SOURCE_BUCKET, Key=key)["Body"]
        target_s3.upload_fileobj(body, target_bucket, target_key)
        print(f"  [{i}/{len(files)}] {key.split('/')[-1]}")

    return f"s3://{target_bucket}/{target_prefix}/"
