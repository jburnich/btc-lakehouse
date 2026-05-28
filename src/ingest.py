import boto3
from pathlib import Path
from botocore import UNSIGNED
from botocore.config import Config

BUCKET = "aws-public-blockchain"
PREFIX = "v1.0/btc/transactions"
REGION = "us-east-2"


def fetch_partition(date: str, output_dir: Path = Path("data/raw")) -> Path:
    """Download a daily BTC transaction partition locally."""

    # UNSIGNED to skip signing on public buckets
    s3 = boto3.client(
        "s3", region_name=REGION, config=Config(signature_version=UNSIGNED)
    )

    prefix = f"{PREFIX}/date={date}/"
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)

    if "Contents" not in response:
        raise ValueError(f"No data found for date={date}")

    partition_dir = output_dir / f"date={date}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    for obj in response["Contents"]:
        key = obj["Key"]
        filename = partition_dir / Path(key).name
        s3.download_file(BUCKET, key, str(filename))

    return partition_dir
