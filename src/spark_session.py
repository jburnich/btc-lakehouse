import os
from pyspark.sql import SparkSession


def create_session(app_name: str = "btc-lakehouse") -> SparkSession:
    """Create a SparkSession configured to read and write from S3."""

    aws_region = os.environ.get("AWS_REGION")
    if not aws_region:
        raise EnvironmentError("AWS_REGION is not set")

    configs = {
        # hadoop-aws and AWS SDK v2 versions must be compatible
        # see https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws/3.5.0/dependencies
        "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.5.0,software.amazon.awssdk:bundle:2.35.4",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": f"s3.{aws_region}.amazonaws.com",
        # reads AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY from env
        "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider",
        "spark.sql.session.timeZone": "UTC",
    }

    builder = SparkSession.builder.appName(app_name)
    for key, value in configs.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()

    return spark
