import sys
import json
import boto3
from pyspark.sql import SparkSession

CONF_S3_KEY = "conf/tables.json"
DATABASE = "glue.btc_lakehouse"


def get_spark() -> SparkSession:
    """Create SparkSession with Iceberg configs for Glue catalog."""

    return SparkSession.builder.appName("btc-lakehouse").getOrCreate()


def load_schema(bucket: str) -> dict:
    """Load table schema config from S3."""

    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=CONF_S3_KEY)
    return json.loads(obj["Body"].read())


def sync(spark: SparkSession, bucket: str) -> None:
    """Idempotent sync of Iceberg tables with src/tables.json."""

    schema = load_schema(bucket)

    for table, config in schema.items():
        sync_table(spark, table, config, bucket)

    drop_removed_tables(spark, schema)


def sync_table(spark: SparkSession, table: str, config: dict, bucket: str) -> None:
    """Create or alter an Iceberg table to match the provided schema config."""

    col_defs = ",\n    ".join(
        f"{col} {dtype}" for col, dtype in config["columns"].items()
    )
    location = f"s3://{bucket}/{config['location']}"
    partition_clause = (
        f"PARTITIONED BY ({config['partition_by']})" if "partition_by" in config else ""
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{table} (
            {col_defs}
        )
        USING iceberg
        {partition_clause}
        LOCATION '{location}'
        TBLPROPERTIES ('format-version' = '2')
    """)

    existing = {
        row.col_name: row.data_type
        for row in spark.sql(f"DESCRIBE {DATABASE}.{table}").collect()
        if row.col_name and not row.col_name.startswith("#")
    }
    expected = set(config["columns"].keys())

    type_conflicts = [
        col
        for col, dtype in config["columns"].items()
        if col in existing and existing[col].lower() != dtype.lower()
    ]
    if type_conflicts:
        raise RuntimeError(
            f"Column type change detected in {table} for: {', '.join(type_conflicts)}. "
            "Type changes require a custom migration script."
        )

    for col, dtype in config["columns"].items():
        if col not in existing:
            spark.sql(f"ALTER TABLE {DATABASE}.{table} ADD COLUMN {col} {dtype}")

    for col in set(existing.keys()) - expected:
        spark.sql(f"ALTER TABLE {DATABASE}.{table} DROP COLUMN {col}")


def drop_removed_tables(spark: SparkSession, schema: dict) -> None:
    """Drop Iceberg tables that are not present in the schema config."""

    existing_tables = {
        row.tableName for row in spark.sql(f"SHOW TABLES IN {DATABASE}").collect()
    }
    for table in existing_tables - set(schema.keys()):
        spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{table}")


if __name__ == "__main__":
    bucket = sys.argv[1]
    spark = get_spark()
    sync(spark, bucket)
    spark.stop()
