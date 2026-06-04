import json
import os
import sys
from pathlib import Path

from pyiceberg.catalog import load_catalog

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

DATABASE = "btc_lakehouse"
TABLES_JSON = Path(__file__).parents[1] / "tables.json"

TYPE_MAP = {
    "bigint": LongType(),
    "double": DoubleType(),
    "string": StringType(),
    "date": DateType(),
    "timestamp": TimestampType(),
    "timestamptz": TimestamptzType(),
}


def sync_columns(table, columns: dict) -> None:
    """Add new columns and drop removed ones to match the provided column config."""

    existing = {field.name: field.field_type for field in table.schema().fields}
    expected = set(columns.keys())

    type_conflicts = [
        col
        for col, dtype in columns.items()
        if col in existing and existing[col] != TYPE_MAP[dtype.lower()]
    ]
    if type_conflicts:
        raise RuntimeError(
            f"Column type change in {table.name()} for: {', '.join(type_conflicts)}. "
            "Type changes require a custom migration script."
        )

    to_add = [(col, dtype) for col, dtype in columns.items() if col not in existing]
    to_drop = set(existing.keys()) - expected

    if not to_add and not to_drop:
        print(f"  {table.name()[-1]}: {len(existing)} columns — no changes")
        return

    with table.update_schema() as update:
        for col, dtype in to_add:
            update.add_column(col, TYPE_MAP[dtype.lower()])
        for col in to_drop:
            update.delete_column(col)

    changes = []
    if to_add:
        changes.append(f"+{len(to_add)} columns ({', '.join(c for c, _ in to_add)})")
    if to_drop:
        changes.append(f"-{len(to_drop)} columns ({', '.join(to_drop)})")
    print(f"  {table.name()[-1]}: {len(existing)} columns — {', '.join(changes)}")


def main():

    # Load AWS config from environment variables
    bucket = os.environ.get("AWS_BUCKET_NAME")
    if not bucket:
        print("Error: missing environment variable AWS_BUCKET_NAME", file=sys.stderr)
        sys.exit(1)

    region = os.environ.get("AWS_REGION_BTC")
    if not region:
        print("Error: missing environment variable AWS_REGION_BTC", file=sys.stderr)
        sys.exit(1)

    # Connect to Glue Catalog and get existing tables
    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
            "warehouse": f"s3://{bucket}/gold/",
            "region_name": region,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )

    # Create database if it doesn't exist
    existing_namespaces = {ns[0] for ns in catalog.list_namespaces()}
    if DATABASE not in existing_namespaces:
        catalog.create_namespace(DATABASE)

    # Load table schema configuration
    schema_config = json.loads(TABLES_JSON.read_text())
    existing = {table_name for _, table_name in catalog.list_tables(DATABASE)}

    # Sync tables based on configuration
    for table_name, config in schema_config.items():
        columns = config["columns"]
        schema = Schema(
            *(
                NestedField(
                    field_id=i + 1,
                    name=col,
                    field_type=TYPE_MAP[dtype.lower()],
                    required=False,
                )
                for i, (col, dtype) in enumerate(columns.items())
            )
        )

        partition_col = config.get("partition_by")
        if partition_col:
            source_id = schema.find_field(partition_col).field_id
            partition_spec = PartitionSpec(
                PartitionField(
                    source_id=source_id,
                    field_id=1000,
                    transform=IdentityTransform(),
                    name=partition_col,
                )
            )
        else:
            partition_spec = PartitionSpec()

        if table_name not in existing:
            catalog.create_table(
                identifier=f"{DATABASE}.{table_name}",
                schema=schema,
                partition_spec=partition_spec,
                location=f"s3://{bucket}/{config['location']}",
            )
            print(f"  {table_name}: created ({len(columns)} columns)")
        else:
            sync_columns(catalog.load_table(f"{DATABASE}.{table_name}"), columns)

    dropped = existing - set(schema_config.keys())
    for table_name in dropped:
        catalog.drop_table(f"{DATABASE}.{table_name}")
        print(f"  {table_name}: dropped")

    print(f"Synced {len(schema_config)} tables ({len(dropped)} dropped)")
