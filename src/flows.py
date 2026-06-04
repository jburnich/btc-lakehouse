import sys
from datetime import date, timedelta

from prefect import flow, task

from cli.sync_tables import main as sync_tables_main
from cli.ingest import main as _ingest_main
from cli.transform import main as _transform_main


@task(log_prints=True)
def sync_tables() -> None:
    sync_tables_main()


@task(log_prints=True)
def ingest(date_str: str) -> None:
    sys.argv = ["ingest", date_str]
    _ingest_main()


@task(log_prints=True)
def transform(date_str: str) -> None:
    sys.argv = ["transform", date_str]
    _transform_main()


@flow(log_prints=True)
def sync_tables_flow() -> None:
    sync_tables()


@flow(log_prints=True)
def ingest_flow(date_str: str = str(date.today() - timedelta(days=1))) -> None:
    ingest(date_str)


@flow(log_prints=True)
def transform_flow(date_str: str = str(date.today() - timedelta(days=1))) -> None:
    transform(date_str)


@flow(log_prints=True)
def pipeline_flow(date_str: str = str(date.today() - timedelta(days=1))) -> None:
    raw = ingest(date_str)
    transform(date_str, wait_for=[raw])
