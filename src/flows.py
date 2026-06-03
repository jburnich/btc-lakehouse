import sys
from datetime import date, timedelta

from prefect import flow, task

from cli.sync_tables import main as sync_tables_main
from cli.ingest import main as _ingest_main


@task
def sync_tables() -> None:
    sync_tables_main()


@task
def ingest(date_str: str) -> None:
    sys.argv = ["ingest", date_str]
    _ingest_main()


@flow
def sync_tables_flow() -> None:
    sync_tables()


@flow
def ingest_flow(date_str: str = str(date.today() - timedelta(days=1))) -> None:
    ingest(date_str)
