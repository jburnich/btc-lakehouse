import sys
from pathlib import Path

from prefect import flow, task

sys.path.insert(0, str(Path(__file__).parent))

from cli.sync_tables import main as sync_tables_main


@task
def sync_tables() -> None:
    sync_tables_main()


@flow
def sync_tables_flow() -> None:
    sync_tables()
