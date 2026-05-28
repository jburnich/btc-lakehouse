from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.ingest import fetch_partition


@pytest.fixture
def mock_s3(tmp_path):
    with patch("src.ingest.boto3.client") as mock_client:
        s3 = MagicMock()
        mock_client.return_value = s3

        s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "v1.0/btc/transactions/date=2026-01-01/part-00000.parquet"}
            ]
        }
        s3.download_file.side_effect = lambda bucket, key, dest: Path(dest).touch()

        yield s3


def test_fetch_partition_creates_files(mock_s3, tmp_path):
    result = fetch_partition("2026-01-01", output_dir=tmp_path)

    assert result == tmp_path / "date=2026-01-01"
    assert (result / "part-00000.parquet").exists()


def test_fetch_partition_raises_on_missing_date(mock_s3, tmp_path):
    mock_s3.list_objects_v2.return_value = {}

    with pytest.raises(ValueError, match="No data found"):
        fetch_partition("2000-01-01", output_dir=tmp_path)
