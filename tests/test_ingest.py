import pytest
from unittest.mock import MagicMock, patch
from io import BytesIO

from src.ingest import fetch_partition


@pytest.fixture
def mock_s3(monkeypatch):
    monkeypatch.setenv("AWS_BUCKET_NAME", "test-bucket")
    monkeypatch.setenv("AWS_REGION", "eu-west-1")

    with patch("src.ingest.boto3.client") as mock_client:
        source_s3 = MagicMock()
        target_s3 = MagicMock()
        mock_client.side_effect = [source_s3, target_s3]

        source_s3.list_objects_v2.return_value = {
            "Contents": [
                {"Key": "v1.0/btc/transactions/date=2026-01-01/part-00000.parquet"}
            ]
        }
        source_s3.get_object.return_value = {"Body": BytesIO(b"parquet-data")}

        yield source_s3, target_s3


def test_fetch_partition_uploads_to_s3(mock_s3):
    _, target_s3 = mock_s3
    result = fetch_partition("2026-01-01")

    assert result == "s3://test-bucket/raw/btc/transactions/date=2026-01-01/"
    target_s3.upload_fileobj.assert_called_once()


def test_fetch_partition_raises_on_missing_date(mock_s3):
    source_s3, _ = mock_s3
    source_s3.list_objects_v2.return_value = {}

    with pytest.raises(ValueError, match="No data found"):
        fetch_partition("2000-01-01")


def test_fetch_partition_raises_on_missing_env(monkeypatch):
    monkeypatch.delenv("AWS_BUCKET_NAME", raising=False)
    monkeypatch.delenv("AWS_REGION", raising=False)

    with pytest.raises(
        EnvironmentError, match="AWS_BUCKET_NAME and AWS_REGION must be set"
    ):
        fetch_partition("2026-01-01")
