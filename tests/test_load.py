"""Tests for etl.load_minio and etl.load_oracle modules."""

from unittest.mock import patch, MagicMock
import pandas as pd

from etl.load_minio import upload_dataframe
from etl.load_oracle import upsert_dataframe, _build_merge_sql


# ---------------------------------------------------------------------------
# MinIO tests
# ---------------------------------------------------------------------------

@patch("etl.load_minio._get_client")
def test_upload_dataframe_to_minio(mock_client_factory):
    """Upload a DataFrame as Parquet to MinIO."""
    mock_client = MagicMock()
    mock_client.bucket_exists.return_value = True
    mock_client_factory.return_value = mock_client

    df = pd.DataFrame({"id": ["1", "2"], "name": ["a", "b"]})
    path = upload_dataframe(df, "rfis")

    assert "rfis" in path
    mock_client.put_object.assert_called_once()


@patch("etl.load_minio._get_client")
def test_upload_creates_bucket_if_missing(mock_client_factory):
    """Bucket is created when it does not exist."""
    mock_client = MagicMock()
    mock_client.bucket_exists.return_value = False
    mock_client_factory.return_value = mock_client

    df = pd.DataFrame({"id": ["1"]})
    upload_dataframe(df, "submittals")

    mock_client.make_bucket.assert_called_once()


def test_upload_empty_dataframe_skips():
    """Empty DataFrame should skip upload."""
    path = upload_dataframe(pd.DataFrame(), "rfis")
    assert path == ""


# ---------------------------------------------------------------------------
# Oracle tests
# ---------------------------------------------------------------------------

def test_build_merge_sql():
    """MERGE SQL should reference all columns and key."""
    sql = _build_merge_sql("ACC_RFIS", ["ID", "TITLE", "STATUS"], "ID")

    assert "MERGE INTO ACC_RFIS" in sql
    assert "ON (tgt.ID = src.ID)" in sql
    assert "WHEN MATCHED" in sql
    assert "WHEN NOT MATCHED" in sql


@patch("etl.load_oracle._get_connection")
def test_upsert_dataframe(mock_conn_factory):
    """Upsert executes MERGE for each row."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    mock_conn_factory.return_value = mock_conn

    df = pd.DataFrame({"id": ["1", "2"], "title": ["A", "B"]})
    count = upsert_dataframe(df, "ACC_RFIS")

    assert count == 2
    mock_cursor.executemany.assert_called_once()
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()


def test_upsert_empty_dataframe():
    """Empty DataFrame should skip Oracle load."""
    count = upsert_dataframe(pd.DataFrame(), "ACC_RFIS")
    assert count == 0
