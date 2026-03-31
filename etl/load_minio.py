"""Load transformed DataFrames as Parquet files into MinIO."""

import io
import logging
from datetime import datetime, timezone

import pandas as pd
from minio import Minio
from config.settings import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET,
    MINIO_SECURE,
)

logger = logging.getLogger(__name__)


def _get_client() -> Minio:
    """Create a MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def _ensure_bucket(client: Minio, bucket: str) -> None:
    """Create bucket if it does not exist."""
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        logger.info("Created MinIO bucket: %s", bucket)


def upload_dataframe(df: pd.DataFrame, object_prefix: str) -> str:
    """Upload a DataFrame as a Parquet file to MinIO.

    Args:
        df: The DataFrame to upload.
        object_prefix: e.g. "rfis" or "submittals".

    Returns:
        The object path in MinIO.
    """
    if df.empty:
        logger.warning("Empty DataFrame — skipping upload for %s", object_prefix)
        return ""

    client = _get_client()
    _ensure_bucket(client, MINIO_BUCKET)

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    object_name = f"{object_prefix}/{date_str}/{object_prefix}.parquet"

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    client.put_object(
        MINIO_BUCKET,
        object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream",
    )

    logger.info(
        "Uploaded %s to MinIO: %s/%s (%d rows)",
        object_prefix,
        MINIO_BUCKET,
        object_name,
        len(df),
    )
    return f"{MINIO_BUCKET}/{object_name}"
