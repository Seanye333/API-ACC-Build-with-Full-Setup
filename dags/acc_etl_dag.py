"""Airflow DAG — ACC Build ETL (RFIs & Submittals).

Runs daily: Extract → Transform → Load MinIO → Load Oracle.
"""

import json
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------

def task_extract(**context):
    """Extract RFIs and Submittals from ACC Build API."""
    from etl.extract import extract_rfis, extract_submittals

    rfis = extract_rfis()
    submittals = extract_submittals()

    context["ti"].xcom_push(key="raw_rfis", value=json.dumps(rfis))
    context["ti"].xcom_push(key="raw_submittals", value=json.dumps(submittals))
    logger.info("Extracted %d RFIs, %d Submittals", len(rfis), len(submittals))


def task_transform(**context):
    """Transform raw JSON into clean DataFrames, push as JSON to XCom."""
    from etl.transform import transform_rfis, transform_submittals

    ti = context["ti"]
    raw_rfis = json.loads(ti.xcom_pull(task_ids="extract", key="raw_rfis"))
    raw_submittals = json.loads(
        ti.xcom_pull(task_ids="extract", key="raw_submittals")
    )

    df_rfis = transform_rfis(raw_rfis)
    df_submittals = transform_submittals(raw_submittals)

    ti.xcom_push(key="rfis_json", value=df_rfis.to_json(orient="records"))
    ti.xcom_push(
        key="submittals_json", value=df_submittals.to_json(orient="records")
    )
    logger.info(
        "Transformed — RFIs: %d rows, Submittals: %d rows",
        len(df_rfis),
        len(df_submittals),
    )


def task_load_minio(**context):
    """Upload transformed Parquet files to MinIO."""
    import pandas as pd
    from etl.load_minio import upload_dataframe

    ti = context["ti"]
    df_rfis = pd.read_json(ti.xcom_pull(task_ids="transform", key="rfis_json"))
    df_submittals = pd.read_json(
        ti.xcom_pull(task_ids="transform", key="submittals_json")
    )

    rfi_path = upload_dataframe(df_rfis, "rfis")
    sub_path = upload_dataframe(df_submittals, "submittals")
    logger.info("MinIO uploads: %s, %s", rfi_path, sub_path)


def task_load_oracle(**context):
    """Upsert transformed data into Oracle DB."""
    import pandas as pd
    from etl.load_oracle import upsert_dataframe

    ti = context["ti"]
    df_rfis = pd.read_json(ti.xcom_pull(task_ids="transform", key="rfis_json"))
    df_submittals = pd.read_json(
        ti.xcom_pull(task_ids="transform", key="submittals_json")
    )

    rfi_count = upsert_dataframe(df_rfis, "ACC_RFIS", key_col="IID")
    sub_count = upsert_dataframe(df_submittals, "ACC_SUBMITTALS", key_col="ID")
    logger.info("Oracle loads: %d RFIs, %d Submittals", rfi_count, sub_count)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="acc_build_etl",
    default_args=default_args,
    description="Daily ETL: ACC Build RFIs & Submittals → MinIO → Oracle",
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["acc", "etl", "construction"],
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=task_extract,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
    )

    load_minio = PythonOperator(
        task_id="load_minio",
        python_callable=task_load_minio,
    )

    load_oracle = PythonOperator(
        task_id="load_oracle",
        python_callable=task_load_oracle,
    )

    extract >> transform >> load_minio >> load_oracle
