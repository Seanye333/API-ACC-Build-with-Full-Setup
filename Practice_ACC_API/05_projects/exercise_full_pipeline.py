"""
=============================================================================
Exercise 5: Full ETL Pipeline Practice
=============================================================================
Combine everything: Extract → Transform → Load (to file, simulating MinIO).

This exercise ties together authentication, API calls, data transformation,
and storage — the full pipeline our Airflow DAG runs daily.

Reference: https://aps.autodesk.com/en/docs/acc/v1/overview/introduction/
=============================================================================
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import io
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from mock_data.mock_responses import (
    AUTH_TOKEN_RESPONSE,
    RFIS_SEARCH_RESPONSE,
    SUBMITTALS_ITEMS_RESPONSE,
)


# =============================================================================
# EXERCISE 5A: Simulate the Full Extract Step
# =============================================================================

def extract_all_data(auth_response: dict, rfis_response: dict, submittals_response: dict) -> dict:
    """
    TODO: Simulate the extract step of the ETL pipeline.

    In a real pipeline, you would:
    1. Get an access token from auth_response
    2. Use it to call the RFI search endpoint → rfis_response
    3. Use it to call the Submittal items endpoint → submittals_response

    Here, just extract and return the raw data from the mock responses.

    Args:
        auth_response: Mock auth token response.
        rfis_response: Mock RFI search response.
        submittals_response: Mock submittal items response.

    Returns:
        dict with keys:
            "access_token": the token string
            "raw_rfis": list of RFI dicts
            "raw_submittals": list of submittal dicts
            "extract_timestamp": current UTC timestamp as string
    """
    # YOUR CODE HERE
    return {}


# =============================================================================
# EXERCISE 5B: Full Transform Step
# =============================================================================

def transform_rfis(raw_rfis: list[dict]) -> pd.DataFrame:
    """
    TODO: Apply ALL transformations to RFI data.

    Steps:
    1. Flatten JSON with pd.json_normalize
    2. Rename columns to target schema (IID, TITLE, STATUS, etc.)
    3. Add OPEN_RFI_STATUS
    4. Add DUE_DATA_DATE_DELTA and RFI_BY_REQUIRED_DATE_STATUS
    5. Add RFI_BY_COST_SCHEDULE_STATUS
    6. Add RFI_BY_CATEGORY_STATUS
    7. Add AGEING_OPEN and AGEING_CLOSED
    8. Add DATA_DATE and LAST_MODIFIED_DATE metadata

    Hint: Reuse logic from Exercise 3 solutions.

    Returns:
        Fully transformed DataFrame with all derived columns.
    """
    if not raw_rfis:
        return pd.DataFrame()

    # YOUR CODE HERE — combine all transformations
    return pd.DataFrame()


def transform_submittals(raw_submittals: list[dict]) -> pd.DataFrame:
    """
    TODO: Apply ALL transformations to Submittal data.

    Steps:
    1. Flatten JSON
    2. Rename columns to target schema (NUMBER_ID, ID, TITLE, etc.)
    3. Add OPEN_SUBMITTALS_STATUS
    4. Add APPROVAL_DATA_DATE_DELTA and SUBMITTALS_BY_APPROVAL_DATE_STATUS
    5. Add OPEN_SUBMITTALS_IN_COURT_STATUS
    6. Add AGEING_OPEN and AGEING_CLOSED
    7. Add DATA_DATE and LAST_MODIFIED_DATE metadata

    Returns:
        Fully transformed DataFrame.
    """
    if not raw_submittals:
        return pd.DataFrame()

    # YOUR CODE HERE
    return pd.DataFrame()


# =============================================================================
# EXERCISE 5C: Simulate Load to MinIO (Save as Parquet)
# =============================================================================

def save_to_parquet_buffer(df: pd.DataFrame) -> io.BytesIO:
    """
    TODO: Save a DataFrame to a Parquet file in memory (BytesIO buffer).

    In the real pipeline, this buffer gets uploaded to MinIO.
    Here, just create the buffer.

    Steps:
    1. Create an io.BytesIO() buffer
    2. Use df.to_parquet(buffer, index=False, engine="pyarrow")
    3. Seek to beginning: buffer.seek(0)
    4. Return the buffer

    Returns:
        BytesIO buffer containing Parquet data.
    """
    # YOUR CODE HERE
    return io.BytesIO()


def build_minio_path(data_type: str, date: str = None) -> str:
    """
    TODO: Build the MinIO object path for storing data.

    Format: {data_type}/{date}/{data_type}.parquet

    Examples:
        build_minio_path("rfis") → "rfis/2025-04-04/rfis.parquet"
        build_minio_path("submittals") → "submittals/2025-04-04/submittals.parquet"

    Args:
        data_type: "rfis" or "submittals"
        date: Date string (defaults to today).

    Returns:
        The object path string.
    """
    # YOUR CODE HERE
    return ""


# =============================================================================
# EXERCISE 5D: Simulate Load to Oracle (Build MERGE SQL)
# =============================================================================

def build_merge_sql(table: str, columns: list[str], key_col: str) -> str:
    """
    TODO: Build an Oracle MERGE (upsert) SQL statement.

    The MERGE statement:
    - Matches rows by key_col
    - Updates all non-key columns when matched
    - Inserts a new row when not matched

    Template:
        MERGE INTO {table} tgt
        USING (
            SELECT :COL1 AS COL1, :COL2 AS COL2, ...
            FROM dual
        ) src
        ON (tgt.{key_col} = src.{key_col})
        WHEN MATCHED THEN
            UPDATE SET tgt.COL2 = src.COL2, tgt.COL3 = src.COL3, ...
        WHEN NOT MATCHED THEN
            INSERT (COL1, COL2, COL3, ...)
            VALUES (src.COL1, src.COL2, src.COL3, ...)

    Args:
        table: Target table name (e.g. "ACC_RFIS")
        columns: List of column names
        key_col: Primary key column for matching

    Returns:
        The MERGE SQL string.
    """
    # YOUR CODE HERE
    return ""


def dataframe_to_oracle_rows(df: pd.DataFrame) -> list[dict]:
    """
    TODO: Convert a DataFrame to a list of dicts for Oracle executemany.

    Replace NaN/NaT values with None (Oracle doesn't understand NaN).

    Args:
        df: The DataFrame.

    Returns:
        List of row dictionaries.
    """
    # YOUR CODE HERE
    return []


# =============================================================================
# EXERCISE 5E: Tie It All Together
# =============================================================================

def run_full_pipeline() -> dict:
    """
    TODO: Execute the full ETL pipeline using mock data.

    Steps:
    1. Extract: Get raw data from mock responses
    2. Transform: Convert to DataFrames with all derived columns
    3. Load: Create Parquet buffers and MERGE SQL statements

    Returns:
        dict with:
            "rfi_count": number of RFI rows
            "submittal_count": number of submittal rows
            "rfi_columns": list of RFI DataFrame columns
            "submittal_columns": list of Submittal DataFrame columns
            "rfi_parquet_size": size of RFI Parquet buffer in bytes
            "submittal_parquet_size": size of Submittal Parquet buffer
            "rfi_merge_sql": the MERGE SQL for RFIs (first 50 chars)
            "submittal_merge_sql": the MERGE SQL for submittals (first 50 chars)
    """
    # YOUR CODE HERE
    return {}


# =============================================================================
# SOLUTIONS
# =============================================================================
#
#
#
#
#
#
#
#
#
#

def solution_5a(auth_response, rfis_response, submittals_response):
    return {
        "access_token": auth_response["access_token"],
        "raw_rfis": rfis_response["results"],
        "raw_submittals": submittals_response["results"],
        "extract_timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }


def solution_5b_rfis(raw_rfis):
    if not raw_rfis:
        return pd.DataFrame()

    now = datetime.now(timezone.utc)
    data_date = now.strftime("%Y-%m-%d")
    dd = pd.to_datetime(data_date, utc=True)

    df = pd.json_normalize(raw_rfis, sep=".")
    col_map = {
        "id": "IID", "title": "TITLE", "rfiType": "RFI_TYPE",
        "assignedTo": "PERSON_RESPONSIBLE", "question": "QUESTION",
        "suggestedAnswer": "SUGGESTED_ANSWER", "location.name": "LOCATION",
        "location.details": "LOCATION_DETAILS", "status": "STATUS",
        "officialResponse": "OFFICIAL_RESPONSE", "dueDate": "DUE_DATE",
        "costImpact": "COST_IMPACT", "scheduleImpact": "SCHEDULE_IMPACT",
        "priority": "PRIORITY", "discipline": "DISCIPLINE",
        "category": "CATEGORY", "createdBy": "CREATED_BY",
        "createdAt": "CREATED_ON", "updatedAt": "UPDATED_ON",
        "closedAt": "CLOSED_ON", "companyName": "COMPANY_NAME",
        "projectId": "PROJECT_ID", "projectName": "PROJECT_NAME",
    }
    out = pd.DataFrame()
    for api_f, tgt in col_map.items():
        out[tgt] = df[api_f] if api_f in df.columns else None

    # Date columns
    out["DUE_DATE"] = pd.to_datetime(out["DUE_DATE"], utc=True)
    out["CREATED_ON"] = pd.to_datetime(out["CREATED_ON"], utc=True)
    out["UPDATED_ON"] = pd.to_datetime(out["UPDATED_ON"], utc=True)
    out["CLOSED_ON"] = pd.to_datetime(out["CLOSED_ON"], utc=True)

    # Derived
    open_statuses = ["Open", "Open Answered", "Open In review"]
    is_open = out["STATUS"].isin(open_statuses)
    out["OPEN_RFI_STATUS"] = np.where(
        is_open & (out["DUE_DATE"] < now), "Open/Late",
        np.where(is_open & (out["DUE_DATE"] >= now), "Open", None)
    )
    out["DUE_DATA_DATE_DELTA"] = (out["DUE_DATE"].dt.normalize() - dd.normalize()).dt.days
    out["RFI_BY_REQUIRED_DATE_STATUS"] = np.where(
        out["DUE_DATA_DATE_DELTA"] < 0, "Overdue",
        np.where(out["DUE_DATA_DATE_DELTA"] <= 6, "Due <1 week",
                 np.where(out["DUE_DATA_DATE_DELTA"] > 6, "Due >1 week", None))
    )
    out["RFI_BY_COST_SCHEDULE_STATUS"] = np.where(
        (out["COST_IMPACT"] == "Yes") & (out["SCHEDULE_IMPACT"] == "Yes"), "Both",
        np.where(out["COST_IMPACT"] == "Yes", "Cost Only",
                 np.where(out["SCHEDULE_IMPACT"] == "Yes", "Schedule Only", None))
    )
    out["RFI_BY_CATEGORY_STATUS"] = np.where(
        out["CATEGORY"].str.contains(",", na=False), "Multiple",
        np.where(out["CATEGORY"].isnull() | (out["CATEGORY"] == ""), "Not Defined", out["CATEGORY"])
    )
    is_closed = out["STATUS"] == "Closed"
    out["AGEING_OPEN"] = np.where(
        ~is_closed, (dd.normalize() - out["CREATED_ON"].dt.normalize()).dt.days, None
    )
    out["AGEING_CLOSED"] = np.where(
        is_closed, (out["CLOSED_ON"].dt.normalize() - out["CREATED_ON"].dt.normalize()).dt.days, None
    )
    out["DATA_DATE"] = data_date
    out["LAST_MODIFIED_DATE"] = now.strftime("%Y-%m-%d %H:%M:%S")
    return out


def solution_5b_submittals(raw_submittals):
    if not raw_submittals:
        return pd.DataFrame()

    now = datetime.now(timezone.utc)
    data_date = now.strftime("%Y-%m-%d")
    dd = pd.to_datetime(data_date, utc=True)

    df = pd.json_normalize(raw_submittals, sep=".")
    col_map = {
        "customIdentifier": "NUMBER_ID", "id": "ID", "specSection": "SPEC",
        "title": "TITLE", "itemType": "TYPE", "status": "STATUS",
        "priority": "PRIORITY", "assignedTo": "PERSON_RESPONSIBLE",
        "pendingActionFrom": "PENDING_ACTION_FROM", "revisionNumber": "REVISION",
        "responsibleContractor": "RESPONSIBLE_CONTRACTOR", "manager": "MANAGER",
        "createdAt": "CREATED_AT", "updatedAt": "UPDATED_AT",
        "requiredApprovalDate": "REQUIRED_APPROVAL_DATE",
        "leadTimeDays": "LEAD_TIME_DAYS", "companyName": "COMPANY_NAME",
        "projectId": "PROJECT_ID", "projectName": "PROJECT_NAME",
    }
    out = pd.DataFrame()
    for api_f, tgt in col_map.items():
        out[tgt] = df[api_f] if api_f in df.columns else None

    out["CREATED_AT"] = pd.to_datetime(out["CREATED_AT"], utc=True)
    out["UPDATED_AT"] = pd.to_datetime(out["UPDATED_AT"], utc=True)
    out["REQUIRED_APPROVAL_DATE"] = pd.to_datetime(out["REQUIRED_APPROVAL_DATE"], utc=True)

    open_statuses = ["pending", "submitted", "in_review"]
    is_open = out["STATUS"].str.lower().isin(open_statuses)
    out["OPEN_SUBMITTALS_STATUS"] = np.where(is_open, "Open", "Closed")
    out["APPROVAL_DATA_DATE_DELTA"] = (
        out["REQUIRED_APPROVAL_DATE"].dt.normalize() - dd.normalize()
    ).dt.days
    out["SUBMITTALS_BY_APPROVAL_DATE_STATUS"] = np.select(
        [out["REQUIRED_APPROVAL_DATE"].isna(),
         out["APPROVAL_DATA_DATE_DELTA"] < 0,
         out["APPROVAL_DATA_DATE_DELTA"] >= 0],
        ["No Approval Date", "Overdue", "On Track"], default="Unknown"
    )
    out["OPEN_SUBMITTALS_IN_COURT_STATUS"] = np.where(
        is_open, out["PENDING_ACTION_FROM"].fillna("Unknown"), "Closed"
    )
    out["AGEING_OPEN"] = np.where(
        is_open, (dd.normalize() - out["CREATED_AT"].dt.normalize()).dt.days, None
    )
    out["AGEING_CLOSED"] = np.where(
        ~is_open, (out["UPDATED_AT"].dt.normalize() - out["CREATED_AT"].dt.normalize()).dt.days, None
    )
    out["DATA_DATE"] = data_date
    out["LAST_MODIFIED_DATE"] = now.strftime("%Y-%m-%d %H:%M:%S")
    return out


def solution_5c_parquet(df):
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    return buf


def solution_5c_path(data_type, date=None):
    if date is None:
        date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"{data_type}/{date}/{data_type}.parquet"


def solution_5d_merge(table, columns, key_col):
    update_set = ", ".join(f"tgt.{c} = src.{c}" for c in columns if c != key_col)
    tgt_cols = ", ".join(columns)
    src_cols = ", ".join(f"src.{c}" for c in columns)
    select_binds = ", ".join(f":{c} AS {c}" for c in columns)
    return f"""MERGE INTO {table} tgt
USING (SELECT {select_binds} FROM dual) src
ON (tgt.{key_col} = src.{key_col})
WHEN MATCHED THEN UPDATE SET {update_set}
WHEN NOT MATCHED THEN INSERT ({tgt_cols}) VALUES ({src_cols})"""


def solution_5d_rows(df):
    return df.where(pd.notna(df), None).to_dict("records")


def solution_5e():
    # Extract
    extracted = solution_5a(AUTH_TOKEN_RESPONSE, RFIS_SEARCH_RESPONSE, SUBMITTALS_ITEMS_RESPONSE)

    # Transform
    df_rfis = solution_5b_rfis(extracted["raw_rfis"])
    df_subs = solution_5b_submittals(extracted["raw_submittals"])

    # Load
    rfi_buf = solution_5c_parquet(df_rfis)
    sub_buf = solution_5c_parquet(df_subs)
    rfi_sql = solution_5d_merge("ACC_RFIS", list(df_rfis.columns), "IID")
    sub_sql = solution_5d_merge("ACC_SUBMITTALS", list(df_subs.columns), "ID")

    return {
        "rfi_count": len(df_rfis),
        "submittal_count": len(df_subs),
        "rfi_columns": list(df_rfis.columns),
        "submittal_columns": list(df_subs.columns),
        "rfi_parquet_size": len(rfi_buf.getvalue()),
        "submittal_parquet_size": len(sub_buf.getvalue()),
        "rfi_merge_sql": rfi_sql[:50],
        "submittal_merge_sql": sub_sql[:50],
    }


# =============================================================================
# RUN TO CHECK
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("Exercise 5: Full ETL Pipeline")
    print("=" * 60)

    # 5A
    extracted = extract_all_data(AUTH_TOKEN_RESPONSE, RFIS_SEARCH_RESPONSE, SUBMITTALS_ITEMS_RESPONSE)
    if extracted.get("access_token") and len(extracted.get("raw_rfis", [])) == 5:
        print("[PASS] 5A: Extract step correct")
    else:
        print("[FAIL] 5A: Expected access_token and 5 raw RFIs")

    # 5B
    df_rfis = transform_rfis(RFIS_SEARCH_RESPONSE["results"])
    if not df_rfis.empty and "AGEING_OPEN" in df_rfis.columns:
        print("[PASS] 5B: RFI transform complete with derived columns")
    else:
        print("[FAIL] 5B: Expected DataFrame with AGEING_OPEN column")

    df_subs = transform_submittals(SUBMITTALS_ITEMS_RESPONSE["results"])
    if not df_subs.empty and "OPEN_SUBMITTALS_STATUS" in df_subs.columns:
        print("[PASS] 5B: Submittal transform complete")
    else:
        print("[FAIL] 5B: Expected DataFrame with OPEN_SUBMITTALS_STATUS")

    # 5C
    if not df_rfis.empty:
        buf = save_to_parquet_buffer(df_rfis)
        if buf.getvalue():
            print("[PASS] 5C: Parquet buffer created (%d bytes)" % len(buf.getvalue()))
        else:
            print("[FAIL] 5C: Parquet buffer is empty")

    path = build_minio_path("rfis")
    if "rfis/" in path and ".parquet" in path:
        print("[PASS] 5C: MinIO path correct")
    else:
        print("[FAIL] 5C: Path should be like rfis/2025-04-04/rfis.parquet")

    # 5D
    sql = build_merge_sql("ACC_RFIS", ["IID", "TITLE", "STATUS"], "IID")
    if "MERGE INTO ACC_RFIS" in sql:
        print("[PASS] 5D: MERGE SQL generated")
    else:
        print("[FAIL] 5D: SQL should contain 'MERGE INTO ACC_RFIS'")

    # 5E
    result = run_full_pipeline()
    if result.get("rfi_count", 0) > 0 and result.get("submittal_count", 0) > 0:
        print("[PASS] 5E: Full pipeline executed!")
        print(f"  RFIs: {result['rfi_count']} rows, {len(result.get('rfi_columns', []))} columns")
        print(f"  Submittals: {result['submittal_count']} rows")
    else:
        print("[FAIL] 5E: Pipeline should return counts > 0")

    print()
    print("=" * 60)
    print("Congratulations! You've completed all ACC API exercises!")
    print("=" * 60)
    print()
    print("Next steps:")
    print("  1. Get your ACC API credentials set up")
    print("  2. Run: python scripts/get_refresh_token.py")
    print("  3. Fill in .env with your credentials")
    print("  4. Run: cd docker && docker-compose up -d")
    print("  5. Open Airflow at http://localhost:8080")
    print("  6. Enable the acc_build_etl DAG")
