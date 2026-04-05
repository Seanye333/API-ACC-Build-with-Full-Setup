"""
=============================================================================
Exercise 3: ACC RFI API
=============================================================================
Learn how to work with RFIs (Requests for Information) from ACC Build.

Reference: https://aps.autodesk.com/en/docs/acc/v1/overview/field-guide/rfis

API ENDPOINTS:
  POST /construction/rfis/v2/projects/{projectId}/rfis:search  → Search RFIs
  GET  /construction/rfis/v2/projects/{projectId}/rfis/{rfiId}  → Get single RFI

KEY CONCEPTS:
- RFIs use POST for searching (not GET) — send {} to get all
- Pagination uses offset/limit in query params
- Requires 3-legged OAuth token
- Response contains "results" array and "pagination" object
=============================================================================
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import datetime, timezone
from mock_data.mock_responses import RFIS_SEARCH_RESPONSE


# =============================================================================
# EXERCISE 3A: Build the RFI Search URL
# =============================================================================

def build_rfi_search_url(base_url: str, project_id: str) -> str:
    """
    TODO: Build the URL for the RFI search endpoint.

    Format: {base_url}/construction/rfis/v2/projects/{project_id}/rfis:search

    Args:
        base_url: "https://developer.api.autodesk.com"
        project_id: The ACC project ID (without "b." prefix)

    Returns:
        The full RFI search URL.
    """
    # YOUR CODE HERE
    return ""


# =============================================================================
# EXERCISE 3B: Extract RFIs from Response
# =============================================================================

def extract_rfis(response: dict) -> list[dict]:
    """
    TODO: Extract the list of RFI records from the API response.

    The response structure is:
    {
        "pagination": {"limit": 100, "offset": 0, "totalResults": 5},
        "results": [ ... list of RFI objects ... ]
    }

    Args:
        response: The JSON response from POST rfis:search

    Returns:
        List of RFI dictionaries.
    """
    # YOUR CODE HERE
    return []


# =============================================================================
# EXERCISE 3C: Flatten RFIs into a DataFrame
# =============================================================================

def rfis_to_dataframe(rfis: list[dict]) -> pd.DataFrame:
    """
    TODO: Convert RFI records into a flat pandas DataFrame.

    Use pd.json_normalize() to flatten nested fields like "location.name".

    Map these API fields to our column names:
        "id"              → "IID"
        "title"           → "TITLE"
        "rfiType"         → "RFI_TYPE"
        "assignedTo"      → "PERSON_RESPONSIBLE"
        "question"        → "QUESTION"
        "suggestedAnswer"  → "SUGGESTED_ANSWER"
        "location.name"   → "LOCATION"
        "location.details" → "LOCATION_DETAILS"
        "status"          → "STATUS"
        "officialResponse" → "OFFICIAL_RESPONSE"
        "dueDate"         → "DUE_DATE"
        "costImpact"      → "COST_IMPACT"
        "scheduleImpact"  → "SCHEDULE_IMPACT"
        "priority"        → "PRIORITY"
        "discipline"      → "DISCIPLINE"
        "category"        → "CATEGORY"
        "createdBy"       → "CREATED_BY"
        "createdAt"       → "CREATED_ON"
        "updatedAt"       → "UPDATED_ON"
        "closedAt"        → "CLOSED_ON"
        "companyName"     → "COMPANY_NAME"

    Args:
        rfis: List of RFI dictionaries from the API.

    Returns:
        DataFrame with the mapped column names.
    """
    # YOUR CODE HERE
    # Hint: Use pd.json_normalize(rfis, sep=".") to flatten
    # Then rename columns using a dictionary mapping
    return pd.DataFrame()


# =============================================================================
# EXERCISE 3D: Calculate Derived Fields
# =============================================================================

def add_open_rfi_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    TODO: Add the OPEN_RFI_STATUS column.

    Business logic (from the existing RFI module):
    - If status is "Open" or "Open Answered" or "Open In review"
      AND due_date < today → "Open/Late"
    - If status is "Open" or "Open Answered" or "Open In review"
      AND due_date >= today → "Open"
    - Otherwise → None

    Args:
        df: DataFrame with STATUS and DUE_DATE columns.

    Returns:
        DataFrame with OPEN_RFI_STATUS column added.
    """
    # YOUR CODE HERE
    return df


def add_due_date_delta(df: pd.DataFrame, data_date: str = None) -> pd.DataFrame:
    """
    TODO: Add DUE_DATA_DATE_DELTA and RFI_BY_REQUIRED_DATE_STATUS columns.

    DUE_DATA_DATE_DELTA = (DUE_DATE - DATA_DATE) in days

    RFI_BY_REQUIRED_DATE_STATUS:
    - If delta < 0 → "Overdue"
    - If delta <= 6 → "Due <1 week"
    - If delta > 6 → "Due >1 week"

    Args:
        df: DataFrame with DUE_DATE column.
        data_date: Reference date string (defaults to today).

    Returns:
        DataFrame with new columns added.
    """
    # YOUR CODE HERE
    return df


def add_cost_schedule_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    TODO: Add RFI_BY_COST_SCHEDULE_STATUS column.

    Business logic:
    - cost=Yes AND schedule=Yes → "Both"
    - cost=Yes only → "Cost Only"
    - schedule=Yes only → "Schedule Only"
    - Neither → None

    Args:
        df: DataFrame with COST_IMPACT and SCHEDULE_IMPACT columns.

    Returns:
        DataFrame with RFI_BY_COST_SCHEDULE_STATUS added.
    """
    # YOUR CODE HERE
    return df


def add_ageing(df: pd.DataFrame, data_date: str = None) -> pd.DataFrame:
    """
    TODO: Add AGEING_OPEN and AGEING_CLOSED columns.

    AGEING_OPEN: For non-Closed RFIs → (data_date - created_on) in days
    AGEING_CLOSED: For Closed RFIs → (closed_on - created_on) in days

    Args:
        df: DataFrame with STATUS, CREATED_ON, CLOSED_ON columns.
        data_date: Reference date string (defaults to today).

    Returns:
        DataFrame with ageing columns added.
    """
    # YOUR CODE HERE
    return df


def add_category_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    TODO: Add RFI_BY_CATEGORY_STATUS column.

    Business logic:
    - If category contains "," (multiple) → "Multiple"
    - If category is null or empty → "Not Defined"
    - Otherwise → the category value itself

    Args:
        df: DataFrame with CATEGORY column.

    Returns:
        DataFrame with RFI_BY_CATEGORY_STATUS added.
    """
    # YOUR CODE HERE
    return df


# =============================================================================
# EXERCISE 3E: Handle Pagination
# =============================================================================

def needs_next_page(response: dict, page_limit: int = 100) -> bool:
    """
    TODO: Determine if there are more pages to fetch.

    Check if the number of results returned equals the page limit,
    meaning there might be more data.

    Args:
        response: The API response dict.
        page_limit: Number of items per page.

    Returns:
        True if another page should be fetched.
    """
    # YOUR CODE HERE
    return False


def get_next_offset(current_offset: int, page_limit: int = 100) -> int:
    """
    TODO: Calculate the offset for the next page.

    Args:
        current_offset: The current offset value.
        page_limit: Number of items per page.

    Returns:
        The next offset value.
    """
    # YOUR CODE HERE
    return 0


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
#

def solution_3a(base_url, project_id):
    return f"{base_url}/construction/rfis/v2/projects/{project_id}/rfis:search"


def solution_3b(response):
    return response.get("results", [])


def solution_3c(rfis):
    df = pd.json_normalize(rfis, sep=".")
    column_map = {
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
    }
    out = pd.DataFrame()
    for api_field, target_col in column_map.items():
        out[target_col] = df[api_field] if api_field in df.columns else None
    return out


def solution_3d_open_status(df):
    open_statuses = ["Open", "Open Answered", "Open In review"]
    df["DUE_DATE"] = pd.to_datetime(df["DUE_DATE"])
    now = pd.Timestamp.now(tz="UTC")
    df["OPEN_RFI_STATUS"] = np.where(
        df["STATUS"].isin(open_statuses) & (df["DUE_DATE"] < now), "Open/Late",
        np.where(df["STATUS"].isin(open_statuses) & (df["DUE_DATE"] >= now), "Open", None)
    )
    return df


def solution_3d_delta(df, data_date=None):
    if data_date is None:
        data_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df["DUE_DATE"] = pd.to_datetime(df["DUE_DATE"])
    dd = pd.to_datetime(data_date, utc=True)
    df["DUE_DATA_DATE_DELTA"] = (df["DUE_DATE"].dt.normalize() - dd.normalize()).dt.days
    df["RFI_BY_REQUIRED_DATE_STATUS"] = np.where(
        df["DUE_DATA_DATE_DELTA"] < 0, "Overdue",
        np.where(df["DUE_DATA_DATE_DELTA"] <= 6, "Due <1 week",
                 np.where(df["DUE_DATA_DATE_DELTA"] > 6, "Due >1 week", None))
    )
    return df


def solution_3d_cost_schedule(df):
    df["RFI_BY_COST_SCHEDULE_STATUS"] = np.where(
        (df["COST_IMPACT"] == "Yes") & (df["SCHEDULE_IMPACT"] == "Yes"), "Both",
        np.where(df["COST_IMPACT"] == "Yes", "Cost Only",
                 np.where(df["SCHEDULE_IMPACT"] == "Yes", "Schedule Only", None))
    )
    return df


def solution_3d_ageing(df, data_date=None):
    if data_date is None:
        data_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dd = pd.to_datetime(data_date, utc=True)
    df["CREATED_ON"] = pd.to_datetime(df["CREATED_ON"], utc=True)
    df["CLOSED_ON"] = pd.to_datetime(df["CLOSED_ON"], utc=True)
    df["AGEING_OPEN"] = np.where(
        df["STATUS"] != "Closed",
        (dd.normalize() - df["CREATED_ON"].dt.normalize()).dt.days, None
    )
    df["AGEING_CLOSED"] = np.where(
        df["STATUS"] == "Closed",
        (df["CLOSED_ON"].dt.normalize() - df["CREATED_ON"].dt.normalize()).dt.days, None
    )
    return df


def solution_3d_category(df):
    df["RFI_BY_CATEGORY_STATUS"] = np.where(
        df["CATEGORY"].str.contains(",", na=False), "Multiple",
        np.where(df["CATEGORY"].isnull() | (df["CATEGORY"] == ""), "Not Defined",
                 df["CATEGORY"])
    )
    return df


def solution_3e_needs_next(response, page_limit=100):
    return len(response.get("results", [])) >= page_limit


def solution_3e_offset(current_offset, page_limit=100):
    return current_offset + page_limit


# =============================================================================
# RUN TO CHECK
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("Exercise 3: ACC RFI API")
    print("=" * 60)

    # 3A
    url = build_rfi_search_url("https://developer.api.autodesk.com", "project-001")
    if "projects/project-001/rfis:search" in url:
        print("[PASS] 3A: RFI search URL correct")
    else:
        print("[FAIL] 3A: URL should end with projects/project-001/rfis:search")

    # 3B
    rfis = extract_rfis(RFIS_SEARCH_RESPONSE)
    if len(rfis) == 5:
        print("[PASS] 3B: Extracted 5 RFIs")
    else:
        print("[FAIL] 3B: Expected 5 RFIs, got %d" % len(rfis))

    # 3C
    df = rfis_to_dataframe(rfis)
    if not df.empty and "IID" in df.columns and "STATUS" in df.columns:
        print("[PASS] 3C: DataFrame created with correct columns")
    else:
        print("[FAIL] 3C: DataFrame should have IID and STATUS columns")

    # 3D — run on solution DataFrame if student hasn't completed 3C
    if df.empty:
        df = solution_3c(rfis)

    df_test = add_open_rfi_status(df.copy())
    if "OPEN_RFI_STATUS" in df_test.columns:
        print("[PASS] 3D: OPEN_RFI_STATUS column added")
    else:
        print("[FAIL] 3D: OPEN_RFI_STATUS column missing")

    df_test = add_due_date_delta(df.copy())
    if "DUE_DATA_DATE_DELTA" in df_test.columns:
        print("[PASS] 3D: DUE_DATA_DATE_DELTA column added")
    else:
        print("[FAIL] 3D: DUE_DATA_DATE_DELTA column missing")

    df_test = add_cost_schedule_status(df.copy())
    if "RFI_BY_COST_SCHEDULE_STATUS" in df_test.columns:
        print("[PASS] 3D: Cost/schedule status column added")
    else:
        print("[FAIL] 3D: RFI_BY_COST_SCHEDULE_STATUS column missing")

    df_test = add_category_status(df.copy())
    if "RFI_BY_CATEGORY_STATUS" in df_test.columns:
        print("[PASS] 3D: Category status column added")
    else:
        print("[FAIL] 3D: RFI_BY_CATEGORY_STATUS column missing")

    # 3E
    if needs_next_page({"results": [1] * 100}) is True:
        print("[PASS] 3E: Pagination detection correct")
    else:
        print("[FAIL] 3E: Should return True when results == limit")

    if get_next_offset(0, 100) == 100:
        print("[PASS] 3E: Next offset calculated correctly")
    else:
        print("[FAIL] 3E: Next offset should be 100")

    print()
    print("Tip: Solutions are in the solution_* functions. Try before peeking!")
