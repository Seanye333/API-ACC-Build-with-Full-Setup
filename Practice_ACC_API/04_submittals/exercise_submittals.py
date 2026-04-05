"""
=============================================================================
Exercise 4: ACC Submittal API
=============================================================================
Learn how to work with Submittals from ACC Build.

Reference: https://aps.autodesk.com/en/docs/acc/v1/overview/field-guide/submittals

API ENDPOINTS:
  GET /construction/submittals/v2/projects/{projectId}/items  → List items
  GET /construction/submittals/v2/projects/{projectId}/packages → List packages

KEY CONCEPTS:
- Submittals use GET (not POST) for listing
- Pagination uses offset/limit query parameters
- Items have types: shop_drawing, product_data, sample, etc.
- Status flow: pending → submitted → in_review → closed
- Requires 3-legged OAuth token
=============================================================================
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
import numpy as np
from datetime import datetime, timezone
from mock_data.mock_responses import SUBMITTALS_ITEMS_RESPONSE


# =============================================================================
# EXERCISE 4A: Build Submittal URL with Query Parameters
# =============================================================================

def build_submittal_url(base_url: str, project_id: str, limit: int = 100, offset: int = 0) -> str:
    """
    TODO: Build the URL for listing submittal items with pagination params.

    Format: {base_url}/construction/submittals/v2/projects/{project_id}/items?limit={limit}&offset={offset}

    Args:
        base_url: "https://developer.api.autodesk.com"
        project_id: The ACC project ID
        limit: Items per page
        offset: Starting offset

    Returns:
        The full URL with query parameters.
    """
    # YOUR CODE HERE
    return ""


# =============================================================================
# EXERCISE 4B: Extract and Flatten Submittals
# =============================================================================

def submittals_to_dataframe(response: dict) -> pd.DataFrame:
    """
    TODO: Extract submittals from API response and create a DataFrame.

    Map these API fields to our column names:
        "customIdentifier"   → "NUMBER_ID"
        "id"                 → "ID"
        "specSection"        → "SPEC"
        "title"              → "TITLE"
        "itemType"           → "TYPE"
        "status"             → "STATUS"
        "priority"           → "PRIORITY"
        "assignedTo"         → "PERSON_RESPONSIBLE"
        "pendingActionFrom"  → "PENDING_ACTION_FROM"
        "revisionNumber"     → "REVISION"
        "responsibleContractor" → "RESPONSIBLE_CONTRACTOR"
        "manager"            → "MANAGER"
        "createdAt"          → "CREATED_AT"
        "updatedAt"          → "UPDATED_AT"
        "requiredApprovalDate" → "REQUIRED_APPROVAL_DATE"
        "leadTimeDays"       → "LEAD_TIME_DAYS"
        "companyName"        → "COMPANY_NAME"

    Args:
        response: API response dict with "results" key.

    Returns:
        DataFrame with mapped columns.
    """
    # YOUR CODE HERE
    return pd.DataFrame()


# =============================================================================
# EXERCISE 4C: Calculate Derived Fields
# =============================================================================

def add_open_submittal_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    TODO: Add OPEN_SUBMITTALS_STATUS column.

    Logic:
    - If status in ("pending", "submitted", "in_review") → "Open"
    - Otherwise → "Closed"

    Args:
        df: DataFrame with STATUS column.

    Returns:
        DataFrame with OPEN_SUBMITTALS_STATUS added.
    """
    # YOUR CODE HERE
    return df


def add_approval_date_delta(df: pd.DataFrame, data_date: str = None) -> pd.DataFrame:
    """
    TODO: Add APPROVAL_DATA_DATE_DELTA and SUBMITTALS_BY_APPROVAL_DATE_STATUS.

    APPROVAL_DATA_DATE_DELTA = (REQUIRED_APPROVAL_DATE - DATA_DATE) in days

    SUBMITTALS_BY_APPROVAL_DATE_STATUS:
    - If no approval date → "No Approval Date"
    - If delta < 0 → "Overdue"
    - If delta >= 0 → "On Track"

    Args:
        df: DataFrame with REQUIRED_APPROVAL_DATE column.
        data_date: Reference date (defaults to today).

    Returns:
        DataFrame with new columns.
    """
    # YOUR CODE HERE
    return df


def add_in_court_status(df: pd.DataFrame) -> pd.DataFrame:
    """
    TODO: Add OPEN_SUBMITTALS_IN_COURT_STATUS column.

    This shows whose "court" the open submittal is in.

    Logic:
    - If open → use PENDING_ACTION_FROM value (e.g. "Reviewer", "Manager")
    - If closed → "Closed"
    - If PENDING_ACTION_FROM is null → "Unknown"

    Args:
        df: DataFrame with STATUS and PENDING_ACTION_FROM columns.

    Returns:
        DataFrame with court status added.
    """
    # YOUR CODE HERE
    return df


def add_submittal_ageing(df: pd.DataFrame, data_date: str = None) -> pd.DataFrame:
    """
    TODO: Add AGEING_OPEN and AGEING_CLOSED for submittals.

    AGEING_OPEN: For open submittals → (data_date - created_at) in days
    AGEING_CLOSED: For closed submittals → (updated_at - created_at) in days

    Args:
        df: DataFrame with STATUS, CREATED_AT, UPDATED_AT columns.
        data_date: Reference date (defaults to today).

    Returns:
        DataFrame with ageing columns.
    """
    # YOUR CODE HERE
    return df


# =============================================================================
# EXERCISE 4D: Filter and Analyze Submittals
# =============================================================================

def get_submittals_by_type(df: pd.DataFrame) -> dict:
    """
    TODO: Count submittals by TYPE.

    Args:
        df: DataFrame with TYPE column.

    Returns:
        dict like {"shop_drawing": 1, "product_data": 2, "sample": 1}
    """
    # YOUR CODE HERE
    return {}


def get_overdue_submittals(df: pd.DataFrame, data_date: str = None) -> pd.DataFrame:
    """
    TODO: Return only submittals that are open AND past their approval date.

    Args:
        df: DataFrame with STATUS and REQUIRED_APPROVAL_DATE.
        data_date: Reference date.

    Returns:
        Filtered DataFrame.
    """
    # YOUR CODE HERE
    return pd.DataFrame()


def get_submittals_by_responsible(df: pd.DataFrame) -> dict:
    """
    TODO: Count open submittals grouped by PERSON_RESPONSIBLE.

    Only count submittals that are still open.

    Args:
        df: DataFrame with STATUS and PERSON_RESPONSIBLE columns.

    Returns:
        dict like {"Sarah Lee": 1, "Emily Davis": 1}
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
#

def solution_4a(base_url, project_id, limit=100, offset=0):
    return f"{base_url}/construction/submittals/v2/projects/{project_id}/items?limit={limit}&offset={offset}"


def solution_4b(response):
    results = response.get("results", [])
    df = pd.json_normalize(results, sep=".")
    column_map = {
        "customIdentifier": "NUMBER_ID", "id": "ID", "specSection": "SPEC",
        "title": "TITLE", "itemType": "TYPE", "status": "STATUS",
        "priority": "PRIORITY", "assignedTo": "PERSON_RESPONSIBLE",
        "pendingActionFrom": "PENDING_ACTION_FROM", "revisionNumber": "REVISION",
        "responsibleContractor": "RESPONSIBLE_CONTRACTOR", "manager": "MANAGER",
        "createdAt": "CREATED_AT", "updatedAt": "UPDATED_AT",
        "requiredApprovalDate": "REQUIRED_APPROVAL_DATE",
        "leadTimeDays": "LEAD_TIME_DAYS", "companyName": "COMPANY_NAME",
    }
    out = pd.DataFrame()
    for api_field, target in column_map.items():
        out[target] = df[api_field] if api_field in df.columns else None
    return out


def solution_4c_open_status(df):
    open_statuses = ["pending", "submitted", "in_review"]
    df["OPEN_SUBMITTALS_STATUS"] = np.where(
        df["STATUS"].str.lower().isin(open_statuses), "Open", "Closed"
    )
    return df


def solution_4c_approval_delta(df, data_date=None):
    if data_date is None:
        data_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dd = pd.to_datetime(data_date, utc=True)
    df["REQUIRED_APPROVAL_DATE"] = pd.to_datetime(df["REQUIRED_APPROVAL_DATE"], utc=True)
    df["APPROVAL_DATA_DATE_DELTA"] = (
        df["REQUIRED_APPROVAL_DATE"].dt.normalize() - dd.normalize()
    ).dt.days
    df["SUBMITTALS_BY_APPROVAL_DATE_STATUS"] = np.select(
        [df["REQUIRED_APPROVAL_DATE"].isna(),
         df["APPROVAL_DATA_DATE_DELTA"] < 0,
         df["APPROVAL_DATA_DATE_DELTA"] >= 0],
        ["No Approval Date", "Overdue", "On Track"],
        default="Unknown"
    )
    return df


def solution_4c_in_court(df):
    open_statuses = ["pending", "submitted", "in_review"]
    is_open = df["STATUS"].str.lower().isin(open_statuses)
    df["OPEN_SUBMITTALS_IN_COURT_STATUS"] = np.where(
        is_open, df["PENDING_ACTION_FROM"].fillna("Unknown"), "Closed"
    )
    return df


def solution_4c_ageing(df, data_date=None):
    if data_date is None:
        data_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dd = pd.to_datetime(data_date, utc=True)
    df["CREATED_AT"] = pd.to_datetime(df["CREATED_AT"], utc=True)
    df["UPDATED_AT"] = pd.to_datetime(df["UPDATED_AT"], utc=True)
    open_statuses = ["pending", "submitted", "in_review"]
    is_open = df["STATUS"].str.lower().isin(open_statuses)
    df["AGEING_OPEN"] = np.where(
        is_open, (dd.normalize() - df["CREATED_AT"].dt.normalize()).dt.days, None
    )
    df["AGEING_CLOSED"] = np.where(
        ~is_open, (df["UPDATED_AT"].dt.normalize() - df["CREATED_AT"].dt.normalize()).dt.days, None
    )
    return df


def solution_4d_by_type(df):
    return df["TYPE"].value_counts().to_dict()


def solution_4d_overdue(df, data_date=None):
    if data_date is None:
        data_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dd = pd.to_datetime(data_date, utc=True)
    df["REQUIRED_APPROVAL_DATE"] = pd.to_datetime(df["REQUIRED_APPROVAL_DATE"], utc=True)
    open_statuses = ["pending", "submitted", "in_review"]
    is_open = df["STATUS"].str.lower().isin(open_statuses)
    is_overdue = df["REQUIRED_APPROVAL_DATE"] < dd
    return df[is_open & is_overdue].copy()


def solution_4d_by_responsible(df):
    open_statuses = ["pending", "submitted", "in_review"]
    open_df = df[df["STATUS"].str.lower().isin(open_statuses)]
    return open_df["PERSON_RESPONSIBLE"].value_counts().to_dict()


# =============================================================================
# RUN TO CHECK
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("Exercise 4: ACC Submittal API")
    print("=" * 60)

    # 4A
    url = build_submittal_url("https://developer.api.autodesk.com", "proj-001", 50, 0)
    if "projects/proj-001/items?limit=50&offset=0" in url:
        print("[PASS] 4A: Submittal URL correct")
    else:
        print("[FAIL] 4A: URL should contain projects/proj-001/items?limit=50&offset=0")

    # 4B
    df = submittals_to_dataframe(SUBMITTALS_ITEMS_RESPONSE)
    if not df.empty and "NUMBER_ID" in df.columns and len(df) == 4:
        print("[PASS] 4B: DataFrame created with 4 submittals")
    else:
        print("[FAIL] 4B: Expected DataFrame with 4 rows and NUMBER_ID column")
        df = solution_4b(SUBMITTALS_ITEMS_RESPONSE)

    # 4C
    df_test = add_open_submittal_status(df.copy())
    if "OPEN_SUBMITTALS_STATUS" in df_test.columns:
        print("[PASS] 4C: Open status column added")
    else:
        print("[FAIL] 4C: OPEN_SUBMITTALS_STATUS missing")

    df_test = add_approval_date_delta(df.copy())
    if "APPROVAL_DATA_DATE_DELTA" in df_test.columns:
        print("[PASS] 4C: Approval delta column added")
    else:
        print("[FAIL] 4C: APPROVAL_DATA_DATE_DELTA missing")

    df_test = add_in_court_status(df.copy())
    if "OPEN_SUBMITTALS_IN_COURT_STATUS" in df_test.columns:
        print("[PASS] 4C: In-court status column added")
    else:
        print("[FAIL] 4C: OPEN_SUBMITTALS_IN_COURT_STATUS missing")

    # 4D
    type_counts = get_submittals_by_type(df)
    if type_counts.get("product_data") == 2:
        print("[PASS] 4D: Type counts correct")
    else:
        print("[FAIL] 4D: Expected 2 product_data submittals")

    responsible = get_submittals_by_responsible(df)
    if isinstance(responsible, dict) and len(responsible) > 0:
        print("[PASS] 4D: Responsible counts returned")
    else:
        print("[FAIL] 4D: Expected dict of person → count")

    print()
    print("Tip: Solutions are in the solution_* functions. Try before peeking!")
