"""Transform raw ACC API JSON into clean, flat DataFrames.

Applies explicit column mappings for RFI and Submittal data,
computes derived/calculated fields, and outputs fixed-schema DataFrames.
"""

import logging
from datetime import datetime, timezone
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column mappings: ACC API field → Oracle column name
# Keys = ACC API JSON paths (dot-separated for nested),  Values = target column
# ---------------------------------------------------------------------------

RFI_COLUMN_MAP = {
    "id": "IID",
    "title": "TITLE",
    "rfiType": "RFI_TYPE",
    "assignedTo": "PERSON_RESPONSIBLE",
    "question": "QUESTION",
    "suggestedAnswer": "SUGGESTED_ANSWER",
    "location.name": "LOCATION",
    "location.details": "LOCATION_DETAILS",
    "status": "STATUS",
    "officialResponse": "OFFICIAL_RESPONSE",
    "dueDate": "DUE_DATE",
    "costImpact": "COST_IMPACT",
    "scheduleImpact": "SCHEDULE_IMPACT",
    "priority": "PRIORITY",
    "discipline": "DISCIPLINE",
    "category": "CATEGORY",
    "externalId": "EXTERNAL_ID",
    "createdBy": "CREATED_BY",
    "createdAt": "CREATED_ON",
    "updatedAt": "UPDATED_ON",
    "closedAt": "CLOSED_ON",
    "postedToDrawings": "POSTED_TO_DRAWINGS",
    "companyName": "COMPANY_NAME",
    "projectId": "PROJECT_ID",
    "projectName": "PROJECT_NAME",
}

SUBMITTAL_COLUMN_MAP = {
    "customIdentifier": "NUMBER_ID",
    "id": "ID",
    "specSection": "SPEC",
    "specSubSection": "SPEC_SUB_SECTION",
    "priority": "PRIORITY",
    "assignedTo": "PERSON_RESPONSIBLE",
    "pendingActionFrom": "PENDING_ACTION_FROM",
    "personResponsibleDueDate": "PERSON_RESPONSIBLE_DUE_DATE",
    "status": "STATUS",
    "revisionNumber": "REVISION",
    "title": "TITLE",
    "itemType": "TYPE",
    "packageNumber": "PACKAGE_NUMBER",
    "packageName": "PACKAGE",
    "reviewResponses": "REVIEW_RESPONSES",
    "finalResponse": "FINAL_RESPONSE",
    "responsibleContractor": "RESPONSIBLE_CONTRACTOR",
    "manager": "MANAGER",
    "reviewers": "REVIEWERS",
    "distributionList": "DISTRIBUTION_LIST",
    "sentToSubmitter": "SENT_TO_SUBMITTER",
    "submitterDueDate": "SUBMITTER_DUE_DATE",
    "receivedFromSubmitter": "RECEIVED_FROM_SUBMITTER",
    "submitDaysOff": "SUBMIT_DAYS_OFF",
    "managerDueDate": "MANAGER_DUE_DATE",
    "sentForReview": "SENT_FOR_REVIEW",
    "receivedFromReview": "RECEIVED_FROM_REVIEW",
    "published": "PUBLISHED",
    "requiredDate": "REQUIRED_DATE",
    "requiredApprovalDate": "REQUIRED_APPROVAL_DATE",
    "requiredOnJobSiteDate": "REQUIRED_ON_JOB_SITE_DATE",
    "leadTimeDays": "LEAD_TIME_DAYS",
    "createdAt": "CREATED_AT",
    "createdBy": "CREATED_BY",
    "updatedAt": "UPDATED_AT",
    "updatedBy": "UPDATED_BY",
    "description": "DESCRIPTION",
    "companyName": "COMPANY_NAME",
    "projectId": "PROJECT_ID",
    "projectName": "PROJECT_NAME",
    "specNumber": "SPEC_NUMBER",
}

# Statuses considered "open" for ageing / status calculations
_OPEN_STATUSES = {"open", "submitted", "in_review", "pending", "draft"}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _flatten(records: list[dict[str, Any]]) -> pd.DataFrame:
    """Flatten nested JSON records into a flat DataFrame."""
    return pd.json_normalize(records, sep=".")


def _safe_get(df: pd.DataFrame, api_field: str):
    """Return a Series for an API field, or NaN if the field is missing."""
    if api_field in df.columns:
        return df[api_field]
    return pd.Series(np.nan, index=df.index)


def _map_columns(df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
    """Select and rename columns according to the mapping.

    Missing API fields are filled with NaN.
    """
    out = pd.DataFrame()
    for api_field, target_col in col_map.items():
        out[target_col] = _safe_get(df, api_field)
    return out


def _to_datetime(series: pd.Series) -> pd.Series:
    """Parse a Series to datetime, coercing errors to NaT."""
    return pd.to_datetime(series, errors="coerce", utc=True)


def _days_between(start: pd.Series, end: pd.Series) -> pd.Series:
    """Return whole-day difference between two datetime Series."""
    delta = _to_datetime(end) - _to_datetime(start)
    return delta.dt.days


# ---------------------------------------------------------------------------
# RFI transform
# ---------------------------------------------------------------------------

def _compute_rfi_derived(df: pd.DataFrame) -> pd.DataFrame:
    """Add calculated / derived columns for RFIs."""
    now = datetime.now(timezone.utc)
    data_date = now.strftime("%Y-%m-%d")

    status_lower = df["STATUS"].astype(str).str.lower()
    is_open = status_lower.isin(_OPEN_STATUSES)

    # COMPANY_NAME_UPDATED — placeholder, same as COMPANY_NAME by default
    df["COMPANY_NAME_UPDATED"] = df["COMPANY_NAME"]

    # OPEN_RFI_STATUS — "Open" or "Closed" based on status
    df["OPEN_RFI_STATUS"] = np.where(is_open, "Open", "Closed")

    # DUE_DATA_DATE_DELTA — days between DUE_DATE and DATA_DATE
    df["DUE_DATA_DATE_DELTA"] = _days_between(
        pd.Series(data_date, index=df.index), df["DUE_DATE"]
    )

    # RFI_BY_REQUIRED_DATE_STATUS — overdue / on-track / no-due-date
    due = _to_datetime(df["DUE_DATE"])
    df["RFI_BY_REQUIRED_DATE_STATUS"] = np.select(
        [due.isna(), due < now, due >= now],
        ["No Due Date", "Overdue", "On Track"],
        default="Unknown",
    )

    # RFI_BY_CATEGORY_STATUS — pass-through of CATEGORY (can customise later)
    df["RFI_BY_CATEGORY_STATUS"] = df["CATEGORY"]

    # RFI_BY_COST_SCHEDULE_STATUS — combined cost/schedule impact label
    df["RFI_BY_COST_SCHEDULE_STATUS"] = np.where(
        (df["COST_IMPACT"].astype(str).str.lower() == "yes")
        | (df["SCHEDULE_IMPACT"].astype(str).str.lower() == "yes"),
        "Has Impact",
        "No Impact",
    )

    # AGEING_OPEN — days since creation for open RFIs
    created = _to_datetime(df["CREATED_ON"])
    df["AGEING_OPEN"] = np.where(
        is_open, (pd.Timestamp(now) - created).dt.days, None
    )

    # AGEING_CLOSED — days between creation and closure for closed RFIs
    closed = _to_datetime(df["CLOSED_ON"])
    df["AGEING_CLOSED"] = np.where(
        ~is_open, (closed - created).dt.days, None
    )

    # Metadata
    df["DATA_DATE"] = data_date
    df["LAST_MODIFIED_DATE"] = now.strftime("%Y-%m-%d %H:%M:%S")

    return df


def transform_rfis(raw_rfis: list[dict[str, Any]]) -> pd.DataFrame:
    """Transform raw RFI records into the fixed-schema DataFrame."""
    if not raw_rfis:
        logger.warning("No RFI records to transform")
        return pd.DataFrame()

    flat = _flatten(raw_rfis)
    df = _map_columns(flat, RFI_COLUMN_MAP)
    df = _compute_rfi_derived(df)

    logger.info("Transformed %d RFI records (%d columns)", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# Submittal transform
# ---------------------------------------------------------------------------

def _compute_submittal_derived(df: pd.DataFrame) -> pd.DataFrame:
    """Add calculated / derived columns for Submittals."""
    now = datetime.now(timezone.utc)
    data_date = now.strftime("%Y-%m-%d")

    status_lower = df["STATUS"].astype(str).str.lower()
    is_open = status_lower.isin(_OPEN_STATUSES)

    # APPROVAL_DATA_DATE_DELTA — days between REQUIRED_APPROVAL_DATE and DATA_DATE
    df["APPROVAL_DATA_DATE_DELTA"] = _days_between(
        pd.Series(data_date, index=df.index), df["REQUIRED_APPROVAL_DATE"]
    )

    # OPEN_SUBMITTALS_STATUS — "Open" or "Closed"
    df["OPEN_SUBMITTALS_STATUS"] = np.where(is_open, "Open", "Closed")

    # SUBMITTALS_BY_APPROVAL_DATE_STATUS — overdue / on-track
    approval = _to_datetime(df["REQUIRED_APPROVAL_DATE"])
    df["SUBMITTALS_BY_APPROVAL_DATE_STATUS"] = np.select(
        [approval.isna(), approval < now, approval >= now],
        ["No Approval Date", "Overdue", "On Track"],
        default="Unknown",
    )

    # SENT_FOR_REVIEW_DATA_DATE_DELTA — days between SENT_FOR_REVIEW and DATA_DATE
    df["SENT_FOR_REVIEW_DATA_DATE_DELTA"] = _days_between(
        df["SENT_FOR_REVIEW"], pd.Series(data_date, index=df.index)
    )

    # OPEN_SUBMITTALS_IN_COURT_STATUS — who's court the open submittal is in
    df["OPEN_SUBMITTALS_IN_COURT_STATUS"] = np.where(
        is_open, df["PENDING_ACTION_FROM"].fillna("Unknown"), "Closed"
    )

    # AGEING_OPEN — days since creation for open submittals
    created = _to_datetime(df["CREATED_AT"])
    df["AGEING_OPEN"] = np.where(
        is_open, (pd.Timestamp(now) - created).dt.days, None
    )

    # AGEING_CLOSED — days between creation and last update for closed
    updated = _to_datetime(df["UPDATED_AT"])
    df["AGEING_CLOSED"] = np.where(
        ~is_open, (updated - created).dt.days, None
    )

    # Metadata
    df["DATA_DATE"] = data_date
    df["LAST_MODIFIED_DATE"] = now.strftime("%Y-%m-%d %H:%M:%S")

    return df


def transform_submittals(raw_submittals: list[dict[str, Any]]) -> pd.DataFrame:
    """Transform raw Submittal records into the fixed-schema DataFrame."""
    if not raw_submittals:
        logger.warning("No Submittal records to transform")
        return pd.DataFrame()

    flat = _flatten(raw_submittals)
    df = _map_columns(flat, SUBMITTAL_COLUMN_MAP)
    df = _compute_submittal_derived(df)

    logger.info(
        "Transformed %d Submittal records (%d columns)", len(df), len(df.columns)
    )
    return df
