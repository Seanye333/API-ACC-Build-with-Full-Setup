"""Tests for etl.transform module."""

from etl.transform import transform_rfis, transform_submittals


# ---------------------------------------------------------------------------
# RFI transform tests
# ---------------------------------------------------------------------------

def test_transform_rfis_has_required_columns():
    """Transformed RFIs should contain all expected columns."""
    raw = [
        {
            "id": "rfi-1",
            "title": "Test RFI",
            "rfiType": "Design",
            "assignedTo": "user1",
            "question": "What size?",
            "suggestedAnswer": "12 inches",
            "status": "open",
            "dueDate": "2025-06-01T00:00:00Z",
            "costImpact": "Yes",
            "scheduleImpact": "No",
            "priority": "High",
            "discipline": "Structural",
            "category": "Design",
            "createdBy": "admin",
            "createdAt": "2025-01-15T10:00:00Z",
            "updatedAt": "2025-02-01T12:00:00Z",
        }
    ]

    df = transform_rfis(raw)

    expected_cols = [
        "IID", "TITLE", "RFI_TYPE", "PERSON_RESPONSIBLE", "QUESTION",
        "SUGGESTED_ANSWER", "STATUS", "OFFICIAL_RESPONSE", "DUE_DATE",
        "COST_IMPACT", "SCHEDULE_IMPACT", "PRIORITY", "DISCIPLINE",
        "CATEGORY", "CREATED_BY", "CREATED_ON", "UPDATED_ON",
        "OPEN_RFI_STATUS", "DUE_DATA_DATE_DELTA",
        "RFI_BY_REQUIRED_DATE_STATUS", "RFI_BY_COST_SCHEDULE_STATUS",
        "AGEING_OPEN", "DATA_DATE", "LAST_MODIFIED_DATE",
    ]
    for col in expected_cols:
        assert col in df.columns, f"Missing column: {col}"

    assert len(df) == 1
    assert df["IID"].iloc[0] == "rfi-1"
    assert df["OPEN_RFI_STATUS"].iloc[0] == "Open"
    assert df["RFI_BY_COST_SCHEDULE_STATUS"].iloc[0] == "Has Impact"


def test_transform_rfis_closed_ageing():
    """Closed RFIs should have AGEING_CLOSED populated."""
    raw = [
        {
            "id": "rfi-2",
            "title": "Closed RFI",
            "status": "closed",
            "createdAt": "2025-01-01T00:00:00Z",
            "closedAt": "2025-01-10T00:00:00Z",
        }
    ]

    df = transform_rfis(raw)

    assert df["OPEN_RFI_STATUS"].iloc[0] == "Closed"
    assert df["AGEING_CLOSED"].iloc[0] == 9


def test_transform_rfis_empty():
    """Empty input returns empty DataFrame."""
    df = transform_rfis([])
    assert df.empty


# ---------------------------------------------------------------------------
# Submittal transform tests
# ---------------------------------------------------------------------------

def test_transform_submittals_has_required_columns():
    """Transformed Submittals should contain all expected columns."""
    raw = [
        {
            "id": "sub-1",
            "customIdentifier": "SUB-001",
            "title": "Shop Drawing A",
            "status": "submitted",
            "itemType": "shop_drawing",
            "priority": "Normal",
            "assignedTo": "reviewer1",
            "createdAt": "2025-03-01T00:00:00Z",
            "updatedAt": "2025-03-10T00:00:00Z",
        }
    ]

    df = transform_submittals(raw)

    expected_cols = [
        "NUMBER_ID", "ID", "TITLE", "STATUS", "TYPE", "PRIORITY",
        "PERSON_RESPONSIBLE", "CREATED_AT", "UPDATED_AT",
        "OPEN_SUBMITTALS_STATUS", "APPROVAL_DATA_DATE_DELTA",
        "AGEING_OPEN", "DATA_DATE", "LAST_MODIFIED_DATE",
    ]
    for col in expected_cols:
        assert col in df.columns, f"Missing column: {col}"

    assert len(df) == 1
    assert df["ID"].iloc[0] == "sub-1"
    assert df["NUMBER_ID"].iloc[0] == "SUB-001"
    assert df["OPEN_SUBMITTALS_STATUS"].iloc[0] == "Open"


def test_transform_submittals_empty():
    """Empty input returns empty DataFrame."""
    df = transform_submittals([])
    assert df.empty
