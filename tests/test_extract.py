"""Tests for etl.extract module."""

from unittest.mock import patch, MagicMock
from etl.extract import extract_rfis, extract_submittals


def _mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.json.return_value = json_data
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    return resp


@patch("etl.extract.auth_headers", return_value={"Authorization": "Bearer test"})
@patch("etl.extract.requests.post")
def test_extract_rfis_single_page(mock_post, mock_auth):
    """Extract RFIs with a single page of results."""
    mock_post.return_value = _mock_response({
        "results": [
            {"id": "rfi-1", "title": "Test RFI 1"},
            {"id": "rfi-2", "title": "Test RFI 2"},
        ]
    })

    result = extract_rfis()

    assert len(result) == 2
    assert result[0]["id"] == "rfi-1"
    mock_post.assert_called_once()


@patch("etl.extract.auth_headers", return_value={"Authorization": "Bearer test"})
@patch("etl.extract.requests.post")
def test_extract_rfis_empty(mock_post, mock_auth):
    """Extract RFIs when no results exist."""
    mock_post.return_value = _mock_response({"results": []})

    result = extract_rfis()

    assert result == []


@patch("etl.extract.auth_headers", return_value={"Authorization": "Bearer test"})
@patch("etl.extract.requests.get")
def test_extract_submittals_single_page(mock_get, mock_auth):
    """Extract Submittals with a single page of results."""
    mock_get.return_value = _mock_response({
        "results": [
            {"id": "sub-1", "title": "Submittal 1"},
        ]
    })

    result = extract_submittals()

    assert len(result) == 1
    assert result[0]["id"] == "sub-1"


@patch("etl.extract.auth_headers", return_value={"Authorization": "Bearer test"})
@patch("etl.extract.requests.post")
def test_extract_rfis_pagination(mock_post, mock_auth):
    """Extract RFIs across multiple pages."""
    page1 = [{"id": f"rfi-{i}"} for i in range(100)]
    page2 = [{"id": "rfi-100"}, {"id": "rfi-101"}]

    mock_post.side_effect = [
        _mock_response({"results": page1}),
        _mock_response({"results": page2}),
    ]

    result = extract_rfis()

    assert len(result) == 102
    assert mock_post.call_count == 2
