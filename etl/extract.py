"""Extract RFI and Submittal data from ACC Build API."""

import logging
from typing import Any

import requests
from config.settings import ACC_BASE_URL, ACC_PROJECT_ID
from etl.auth import auth_headers

logger = logging.getLogger(__name__)

PAGE_LIMIT = 100


def _paginated_get(url: str, params: dict | None = None) -> list[dict[str, Any]]:
    """GET with offset/limit pagination. Returns all results."""
    params = params or {}
    params.setdefault("limit", PAGE_LIMIT)
    params.setdefault("offset", 0)

    all_results = []
    while True:
        logger.info("GET %s  offset=%s", url, params["offset"])
        resp = requests.get(
            url, headers=auth_headers(), params=params, timeout=60
        )
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", data.get("data", []))
        if not results:
            break

        all_results.extend(results)

        if len(results) < params["limit"]:
            break
        params["offset"] += params["limit"]

    logger.info("Fetched %d records from %s", len(all_results), url)
    return all_results


def _paginated_post(url: str, body: dict | None = None) -> list[dict[str, Any]]:
    """POST-based search with offset/limit pagination."""
    body = body or {}
    offset = 0

    all_results = []
    while True:
        params = {"limit": PAGE_LIMIT, "offset": offset}
        logger.info("POST %s  offset=%s", url, offset)
        resp = requests.post(
            url,
            headers={**auth_headers(), "Content-Type": "application/json"},
            json=body,
            params=params,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        results = data.get("results", data.get("data", []))
        if not results:
            break

        all_results.extend(results)

        if len(results) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT

    logger.info("Fetched %d records from %s", len(all_results), url)
    return all_results


def extract_rfis() -> list[dict[str, Any]]:
    """Extract all RFIs from the configured ACC project."""
    url = f"{ACC_BASE_URL}/construction/rfis/v2/projects/{ACC_PROJECT_ID}/rfis:search"
    return _paginated_post(url, body={})


def extract_submittals() -> list[dict[str, Any]]:
    """Extract all Submittal items from the configured ACC project."""
    url = f"{ACC_BASE_URL}/construction/submittals/v2/projects/{ACC_PROJECT_ID}/items"
    return _paginated_get(url)
