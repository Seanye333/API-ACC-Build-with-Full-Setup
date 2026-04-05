"""
=============================================================================
Exercise 2: Data Management API — Hubs & Projects
=============================================================================
Before accessing RFIs/Submittals, you need to discover your Hub (account)
and Project IDs using the Data Management API.

Reference: https://aps.autodesk.com/en/docs/data/v2/overview/

API ENDPOINTS:
  GET /project/v1/hubs                    → List all hubs (accounts)
  GET /project/v1/hubs/{hub_id}/projects  → List projects in a hub

KEY CONCEPT:
  Hub ID format:    "b.{account_id}"
  Project ID format: "b.{project_id}"
  The "b." prefix indicates BIM 360 / ACC.
=============================================================================
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mock_data.mock_responses import HUBS_RESPONSE, PROJECTS_RESPONSE


# =============================================================================
# EXERCISE 2A: Extract Hub Information
# =============================================================================

def get_hub_ids(hubs_response: dict) -> list[dict]:
    """
    TODO: Extract hub ID and name from the hubs API response.

    The response follows JSON:API format:
    {
        "data": [
            {
                "type": "hubs",
                "id": "b.abc123-def456",
                "attributes": {
                    "name": "ACME Construction Company",
                    ...
                }
            }
        ]
    }

    Args:
        hubs_response: The JSON response from GET /project/v1/hubs

    Returns:
        List of dicts: [{"hub_id": "b.abc123", "name": "ACME Construction"}]
    """
    # YOUR CODE HERE
    return []


# =============================================================================
# EXERCISE 2B: Extract Project Information
# =============================================================================

def get_project_list(projects_response: dict) -> list[dict]:
    """
    TODO: Extract project ID and name from the projects API response.

    Args:
        projects_response: The JSON response from GET /project/v1/hubs/{hub_id}/projects

    Returns:
        List of dicts: [{"project_id": "b.project-001", "name": "Highway Bridge"}]
    """
    # YOUR CODE HERE
    return []


# =============================================================================
# EXERCISE 2C: Extract Account ID from Hub ID
# =============================================================================

def hub_id_to_account_id(hub_id: str) -> str:
    """
    TODO: Convert a hub ID to an account ID by removing the "b." prefix.

    In ACC APIs (RFI, Submittal), you use the raw account/project ID
    WITHOUT the "b." prefix.

    Example: "b.abc123-def456" → "abc123-def456"

    Args:
        hub_id: The hub ID with "b." prefix.

    Returns:
        The account ID without prefix.
    """
    # YOUR CODE HERE
    return ""


# =============================================================================
# EXERCISE 2D: Build the Projects URL
# =============================================================================

def build_projects_url(hub_id: str) -> str:
    """
    TODO: Build the full URL to list projects for a given hub.

    Base: https://developer.api.autodesk.com
    Endpoint: /project/v1/hubs/{hub_id}/projects

    Args:
        hub_id: The hub ID (e.g. "b.abc123-def456")

    Returns:
        The full URL string.
    """
    # YOUR CODE HERE
    return ""


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

def solution_2a(hubs_response):
    return [
        {"hub_id": hub["id"], "name": hub["attributes"]["name"]}
        for hub in hubs_response["data"]
    ]


def solution_2b(projects_response):
    return [
        {"project_id": proj["id"], "name": proj["attributes"]["name"]}
        for proj in projects_response["data"]
    ]


def solution_2c(hub_id):
    return hub_id.replace("b.", "", 1)


def solution_2d(hub_id):
    base = "https://developer.api.autodesk.com"
    return f"{base}/project/v1/hubs/{hub_id}/projects"


# =============================================================================
# RUN TO CHECK
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("Exercise 2: Data Management — Hubs & Projects")
    print("=" * 60)

    # 2A
    hubs = get_hub_ids(HUBS_RESPONSE)
    if hubs and hubs[0].get("hub_id") == "b.abc123-def456":
        print("[PASS] 2A: Hub IDs extracted correctly")
    else:
        print("[FAIL] 2A: Expected hub_id='b.abc123-def456'")

    # 2B
    projects = get_project_list(PROJECTS_RESPONSE)
    if len(projects) == 2 and projects[0].get("project_id") == "b.project-001":
        print("[PASS] 2B: Project list extracted correctly")
    else:
        print("[FAIL] 2B: Expected 2 projects, first with id='b.project-001'")

    # 2C
    account_id = hub_id_to_account_id("b.abc123-def456")
    if account_id == "abc123-def456":
        print("[PASS] 2C: Account ID extracted correctly")
    else:
        print("[FAIL] 2C: Expected 'abc123-def456', got '%s'" % account_id)

    # 2D
    url = build_projects_url("b.abc123-def456")
    if "hubs/b.abc123-def456/projects" in url:
        print("[PASS] 2D: Projects URL built correctly")
    else:
        print("[FAIL] 2D: URL should contain 'hubs/b.abc123-def456/projects'")
