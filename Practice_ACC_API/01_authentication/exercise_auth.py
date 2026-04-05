"""
=============================================================================
Exercise 1: ACC API Authentication
=============================================================================
Learn how OAuth 2.0 works with Autodesk Platform Services (APS).

Reference: https://aps.autodesk.com/en/docs/oauth/v2/overview/

KEY CONCEPTS:
- 2-Legged OAuth: Server-to-server, no user context (client_credentials)
- 3-Legged OAuth: User-facing, requires login (authorization_code + refresh_token)
- ACC RFI/Submittal APIs require 3-legged OAuth (user context needed)

TOKEN ENDPOINT: POST https://developer.api.autodesk.com/authentication/v2/token
=============================================================================
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mock_data.mock_responses import AUTH_TOKEN_RESPONSE

# =============================================================================
# EXERCISE 1A: Understand the 2-Legged OAuth Flow
# =============================================================================
# The 2-legged flow uses client_credentials grant type.
# Fill in the request body that you would send to the token endpoint.

def build_2legged_request():
    """
    TODO: Return a dictionary representing the POST body for 2-legged auth.

    Required fields:
    - grant_type: "client_credentials"
    - client_id: your app's client ID
    - client_secret: your app's client secret
    - scope: "data:read" (minimum for reading ACC data)
    """
    request_body = {
        # YOUR CODE HERE
        # "grant_type": ???,
        # "client_id": ???,
        # "client_secret": ???,
        # "scope": ???,
    }
    return request_body


# =============================================================================
# EXERCISE 1B: Parse the Token Response
# =============================================================================
# When the auth request succeeds, APS returns a JSON response.
# Parse it to extract what you need.

def parse_token_response(response: dict) -> dict:
    """
    TODO: Extract the access token and expiration from the response.

    Args:
        response: The JSON response from the token endpoint.
                  Example: AUTH_TOKEN_RESPONSE

    Returns:
        dict with keys: "token", "expires_in_seconds", "token_type"
    """
    result = {
        # YOUR CODE HERE
        # "token": ???,
        # "expires_in_seconds": ???,
        # "token_type": ???,
    }
    return result


# =============================================================================
# EXERCISE 1C: Build Authorization Header
# =============================================================================
# After getting a token, you use it in the Authorization header of API requests.

def build_auth_header(access_token: str) -> dict:
    """
    TODO: Return the Authorization header dictionary.

    The format is: {"Authorization": "Bearer <access_token>"}

    Args:
        access_token: The access token string.

    Returns:
        dict with the Authorization header.
    """
    # YOUR CODE HERE
    return {}


# =============================================================================
# EXERCISE 1D: Understand 3-Legged OAuth Flow
# =============================================================================
# For ACC RFI/Submittal APIs, you need 3-legged auth.
# The flow is:
#   1. Redirect user to authorize URL
#   2. User logs in and grants permission
#   3. APS redirects back with an authorization code
#   4. Exchange the code for access_token + refresh_token
#   5. Use refresh_token to get new access_tokens (automated)

def build_authorize_url(client_id: str, callback_url: str, scope: str = "data:read") -> str:
    """
    TODO: Build the authorization URL for 3-legged OAuth.

    Format:
    https://developer.api.autodesk.com/authentication/v2/authorize
        ?response_type=code
        &client_id={client_id}
        &redirect_uri={callback_url}
        &scope={scope}

    Args:
        client_id: Your app's client ID.
        callback_url: The callback URL registered in your app.
        scope: OAuth scope string.

    Returns:
        The full authorization URL as a string.
    """
    # YOUR CODE HERE
    return ""


def build_refresh_token_request(client_id: str, client_secret: str, refresh_token: str) -> dict:
    """
    TODO: Return the POST body for refreshing an access token.

    Required fields:
    - grant_type: "refresh_token"
    - client_id: your client ID
    - client_secret: your client secret
    - refresh_token: the refresh token

    Returns:
        dict representing the POST body.
    """
    # YOUR CODE HERE
    return {}


# =============================================================================
# SOLUTIONS (scroll down after attempting)
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
#
#
#
#
#
#
#

def solution_1a():
    return {
        "grant_type": "client_credentials",
        "client_id": "your_client_id",
        "client_secret": "your_client_secret",
        "scope": "data:read",
    }


def solution_1b(response):
    return {
        "token": response["access_token"],
        "expires_in_seconds": response["expires_in"],
        "token_type": response["token_type"],
    }


def solution_1c(access_token):
    return {"Authorization": f"Bearer {access_token}"}


def solution_1d_url(client_id, callback_url, scope="data:read"):
    return (
        f"https://developer.api.autodesk.com/authentication/v2/authorize"
        f"?response_type=code"
        f"&client_id={client_id}"
        f"&redirect_uri={callback_url}"
        f"&scope={scope}"
    )


def solution_1d_refresh(client_id, client_secret, refresh_token):
    return {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }


# =============================================================================
# RUN THIS FILE TO CHECK YOUR ANSWERS
# =============================================================================
if __name__ == "__main__":
    print("=" * 60)
    print("Exercise 1: ACC API Authentication")
    print("=" * 60)

    # Test 1A
    req = build_2legged_request()
    if req.get("grant_type") == "client_credentials" and "scope" in req:
        print("[PASS] 1A: 2-Legged request body correct")
    else:
        print("[FAIL] 1A: Check grant_type and scope")
        print(f"  Expected: grant_type='client_credentials', scope='data:read'")

    # Test 1B
    parsed = parse_token_response(AUTH_TOKEN_RESPONSE)
    if parsed.get("token") == AUTH_TOKEN_RESPONSE["access_token"]:
        print("[PASS] 1B: Token response parsed correctly")
    else:
        print("[FAIL] 1B: Could not extract access_token from response")

    # Test 1C
    header = build_auth_header("test_token_123")
    if header.get("Authorization") == "Bearer test_token_123":
        print("[PASS] 1C: Auth header built correctly")
    else:
        print("[FAIL] 1C: Header should be 'Bearer test_token_123'")

    # Test 1D
    url = build_authorize_url("my_client_id", "http://localhost:8090/callback")
    if "response_type=code" in url and "my_client_id" in url:
        print("[PASS] 1D: Authorize URL built correctly")
    else:
        print("[FAIL] 1D: URL should contain response_type=code and client_id")

    refresh_req = build_refresh_token_request("cid", "csecret", "rtoken")
    if refresh_req.get("grant_type") == "refresh_token":
        print("[PASS] 1D: Refresh token request correct")
    else:
        print("[FAIL] 1D: grant_type should be 'refresh_token'")

    print()
    print("Tip: Compare with solution_* functions at the bottom of this file.")
