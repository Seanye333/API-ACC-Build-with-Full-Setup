"""Autodesk Platform Services OAuth 2.0 authentication (three-legged).

Uses a refresh token for automated ETL access. The refresh token is obtained
once via the one-time setup script (scripts/get_refresh_token.py), then stored
in .env. This module uses it to get fresh access tokens automatically.
"""

import time
import logging
import requests
from config.settings import (
    APS_CLIENT_ID,
    APS_CLIENT_SECRET,
    APS_AUTH_URL,
    APS_REFRESH_TOKEN,
    APS_CALLBACK_URL,
)

logger = logging.getLogger(__name__)

_token_cache = {"access_token": None, "refresh_token": None, "expires_at": 0}


def _refresh_access_token() -> dict:
    """Exchange the refresh token for a new access token + refresh token."""
    refresh_token = _token_cache["refresh_token"] or APS_REFRESH_TOKEN

    if not refresh_token:
        raise ValueError(
            "No refresh token available. Run 'python scripts/get_refresh_token.py' "
            "to perform the one-time 3-legged OAuth setup."
        )

    logger.info("Refreshing APS access token (3-legged OAuth)")
    resp = requests.post(
        APS_AUTH_URL,
        data={
            "grant_type": "refresh_token",
            "client_id": APS_CLIENT_ID,
            "client_secret": APS_CLIENT_SECRET,
            "refresh_token": refresh_token,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def get_access_token() -> str:
    """Return a valid APS access token, refreshing if expired."""
    now = time.time()
    if _token_cache["access_token"] and now < _token_cache["expires_at"] - 60:
        return _token_cache["access_token"]

    payload = _refresh_access_token()

    _token_cache["access_token"] = payload["access_token"]
    _token_cache["expires_at"] = now + payload["expires_in"]

    # APS returns a new refresh token each time — cache it for next refresh
    if "refresh_token" in payload:
        _token_cache["refresh_token"] = payload["refresh_token"]
        logger.info(
            "New refresh token received — update .env if running locally. "
            "Token: %s...", payload["refresh_token"][:20]
        )

    logger.info("Access token obtained, expires in %ds", payload["expires_in"])
    return _token_cache["access_token"]


def get_authorize_url() -> str:
    """Return the URL to start the 3-legged OAuth flow (used by setup script)."""
    return (
        f"https://developer.api.autodesk.com/authentication/v2/authorize"
        f"?response_type=code"
        f"&client_id={APS_CLIENT_ID}"
        f"&redirect_uri={APS_CALLBACK_URL}"
        f"&scope=data:read"
    )


def exchange_code_for_tokens(auth_code: str) -> dict:
    """Exchange an authorization code for access + refresh tokens (used by setup script)."""
    resp = requests.post(
        APS_AUTH_URL,
        data={
            "grant_type": "authorization_code",
            "code": auth_code,
            "client_id": APS_CLIENT_ID,
            "client_secret": APS_CLIENT_SECRET,
            "redirect_uri": APS_CALLBACK_URL,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def auth_headers() -> dict:
    """Return Authorization headers for ACC API calls."""
    return {"Authorization": f"Bearer {get_access_token()}"}
