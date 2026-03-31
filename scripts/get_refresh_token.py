"""One-time setup script to obtain a 3-legged OAuth refresh token.

Run this once:
    python scripts/get_refresh_token.py

Steps:
  1. Opens your browser to the Autodesk login page
  2. You log in and authorize the app
  3. Autodesk redirects to http://localhost:8090/callback with an auth code
  4. This script exchanges the code for access + refresh tokens
  5. Prints the refresh token — copy it into your .env file

After this, the ETL pipeline uses the refresh token to get new access tokens
automatically without user interaction.
"""

import sys
import os
import webbrowser
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.auth import get_authorize_url, exchange_code_for_tokens


class CallbackHandler(BaseHTTPRequestHandler):
    """Handle the OAuth callback from Autodesk."""

    auth_code = None

    def do_GET(self):
        query = parse_qs(urlparse(self.path).query)

        if "code" in query:
            CallbackHandler.auth_code = query["code"][0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                b"<h2>Authorization successful!</h2>"
                b"<p>You can close this tab and return to the terminal.</p>"
            )
        else:
            error = query.get("error", ["unknown"])[0]
            self.send_response(400)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(f"<h2>Error: {error}</h2>".encode())

    def log_message(self, format, *args):
        pass  # Suppress default logging


def main():
    port = 8090
    authorize_url = get_authorize_url()

    print("=" * 60)
    print("ACC Build ETL — 3-Legged OAuth Setup")
    print("=" * 60)
    print()
    print("Opening your browser to authorize the application...")
    print(f"If the browser doesn't open, visit:\n{authorize_url}")
    print()
    print(f"Waiting for callback on http://localhost:{port}/callback ...")
    print()

    webbrowser.open(authorize_url)

    server = HTTPServer(("localhost", port), CallbackHandler)
    server.handle_request()  # Handle one request then stop

    if not CallbackHandler.auth_code:
        print("ERROR: No authorization code received.")
        sys.exit(1)

    print("Authorization code received. Exchanging for tokens...")
    tokens = exchange_code_for_tokens(CallbackHandler.auth_code)

    print()
    print("=" * 60)
    print("SUCCESS! Add these to your .env file:")
    print("=" * 60)
    print()
    print(f"APS_REFRESH_TOKEN={tokens['refresh_token']}")
    print()
    print(f"Access token (expires in {tokens['expires_in']}s):")
    print(f"  {tokens['access_token'][:40]}...")
    print()
    print("NOTE: The refresh token is valid until revoked or unused for 90 days.")
    print("The ETL pipeline will automatically refresh it on each run.")


if __name__ == "__main__":
    main()
