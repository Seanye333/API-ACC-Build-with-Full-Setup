"""Centralised configuration loaded from environment variables."""

import os
from dotenv import load_dotenv

load_dotenv()


# --- Autodesk Platform Services (APS) / ACC ---
APS_CLIENT_ID = os.getenv("APS_CLIENT_ID")
APS_CLIENT_SECRET = os.getenv("APS_CLIENT_SECRET")
ACC_ACCOUNT_ID = os.getenv("ACC_ACCOUNT_ID")
ACC_PROJECT_ID = os.getenv("ACC_PROJECT_ID")

APS_REFRESH_TOKEN = os.getenv("APS_REFRESH_TOKEN")
APS_CALLBACK_URL = os.getenv("APS_CALLBACK_URL", "http://localhost:8090/callback")

APS_AUTH_URL = "https://developer.api.autodesk.com/authentication/v2/token"
ACC_BASE_URL = "https://developer.api.autodesk.com"

# --- MinIO ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "acc-etl")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# --- Oracle DB ---
ORACLE_USER = os.getenv("ORACLE_USER")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD")
ORACLE_DSN = os.getenv("ORACLE_DSN")
