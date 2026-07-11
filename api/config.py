"""Environment-driven configuration shared by all API routes."""

import os

S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
QDRANT_URL = os.getenv("QDRANT_URL", "http://localhost:6333")

RAW_BUCKET = os.getenv("RAW_BUCKET", "raw")
STAGING_BUCKET = os.getenv("STAGING_BUCKET", "staging")
CURATED_BUCKET = os.getenv("CURATED_BUCKET", "curated")

VECTOR_COLLECTIONS = ["sec_edgar", "financial_documents"]
