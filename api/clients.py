from functools import lru_cache

import boto3
from qdrant_client import QdrantClient

from api.config import QDRANT_URL, S3_ENDPOINT_URL


def get_s3_client():
    return boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


@lru_cache(maxsize=1)
def get_qdrant_client():
    return QdrantClient(url=QDRANT_URL)


@lru_cache(maxsize=1)
def get_embedder():
    from sentence_transformers import SentenceTransformer

    # Must match the model used in scripts/staging_to_curated.py, otherwise
    # query vectors won't be comparable to the vectors stored in Qdrant.
    return SentenceTransformer("all-MiniLM-L6-v2")
