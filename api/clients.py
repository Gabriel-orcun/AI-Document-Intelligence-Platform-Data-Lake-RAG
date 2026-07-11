"""Builds and caches the S3, Qdrant, and embedding clients used by the API."""

from functools import lru_cache

import boto3
from qdrant_client import QdrantClient

from api.config import QDRANT_URL, S3_ENDPOINT_URL


def get_s3_client():
    """Create a new S3 client.

    Args: none.
    Returns: boto3 S3 client.
    """
    return boto3.client("s3", endpoint_url=S3_ENDPOINT_URL)


@lru_cache(maxsize=1)
def get_qdrant_client():
    """Create and cache the Qdrant client.

    Args: none.
    Returns: QdrantClient instance, shared across requests.
    """
    return QdrantClient(url=QDRANT_URL)


@lru_cache(maxsize=1)
def get_embedder():
    """Load and cache the sentence embedding model.

    Args: none.
    Returns: SentenceTransformer instance, shared across requests.
    """
    from sentence_transformers import SentenceTransformer

    # Must match the model used in scripts/staging_to_curated.py, otherwise
    # query vectors won't be comparable to the vectors stored in Qdrant.
    return SentenceTransformer("all-MiniLM-L6-v2")
