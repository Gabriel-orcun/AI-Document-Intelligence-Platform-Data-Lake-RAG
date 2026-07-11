"""Stats route: object counts and vector counts across the data lake."""

from fastapi import APIRouter

from api.clients import get_qdrant_client, get_s3_client
from api.config import CURATED_BUCKET, RAW_BUCKET, STAGING_BUCKET, VECTOR_COLLECTIONS

router = APIRouter(tags=["stats"])


def bucket_stats(s3, bucket):
    """Count objects and total size in a bucket.

    Args: s3 client, bucket name.
    Returns: dict with object count, total size, and status.
    """
    paginator = s3.get_paginator("list_objects_v2")

    count = 0
    total_size = 0

    try:
        for page in paginator.paginate(Bucket=bucket):
            for obj in page.get("Contents", []):
                count += 1
                total_size += obj["Size"]
    except Exception:
        return {"object_count": None, "total_size_bytes": None, "status": "unreachable"}

    return {"object_count": count, "total_size_bytes": total_size, "status": "ok"}


@router.get("/stats")
def stats():
    """Report volume metrics for every bucket and vector collection.

    Args: none.
    Returns: dict with per-bucket stats and per-collection point counts.
    """
    s3 = get_s3_client()

    buckets = {
        "raw": bucket_stats(s3, RAW_BUCKET),
        "staging": bucket_stats(s3, STAGING_BUCKET),
        "curated": bucket_stats(s3, CURATED_BUCKET),
    }

    vector_store = {}

    try:
        qdrant = get_qdrant_client()

        for collection in VECTOR_COLLECTIONS:
            if qdrant.collection_exists(collection):
                info = qdrant.get_collection(collection)
                vector_store[collection] = {"points_count": info.points_count}
            else:
                vector_store[collection] = {"points_count": 0}
    except Exception as exc:
        vector_store["error"] = str(exc)

    return {"buckets": buckets, "vector_store": vector_store}
