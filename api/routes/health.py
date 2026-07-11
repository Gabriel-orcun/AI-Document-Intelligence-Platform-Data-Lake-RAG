from fastapi import APIRouter
from botocore.exceptions import ClientError

from api.clients import get_qdrant_client, get_s3_client
from api.config import CURATED_BUCKET, RAW_BUCKET, STAGING_BUCKET

router = APIRouter(tags=["health"])


@router.get("/health")
def health():
    services = {}

    s3 = get_s3_client()

    for name, bucket in [("s3_raw", RAW_BUCKET), ("s3_staging", STAGING_BUCKET), ("s3_curated", CURATED_BUCKET)]:
        try:
            s3.head_bucket(Bucket=bucket)
            services[name] = "up"
        except ClientError:
            services[name] = "down"

    try:
        get_qdrant_client().get_collections()
        services["qdrant"] = "up"
    except Exception:
        services["qdrant"] = "down"

    status = "healthy" if all(state == "up" for state in services.values()) else "degraded"

    return {"status": status, "services": services}
