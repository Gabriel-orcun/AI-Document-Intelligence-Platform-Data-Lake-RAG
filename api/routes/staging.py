from typing import Optional

from fastapi import APIRouter, Query

from api.clients import get_s3_client
from api.config import STAGING_BUCKET
from api.s3_utils import get_object_response, list_objects

router = APIRouter(prefix="/staging", tags=["staging"])


@router.get("")
def list_staging(prefix: str = "", limit: int = Query(50, le=1000), token: Optional[str] = None):
    s3 = get_s3_client()
    return list_objects(s3, STAGING_BUCKET, prefix=prefix, limit=limit, continuation_token=token)


@router.get("/{key:path}")
def get_staging_object(key: str):
    s3 = get_s3_client()
    return get_object_response(s3, STAGING_BUCKET, key)
