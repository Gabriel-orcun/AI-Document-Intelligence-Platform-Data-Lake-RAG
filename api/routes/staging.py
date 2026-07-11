"""Routes for browsing the staging zone bucket."""

from typing import Optional

from fastapi import APIRouter, Query

from api.clients import get_s3_client
from api.config import STAGING_BUCKET
from api.s3_utils import get_object_response, list_objects

router = APIRouter(prefix="/staging", tags=["staging"])


@router.get("")
def list_staging(
    prefix: str = "", limit: int = Query(50, le=1000), token: Optional[str] = None
):
    """List staging objects, optionally filtered by prefix.

    Args: key prefix, page size, pagination token.
    Returns: dict with the object list and a token for the next page.
    """
    s3 = get_s3_client()
    return list_objects(
        s3, STAGING_BUCKET, prefix=prefix, limit=limit, continuation_token=token
    )


@router.get("/{key:path}")
def get_staging_object(key: str):
    """Fetch one staging object by key.

    Args: object key.
    Returns: the object bytes, or 404 if missing.
    """
    s3 = get_s3_client()
    return get_object_response(s3, STAGING_BUCKET, key)
