from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from api.clients import get_embedder, get_qdrant_client, get_s3_client
from api.config import CURATED_BUCKET, VECTOR_COLLECTIONS
from api.s3_utils import get_object_response, list_objects

router = APIRouter(prefix="/curated", tags=["curated"])


@router.get("")
def list_curated(prefix: str = "", limit: int = Query(50, le=1000), token: Optional[str] = None):
    s3 = get_s3_client()
    return list_objects(s3, CURATED_BUCKET, prefix=prefix, limit=limit, continuation_token=token)


@router.get("/search")
def search_curated(
    q: str,
    source: Optional[str] = Query(None, description="sec_edgar or financial_documents, omit to search both"),
    top_k: int = Query(5, le=50)
):
    if source is not None and source not in VECTOR_COLLECTIONS:
        raise HTTPException(status_code=400, detail=f"source must be one of {VECTOR_COLLECTIONS}")

    collections = [source] if source else VECTOR_COLLECTIONS

    vector = get_embedder().encode(q).tolist()
    qdrant = get_qdrant_client()

    results = []

    for collection in collections:
        if not qdrant.collection_exists(collection):
            continue

        hits = qdrant.query_points(
            collection_name=collection,
            query=vector,
            limit=top_k
        ).points

        for hit in hits:
            results.append({
                "collection": collection,
                "score": hit.score,
                **hit.payload
            })

    results.sort(key=lambda r: r["score"], reverse=True)

    return {"query": q, "results": results[:top_k]}


@router.get("/{key:path}")
def get_curated_object(key: str):
    s3 = get_s3_client()
    return get_object_response(s3, CURATED_BUCKET, key)
