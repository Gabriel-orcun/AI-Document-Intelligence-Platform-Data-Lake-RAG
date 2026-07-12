"""Advanced-level routes: ad-hoc text ingestion, plain and optimized.

/ingest processes texts one at a time (baseline).
/ingest_fast batches the embedding call, the Qdrant upsert, and threads the
S3 writes -- same pipeline, fewer round-trips.
"""

import hashlib
import json
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi import APIRouter
from pydantic import BaseModel
from qdrant_client.http import models as qmodels

from api.clients import get_embedder, get_qdrant_client, get_s3_client
from api.config import CURATED_BUCKET
from scripts.staging_to_curated import chunk_text, ensure_bucket, ensure_collection

router = APIRouter(tags=["ingest"])

INGEST_COLLECTION = "ingested_documents"


class IngestData(BaseModel):
    texts: list[str]


class IngestRequest(BaseModel):
    data: IngestData


def _point(doc_id, chunk_index, vector, text):
    """Build a Qdrant point for one chunk.

    Args: document id, chunk index, embedding vector, chunk text.
    Returns: qdrant_client PointStruct.
    """
    import uuid

    point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{doc_id}:{chunk_index}"))
    payload = {
        "source": "ingest",
        "document_id": doc_id,
        "chunk_index": chunk_index,
        "text": text,
    }
    return qmodels.PointStruct(id=point_id, vector=vector, payload=payload)


def _write_curated(s3, doc_id, text, chunk_count):
    """Write one ingested document's curated JSON to S3.

    Args: s3 client, document id, full text, chunk count.
    Returns: none.
    """
    s3.put_object(
        Bucket=CURATED_BUCKET,
        Key=f"ingested/{doc_id}.json",
        Body=json.dumps(
            {"document_id": doc_id, "text": text, "chunk_count": chunk_count}, indent=2
        ).encode("utf-8"),
    )


@router.post("/ingest")
def ingest(request: IngestRequest):
    """Chunk, embed, and index each text one at a time.

    Args: JSON body with data.texts (list of strings).
    Returns: dict with processed count and elapsed time in seconds.
    """
    start = time.perf_counter()

    s3 = get_s3_client()
    qdrant = get_qdrant_client()
    embedder = get_embedder()

    ensure_bucket(s3, CURATED_BUCKET)
    ensure_collection(qdrant, INGEST_COLLECTION)

    for text in request.data.texts:
        doc_id = hashlib.sha256(text.encode("utf-8")).hexdigest()
        chunks = chunk_text(text)

        for i, chunk in enumerate(chunks):
            vector = embedder.encode(chunk, show_progress_bar=False).tolist()
            qdrant.upsert(
                collection_name=INGEST_COLLECTION,
                points=[_point(doc_id, i, vector, chunk)],
            )

        _write_curated(s3, doc_id, text, len(chunks))

    elapsed = time.perf_counter() - start

    return {"processed": len(request.data.texts), "elapsed_seconds": round(elapsed, 4)}


@router.post("/ingest_fast")
def ingest_fast(request: IngestRequest):
    """Chunk, batch-embed, and batch-index all texts together.

    Args: JSON body with data.texts (list of strings).
    Returns: dict with processed count and elapsed time in seconds.
    """
    start = time.perf_counter()

    s3 = get_s3_client()
    qdrant = get_qdrant_client()
    embedder = get_embedder()

    ensure_bucket(s3, CURATED_BUCKET)
    ensure_collection(qdrant, INGEST_COLLECTION)

    doc_ids = [
        hashlib.sha256(text.encode("utf-8")).hexdigest() for text in request.data.texts
    ]
    per_doc_chunks = [chunk_text(text) for text in request.data.texts]

    flat_chunks = [chunk for chunks in per_doc_chunks for chunk in chunks]
    flat_owners = [
        (doc_ids[doc_idx], chunk_idx)
        for doc_idx, chunks in enumerate(per_doc_chunks)
        for chunk_idx in range(len(chunks))
    ]

    if flat_chunks:
        vectors = embedder.encode(
            flat_chunks, show_progress_bar=False, batch_size=64
        ).tolist()

        points = [
            _point(doc_id, chunk_idx, vector, chunk)
            for (doc_id, chunk_idx), vector, chunk in zip(
                flat_owners, vectors, flat_chunks
            )
        ]
        qdrant.upsert(collection_name=INGEST_COLLECTION, points=points)

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(
            executor.map(
                lambda args: _write_curated(s3, *args),
                zip(doc_ids, request.data.texts, [len(c) for c in per_doc_chunks]),
            )
        )

    elapsed = time.perf_counter() - start

    return {"processed": len(request.data.texts), "elapsed_seconds": round(elapsed, 4)}
