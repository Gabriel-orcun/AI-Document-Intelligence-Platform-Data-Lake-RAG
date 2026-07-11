"""Turns staged SEC filings and financial images into chunked, embedded, curated data."""

import argparse
import hashlib
import json
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
from bs4 import BeautifulSoup
from botocore.exceptions import ClientError
from qdrant_client import QdrantClient
from qdrant_client.http import models as qmodels
from tqdm import tqdm

from scripts.s3_metadata import merge_and_write_metadata

CHUNK_SIZE = 1000
CHUNK_OVERLAP = 150
EMBEDDING_MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
DEFAULT_WORKERS = 6

NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")

_thread_local = threading.local()


def get_thread_s3(endpoint_url):
    """Get or create the S3 client for the current thread.

    Args: S3 endpoint URL.
    Returns: boto3 S3 client, one per thread.
    """
    if not hasattr(_thread_local, "s3"):
        _thread_local.s3 = boto3.client("s3", endpoint_url=endpoint_url)
    return _thread_local.s3


def ensure_bucket(s3, bucket):
    """Create the bucket if it doesn't exist yet.

    Args: s3 client, bucket name.
    Returns: none.
    """
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)


def ensure_collection(qdrant, name):
    """Create the Qdrant collection if it doesn't exist yet.

    Args: Qdrant client, collection name.
    Returns: none.
    """
    if not qdrant.collection_exists(name):
        qdrant.create_collection(
            collection_name=name,
            vectors_config=qmodels.VectorParams(
                size=EMBEDDING_DIM, distance=qmodels.Distance.COSINE
            ),
        )


def list_s3_files(s3, bucket, prefix):
    """List every object key under a prefix, metadata.json excluded.

    Args: s3 client, bucket name, key prefix.
    Returns: list of object keys.
    """
    files = []

    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/") and not key.endswith("metadata.json"):
                files.append(key)

    return files


def chunk_text(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP):
    """Split text into overlapping chunks for embedding.

    Args: text, chunk size in characters, overlap in characters.
    Returns: list of text chunks.
    """
    text = text.strip()

    if not text:
        return []

    chunks = []
    start = 0
    text_len = len(text)

    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunk = text[start:end].strip()

        if chunk:
            chunks.append(chunk)

        if end == text_len:
            break

        start = end - overlap

    return chunks


def extract_text_from_html(html_bytes):
    """Strip an HTML filing down to clean plain text.

    Args: raw HTML bytes.
    Returns: plain text string.
    """
    soup = BeautifulSoup(html_bytes, "lxml")

    for tag in soup(["script", "style", "head", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator=" ")
    text = " ".join(text.split())

    return text


def point_id(document_id, chunk_index):
    """Build a deterministic Qdrant point id for a chunk.

    Args: document id, chunk index.
    Returns: uuid string, stable across reruns of the same chunk.
    """
    return str(uuid.uuid5(NAMESPACE, f"{document_id}:{chunk_index}"))


def upsert_chunks(qdrant, collection, embedder, document_id, chunks, base_payload):
    """Embed chunks and upsert them into a Qdrant collection.

    Args: Qdrant client, collection name, embedder, document id, text chunks, shared payload fields.
    Returns: number of points written.
    """
    if not chunks:
        return 0

    vectors = embedder.encode(chunks, show_progress_bar=False).tolist()

    points = []

    for i, (chunk, vector) in enumerate(zip(chunks, vectors)):
        payload = dict(base_payload)
        payload["chunk_index"] = i
        payload["text"] = chunk

        points.append(
            qmodels.PointStruct(
                id=point_id(document_id, i), vector=vector, payload=payload
            )
        )

    qdrant.upsert(collection_name=collection, points=points)

    return len(points)


def _process_sec_file(
    file, endpoint_url, staging_bucket, curated_bucket, qdrant, embedder
):
    """Curate one SEC filing: parse, chunk, embed, write to S3 and Qdrant.

    Args: staging key, S3 endpoint URL, staging bucket, curated bucket, Qdrant client, embedder.
    Returns: metadata dict describing the outcome for this file.
    """
    s3 = get_thread_s3(endpoint_url)

    entry = {"file": file}

    try:
        content = s3.get_object(Bucket=staging_bucket, Key=file)["Body"].read()

        document_id = hashlib.sha256(content).hexdigest()

        path_parts = Path(file).parts
        ticker = path_parts[1] if len(path_parts) > 1 else "UNKNOWN"
        filename = Path(file).stem
        filing_date = filename.split("_")[0] if "_" in filename else None

        text = extract_text_from_html(content)

        if not text:
            entry.update({"document_id": document_id, "status": "empty_text"})
            return entry

        chunks = chunk_text(text)

        base_payload = {
            "source": "sec_edgar",
            "document_id": document_id,
            "ticker": ticker,
            "filing_date": filing_date,
            "file": file,
        }

        n_chunks = upsert_chunks(
            qdrant, "sec_edgar", embedder, document_id, chunks, base_payload
        )

        destination = f"sec_edgar/{ticker}/{filename}.json"

        s3.put_object(
            Bucket=curated_bucket,
            Key=destination,
            Body=json.dumps(
                {
                    "document_id": document_id,
                    "ticker": ticker,
                    "filing_date": filing_date,
                    "source_file": file,
                    "text": text,
                    "chunk_count": n_chunks,
                },
                indent=2,
            ).encode("utf-8"),
        )

        entry.update(
            {
                "document_id": document_id,
                "ticker": ticker,
                "filing_date": filing_date,
                "curated_path": destination,
                "chunk_count": n_chunks,
                "status": "curated",
            }
        )

    except Exception as exc:
        entry.update({"status": "error", "error": str(exc)})

    return entry


def process_sec_curated(
    endpoint_url,
    staging_bucket,
    curated_bucket,
    qdrant,
    embedder,
    limit=None,
    workers=DEFAULT_WORKERS,
    keys=None,
):
    """Curate SEC filings from staging, in parallel across threads.

    Args: S3 endpoint URL, staging bucket, curated bucket, Qdrant client, embedder, max files, thread count, explicit key list.
    Returns: list of metadata entries, one per processed file.
    """
    s3 = get_thread_s3(endpoint_url)

    files = (
        keys if keys is not None else list_s3_files(s3, staging_bucket, "sec_edgar/")
    )

    if limit is not None:
        files = files[:limit]

    metadata = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [
            executor.submit(
                _process_sec_file,
                file,
                endpoint_url,
                staging_bucket,
                curated_bucket,
                qdrant,
                embedder,
            )
            for file in files
        ]

        for future in tqdm(
            as_completed(futures), total=len(futures), desc="SEC EDGAR curated"
        ):
            metadata.append(future.result())

    merge_and_write_metadata(
        s3, curated_bucket, "sec_edgar/metadata.json", metadata, id_field="file"
    )

    return metadata


def _process_financial_file(
    file,
    split,
    endpoint_url,
    staging_bucket,
    curated_bucket,
    qdrant,
    embedder,
    ocr_reader,
):
    """Curate one financial document image: OCR, chunk, embed, write to S3 and Qdrant.

    Args: staging key, split name, S3 endpoint URL, staging bucket, curated bucket, Qdrant client, embedder, OCR reader.
    Returns: metadata dict describing the outcome for this file.
    """
    s3 = get_thread_s3(endpoint_url)

    entry = {"file": file, "split": split}

    try:
        content = s3.get_object(Bucket=staging_bucket, Key=file)["Body"].read()

        document_id = hashlib.sha256(content).hexdigest()

        filename = Path(file).name
        label = filename.split("_")[0]

        ocr_result = ocr_reader.readtext(content, detail=0, paragraph=True)
        text = " ".join(ocr_result).strip()

        if not text:
            entry.update(
                {"document_id": document_id, "label": label, "status": "ocr_empty"}
            )
            return entry

        chunks = chunk_text(text)

        base_payload = {
            "source": "financial",
            "document_id": document_id,
            "label": label,
            "split": split,
            "file": file,
        }

        n_chunks = upsert_chunks(
            qdrant, "financial_documents", embedder, document_id, chunks, base_payload
        )

        destination = f"financial/{split}/{Path(filename).stem}.json"

        s3.put_object(
            Bucket=curated_bucket,
            Key=destination,
            Body=json.dumps(
                {
                    "document_id": document_id,
                    "label": label,
                    "split": split,
                    "source_file": file,
                    "text": text,
                    "chunk_count": n_chunks,
                },
                indent=2,
            ).encode("utf-8"),
        )

        entry.update(
            {
                "document_id": document_id,
                "label": label,
                "curated_path": destination,
                "chunk_count": n_chunks,
                "status": "curated",
            }
        )

    except Exception as exc:
        entry.update({"status": "error", "error": str(exc)})

    return entry


def process_financial_curated(
    endpoint_url,
    staging_bucket,
    curated_bucket,
    qdrant,
    embedder,
    ocr_reader,
    limit=None,
    workers=DEFAULT_WORKERS,
):
    """Curate financial document images from staging, split by split, in parallel.

    Args: S3 endpoint URL, staging bucket, curated bucket, Qdrant client, embedder, OCR reader, max files per split, thread count.
    Returns: list of metadata entries, one per processed file.
    """
    s3 = get_thread_s3(endpoint_url)

    prefixes = [
        "financial/train/images/",
        "financial/validation/images/",
        "financial/test/images/",
    ]

    metadata = []

    for prefix in prefixes:
        split = prefix.split("/")[1]

        files = list_s3_files(s3, staging_bucket, prefix)

        if limit is not None:
            files = files[:limit]

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(
                    _process_financial_file,
                    file,
                    split,
                    endpoint_url,
                    staging_bucket,
                    curated_bucket,
                    qdrant,
                    embedder,
                    ocr_reader,
                )
                for file in files
            ]

            for future in tqdm(
                as_completed(futures),
                total=len(futures),
                desc=f"financial curated ({split})",
            ):
                metadata.append(future.result())

    merge_and_write_metadata(
        s3, curated_bucket, "financial/metadata.json", metadata, id_field="file"
    )

    return metadata


def main():
    """CLI entry point: curate SEC filings and/or financial images.

    Args: none, reads CLI flags.
    Returns: none.
    """
    parser = argparse.ArgumentParser()

    parser.add_argument("--endpoint-url", default="http://localhost:4566")
    parser.add_argument("--qdrant-url", default="http://localhost:6333")
    parser.add_argument("--staging-bucket", default="staging")
    parser.add_argument("--curated-bucket", default="curated")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of files per source, for testing",
    )
    parser.add_argument("--only", choices=["sec", "financial"], default=None)
    parser.add_argument(
        "--sec-workers",
        type=int,
        default=6,
        help="Concurrent threads for SEC (I/O-bound: S3 + HTML parse + embed)",
    )
    parser.add_argument(
        "--financial-workers",
        type=int,
        default=3,
        help="Concurrent threads for financial OCR (CPU-bound: EasyOCR contends beyond ~3 threads)",
    )

    args = parser.parse_args()

    s3 = get_thread_s3(args.endpoint_url)
    qdrant = QdrantClient(url=args.qdrant_url)

    ensure_bucket(s3, args.curated_bucket)
    ensure_collection(qdrant, "sec_edgar")
    ensure_collection(qdrant, "financial_documents")

    from sentence_transformers import SentenceTransformer

    embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

    if args.only != "financial":
        process_sec_curated(
            args.endpoint_url,
            args.staging_bucket,
            args.curated_bucket,
            qdrant,
            embedder,
            limit=args.limit,
            workers=args.sec_workers,
        )

    if args.only != "sec":
        import easyocr

        ocr_reader = easyocr.Reader(["en"], gpu=False)
        process_financial_curated(
            args.endpoint_url,
            args.staging_bucket,
            args.curated_bucket,
            qdrant,
            embedder,
            ocr_reader,
            limit=args.limit,
            workers=args.financial_workers,
        )

    print("STAGING -> CURATED terminé")


if __name__ == "__main__":
    main()
