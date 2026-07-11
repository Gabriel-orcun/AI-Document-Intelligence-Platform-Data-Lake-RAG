import os
import sys
from datetime import datetime

REPO_ROOT = "/opt/airflow/repo"

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from airflow.decorators import dag, task

# Static Kaggle image dataset: no new data ever appears at the source, so this
# DAG is manually triggered once (schedule=None) rather than run on a schedule,
# unlike sec_edgar_pipeline which pulls from a live API.
# FINANCIAL_CURATED_LIMIT caps images processed per split, for a fast demo run
# (see README "quick start"); unset it to process the full dataset.
_raw_limit = os.environ.get("FINANCIAL_CURATED_LIMIT")
CURATED_LIMIT = int(_raw_limit) if _raw_limit else None


def s3_client():
    import boto3
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])


@dag(
    dag_id="financial_bootstrap_pipeline",
    description="One-shot split+upload of the Kaggle financial document dataset -> staging -> curated (OCR + RAG index)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ingestion", "financial", "rag"]
)
def financial_bootstrap_pipeline():

    @task
    def split_and_upload():
        from scripts.ingestion.upload import split_dataset, upload_s3

        source_dir = os.environ.get("FINANCIAL_SOURCE_DIR", "/opt/airflow/repo/data/financial/images")
        output_dir = os.environ.get("FINANCIAL_SPLIT_DIR", "/tmp/financial_split")

        split_dataset(source_dir=source_dir, output_dir=output_dir)

        upload_s3(
            local_dir=output_dir,
            bucket=os.environ.get("RAW_BUCKET", "raw"),
            prefix="financial",
            endpoint=os.environ["S3_ENDPOINT_URL"]
        )

        return {"status": "uploaded"}

    @task
    def raw_to_staging(_previous):
        from scripts.raw_to_staging import process_financial

        s3 = s3_client()
        process_financial(s3, os.environ.get("RAW_BUCKET", "raw"), os.environ.get("STAGING_BUCKET", "staging"))

        return {"status": "staged"}

    @task
    def staging_to_curated(_previous):
        from qdrant_client import QdrantClient
        from sentence_transformers import SentenceTransformer
        import easyocr

        from scripts.staging_to_curated import (
            EMBEDDING_MODEL_NAME, ensure_bucket, ensure_collection, process_financial_curated
        )

        s3 = s3_client()
        qdrant = QdrantClient(url=os.environ["QDRANT_URL"])
        curated_bucket = os.environ.get("CURATED_BUCKET", "curated")

        ensure_bucket(s3, curated_bucket)
        ensure_collection(qdrant, "financial_documents")

        embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)
        ocr_reader = easyocr.Reader(["en"], gpu=False)

        metadata = process_financial_curated(
            os.environ["S3_ENDPOINT_URL"],
            os.environ.get("STAGING_BUCKET", "staging"),
            curated_bucket,
            qdrant,
            embedder,
            ocr_reader,
            limit=CURATED_LIMIT,
            workers=3
        )

        return {"curated_count": len(metadata)}

    uploaded = split_and_upload()
    staged = raw_to_staging(uploaded)
    staging_to_curated(staged)


financial_bootstrap_pipeline()
