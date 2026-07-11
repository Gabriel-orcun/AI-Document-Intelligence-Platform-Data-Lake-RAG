"""Daily incremental SEC EDGAR ingestion, staging, and curation DAG."""

import os
import sys
from datetime import datetime, timedelta

REPO_ROOT = "/opt/airflow/repo"

if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from airflow.decorators import dag, task
from airflow.models import Variable

BATCH_SIZE = 10
FILINGS_PER_COMPANY = 1
CURATED_LIMIT_PER_RUN = 40


def s3_client():
    """Create an S3 client from the S3_ENDPOINT_URL env var.

    Args: none.
    Returns: boto3 S3 client.
    """
    import boto3

    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])


@dag(
    dag_id="sec_edgar_pipeline",
    description="Incremental SEC EDGAR ingestion -> staging -> curated (RAG index)",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["ingestion", "sec_edgar", "rag"],
)
def sec_edgar_pipeline():
    """DAG: ingest new SEC filings, stage them, then curate them.

    Args: none.
    Returns: none, defines the task graph.
    """

    @task
    def ingest_sec_edgar():
        """Download the next batch of 10-K filings and upload them to raw.

        Args: none, reads the sec_edgar_cursor Airflow Variable.
        Returns: dict with the new raw keys and the cursor before this run.
        """
        import tempfile

        from scripts.ingestion.sec_api import (
            download_document,
            get_company_filings,
            get_company_list,
            upload_to_s3,
        )

        cursor = int(Variable.get("sec_edgar_cursor", default_var=0))

        companies = get_company_list()
        batch = [
            companies[i % len(companies)] for i in range(cursor, cursor + BATCH_SIZE)
        ]

        raw_keys = []

        with tempfile.TemporaryDirectory() as tmp_dir:
            for company in batch:
                try:
                    filings = get_company_filings(company["cik"])
                except Exception as exc:
                    print(f"skip {company['ticker']}: {exc}")
                    continue

                for filing in filings[:FILINGS_PER_COMPANY]:
                    success, filepath = download_document(
                        company["cik"], company["ticker"], filing, tmp_dir
                    )

                    if success:
                        raw_keys.append(
                            f"sec_edgar/{company['ticker']}/{filing['date']}_10K.html"
                        )

            if raw_keys:
                upload_to_s3(
                    tmp_dir,
                    os.environ.get("RAW_BUCKET", "raw"),
                    "sec_edgar",
                    os.environ["S3_ENDPOINT_URL"],
                )

        Variable.set("sec_edgar_cursor", cursor + BATCH_SIZE)

        return {"raw_keys": raw_keys, "cursor": cursor}

    @task
    def stage_new_filings(ingest_result: dict):
        """Validate and stage exactly the files this run ingested.

        Args: dict returned by ingest_sec_edgar (via XCom).
        Returns: dict with the staged keys.
        """
        from scripts.raw_to_staging import process_sec

        raw_keys = ingest_result["raw_keys"]

        if not raw_keys:
            return {"staged_keys": []}

        s3 = s3_client()
        process_sec(
            s3,
            os.environ.get("RAW_BUCKET", "raw"),
            os.environ.get("STAGING_BUCKET", "staging"),
            keys=raw_keys,
        )

        return {"staged_keys": raw_keys}

    @task
    def curate_new_filings(stage_result: dict):
        """Chunk, embed, and index exactly the files this run staged.

        Args: dict returned by stage_new_filings (via XCom).
        Returns: dict with the number of curated documents.
        """
        from qdrant_client import QdrantClient
        from sentence_transformers import SentenceTransformer

        from scripts.staging_to_curated import (
            EMBEDDING_MODEL_NAME,
            ensure_bucket,
            ensure_collection,
            process_sec_curated,
        )

        staged_keys = stage_result["staged_keys"]

        if not staged_keys:
            return {"curated_count": 0}

        s3 = s3_client()
        qdrant = QdrantClient(url=os.environ["QDRANT_URL"])
        curated_bucket = os.environ.get("CURATED_BUCKET", "curated")

        ensure_bucket(s3, curated_bucket)
        ensure_collection(qdrant, "sec_edgar")

        embedder = SentenceTransformer(EMBEDDING_MODEL_NAME)

        metadata = process_sec_curated(
            os.environ["S3_ENDPOINT_URL"],
            os.environ.get("STAGING_BUCKET", "staging"),
            curated_bucket,
            qdrant,
            embedder,
            keys=staged_keys,
            limit=CURATED_LIMIT_PER_RUN,
            workers=4,
        )

        return {"curated_count": len(metadata)}

    ingested = ingest_sec_edgar()
    staged = stage_new_filings(ingested)
    curate_new_filings(staged)


sec_edgar_pipeline()
