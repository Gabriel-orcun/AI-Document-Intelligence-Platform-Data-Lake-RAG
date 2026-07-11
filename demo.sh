#!/usr/bin/env bash
# Demo end-to-end : infra -> ingestion -> pipeline -> API.
# A la fin, l'API tourne sur http://localhost:8000 (docs interactives sur /docs).
# Modifiable via variables d'environnement, ex: SEC_LIMIT=50 ./demo.sh
set -euo pipefail

cd "$(dirname "$0")"

SEC_LIMIT="${SEC_LIMIT:-30}"
API_PORT="${API_PORT:-8000}"

echo "=== 1/6 Installation des dependances (uv sync) ==="
uv sync

echo "=== 2/6 Demarrage de LocalStack (S3) + Qdrant ==="
docker compose -f docker/docker-compose.yml up -d

echo "Attente que les services soient prets..."
until curl -sf http://localhost:4566/_localstack/health >/dev/null 2>&1; do sleep 2; done
until curl -sf http://localhost:6333/collections >/dev/null 2>&1; do sleep 2; done
echo "LocalStack + Qdrant prets."

echo "=== 3/6 Ingestion SEC EDGAR (limite: $SEC_LIMIT filings) ==="
uv run python scripts/ingestion/sec_api.py --limit "$SEC_LIMIT"

echo "=== 4/6 Ingestion dataset financier (echantillon prof/data_demo) ==="
uv run python scripts/ingestion/upload.py --source-dir prof/data_demo --output-dir data/financial_split

echo "=== 5/6 raw -> staging -> curated ==="
uv run python scripts/raw_to_staging.py
uv run python scripts/staging_to_curated.py

echo "=== 6/6 Demarrage de l'API sur le port $API_PORT ==="
echo "Une fois lancee : http://localhost:$API_PORT/docs"
echo "(Ctrl+C pour arreter)"
uv run uvicorn api.main:app --port "$API_PORT"
