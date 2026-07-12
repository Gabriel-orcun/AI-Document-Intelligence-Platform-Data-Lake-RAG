# AI Document Intelligence — Data Lake pour RAG financier

## 1. But du projet

Le projet ingère deux sources hétérogènes — un dataset d'images de documents financiers (Kaggle https://www.kaggle.com/datasets/swatigupta555/financial-document-classification)
et les filings 10-K de la SEC (API SEC EDGAR) — les fait transiter par un pipeline
raw → staging → curated, et indexe le contenu curé dans une base vectorielle pour bâtir un
système RAG (Retrieval-Augmented Generation) interrogeable via une API.

```
                    ┌─────────────┐
  Kaggle dataset ──▶│             │
  (images, fichier) │     RAW     │  S3 (LocalStack)
  SEC EDGAR API ────▶│  bucket raw │
                    └──────┬──────┘
                           │ scripts/raw_to_staging.py
                           │ (validation, hashing, statut)
                           ▼
                    ┌─────────────┐
                    │   STAGING   │  S3 (LocalStack)
                    │ bucket      │
                    │ staging     │
                    └──────┬──────┘
                           │ scripts/staging_to_curated.py
                           │ (OCR / parsing HTML, chunking,
                           │  embeddings)
                           ▼
              ┌────────────┴────────────┐
              ▼                         ▼
      ┌───────────────┐        ┌───────────────┐
      │    CURATED     │        │    QDRANT      │
      │ S3 bucket      │        │ vector store   │
      │ curated        │        │ (recherche     │
      │ (JSON texte)   │        │  sémantique)   │
      └───────────────┘        └───────────────┘
                           │
                           ▼
                   ┌───────────────┐
                   │  API Gateway   │  FastAPI
                   │  /raw /staging │
                   │  /curated      │
                   │  /health /stats│
                   │  /ingest       │
                   └───────────────┘
```

**Sources de données**

| Source                                                                                                                    | Type             | Contenu                                                                                                                         |
| ------------------------------------------------------------------------------------------------------------------------- | ---------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Dataset Kaggle de documents [financiers](https://www.kaggle.com/datasets/swatigupta555/financial-document-classification) | Fichier (images) | ~1600 images, 16 classes (invoice, budget, resume, handwritten, ...). Échantillon de 80 images versionné dans `prof/data_demo/` |
| [SEC EDGAR](https://www.sec.gov/edgar)                                                                                    | API              | Filings 10-K (rapports annuels) au format HTML                                                                                  |

## 2. Lancer le projet

Prérequis : Docker Desktop, [uv](https://docs.astral.sh/uv/), Python ≥ 3.10.

```bash
git clone <repo>
cd Projet_Data_lake
uv sync

cd docker && docker compose up -d && cd ..

uv run python scripts/ingestion/sec_api.py --limit 30
uv run python scripts/ingestion/upload.py --source-dir prof/data_demo --output-dir data/financial_split
uv run python scripts/raw_to_staging.py
uv run python scripts/staging_to_curated.py

uv run uvicorn api.main:app --reload --port 8000
```

Ou en une commande : `./demo.sh` (enchaîne toutes les étapes ci-dessus).

API sur `http://localhost:8000`, docs interactives sur `http://localhost:8000/docs`.

Orchestration automatisée (optionnel) : `cd airflow && docker compose up -d --build`, UI sur
`http://localhost:8080`.

## 3. Tester le projet

#### Santé et volumétrie

```bash
curl http://localhost:8000/health
curl http://localhost:8000/stats
```

#### Lister et récupérer un objet par clé (`/raw`, `/staging`, `/curated`)

```bash
curl "http://localhost:8000/raw?prefix=sec_edgar/AAPL/&limit=5"
curl "http://localhost:8000/staging?prefix=financial/train/&limit=5"
curl "http://localhost:8000/curated?prefix=sec_edgar/&limit=5"

curl "http://localhost:8000/raw/sec_edgar/AAPL/2015-10-28_10K.html"
curl "http://localhost:8000/staging/financial/train/images/invoice_0000.jpeg" --output invoice_0000.jpeg
curl "http://localhost:8000/curated/sec_edgar/AAPL/2015-10-28_10K.json"
```

#### `/curated/search` — recherche sémantique (RAG)

```bash
# 1. Question en anglais sur les filings SEC
curl "http://localhost:8000/curated/search?q=cybersecurity+risk+factors&source=sec_edgar&top_k=3"

# 2. Question sur les documents financiers (OCR)
curl "http://localhost:8000/curated/search?q=invoice+amount+due&source=financial_documents&top_k=5"

# 3. Sans "source" : cherche dans les deux collections
curl "http://localhost:8000/curated/search?q=quarterly+revenue+growth&top_k=5"

# 4. Cas d'erreur : source invalide -> 400 attendu
curl -i "http://localhost:8000/curated/search?q=test&source=bidon"
```

#### `/ingest` et `/ingest_fast` — niveau avancé

```bash
# 1. /ingest, un seul texte
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"data":{"texts":["La société annonce une croissance de son chiffre d'"'"'affaires trimestriel."]}}'

# 2. /ingest, plusieurs textes d'un coup
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{"data":{"texts":["Premier document à ingérer.","Deuxième document à ingérer.","Troisième document à ingérer."]}}'

# 3. /ingest_fast, un seul texte (comparer elapsed_seconds avec l'exemple 1)
curl -X POST http://localhost:8000/ingest_fast \
  -H "Content-Type: application/json" \
  -d '{"data":{"texts":["La société annonce une croissance de son chiffre d'"'"'affaires trimestriel."]}}'

# 4. /ingest_fast, plusieurs textes (comparer elapsed_seconds avec l'exemple 2)
curl -X POST http://localhost:8000/ingest_fast \
  -H "Content-Type: application/json" \
  -d '{"data":{"texts":["Premier document à ingérer.","Deuxième document à ingérer.","Troisième document à ingérer."]}}'
```

Benchmark complet (batch de 1 et de 100, plusieurs répétitions) : `uv run python scripts/benchmark_ingest.py`.
