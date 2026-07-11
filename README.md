# AI Document Intelligence — Data Lake pour RAG financier

Data lake construit pour le projet final Data Lakes & Data Integration (EFREI 2025-2026). Le
projet ingère deux sources hétérogènes — un dataset d'images de documents financiers (Kaggle)
et les filings 10-K de la SEC (API SEC EDGAR) — les fait transiter par un pipeline
raw → staging → curated, et indexe le contenu curé dans une base vectorielle pour bâtir un
système RAG (Retrieval-Augmented Generation) interrogeable via une API.

## Architecture

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
                   └───────────────┘
```

Orchestration : **Apache Airflow** (deux DAGs, voir plus bas) au lieu de DVC — choisi pour le
scheduling natif de l'ingestion API et l'usage de XCom/Variables pour l'incrémentalité.

## Sources de données

| Source | Type | Contenu |
|---|---|---|
| Dataset Kaggle de documents financiers | Dataset fichier (images) | ~1600 images de documents financiers, 16 classes (invoice, budget, resume, memo, form, handwritten, scientific report, ...). Un échantillon de 80 images (5/classe) est versionné dans `prof/data_demo/` pour la démo — pas besoin de télécharger le dataset complet pour tester le projet |
| [SEC EDGAR](https://www.sec.gov/edgar) | API | Filings 10-K (rapports annuels) au format HTML, ~75 entreprises du S&P |

## Zones et choix techniques

### Raw — S3 (LocalStack)
Contrainte imposée par le sujet. Les deux sources y atterrissent brutes :
`financial/{train,validation,test}/images/*.jpeg` et `sec_edgar/{TICKER}/{date}_10K.html`.

### Staging — S3 (LocalStack)
Validation et nettoyage minimal, sans changer la nature du contenu :
- Images : ouverture avec Pillow (dimensions, format, détection de corruption)
- HTML SEC : vérification de non-vacuité et de structure HTML valide

Chaque document reçoit un statut (`ready_for_ocr`, `ready_for_extraction`, `invalid`) et un hash
SHA-256 comme identifiant stable. Le `metadata.json` de chaque source est **fusionné** (pas
écrasé) à chaque exécution, pour supporter des runs incrémentaux répétés (voir Airflow).

### Curated — S3 (LocalStack) + Qdrant
C'est ici que les deux sources hétérogènes convergent vers un format exploitable pour le RAG :
- **SEC (HTML → texte)** : parsing avec BeautifulSoup/lxml (suppression scripts/styles/balises),
  texte nettoyé
- **Financial (image → texte)** : OCR avec **EasyOCR** — choisi plutôt que Tesseract car le
  dataset contient une classe `handwritten` (écriture manuscrite), qu'EasyOCR gère mieux
  grâce à ses modèles de deep learning, sans dépendance à un binaire système
- Les deux flux convergent ensuite : découpage en chunks (~1000 caractères, 150 de
  recouvrement), embeddings avec **sentence-transformers** (`all-MiniLM-L6-v2`, 384 dimensions,
  local et gratuit — pas d'appel API payant)

Double écriture en curated :
- **S3 bucket `curated`** : un JSON par document (texte complet + métadonnées), consommable
  directement par `/curated` — répond à l'exigence du sujet d'un accès simple aux données
  ingérées
- **Qdrant** : les vecteurs de chunks, avec le texte et les métadonnées en payload —
  permet la recherche sémantique via `/curated/search`, le cœur du RAG

**Qdrant** a été choisi plutôt que ChromaDB/FAISS pour la recherche vectorielle : c'est un vrai
service (conteneur Docker séparé, comme LocalStack), ce qui est plus proche d'une architecture
de production et évite de coupler le vector store au processus Python qui l'alimente.

## Pipeline d'intégration — Apache Airflow

Deux DAGs, `airflow/dags/` :

- **`sec_edgar_pipeline`** (scheduled `@daily`) : ingestion incrémentale des filings SEC.
  Une `Airflow Variable` (`sec_edgar_cursor`) retient la position dans la liste des entreprises
  d'un run à l'autre, pour ne traiter que de nouvelles entreprises à chaque déclenchement plutôt
  que de tout retélécharger. Les clés S3 exactement ingérées sont passées via **XCom** aux
  tâches `stage_new_filings` et `curate_new_filings`, qui ne (re)traitent que ces fichiers-là —
  pas tout le bucket.
- **`financial_bootstrap_pipeline`** (déclenchement manuel, `schedule=None`) : split + upload +
  staging + curation du dataset Kaggle. Ce dataset est statique (pas d'API), donc pas de sens à
  le planifier — contrairement à `sec_edgar_pipeline` qui suit explicitement la consigne
  "utilisez le scheduling pour ingérer depuis l'API à intervalle régulier".

Airflow tourne en conteneur unique (`airflow standalone`, SQLite + SequentialExecutor) : suffisant
pour l'échelle d'un projet de TP, pas besoin de la stack complète Postgres/Redis/worker.

## API Gateway (FastAPI)

| Endpoint | Description |
|---|---|
| `GET /health` | État de S3 (raw/staging/curated) et Qdrant |
| `GET /stats` | Nombre d'objets et taille par bucket, nombre de vecteurs par collection Qdrant |
| `GET /raw?prefix=&limit=&token=` | Liste paginée des objets raw |
| `GET /raw/{key}` | Récupère un objet raw précis |
| `GET /staging?prefix=&limit=&token=` | Liste paginée des objets staging |
| `GET /staging/{key}` | Récupère un objet staging précis |
| `GET /curated?prefix=&limit=&token=` | Liste paginée des documents curated |
| `GET /curated/{key}` | Récupère un document curated précis (texte + métadonnées) |
| `GET /curated/search?q=&source=&top_k=` | **Recherche sémantique** (retrieval du RAG) : embed la requête, cherche dans Qdrant, renvoie les passages les plus pertinents. `source` optionnel : `sec_edgar` ou `financial_documents` |

Documentation interactive auto-générée : `http://localhost:8000/docs`.

## Installation

Prérequis : Docker Desktop, [uv](https://docs.astral.sh/uv/), Python ≥ 3.10.

```bash
git clone <repo>
cd Projet_Data_lake
uv sync
```

### 1. Démarrer l'infrastructure (S3 + Qdrant)

```bash
cd docker
docker compose up -d
cd ..
```

### 2. Ingestion + pipeline — quick start (démo rapide, quelques minutes)

Pour ne pas attendre le traitement complet des ~2600 documents (~2h, voir plus bas), utilisez
`--limit` sur chaque étape. Un petit échantillon du dataset financier (80 images, 5 par classe
sur les 16 classes) est versionné dans `prof/data_demo/` pour ne pas dépendre d'un téléchargement
Kaggle à faire pour la démo :

```bash
# SEC EDGAR : télécharge et uploade N filings (source API, aucun fichier à fournir)
uv run python scripts/ingestion/sec_api.py --limit 30

# Dataset financier : split train/val/test + upload (échantillon versionné dans prof/data_demo)
uv run python scripts/ingestion/upload.py --source-dir prof/data_demo --output-dir data/financial_split

# raw -> staging (validation)
uv run python scripts/raw_to_staging.py

# staging -> curated (OCR, parsing, chunking, embeddings)
uv run python scripts/staging_to_curated.py
```

Avec cet échantillon, l'étape `staging_to_curated.py` n'a besoin d'aucun `--limit` : 30 filings
SEC + 80 images se traitent en quelques minutes.

### 2bis. Pipeline complet (tout le dataset, ~2h sur CPU)

Nécessite le dataset Kaggle complet (1600 images, 16 classes) téléchargé localement — voir
section [Sources de données](#sources-de-données) — `prof/data_demo/` n'en est qu'un
échantillon de 80 images.

```bash
uv run python scripts/ingestion/sec_api.py --limit 1000
uv run python scripts/ingestion/upload.py --source-dir data/financial/images --output-dir data/financial_split
uv run python scripts/raw_to_staging.py
uv run python scripts/staging_to_curated.py --sec-workers 6 --financial-workers 3
```

Note sur la parallélisation (voir `scripts/staging_to_curated.py`) : le traitement SEC est
plutôt I/O-bound (S3, Qdrant) et bénéficie de 6 threads concurrents (~1.5x plus rapide qu'en
séquentiel) ; l'OCR EasyOCR est CPU-bound et parallélise déjà en interne (PyTorch) — au-delà de
3 threads, la contention le ralentit plutôt que de l'accélérer. D'où deux réglages séparés
(`--sec-workers`, `--financial-workers`).

### 3. Lancer l'API

```bash
uv run uvicorn api.main:app --reload --port 8000
```

Exemples :
```bash
curl http://localhost:8000/health
curl http://localhost:8000/stats
curl "http://localhost:8000/curated/search?q=quels+sont+les+risques+liés+à+la+chaîne+d%27approvisionnement&source=sec_edgar&top_k=3"
```

### 4. Airflow (orchestration automatisée, optionnel)

```bash
cd airflow
docker compose up -d --build
```

UI sur `http://localhost:8080` (identifiants générés au premier démarrage, voir les logs du
conteneur : `docker compose logs airflow | grep password`). Déclencher
`financial_bootstrap_pipeline` manuellement une fois ; `sec_edgar_pipeline` tourne ensuite
automatiquement chaque jour (ou déclenchement manuel pour tester immédiatement).

## Structure du repo

```
api/                    API Gateway FastAPI
  main.py, config.py, clients.py, s3_utils.py
  routes/                raw, staging, curated, health, stats
scripts/
  ingestion/             sec_api.py (SEC EDGAR), upload.py (Kaggle)
  raw_to_staging.py
  staging_to_curated.py  OCR, parsing HTML, chunking, embeddings, Qdrant
  s3_metadata.py          fusion incrémentale des metadata.json
airflow/
  dags/                  sec_edgar_pipeline.py, financial_bootstrap_pipeline.py
  Dockerfile, docker-compose.yml
docker/
  docker-compose.yml     LocalStack (S3) + Qdrant
prof/
  data_demo/              échantillon versionné (80 images, 5/classe) pour tester sans télécharger Kaggle
data/                    dataset Kaggle complet, si téléchargé (non versionné, voir .gitignore)
```

## Limitations et pistes non traitées

- **Niveau avancé** (`/ingest`, `/ingest_fast`, benchmark de performance) : non implémenté.
- **Génération** : le RAG s'arrête à la récupération (`/curated/search` renvoie les passages
  pertinents) ; aucun appel à un LLM pour générer une réponse en langage naturel à partir des
  passages récupérés n'est branché.
- **Classification des documents financiers** : les labels du dataset Kaggle (16 classes) sont
  conservés en métadonnée mais aucun classifieur n'a été entraîné dessus (piste bonus ML/DL
  mentionnée dans le sujet).
