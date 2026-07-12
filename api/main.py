"""API Gateway entry point, wires up all routers."""

from fastapi import FastAPI

from api.routes import curated, health, ingest, raw, staging, stats

app = FastAPI(title="AI Document Intelligence - Data Lake API")

app.include_router(health.router)
app.include_router(stats.router)
app.include_router(raw.router)
app.include_router(staging.router)
app.include_router(curated.router)
app.include_router(ingest.router)
