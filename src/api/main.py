import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.settings import get_settings
from src.api.routes import catalog, health, metrics, raids
from src.api.services.iceberg_service import IcebergService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

iceberg_service = IcebergService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.debug = get_settings().debug
    readiness = iceberg_service.check_readiness()
    if not readiness["ready"]:
        errors = "; ".join(readiness["errors"])
        raise RuntimeError(f"Readiness check fallida en startup: {errors}")
    logger.info("API startup completo; readiness inicial OK")
    yield


app = FastAPI(
    title="Raid Telemetry API",
    version="1.0.0",
    debug=False,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/health", tags=["health"])
app.include_router(catalog.router, prefix="/api/v1/catalog", tags=["catalog"])
app.include_router(raids.router, prefix="/raids", tags=["raids"])
app.include_router(metrics.router, prefix="/metrics", tags=["metrics"])
