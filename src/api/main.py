from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.settings import settings
from src.api.routes import health

app = FastAPI(
    title="Raid Telemetry API",
    version="1.0.0",
    debug=settings.debug,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(health.router, prefix="/health", tags=["health"])


@app.on_event("startup")
async def startup():
    # Subfase 8.2: validar conexión MinIO e Iceberg aquí
    pass


@app.on_event("shutdown")
async def shutdown():
    pass
