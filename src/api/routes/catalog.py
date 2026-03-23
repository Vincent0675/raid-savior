from fastapi import APIRouter, HTTPException

from src.api.services.iceberg_service import IcebergService

router = APIRouter()
iceberg_service = IcebergService()


@router.get("/namespaces")
async def list_namespaces():
    try:
        return {"namespaces": iceberg_service.list_namespaces()}
    except Exception as exc:  # pragma: no cover - error handling
        raise HTTPException(status_code=503, detail=f"Catalog unavailable: {exc}") from exc


@router.get("/tables")
async def list_tables():
    try:
        return {"tables": iceberg_service.list_catalog_tables()}
    except Exception as exc:  # pragma: no cover - error handling
        raise HTTPException(status_code=503, detail=f"Catalog unavailable: {exc}") from exc
