from fastapi import APIRouter, HTTPException

from src.api.services.iceberg_service import IcebergService

router = APIRouter()
iceberg_service = IcebergService()


@router.get("")
async def health():
    return {"status": "ok"}


@router.get("/readiness")
async def readiness():
    result = iceberg_service.check_readiness()
    if not result["ready"]:
        raise HTTPException(status_code=503, detail=result)
    return {"status": "ready", **result}
