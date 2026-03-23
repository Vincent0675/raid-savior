from fastapi import APIRouter, HTTPException

from src.api.schemas import ErrorResponse, GlobalMetricsResponse
from src.api.services.iceberg_service import IcebergService

router = APIRouter()
iceberg_service = IcebergService()


@router.get(
    "/global",
    response_model=GlobalMetricsResponse,
    responses={503: {"model": ErrorResponse}},
)
async def global_metrics():
    try:
        return iceberg_service.get_global_metrics()
    except Exception as exc:  # pragma: no cover - error handling
        raise HTTPException(status_code=503, detail=f"Catalog unavailable: {exc}") from exc
