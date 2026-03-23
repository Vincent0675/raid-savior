from fastapi import APIRouter, HTTPException, Query

from src.api.schemas import ErrorResponse, RaidListResponse, RaidPlayersResponse, RaidSummaryResponse
from src.api.services.iceberg_service import IcebergService

router = APIRouter()
iceberg_service = IcebergService()


@router.get(
    "",
    response_model=RaidListResponse,
    responses={503: {"model": ErrorResponse}},
)
async def list_raids(limit: int = Query(50, ge=1, le=500), offset: int = Query(0, ge=0)):
    try:
        return iceberg_service.list_raids(limit=limit, offset=offset)
    except Exception as exc:  # pragma: no cover - error handling
        raise HTTPException(status_code=503, detail=f"Catalog unavailable: {exc}") from exc


@router.get(
    "/{raid_id}",
    response_model=RaidSummaryResponse,
    responses={404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def get_raid(raid_id: str):
    try:
        raid = iceberg_service.get_raid_summary(raid_id)
    except Exception as exc:  # pragma: no cover - error handling
        raise HTTPException(status_code=503, detail=f"Catalog unavailable: {exc}") from exc

    if raid is None:
        raise HTTPException(status_code=404, detail=f"Raid '{raid_id}' not found")
    return raid


@router.get(
    "/{raid_id}/players",
    response_model=RaidPlayersResponse,
    responses={404: {"model": ErrorResponse}, 503: {"model": ErrorResponse}},
)
async def get_raid_players(raid_id: str):
    try:
        raid = iceberg_service.get_raid_summary(raid_id)
        if raid is None:
            raise HTTPException(status_code=404, detail=f"Raid '{raid_id}' not found")
        players = iceberg_service.list_raid_players(raid_id)
        return {"raid_id": raid_id, "players": players}
    except HTTPException:
        raise
    except Exception as exc:  # pragma: no cover - error handling
        raise HTTPException(status_code=503, detail=f"Catalog unavailable: {exc}") from exc
