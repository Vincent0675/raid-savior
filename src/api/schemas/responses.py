from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RaidSummaryResponse(BaseModel):
    raid_id: str
    event_date: str | None = None
    raid_outcome: str | None = None
    raid_dps: float | None = None

    model_config = ConfigDict(extra="allow")


class RaidListResponse(BaseModel):
    raids: list[RaidSummaryResponse]
    total: int = Field(ge=0)
    limit: int = Field(ge=1, le=500)
    offset: int = Field(ge=0)


class RaidPlayerResponse(BaseModel):
    player_id: str | None = None
    player_name: str | None = None
    class_name: str | None = None
    spec_name: str | None = None
    damage_total: float | None = None
    healing_total: float | None = None

    model_config = ConfigDict(extra="allow")


class RaidPlayersResponse(BaseModel):
    raid_id: str
    players: list[RaidPlayerResponse]


class GlobalMetricsResponse(BaseModel):
    total_raids: int = Field(ge=0)
    success_raids: int = Field(ge=0)
    wipe_raids: int = Field(ge=0)
    wipe_rate_pct: float = Field(ge=0.0)
    avg_raid_dps: float = Field(ge=0.0)


class ErrorResponse(BaseModel):
    detail: Any
