"""
Schema de eventos para WoW Raid Telemetry Pipeline.
Implementa validación estricta con Pydantic v2 (Schema-on-Write).

Versión: 1.0
Fecha: Enero 2026
Autor: Pipeline WoW Team
"""

from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime, timezone
from enum import Enum
from uuid import UUID, uuid4

# ============================================================================
# ENUMERACIONES (Valores Fijos)
# ============================================================================

class EventType(str, Enum):
    """Tipos de eventos soportados."""
    COMBAT_DAMAGE = "combat_damage"
    HEAL = "heal"
    PLAYER_DEATH = "player_death"
    SPELL_CAST = "spell_cast"
    BOSS_PHASE = "boss_phase"
    MANA_REGENERATION = "mana_regeneration"  # Nuevo tipo


class PlayerRole(str, Enum):
    """Roles de jugadores en raid."""
    TANK = "tank"
    HEALER = "healer"
    DPS = "dps"


class PlayerClass(str, Enum):
    """Clases de WoW (Retail, Dragonflight 10.2.5)."""
    WARRIOR = "warrior"
    PALADIN = "paladin"
    HUNTER = "hunter"
    ROGUE = "rogue"
    PRIEST = "priest"
    SHAMAN = "shaman"
    MAGE = "mage"
    WARLOCK = "warlock"
    DRUID = "druid"
    DEATH_KNIGHT = "death_knight"
    MONK = "monk"


class DamageSchool(str, Enum):
    """Escuelas de daño/curación en WoW."""
    PHYSICAL = "physical"
    FIRE = "fire"
    FROST = "frost"
    NATURE = "nature"
    SHADOW = "shadow"
    ARCANE = "arcane"
    HOLY = "holy"
    CHAOS = "chaos"


class EntityType(str, Enum):
    """Tipos de entidades en combate."""
    PLAYER = "player"
    BOSS = "boss"
    ADD = "add"
    INTERACTIVE = "interactive"


class ResourceType(str, Enum):
    """Tipos de recursos (para mana_regeneration)."""
    MANA = "mana"
    ENERGY = "energy"
    RAGE = "rage"
    FOCUS = "focus"
    RUNIC_POWER = "runic_power"


# ============================================================================
# MODELO PRINCIPAL
# ============================================================================

class WoWRaidEvent(BaseModel):
    """
    Schema principal para eventos de raid WoW.
    
    Implementa validación estricta (Schema-on-Write) antes de ingesta en Bronze.
    """
    
    # ====== IDENTIFICACIÓN (Core) ======
    event_id: UUID = Field(
        default_factory=uuid4,
        description="Unique event identifier (UUID v4)"
    )
    
    event_type: EventType = Field(
        description="Type of event (damage, heal, death, etc.)"
    )
    
    timestamp: datetime = Field(
        description="When the event occurred (ISO 8601 with timezone)"
    )
    
    # ====== CONTEXTO (Raid/Encounter) ======
    raid_id: str = Field(
        pattern=r"^raid\d{3}$",  # Regex: raid001, raid002, ...
        description="Raid identifier (e.g., raid001)"
    )
    
    encounter_id: Optional[str] = Field(
        default=None,
        description="Boss encounter identifier (optional)"
    )
    
    encounter_duration_ms: Optional[int] = Field(
        default=None,
        ge=0,
        description="Duration of encounter in milliseconds"
    )
    
    # ====== ACTOR (Source Player) ======
    source_player_id: str = Field(
        min_length=1,
        description="Unique player identifier"
    )
    
    source_player_name: str = Field(
        min_length=1,
        max_length=12,
        description="Player name"
    )
    
    source_player_role: Optional[PlayerRole] = Field(
        default=None,
        description="Player role (tank, healer, dps)"
    )
    
    source_player_class: Optional[PlayerClass] = Field(
        default=None,
        description="Player class (warrior, mage, etc.)"
    )
    
    source_player_level: Optional[int] = Field(
        default=None,
        ge=1,
        le=90,
        description="Player level (1-90)"
    )
    
    # ====== ACCIÓN (Ability) ======
    ability_id: Optional[str] = Field(
        default=None,
        description="Ability/spell identifier"
    )
    
    ability_name: Optional[str] = Field(
        default=None,
        description="Ability/spell name"
    )
    
    ability_school: Optional[DamageSchool] = Field(
        default=None,
        description="Damage/healing school (fire, frost, holy, etc.)"
    )
    
    # ====== DATOS CUANTITATIVOS ======
    damage_amount: Optional[float] = Field(
        default=None,
        ge=0,
        le=1_000_000,
        description="Damage dealt (for combat_damage events)"
    )
    
    healing_amount: Optional[float] = Field(
        default=None,
        ge=0,
        le=500_000,
        description="Healing done (for heal events)"
    )
    
    is_critical_hit: bool = Field(
        default=False,
        description="Was this a critical hit/heal?"
    )
    
    critical_multiplier: float = Field(
        default=1.0,
        ge=1.0,
        le=5.0,
        description="Critical hit multiplier (1.0 = no crit, 2.0 = 2x damage)"
    )
    
    is_resisted: bool = Field(default=False)
    is_blocked: bool = Field(default=False)
    is_absorbed: bool = Field(default=False)
    
    # ====== TARGET (Receptor del evento) ======
    target_entity_id: Optional[str] = Field(
        default=None,
        description="Target entity identifier"
    )
    
    target_entity_name: Optional[str] = Field(
        default=None,
        description="Target entity name"
    )
    
    target_entity_type: Optional[EntityType] = Field(
        default=None,
        description="Type of target (player, boss, add, interactive)"
    )
    
    target_entity_health_pct_before: Optional[float] = Field(
        default=None,
        ge=0,
        le=100,
        description="Target health percentage before event"
    )
    
    target_entity_health_pct_after: Optional[float] = Field(
        default=None,
        ge=0,
        le=100,
        description="Target health percentage after event"
    )
    
    # ====== RECURSOS (Para mana_regeneration) ======
    resource_type: Optional[ResourceType] = Field(
        default=None,
        description="Type of resource (mana, energy, rage, etc.)"
    )
    
    resource_amount_before: Optional[float] = Field(
        default=None,
        ge=0,
        description="Resource amount before event"
    )
    
    resource_amount_after: Optional[float] = Field(
        default=None,
        ge=0,
        description="Resource amount after event"
    )
    
    resource_regeneration_rate: Optional[float] = Field(
        default=None,
        ge=0,
        description="Resource regeneration rate (units per second)"
    )
    
    # ====== METADATA TÉCNICA ======
    ingestion_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When the event was ingested into the system"
    )
    
    source_system: str = Field(
        default="wow-raid-addon-v1.2",
        description="System that generated this event"
    )
    
    data_quality_flags: List[str] = Field(
        default_factory=list,
        description="Data quality flags (e.g., ['late_arrival', 'missing_target'])"
    )
    
    server_latency_ms: Optional[int] = Field(
        default=None,
        ge=0,
        description="Server latency in milliseconds"
    )
    
    client_latency_ms: Optional[int] = Field(
        default=None,
        ge=0,
        description="Client latency in milliseconds"
    )
    
    # ====== CONFIGURACIÓN DEL MODELO ======
    model_config = {
        "extra": "forbid",  # No permitir campos extra (Schema estricto)
        "str_strip_whitespace": True,  # Trim automático de strings
        "validate_default": True,  # Validar valores default también
    }
    
    # ====== VALIDADORES PERSONALIZADOS ======
    
    @field_validator('timestamp')
    @classmethod
    def timestamp_not_future(cls, v: datetime) -> datetime:
        """
        Validador: El timestamp NO puede estar en el futuro.
        
        Analogía: Una marca temporal de fabricación no puede ser posterior a HOY.
        """
        now = datetime.now(timezone.utc)
        if v > now:
            raise ValueError(
                f"timestamp cannot be in the future (got {v}, now is {now})"
            )
        return v
    
    @field_validator('damage_amount')
    @classmethod
    def validate_damage_for_damage_events(cls, v: Optional[float], info) -> Optional[float]:
        """
        Validador: Si event_type es COMBAT_DAMAGE, damage_amount es OBLIGATORIO y > 0.
        
        Lógica condicional: Accede al event_type usando info.data.
        """
        event_type = info.data.get('event_type')
        if event_type == EventType.COMBAT_DAMAGE:
            if v is None or v <= 0:
                raise ValueError(
                    "damage_amount is required and must be > 0 for combat_damage events"
                )
        return v
    
    @field_validator('healing_amount')
    @classmethod
    def validate_healing_for_heal_events(cls, v: Optional[float], info) -> Optional[float]:
        """
        Validador: Si event_type es HEAL, healing_amount es OBLIGATORIO y > 0.
        """
        event_type = info.data.get('event_type')
        if event_type == EventType.HEAL:
            if v is None or v <= 0:
                raise ValueError(
                    "healing_amount is required and must be > 0 for heal events"
                )
        return v
    
    @field_validator('resource_type')
    @classmethod
    def validate_resource_for_mana_events(cls, v: Optional[ResourceType], info) -> Optional[ResourceType]:
        """
        Validador: Si event_type es MANA_REGENERATION, resource_type es OBLIGATORIO.
        """
        event_type = info.data.get('event_type')
        if event_type == EventType.MANA_REGENERATION:
            if v is None:
                raise ValueError(
                    "resource_type is required for mana_regeneration events"
                )
        return v


# ============================================================================
# MODELO PARA LOTES (Batch Ingestion)
# ============================================================================

class EventBatch(BaseModel):
    """
    Lote de eventos para ingesta masiva.
    
    Usado en Fase 2 para recibir múltiples eventos en un solo POST request.
    """
    batch_id: UUID = Field(
        default_factory=uuid4,
        description="Batch identifier"
    )
    
    batch_timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="When the batch was created"
    )
    
    events: List[WoWRaidEvent] = Field(
        min_length=1,
        max_length=10_000,
        description="List of events in this batch (max 10,000)"
    )
    
    model_config = {
        "extra": "forbid"
    }


# ============================================================================
# UTILIDADES
# ============================================================================

def export_json_schema(output_path: str = "eventos_schema.json") -> None:
    """
    Exporta el JSON Schema formal del modelo WoWRaidEvent.
    
    Uso:
        python eventos_schema.py --export-json-schema
    """
    import json
    schema = WoWRaidEvent.model_json_schema()
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(schema, f, indent=2, ensure_ascii=False)
    print(f"✅ JSON Schema exportado a: {output_path}")


if __name__ == "__main__":
    import sys
    if "--export-json-schema" in sys.argv:
        export_json_schema()
    else:
        print("Uso: python eventos_schema.py --export-json-schema")
