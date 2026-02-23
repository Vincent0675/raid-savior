"""
Gold Layer Schemas — WoW Raid Telemetry Pipeline
================================================
Contratos Pydantic v2 para las tablas de la capa Gold.

Modelo semidimensional ligero:
    - dim_player          → "¿Quién es el jugador?"
    - dim_raid            → "¿Qué raid/boss fue?"
    - fact_raid_summary   → KPIs macro por raid (¿Tuvo éxito la raid?)
    - fact_player_raid_stats → KPIs micro por jugador/raid
                              (¿Quién es clave? ¿Qué estilo tiene?)

Diseñado para ser usado como:
    1. Validación de DataFrames antes de escribir a MinIO Gold.
    2. Documentación viva del contrato de datos.
    3. Base para FastAPI response models en fases futuras.

Autor: Byron V. Blatch Rodriguez
Versión: 1.0 — Fase 4
"""

from datetime import date
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


# ============================================================================
# ENUMERACIONES
# ============================================================================

class RaidOutcome(str, Enum):
    """Resultado del encuentro de raid."""
    SUCCESS = "success"
    WIPE = "wipe"


class PlayerStyle(str, Enum):
    """
    Estilos de juego detectables por clustering (Fase C del roadmap).
    Documentados aquí como contrato anticipado.
    """
    # DPS Styles
    AGGRESSIVE_DPS   = "aggressive_dps"    # Mucho daño, menos mecánico (alta habilidad / alto riesgo)
    MECHANIC_DPS     = "mechanic_dps"      # Daño sostenido, sacrificio de dps bruto para seguir mecánicas
    UTILITY_DPS      = "utility_dps"       # Daño sacrificado para aportar buff/herramientas a grupo

    # HEALER Styles
    TANK_HEALER      = "tank_healer"       # Healer enfocado en mantener sano a tanks
    RAID_HEALER      = "raid_healer"       # Healer enfocado en mantener sano a grupos de raid (dps/otros healers)
    RESCUE_HEALER    = "rescue_healer"     # Healer con alta capacidad de reacción para salvar jugadores de muertes tras errores o a punto de morir

    # TANK Styles
    SURVIVAL_TANK    = "survival_tank"     # Tank con altas stats de alta supervivencia, daño recibido relativamente bajo
    AGGRESIVE_TANK   = "aggressive_tank"   # Tank con altas stats enfocado a daño, DPS alto para su rol, daño recibido alto pero controlado
    

    UNKNOWN          = "unknown"           # Sin clasificar (default inicial)


# ============================================================================
# DIMENSIONES
# ============================================================================

class DimPlayerSchema(BaseModel):
    """
    dim_player — Atributos identificativos de cada jugador único.

    Granularidad: 1 fila por player_id.
    Estrategia de actualización: Type 1 SCD
        (se sobreescribe last_seen_date y total_raids en cada proceso).
    """
    
    player_id: str = Field(
        min_length=1,
        description="Identificador único del jugador (coincide con source_player_id de Silver)",
    )
    player_name: str = Field(
        min_length=1,
        max_length=12,
        description="Nombre del personaje",
    )
    player_class: str = Field(
        min_length=1,
        description="Clase del personaje (warrior, mage, priest, etc.)",
    )
    """
    player_spec: str = Field(
        min_length=1,                                                                 # DOCUMENTADO PARA POSTERIOR IMPLEMENTACIÓN
        description="Especialización del personaje (fury, frost, restoration, etc.)",
    )
    """
    player_role: str = Field(
        min_length=1,
        description="Rol principal en raid: tank | healer | dps",
    )
    first_seen_date: date = Field(
        description="Fecha de la primera raid registrada para este jugador",
    )
    last_seen_date: date = Field(
        description="Fecha de la última raid registrada",
    )
    total_raids: int = Field(
        ge=1,
        description="Número total de raids en las que ha participado (acumulado histórico)",
    )

    @field_validator("player_role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        allowed = {"tank", "healer", "dps", "unknown"}
        if v.lower() not in allowed:
            raise ValueError(f"player_role debe ser uno de {allowed}, recibido: '{v}'")
        return v.lower()

    model_config = {"extra": "forbid"}

    """
    @field_validator("player_spec")
    @classmethod                              # DOCUMENTADO PARA POSTERIOR IMPLEMENTACIÓN
    def validate_spec(cls, v: str) -> str: 
        allowed = {}
    """

class DimRaidSchema(BaseModel):
    """
    dim_raid — Metadatos contextuales de cada encuentro de raid.

    Granularidad: 1 fila por raid_id.
    Nota: boss_name y difficulty son opcionales en Fase 4.
          Se rellenarán con datos reales al integrar Warcraft Logs API.
    """

    raid_id: str = Field(
        pattern=r"^raid\d{3}$",
        description="Identificador de la raid (ej. raid005). FK a fact tables.",
    )
    event_date: date = Field(
        description="Fecha del encuentro",
    )
    boss_name: Optional[str] = Field(
        default="Unknown Boss",
        description="Nombre del boss principal del encuentro",
    )
    difficulty: Optional[str] = Field(
        default="Normal",
        description="Nivel de dificultad: Normal | Heroic | Mythic",
    )
    raid_size: int = Field(
        ge=1,
        le=40,
        description="Número de jugadores participantes (n_players de fact_raid_summary)",
    )
    duration_target_ms: float = Field(
        default=360_000.0,
        ge=0.0,
        description="Duración objetivo del encuentro en ms (6 min por defecto)",
    )

    @field_validator("difficulty")
    @classmethod
    def validate_difficulty(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return "Normal"
        allowed = {"Normal", "Heroic", "Mythic"}
        if v not in allowed:
            raise ValueError(f"difficulty debe ser uno de {allowed}, recibido: '{v}'")
        return v

    model_config = {"extra": "forbid"}


# ============================================================================
# TABLAS DE HECHOS
# ============================================================================

class FactRaidSummarySchema(BaseModel):
    """
    fact_raid_summary — KPIs macro por raid.

    Granularidad: 1 fila por raid_id.
    Pregunta de negocio principal: ¿Tuvo éxito la raid?

    Regla de raid_outcome (implementada en aggregators.py):
        success si:
            (boss_min_hp_pct == 0.0 AND duration_ms <= 360_000)
            OR (boss_min_hp_pct < 10.0 AND total_player_deaths <= n_players)
        wipe en caso contrario.
    """

    raid_id: str = Field(
        pattern=r"^raid\d{3}$",
        description="FK a dim_raid.raid_id",
    )
    event_date: date = Field(
        description="Fecha del encuentro",
    )

    # --- Temporales ---
    duration_ms: float = Field(
        ge=0.0,
        description="Duración total del encuentro en milisegundos",
    )

    # --- Volúmenes ---
    total_damage: float = Field(
        ge=0.0,
        description="Daño total infligido por toda la raid",
    )
    total_healing: float = Field(
        ge=0.0,
        description="Curación total realizada por toda la raid",
    )
    total_player_deaths: int = Field(
        ge=0,
        description="Número total de muertes de jugadores durante el encuentro",
    )

    # --- Composición ---
    n_players: int = Field(ge=1, le=40, description="Jugadores únicos participantes")
    n_tanks: int   = Field(ge=0, description="Número de tanks")
    n_healers: int = Field(ge=0, description="Número de healers")
    n_dps: int     = Field(ge=0, description="Número de DPS")

    # --- Rendimiento global ---
    raid_dps: float = Field(ge=0.0, description="DPS promedio global de la raid")
    raid_hps: float = Field(ge=0.0, description="HPS promedio global de la raid")
    boss_min_hp_pct: float = Field(
        ge=0.0,
        le=100.0,
        description="Porcentaje mínimo de vida del boss alcanzado (0.0 = boss derrotado)",
    )

    # --- Resultado ---
    raid_outcome: RaidOutcome = Field(
        description="Resultado del encuentro: success | wipe",
    )

    @model_validator(mode="after")
    def validate_composition(self) -> "FactRaidSummarySchema":  # ← añadir comillas
        """Los roles sumados no pueden superar n_players."""
        total_roles = self.n_tanks + self.n_healers + self.n_dps
        if total_roles > self.n_players:
            raise ValueError(
                f"n_tanks + n_healers + n_dps ({total_roles}) "
                f"> n_players ({self.n_players}). Revisar agregación de roles."
            )
        return self

    model_config = {"extra": "forbid"}


class FactPlayerRaidStatsSchema(BaseModel):
    """
    fact_player_raid_stats — KPIs micro por jugador y raid.

    Granularidad: 1 fila por (raid_id, player_id).
    Preguntas de negocio:
        - ¿Qué jugadores son clave para el éxito de la raid?
        - ¿Qué estilo de juego tiene este jugador?

    Nota: player_name, player_class, player_role están DENORMALIZADOS
    deliberadamente para facilitar queries y dashboards sin JOINs.
    """

    raid_id: str = Field(pattern=r"^raid\d{3}$", description="FK a dim_raid.raid_id")
    event_date: date = Field(description="Fecha del encuentro")

    # --- Identidad del jugador (denormalizado) ---
    player_id: str = Field(min_length=1, description="FK a dim_player.player_id")
    player_name: str = Field(min_length=1, max_length=12)
    player_class: str = Field(min_length=1)
    player_role: str = Field(min_length=1)

    # --- Volúmenes de combate ---
    damage_total: float = Field(ge=0.0, description="Daño total infligido")
    healing_total: float = Field(ge=0.0, description="Curación total realizada")
    damage_events: int = Field(ge=0, description="Número de eventos de daño")
    healing_events: int = Field(ge=0, description="Número de eventos de curación")

    # --- Supervivencia ---
    player_deaths: int = Field(ge=0, description="Veces que el jugador murió")
    total_damage_received: float = Field(ge=0.0, description="Daño total recibido")

    # --- Eficiencia ---
    crit_events: int = Field(ge=0, description="Golpes/curaciones críticas")
    crit_rate: float = Field(
        ge=0.0, le=1.0,
        description="Tasa de críticos: crit_events / (damage_events + healing_events)",
    )
    dps: float = Field(ge=0.0, description="Daño por segundo del jugador")
    hps: float = Field(ge=0.0, description="Curación por segundo del jugador")

    # --- Shares (contribución relativa a la raid) ---
    damage_share: float = Field(
        ge=0.0, le=1.0,
        description="Proporción del daño total de la raid aportado por este jugador",
    )
    healing_share: float = Field(
        ge=0.0, le=1.0,
        description="Proporción de la curación total aportada por este jugador",
    )

    @field_validator("player_role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        allowed = {"tank", "healer", "dps", "unknown"}
        if v.lower() not in allowed:
            raise ValueError(f"player_role debe ser tank | healer | dps, recibido: '{v}'")
        return v.lower()

    @field_validator("crit_rate")
    @classmethod
    def validate_crit_rate_coherence(cls, v: float, info) -> float:
        """crit_rate no puede ser > 1.0 (ya protegido por ge/le, pero lo dejamos explícito)."""
        if v > 1.0:
            raise ValueError(f"crit_rate no puede superar 1.0, recibido: {v}")
        return round(v, 6)  # Redondear a 6 decimales para limpieza

    model_config = {"extra": "forbid"}


# ============================================================================
# TABLA DERIVADA FUTURA — DOCUMENTADA COMO CONTRATO ANTICIPADO
# ============================================================================

class PlayerImpactIndexSchema(BaseModel):
    """
    player_impact_index — Índice sintético de impacto del jugador.

    *** TABLA FUTURA — Fase C del roadmap (Modelado de IA) ***

    Granularidad: 1 fila por (raid_id, player_id).
    Pregunta de negocio: ¿Quién es el MVP de la raid?

    Fórmula propuesta del impact_score (0–100):
        impact_score = (
            0.35 * (damage_share * 100)
          + 0.35 * (healing_share * 100)
          + 0.15 * (crit_rate * 100)
          - 0.10 * (player_deaths * 10)
        )
        → Clamp entre 0 y 100

    Bandas de interpretación:
        80–100  → MVP / Core player
        60–79   → Contribuidor sólido
        40–59   → Rendimiento medio
        0–39    → Bajo impacto / underperforming

    style_cluster se calculará mediante K-Means / DBSCAN
    sobre features de fact_player_raid_stats en Fase C.
    """

    raid_id: str = Field(pattern=r"^raid\d{3}$", description="FK a dim_raid.raid_id")
    event_date: date = Field(description="Fecha del encuentro")
    player_id: str = Field(min_length=1, description="FK a dim_player.player_id")
    player_name: str = Field(min_length=1, max_length=12)

    impact_score: float = Field(
        ge=0.0,
        le=100.0,
        description="Índice de impacto normalizado (0–100). Mayor = más crítico.",
    )
    impact_rank: int = Field(
        ge=1,
        description="Ranking del jugador dentro de la raid (1 = MVP)",
    )
    style_cluster: PlayerStyle = Field(
        default=PlayerStyle.UNKNOWN,
        description="Estilo de juego detectado por clustering",
    )

    model_config = {"extra": "forbid"}
