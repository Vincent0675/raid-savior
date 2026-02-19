"""
Gold Layer ETL — WoW Raid Telemetry Pipeline
============================================
Orquestador de la capa Gold. Responsabilidades:

    1. Leer particiones Parquet desde Silver (MinIO).
    2. Construir dimensiones: dim_player, dim_raid.
    3. Orquestar agregaciones: fact_raid_summary, fact_player_raid_stats.
    4. Validar DataFrames resultantes contra schemas Pydantic v2.
    5. Escribir tablas Gold en MinIO (bucket gold) en formato Parquet + Snappy.

Modelo de datos semidimensional:
    gold/dim_player/                        → Quién es el jugador
    gold/dim_raid/                          → Qué raid/boss fue
    gold/fact_raid_summary/                 → KPIs macro por raid
    gold/fact_player_raid_stats/            → KPIs micro por jugador/raid

Autor: Byron V. Blatch Rodriguez
Versión: 2.0
"""

from __future__ import annotations

import io
import logging
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Type

import pandas as pd
from pydantic import BaseModel, ValidationError

from src.analytics.aggregators import build_player_raid_stats, build_raid_summary
from src.config import Config
from src.schemas.gold_schemas import (
    DimPlayerSchema,
    DimRaidSchema,
    FactPlayerRaidStatsSchema,
    FactRaidSummarySchema,
)
from src.storage.minio_client import MinIOStorageClient

# logger dedicado al módulo
logger = logging.getLogger(__name__)


# ============================================================================
# CONSTANTES DE PATHS (contrato de escritura Gold)
# ============================================================================

# Cada tabla tiene su propio prefijo raíz.
# Dentro, se particiona por raid_id y event_date (Hive-style).
_PATH_TEMPLATES: Dict[str, str] = {
    "dim_player":             "dim_player/player_id={player_id}/dim_player.parquet",
    "dim_raid":               "dim_raid/raid_id={raid_id}/dim_raid.parquet",
    "fact_raid_summary":      "fact_raid_summary/raid_id={raid_id}/event_date={event_date}/fact_raid_summary.parquet",
    "fact_player_raid_stats": "fact_player_raid_stats/raid_id={raid_id}/event_date={event_date}/fact_player_raid_stats.parquet",
}


# ============================================================================
# HELPER: validación de DataFrame contra schema Pydantic
# ============================================================================

def _validate_dataframe(df: pd.DataFrame, schema: Type[BaseModel], table_name: str) -> None:
    """
    Valida cada fila de un DataFrame contra un schema Pydantic v2.

    Lanza ValueError si hay filas inválidas, adjuntando todos los errores
    encontrados para diagnóstico completo (no falla en el primer error).

    Parámetros
    ----------
    df         : DataFrame a validar.
    schema     : Clase Pydantic (ej. FactRaidSummarySchema).
    table_name : Nombre de la tabla (para mensajes de error claros).
    """
    errors: List[str] = []

    for idx, row in df.iterrows():
        try:
            schema(**row.to_dict())
        except ValidationError as exc:
            errors.append(f"  Fila {idx}: {exc.error_count()} error(s) → {exc.errors(include_url=False)}")

    if errors:
        error_summary = "\n".join(errors)
        raise ValueError(
            f"[Gold Validation] '{table_name}' tiene {len(errors)} fila(s) inválida(s):\n"
            f"{error_summary}"
        )

    logger.debug("[Gold Validation] '%s' OK — %d filas validadas.", table_name, len(df))


# ============================================================================
# CLASE PRINCIPAL
# ============================================================================

class GoldLayerETL:
    """
    Orquestador ETL de la capa Gold.

    Lee particiones Parquet de Silver, construye dimensiones y hechos,
    valida contra schemas Pydantic v2 y escribe en MinIO Gold.
    """

    def __init__(self, config: Optional[Config] = None) -> None:
        self.config = config or Config()
        self.storage = MinIOStorageClient()
        self.gold_bucket = self.config.S3_BUCKET_GOLD
        self.silver_bucket = self.config.S3_BUCKET_SILVER

    # ------------------------------------------------------------------ #
    # LECTURA                                                              #
    # ------------------------------------------------------------------ #

    def read_silver_partition(self, raid_id: str, event_date: str) -> pd.DataFrame:
        """
        Lee todos los archivos Parquet de una partición Silver y los concatena.

        Parámetros
        ----------
        raid_id    : Identificador de la raid (ej. 'raid005').
        event_date : Fecha en formato 'YYYY-MM-DD'.

        Returns
        -------
        pd.DataFrame con todos los eventos de esa partición.

        Raises
        ------
        ValueError  si no hay objetos en la partición.
        RuntimeError si falla la lectura de algún objeto individual.
        """
        # prefix alineado con layout Silver
        prefix = f"wow_raid_events/v1/raid_id={raid_id}/event_date={event_date}/"
        logger.info("[Silver → Gold] Leyendo partición: %s", prefix)

        objects = self.storage.list_objects(self.silver_bucket, prefix)

        if not objects:
            raise ValueError(
                f"[read_silver_partition] Sin datos en Silver para "
                f"raid_id='{raid_id}' / event_date='{event_date}'. "
                f"Prefijo buscado: s3://{self.silver_bucket}/{prefix}"
            )

        dfs: List[pd.DataFrame] = []
        failed: List[str] = []

        for obj_key in objects:
            try:
                response = self.storage.s3.get_object(
                    Bucket=self.silver_bucket, Key=obj_key
                )
                parquet_bytes = response["Body"].read()
                df = pd.read_parquet(io.BytesIO(parquet_bytes))
                dfs.append(df)
                logger.debug("  Leído: %s (%d filas)", obj_key, len(df))
            except Exception as exc:
                # ← NUEVO: acumulamos fallos en lugar de explotar al primer error
                logger.warning("  [WARN] No se pudo leer '%s': %s", obj_key, exc)
                failed.append(obj_key)

        if not dfs:
            raise RuntimeError(
                f"[read_silver_partition] Todos los objetos fallaron en lectura. "
                f"Objetos fallidos: {failed}"
            )

        if failed:
            logger.warning(
                "[read_silver_partition] %d objeto(s) saltados por error: %s",
                len(failed), failed,
            )

        df_result = pd.concat(dfs, ignore_index=True)
        logger.info("[Silver → Gold] Total filas leídas: %d", len(df_result))
        return df_result

    # ------------------------------------------------------------------ #
    # CONSTRUCCIÓN DE DIMENSIONES                                          #
    # ------------------------------------------------------------------ #

    def _build_dim_player(self, df_silver: pd.DataFrame) -> pd.DataFrame:
        """
        Construye dim_player a partir del DataFrame Silver normalizado.

        Estrategia: agrupa por player_id y toma el primer valor de
        nombre/clase/rol (son estables dentro de una raid).
        Las fechas first_seen / last_seen se derivan de 'eventdate'.

        Parámetros
        ----------
        df_silver : DataFrame con columnas normalizadas (sourceplayerid, etc.)

        Returns
        -------
        pd.DataFrame con schema DimPlayerSchema.
        """
        logger.debug("[dim_player] Construyendo dimensión de jugadores...")

        dim = (
            df_silver
            .groupby("sourceplayerid", as_index=False)
            .agg(
                player_name=("sourceplayername", "first"),
                player_class=("sourceplayerclass", "first"),
                player_role=("sourceplayerrole", "first"),
                first_seen_date=("eventdate", "min"),
                last_seen_date=("eventdate", "max"),
            )
            .rename(columns={"sourceplayerid": "player_id"})
        )

        # total_raids en Fase 4 = 1 por definición (procesamos 1 raid a la vez)
        # En Fase A (escalabilidad) esto se actualizará de forma incremental
        dim["total_raids"] = 1

        # Convertir eventdate a tipo date de Python si es string
        for col in ("first_seen_date", "last_seen_date"):
            dim[col] = pd.to_datetime(dim[col]).dt.date
        null_class = dim["player_class"].isna().sum()
        null_role  = dim["player_role"].isna().sum()
        if null_class > 0:
            logger.warning("[dim_player] %d jugador(es) sin player_class en Silver → 'unknown'", null_class)
        if null_role > 0:
            logger.warning("[dim_player] %d jugador(es) sin player_role en Silver → 'unknown'", null_role)

        dim["player_class"] = dim["player_class"].fillna("unknown")
        dim["player_role"]  = dim["player_role"].fillna("unknown")

        logger.debug("[dim_player] %d jugadores únicos.", len(dim))
        return dim

    def _build_dim_raid(
        self,
        raid_id: str,
        event_date: str,
        raid_summary: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Construye dim_raid a partir del raid_summary y parámetros de partición.

        En Fase 4, boss_name y difficulty son placeholders.
        Se enriquecerán con Warcraft Logs API en Fase D.

        Parámetros
        ----------
        raid_id      : Identificador de la raid.
        event_date   : Fecha del encuentro (string 'YYYY-MM-DD').
        raid_summary : DataFrame resultado de build_raid_summary().

        Returns
        -------
        pd.DataFrame con schema DimRaidSchema (1 fila).
        """
        logger.debug("[dim_raid] Construyendo dimensión de raid para %s...", raid_id)

        n_players = int(raid_summary["n_players"].iloc[0])

        dim = pd.DataFrame([{
            "raid_id":            raid_id,
            "event_date":         pd.to_datetime(event_date).date(),
            "boss_name":          "Unknown Boss",   # placeholder Fase 4
            "difficulty":         "Normal",          # placeholder Fase 4
            "raid_size":          n_players,
            "duration_target_ms": 360_000.0,         # 6 min — regla de negocio actual
        }])

        logger.debug("[dim_raid] dim_raid construida: %s", dim.to_dict("records"))
        return dim

    # ------------------------------------------------------------------ #
    # ESCRITURA                                                            #
    # ------------------------------------------------------------------ #

    def _write_parquet(self, df: pd.DataFrame, key: str) -> None:
        """
        Serializa un DataFrame a Parquet (Snappy) y lo sube a MinIO Gold.

        Parámetros
        ----------
        df  : DataFrame a escribir.
        key : Clave S3 completa dentro del bucket gold.
        """
        parquet_bytes = df.to_parquet(compression="snappy", index=False)
        self.storage.s3.put_object(
            Bucket=self.gold_bucket,
            Key=key,
            Body=parquet_bytes,
        )
        logger.info("  Escrito: s3://%s/%s (%d filas)", self.gold_bucket, key, len(df))

    def write_gold_tables(
        self,
        dim_player: pd.DataFrame,
        dim_raid: pd.DataFrame,
        fact_raid_summary: pd.DataFrame,
        fact_player_raid_stats: pd.DataFrame,
    ) -> Dict[str, Any]:
        """
        Valida y escribe las 4 tablas Gold en MinIO.

        El orden de escritura es:
            1. dim_player  (una entrada por jugador único en la partición)
            2. dim_raid    (una entrada para esta raid)
            3. fact_raid_summary
            4. fact_player_raid_stats

        Returns
        -------
        Dict con las rutas S3 escritas y conteo de filas por tabla.

        Raises
        ------
        ValueError si alguna tabla no pasa la validación de schema.
        """
        raid_id    = str(fact_raid_summary["raid_id"].iloc[0])
        event_date = str(fact_raid_summary["event_date"].iloc[0])

        logger.info("[write_gold_tables] Iniciando escritura Gold para %s / %s", raid_id, event_date)

        # Validar ANTES de escribir (fail-fast)
        logger.info("[write_gold_tables] Validando schemas...")
        _validate_dataframe(dim_player,           DimPlayerSchema,           "dim_player")
        _validate_dataframe(dim_raid,             DimRaidSchema,             "dim_raid")
        _validate_dataframe(fact_raid_summary,    FactRaidSummarySchema,     "fact_raid_summary")
        _validate_dataframe(fact_player_raid_stats, FactPlayerRaidStatsSchema, "fact_player_raid_stats")
        logger.info("[write_gold_tables] Todos los schemas OK.")

        # Paths usando las constantes de contrato
        keys = {
            "dim_player":             _PATH_TEMPLATES["dim_player"].format(player_id="all"),
            "dim_raid":               _PATH_TEMPLATES["dim_raid"].format(raid_id=raid_id),
            "fact_raid_summary":      _PATH_TEMPLATES["fact_raid_summary"].format(
                                          raid_id=raid_id, event_date=event_date),
            "fact_player_raid_stats": _PATH_TEMPLATES["fact_player_raid_stats"].format(
                                          raid_id=raid_id, event_date=event_date),
        }

        self._write_parquet(dim_player,             keys["dim_player"])
        self._write_parquet(dim_raid,               keys["dim_raid"])
        self._write_parquet(fact_raid_summary,      keys["fact_raid_summary"])
        self._write_parquet(fact_player_raid_stats, keys["fact_player_raid_stats"])

        return {
            "raid_id":                    raid_id,
            "event_date":                 event_date,
            "dim_player_path":            f"s3://{self.gold_bucket}/{keys['dim_player']}",
            "dim_raid_path":              f"s3://{self.gold_bucket}/{keys['dim_raid']}",
            "fact_raid_summary_path":     f"s3://{self.gold_bucket}/{keys['fact_raid_summary']}",
            "fact_player_stats_path":     f"s3://{self.gold_bucket}/{keys['fact_player_raid_stats']}",
            "dim_player_rows":            len(dim_player),
            "dim_raid_rows":              len(dim_raid),
            "fact_raid_summary_rows":     len(fact_raid_summary),
            "fact_player_stats_rows":     len(fact_player_raid_stats),
        }

    # ------------------------------------------------------------------ #
    # ORQUESTADOR PRINCIPAL                                                #
    # ------------------------------------------------------------------ #

    def run_for_partition(self, raid_id: str, event_date: str) -> Dict[str, Any]:
        """
        Pipeline Gold completo para una partición (raid_id + event_date).

        Flujo:
            Silver Parquet
                → normalizar columnas
                → build_raid_summary()     → fact_raid_summary
                → build_player_raid_stats() → fact_player_raid_stats
                → _build_dim_player()      → dim_player
                → _build_dim_raid()        → dim_raid
                → validar schemas
                → escribir Gold en MinIO

        Raises
        ------
        ValueError   si no hay datos Silver o falla validación de schema.
        RuntimeError si falla la lectura de Silver o la escritura en Gold.
        """
        logger.info(
            "=" * 60 + "\n[Gold ETL] Iniciando pipeline para raid_id=%s / event_date=%s",
            raid_id, event_date,
        )

        try:
            # ── 1. Leer Silver ────────────────────────────────────────────
            df_silver = self.read_silver_partition(raid_id, event_date)

            # ── 2. Normalizar nombres de columnas ─────────────────────────
            # Los agregadores de Fase 3 trabajan con nombres sin guiones bajos.
            # Mapeamos aquí para no modificar aggregators.py.
            df_silver = df_silver.rename(columns={
                "event_type":                    "eventtype",
                "source_player_id":              "sourceplayerid",
                "source_player_name":            "sourceplayername",
                "source_player_class":           "sourceplayerclass",
                "source_player_role":            "sourceplayerrole",
                "damage_amount":                 "damageamount",
                "healing_amount":                "healingamount",
                "target_entity_type":            "targetentitytype",
                "target_entity_id":              "targetentityid",
                "target_entity_health_pct_after":"targetentityhealthpctafter",
                "is_critical_hit":               "iscriticalhit",
            })

            # Añadir columnas de partición como contexto explícito
            df_silver["raidid"]    = raid_id
            df_silver["eventdate"] = event_date

            logger.info("[Gold ETL] Silver leído: %d eventos.", len(df_silver))

            # ── 3. Construir tablas de hechos ─────────────────────────────
            fact_raid_summary      = build_raid_summary(df_silver)
            fact_player_raid_stats = build_player_raid_stats(df_silver, fact_raid_summary)

            logger.info(
                "[Gold ETL] Agregaciones OK — raids: %d | jugadores: %d",
                len(fact_raid_summary), len(fact_player_raid_stats),
            )

            for col in ("player_class", "player_role"):
                if col in fact_player_raid_stats.columns:
                    nulls = fact_player_raid_stats[col].isna().sum()
                    if nulls > 0:
                        logger.warning(
                            "[fact_player_raid_stats] %d fila(s) con '%s' nulo → 'unknown'", nulls, col
                        )
                    fact_player_raid_stats[col] = fact_player_raid_stats[col].fillna("unknown")

                    
            # ── 4. Construir dimensiones ──────────────────────────────────
            dim_player = self._build_dim_player(df_silver)
            dim_raid   = self._build_dim_raid(raid_id, event_date, fact_raid_summary)

            # ── 5. Alinear tipos para Pydantic ────────────────────────────
            # event_date en facts debe ser tipo date, no string ni Timestamp
            for df in (fact_raid_summary, fact_player_raid_stats):
                if "event_date" in df.columns:
                    df["event_date"] = pd.to_datetime(df["event_date"]).dt.date

            # ── 6. Validar + Escribir Gold ────────────────────────────────
            result = self.write_gold_tables(
                dim_player=dim_player,
                dim_raid=dim_raid,
                fact_raid_summary=fact_raid_summary,
                fact_player_raid_stats=fact_player_raid_stats,
            )

            logger.info("[Gold ETL] Pipeline completado exitosamente.\n%s", result)
            return result

        except (ValueError, RuntimeError) as exc:
            logger.error("[Gold ETL] FALLO en pipeline Gold: %s", exc)
            raise
        except Exception as exc:
            logger.error("[Gold ETL] ERROR INESPERADO: %s", exc, exc_info=True)
            raise RuntimeError(f"[Gold ETL] Error inesperado: {exc}") from exc
