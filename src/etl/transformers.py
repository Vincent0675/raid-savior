"""
Transformadores para la capa Silver (Signal Conditioning).
Encapsulan la lógica de limpieza, validación y enriquecimiento.
"""

from typing import Any

import pandas as pd


def _normalize_quality_flags(val: object) -> list[str]:
    """
    Coerce un valor de data_quality_flags a list[int] garantizado.
    Señal digital paquetizada: cada flag es un entero discreto.
    Cubre tres casos: lista de ints, lista de strings, None/NaN/vacío.
    """
    if isinstance(val, list):
        return [str(x) for x in val if x is not None]
    return []


class SilverTransformer:
    """
    Actúa como un circuito acondicionador de señal.
    Entrada: DataFrame 'sucio' (tipos mezclados, posibles duplicados).
    Salida: DataFrame 'limpio' y enriquecido.
    """

    @staticmethod
    def cast_types(df: pd.DataFrame) -> pd.DataFrame:
        """
        Conversión Analógico-Digital (ADC) de los datos.
        Convierte strings genéricos a tipos binarios precisos.
        """
        df = df.copy()

        # 1. Timestamps (Time Domain)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        if "ingest_timestamp" in df.columns:
            df["ingest_timestamp"] = pd.to_datetime(df["ingest_timestamp"], utc=True)

        # 2. Magnitudes Físicas (Floats)
        numeric_cols = [
            "damage_amount",
            "healing_amount",
            "target_entity_health_pct_before",
            "target_entity_health_pct_after",
            "resource_amount_before",
            "resource_amount_after",
        ]
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # 3. Identificadores (Categorical/String)
        string_cols = [
            "event_id",
            "raid_id",
            "event_type",
            "source_player_name",
            "target_entity_name",
            "ability_name",
            "source_system",
        ]
        for col in string_cols:
            if col in df.columns:
                df[col] = df[col].astype("string")

        # 4. Enteros (Magnitudes discretas)
        integer_cols = [
            "source_player_level",
            "server_latency_ms",
            "client_latency_ms",
            "encounter_duration_ms",
        ]
        for col in integer_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

        # 5. Arrays de flags (Señal digital paquetizada)
        if "data_quality_flags" in df.columns:
            df["data_quality_flags"] = df["data_quality_flags"].apply(
                _normalize_quality_flags
            )

        return df  # ← SIEMPRE al final del método, fuera de cualquier if

    @staticmethod
    def deduplicate(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """
        Filtro de Ruido: Elimina rebotes de señal (eventos duplicados).
        Retorna: (DataFrame limpio, cantidad de eliminados)
        """
        initial_count = len(df)
        df = df.drop_duplicates(subset=["event_id"], keep="first")
        duplicates_removed = initial_count - len(df)
        return df, duplicates_removed

    @staticmethod
    def validate_ranges(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
        """
        Protección de Sobrevoltaje:
        Descarta valores físicamente imposibles (ej. salud < 0% o > 100%).
        """
        errors: list[str] = []

        for col in [
            "target_entity_health_pct_before",
            "target_entity_health_pct_after",
        ]:
            if col in df.columns:
                mask_invalid = (df[col] < 0) | (df[col] > 100)
                invalid_count = int(mask_invalid.sum())

                if invalid_count > 0:
                    errors.append(
                        f"{col}: {invalid_count} valores fuera de rango [0-100]"
                    )

                df = df[~mask_invalid]

        return df, errors

    @staticmethod
    def enrich(df: pd.DataFrame) -> pd.DataFrame:
        """
        Etapa de Ganancia/Amplificación:
        Calcula nuevas variables derivadas de la señal original.
        """
        df = df.copy()

        # 1. Latencia de Ingesta (Delta T)
        if "ingest_timestamp" in df.columns and "timestamp" in df.columns:
            df["ingest_latency_ms"] = (
                ((df["ingest_timestamp"] - df["timestamp"]).dt.total_seconds() * 1000)
                .fillna(0)
                .astype("int32")
            )

        # 2. Flag de "Massive Hit" (Detector de picos)
        if "damage_amount" in df.columns and "event_type" in df.columns:
            df["is_massive_hit"] = (
                (df["event_type"] == "combat_damage") & (df["damage_amount"] > 10000)
            ).astype("bool")
        else:
            df["is_massive_hit"] = False

        # 3. Fecha del evento (Para particionado en disco)
        if "timestamp" in df.columns:
            df["event_date"] = df["timestamp"].dt.date.astype("string")

        return df

    @staticmethod
    def transform_pipeline(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
        """
        Circuito completo: Tipado → Deduplicación → Validación → Enriquecimiento
        """
        metadata: dict[str, Any] = {}

        df = SilverTransformer.cast_types(df)

        df, dup_count = SilverTransformer.deduplicate(df)
        metadata["duplicates_removed"] = dup_count

        df, range_errors = SilverTransformer.validate_ranges(df)
        metadata["validation_errors"] = range_errors
        metadata["rows_after_validation"] = len(df)

        df = SilverTransformer.enrich(df)

        return df, metadata
