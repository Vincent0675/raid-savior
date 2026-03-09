"""
ETL Principal: Bronze -> Silver.
Lectura de JSON crudo -> Transformación Pandas -> Escritura Parquet particionado.

Soporta dos formatos de entrada:
1. Envelope (HTTP): {"batch_id": "...", "events": [...]}
2. Array directo (S3): [...]

v2.1: Resolución de timestamps en microsegundos para compatibilidad nativa con Spark/Delta (Fase 7)
"""

import hashlib  # ← movido al top-level (ruff: E402)
import io
import json
import os
import re
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from src.etl.transformers import SilverTransformer
from src.storage.minio_client import MinIOStorageClient


SILVER_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("encounter_id", pa.string()),
        pa.field("encounter_duration_ms", pa.int64()),
        pa.field("source_player_id", pa.string()),
        pa.field("source_player_name", pa.string()),
        pa.field("source_player_role", pa.string()),
        pa.field("source_player_class", pa.string()),
        pa.field("source_player_level", pa.int64()),
        pa.field("ability_id", pa.string()),
        pa.field("ability_name", pa.string()),
        pa.field("ability_school", pa.string()),
        pa.field("damage_amount", pa.float64()),
        pa.field("healing_amount", pa.float64()),
        pa.field("is_critical_hit", pa.bool_()),
        pa.field("critical_multiplier", pa.float64()),
        pa.field("is_resisted", pa.bool_()),
        pa.field("is_blocked", pa.bool_()),
        pa.field("is_absorbed", pa.bool_()),
        pa.field("target_entity_id", pa.string()),
        pa.field("target_entity_name", pa.string()),
        pa.field("target_entity_type", pa.string()),
        pa.field("target_entity_health_pct_before", pa.float64()),
        pa.field("target_entity_health_pct_after", pa.float64()),
        pa.field("resource_type", pa.string()),
        pa.field("resource_amount_before", pa.float64()),
        pa.field("resource_amount_after", pa.float64()),
        pa.field("resource_regeneration_rate", pa.float64()),
        pa.field("ingestion_timestamp", pa.string()),
        pa.field("source_system", pa.string()),
        pa.field("data_quality_flags", pa.list_(pa.large_utf8())),
        pa.field("server_latency_ms", pa.int64()),
        pa.field("client_latency_ms", pa.int64()),
        pa.field("is_massive_hit", pa.bool_()),
    ]
)  # Nota: event_date y raid_id se excluyen — se usan como partición y se dropean antes de escribir


class BronzeToSilverETL:
    def __init__(self) -> None:
        self.storage = MinIOStorageClient()
        self.bucket_bronze = os.getenv("S3_BUCKET_BRONZE", "bronze")
        self.bucket_silver = os.getenv("S3_BUCKET_SILVER", "silver")
        self.transformer = SilverTransformer()

    def read_bronze_batch(self, batch_key: str) -> dict[str, Any] | list[Any]:
        """Descarga y deserializa el JSON de Bronze."""
        try:
            response = self.storage.get_object(self.bucket_bronze, batch_key)
            content = response.read().decode("utf-8")
            data = json.loads(content)
            if not isinstance(data, dict | list):
                raise ValueError(f"Expected JSON object, got {type(content).__name__}")
            return data
        except Exception as err:
            raise OSError(f"Error leyendo Bronze [{batch_key}]: {err}") from err

    def save_silver(
        self, df: pd.DataFrame, raid_id: str, batch_id: str
    ) -> dict[str, Any]:  # ← anotación completa (mypy)
        """
        Guarda el DataFrame como Parquet comprimido con Snappy.
        Ruta: raid_id=X / event_date=Y / part-Z.parquet
        """
        if df.empty:
            return {"status": "skipped", "reason": "empty_dataframe"}

        event_date = (
            df["event_date"].iloc[0] if "event_date" in df.columns else "unknown"
        )

        s3_key = (
            f"wow_raid_events/v1/raid_id={raid_id}"
            f"/event_date={event_date}/part-{batch_id}.parquet"
        )

        df_to_save = df.drop(
            columns=[c for c in ("raid_id", "event_date") if c in df.columns]
        )

        df_to_save = df.drop(
            columns=[c for c in ("raid_id", "event_date") if c in df.columns]
        )

        try:
            # ── SCHEMA-ON-WRITE EXPLÍCITO ─────────────────────────────────────
            # Conversión de timezone a UTC (Spark requiere UTC internamente)
            if "timestamp" in df_to_save.columns:
                df_to_save["timestamp"] = df_to_save["timestamp"].dt.tz_convert("UTC")

            # Conversión a Arrow con schema forzado — elimina inferencia de tipos
            arrow_table = pa.Table.from_pandas(
                df_to_save,
                schema=SILVER_SCHEMA,
                preserve_index=False,
                safe=False,  # coerción automática: int→float, etc.
            )

            # Serialización con timestamps en microsegundos (Spark compat)
            out_buffer = io.BytesIO()
            pq.write_table(
                arrow_table,
                out_buffer,
                compression="snappy",
                coerce_timestamps="us",
                allow_truncated_timestamps=True,
            )

            out_buffer.seek(0)
            data_len = out_buffer.getbuffer().nbytes

            self.storage.put_object(
                bucket_name=self.bucket_silver,
                object_name=s3_key,
                data=out_buffer,
                length=data_len,
                content_type="application/octet-stream",
            )

            return {
                "status": "success",
                "s3_path": f"s3://{self.bucket_silver}/{s3_key}",
                "rows": len(df),
                "bytes": data_len,
            }

        except Exception as err:
            raise OSError(f"Error escribiendo Silver: {err}") from err

    def run(self, bronze_key: str) -> dict[str, Any]:  # ← anotación completa (mypy)
        """
        Ejecuta el ciclo completo para un archivo específico.
        """
        print(f"⚡ [ETL] Procesando: {bronze_key}")

        # 1. READ
        raw_data = self.read_bronze_batch(bronze_key)

        # 2. EXTRAER BATCH_ID (filename como fuente de verdad)
        filename_match = re.search(r"batch_([^/]+?)\.json$", bronze_key)
        if filename_match:
            batch_id = filename_match.group(1)
        else:
            batch_id = hashlib.md5(bronze_key.encode()).hexdigest()[:8]
            print(f"  [WARN] batch_id derivado de hash para: {bronze_key}")

        # 3. NORMALIZAR FORMATO
        if isinstance(raw_data, dict):
            events_list = raw_data.get("events", [])
            if not events_list:
                return {"status": "skipped", "reason": "no_events_in_envelope"}
        elif isinstance(raw_data, list):
            events_list = raw_data
            if not events_list:
                return {"status": "skipped", "reason": "empty_array"}
        else:
            return {"status": "error", "reason": f"unknown_json_type: {type(raw_data)}"}  # type: ignore[unreachable]

        # 4. TRANSFORM
        df_raw = pd.DataFrame(events_list)
        df_silver, metadata = self.transformer.transform_pipeline(df_raw)

        # 5. WRITE
        raid_id = str(
            df_silver["raid_id"].iloc[0]
            if "raid_id" in df_silver.columns
            else "unknown"
        )  # ← str() explícito: iloc[0] con StringDtype devuelve pd.NA-capable

        storage_result = self.save_silver(df_silver, raid_id, batch_id)

        return {
            "status": "success",
            "metadata": metadata,
            "storage": storage_result,
        }
