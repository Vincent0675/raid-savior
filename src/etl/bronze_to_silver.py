"""
ETL Principal: Bronze -> Silver.
Lectura de JSON crudo -> Transformación Pandas -> Escritura Parquet particionado.

Soporta dos formatos de entrada:
1. Envelope (HTTP): {"batch_id": "...", "events": [...]}
2. Array directo (S3): [...]

v2.1: Resolución de timestamps en microsegundos para compatibilidad nativa con Spark/Delta (Fase 7)
"""

import os
import pandas as pd
import re
import json
from typing import Any
import io

from src.storage.minio_client import MinIOStorageClient
from src.etl.transformers import SilverTransformer


class BronzeToSilverETL:
    def __init__(self):
        # Cliente MinIO ya existente de la Fase 2
        self.storage = MinIOStorageClient()
        # Leemos buckets del .env (con valores por defecto por seguridad)
        self.bucket_bronze = os.getenv("S3_BUCKET_BRONZE", "bronze")
        self.bucket_silver = os.getenv("S3_BUCKET_SILVER", "silver")
        self.transformer = SilverTransformer()

    def read_bronze_batch(self, batch_key: str) -> dict[str, Any] | list[Any]:
        """Descarga y deserializa el JSON de Bronze"""
        try:
            response = self.storage.get_object(self.bucket_bronze, batch_key)
            # MinIO devuelve un stream, lo leemos y decodificamos
            content = response.read().decode("utf-8")
            data = json.loads(content)
            if not isinstance(data, dict | list):
                raise ValueError(f"Expected JSON object, got {type(content).__name__}")
            return data
        except Exception as err:
            raise OSError(f"Error leyendo Bronze [{batch_key}]: {err}") from err

    def save_silver(self, df: pd.DataFrame, raid_id: str, batch_id: str) -> dict:
        """
        Guarda el DataFrame como Parquet comprimido con Snappy.
        Ruta: raid_id=X / event_date=Y / part-Z.parquet
        """
        if df.empty:
            return {"status": "skipped", "reason": "empty_dataframe"}

        # Obtenemos la fecha para la partición Hive-style
        event_date = (
            df["event_date"].iloc[0] if "event_date" in df.columns else "unknown"
        )

        # Construimos la ruta destino (Key)
        s3_key = f"wow_raid_events/v1/raid_id={raid_id}/event_date={event_date}/part-{batch_id}.parquet"

        # --- CORRECCIÓN: Prevenir conflicto de particiones ---
        # Hacemos una copia para no afectar al DataFrame original en memoria
        df_to_save = df.copy()

        # Eliminamos las columnas que YA están en la ruta de carpetas (partition keys)
        # para que PyArrow no se confunda al leer.
        cols_to_drop = []
        if "raid_id" in df_to_save.columns:
            cols_to_drop.append("raid_id")
        if "event_date" in df_to_save.columns:
            cols_to_drop.append("event_date")

        if cols_to_drop:
            df_to_save = df_to_save.drop(columns=cols_to_drop)

        try:
            # 1. Serializar a Buffer en memoria (usando df_to_save)
            out_buffer = io.BytesIO()

            # ---> FIX PARA SPARK (FASE 7) <---
            # Forzamos los timestamps de datetime64[ns] (Pandas) a TIMESTAMP(unit=MICROS)
            # para que Spark los interprete nativamente como TimestampType y no como LongType.
            df_to_save.to_parquet(
                out_buffer,
                index=False,
                engine="pyarrow",
                compression="snappy",
                coerce_timestamps="us",  # Baja la resolución a microsegundos
                allow_truncated_timestamps=True,  # Permite el truncamiento sin lanzar excepción
            )

            # 2. Subir a MinIO
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

    def run(self, bronze_key: str) -> dict:
        """
        Ejecuta el ciclo completo para un archivo específico.
        Soporta dos formatos de JSON:
        1. Envelope (HTTP): {"batch_id": "...", "events": [...]}
        2. Array directo (S3): [...]
        """
        print(f"⚡ [ETL] Procesando: {bronze_key}")

        # 1. READ
        raw_data = self.read_bronze_batch(bronze_key)

        # ── EXTRACCIÓN DE BATCH_ID (SIEMPRE DESDE EL FILENAME) ────────────
        # El filename es la fuente de verdad: es determinista, único por
        # archivo y no depende del contenido del payload.
        # Regex sin restricción de charset → funciona con 0001, UUIDs, etc.
        filename_match = re.search(r"batch_([^/]+?)\.json$", bronze_key)
        batch_id = filename_match.group(1) if filename_match else None

        if batch_id is None:
            # Fallback de emergencia: nunca debería llegar aquí
            import hashlib

            batch_id = hashlib.md5(bronze_key.encode()).hexdigest()[:8]
            print(f"  [WARN] batch_id derivado de hash para: {bronze_key}")

        # 2. NORMALIZAR FORMATO
        # Detectar si es envelope (dict) o array directo (list)
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

        # 3. TRANSFORM
        df_raw = pd.DataFrame(events_list)
        df_silver, metadata = self.transformer.transform_pipeline(df_raw)

        # 4. WRITE
        raid_id = (
            df_silver["raid_id"].iloc[0]
            if "raid_id" in df_silver.columns
            else "unknown"
        )
        storage_result = self.save_silver(df_silver, raid_id, batch_id)

        return {
            "status": "success",
            "metadata": metadata,
            "storage": storage_result,
        }
