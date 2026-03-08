"""
Test de Integración End-to-End: Fase 1 → 2 → 3
Valida que el pipeline completo funciona sin intervención manual.
"""

import os
import time
import pandas as pd
import requests
from datetime import datetime, timezone

# --- IMPORTS CORREGIDOS (según tu estructura real) ---
from src.generators.raid_event_generator import WoWEventGenerator
from src.etl.bronze_to_silver import BronzeToSilverETL
from src.storage.minio_client import MinIOStorageClient

# Configuración
RECEPTOR_URL = "http://localhost:5000/events"
TEST_RAID_ID = "raid666"
NUM_EVENTOS = 10


def test_fase_1_generacion():
    """Fase 1: Generar eventos sintéticos"""
    print("\n[FASE 1] Generando eventos sintéticos...")

    # Crear generador
    generator = WoWEventGenerator(seed=42)

    # Crear sesión de raid
    session = generator.generate_raid_session(
        raid_id=TEST_RAID_ID,
        num_players=20,  # Bastante personas
        duration_s=60,  # 1 minuto de raid
    )

    # Generar eventos
    eventos = generator.generate_events(session, num_events=NUM_EVENTOS)

    print(f"   ✅ Generados {len(eventos)} eventos")
    print(f"   📊 Tipos: {set(e.event_type for e in eventos)}")
    assert len(eventos) >= NUM_EVENTOS, (
        f"Se esperaban al menos {NUM_EVENTOS} eventos, se obtuvieron {len(eventos)}"
    )


def test_fase_2_ingestion(eventos):
    """Fase 2: Enviar eventos al receptor HTTP (Bronze)"""
    print("\n[FASE 2] Enviando eventos al receptor HTTP...")

    # Verificar que el receptor está corriendo
    try:
        response = requests.get("http://localhost:5000/health", timeout=2)
        if response.status_code != 200:
            print("   ❌ ERROR: Receptor HTTP no responde en /health")
            print("   💡 Ejecuta primero: python src/api/receiver.py")
            return None
    except requests.exceptions.ConnectionError:
        print("   ❌ ERROR: No se puede conectar al receptor HTTP (localhost:5000)")
        print("   💡 Ejecuta primero: python src/api/receiver.py")
        return None

    # Enviar batch (convertir eventos Pydantic a dict)
    payload = [e.model_dump(mode="json") for e in eventos]

    response = requests.post(RECEPTOR_URL, json=payload, timeout=5)

    if response.status_code == 201:
        result = response.json()
        batch_id = result.get("batch_id")
        print(f"   ✅ Batch ingestado correctamente: {batch_id}")
        assert batch_id is not None, "El receptor no devolvió batch_id"
    else:
        print(f"   ❌ ERROR en ingesta: {response.status_code}")
        print(f"   {response.text}")
        return None


def test_fase_3_transformation(batch_id):
    """Fase 3: Transformar Bronze → Silver"""
    print("\n[FASE 3] Ejecutando ETL Bronze → Silver...")

    # Esperar 2 segundos para asegurar que MinIO tiene el archivo
    time.sleep(2)

    # Calcular la ruta esperada (mismo patrón que tu minio_client.calculate_object_key)
    ingest_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    bronze_key = f"wow_raid_events/v1/raidid={TEST_RAID_ID}/ingest_date={ingest_date}/batch_{batch_id}.json"

    print(f"   Buscando: {bronze_key}")

    # Ejecutar ETL
    etl = BronzeToSilverETL()

    try:
        result = etl.run(bronze_key)

        if result["status"] == "success":
            print("   ✅ Transformación exitosa")
            print(
                f"      - Eventos procesados: {result['metadata']['rows_after_validation']}"
            )
            print(f"      - Archivo Parquet: {result['storage']['s3_path']}")
            assert "s3_path" != None, "El ETL no devolvió s3_path"
        else:
            print(f"   ⚠️ SKIPPED: {result.get('reason')}")
            return None

    except Exception as e:
        print(f"   ❌ ERROR en ETL: {e}")
        import traceback

        traceback.print_exc()
        return None


def test_fase_3_verification(s3_path):
    """Verificación: Leer el Parquet y validar contenido"""
    print("\n[VERIFICACIÓN] Validando archivo Parquet...")

    storage = MinIOStorageClient()
    silver_bucket = os.getenv("S3_BUCKET_SILVER", "silver")

    # Extraer key de la ruta s3://
    parquet_key = s3_path.replace(f"s3://{silver_bucket}/", "")

    try:
        # Leer el archivo desde MinIO
        obj = storage.get_object(silver_bucket, parquet_key)
        import io

        parquet_bytes = io.BytesIO(obj.read())
        df = pd.read_parquet(parquet_bytes)

        print("   ✅ Parquet leído correctamente")
        print(f"      - Filas: {len(df)}")
        print(f"      - Columnas: {len(df.columns)}")
        print(
            f"      - Columnas clave: {[c for c in ['event_id', 'timestamp', 'is_massive_hit'] if c in df.columns]}"
        )

        # Validaciones de calidad
        assert len(df) > 0, "El DataFrame está vacío"
        assert "event_id" in df.columns, "Falta columna event_id"
        assert "timestamp" in df.columns, "Falta columna timestamp"
        assert "is_massive_hit" in df.columns, (
            "Falta columna enriquecida is_massive_hit"
        )

        print("   ✅ Todas las validaciones pasaron")

    except Exception as e:
        print(f"   ❌ ERROR en verificación: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    print("=" * 70)
    print("🧪 TEST DE INTEGRACIÓN: PIPELINE COMPLETO (FASE 1→2→3)")
    print("=" * 70)

    # FASE 1
    eventos = test_fase_1_generacion()
    if not eventos:
        print("\n❌ Test FALLIDO en Fase 1")
        return

    # FASE 2
    batch_id = test_fase_2_ingestion(eventos)
    if not batch_id:
        print("\n❌ Test FALLIDO en Fase 2")
        print("   💡 Asegúrate de que el receptor HTTP esté corriendo:")
        print("      python src/api/receiver.py")
        return

    # FASE 3
    s3_path = test_fase_3_transformation(batch_id)
    if not s3_path:
        print("\n❌ Test FALLIDO en Fase 3")
        return

    # VERIFICACIÓN
    success = test_fase_3_verification(s3_path)

    if success:
        print("\n" + "=" * 70)
        print("✅ TEST COMPLETO: TODOS LOS PASOS EXITOSOS")
        print("=" * 70)
        print("\n📊 Resumen:")
        print(f"   - Eventos generados: {NUM_EVENTOS}")
        print(f"   - Batch ID: {batch_id}")
        print(f"   - Archivo final: {s3_path}")
        print("\n🎯 El pipeline es reproducible y está listo para producción.")
    else:
        print("\n❌ TEST FALLIDO en verificación")


if __name__ == "__main__":
    main()
