#!/usr/bin/env python3
"""
Ingesta producción: archivos JSON locales → MinIO Bronze.

Lee la estructura local:
  data/bronze/
  ├── raid001/
  │   ├── batch_000.json   (500 eventos c/u)
  │   └── ... (101 archivos)
  └── raidXXX/

Sube cada archivo a MinIO manteniendo particionamiento Hive-style:
  s3://bronze/wow_raid_events/v1/raid_id={raid_id}/ingest_date={date}/batch_{name}.json

Uso:
    python src/etl/ingest_bronze_production.py
    python src/etl/ingest_bronze_production.py --dry-run   # sin subir nada
"""

import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone

from tqdm import tqdm

# ── Asegurar imports del proyecto ─────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.storage.minio_client import MinIOStorageClient

# ── Configuración ─────────────────────────────────────────────────────────
BRONZE_ROOT  = Path("data/bronze/production")
BUCKET       = "bronze"
S3_PREFIX    = "wow_raid_events/v1"
INGEST_DATE  = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def ensure_bucket(storage: MinIOStorageClient, bucket: str) -> None:
    """Crea el bucket si no existe."""
    try:
        storage.s3.head_bucket(Bucket=bucket)
    except Exception:
        storage.s3.create_bucket(Bucket=bucket)
        print(f"  ✅ Bucket '{bucket}' creado.")


def build_s3_key(raid_id: str, filename: str) -> str:
    """
    Construye la clave S3 con particionamiento Hive-style.
    Ejemplo: wow_raid_events/v1/raid_id=raid001/ingest_date=2026-02-24/batch_000.json
    """
    return f"{S3_PREFIX}/raid_id={raid_id}/ingest_date={INGEST_DATE}/{filename}"


def collect_files(root: Path) -> list[tuple[str, Path]]:
    """
    Recorre las subcarpetas de raids y devuelve pares (raid_id, filepath).
    Soporta tanto subcarpetas como JSONs en la raíz directamente.
    """
    pairs = []
    raid_dirs = sorted(d for d in root.iterdir() if d.is_dir())

    if not raid_dirs:
        # JSON directamente en la raíz (caso plano)
        for f in sorted(root.glob("*.json")):
            pairs.append((root.name, f))
    else:
        for raid_dir in raid_dirs:
            for f in sorted(raid_dir.glob("*.json")):
                pairs.append((raid_dir.name, f))

    return pairs


def main(dry_run: bool = False) -> None:
    print("=" * 65)
    print("  Ingesta Bronze — JSON local → MinIO")
    print(f"  Fuente:  {BRONZE_ROOT.resolve()}")
    print(f"  Destino: s3://{BUCKET}/{S3_PREFIX}/")
    print(f"  Fecha:   {INGEST_DATE}")
    if dry_run:
        print("  ⚠️  MODO DRY-RUN — no se sube nada")
    print("=" * 65)

    # ── Validar directorio fuente ──────────────────────────────────────────
    if not BRONZE_ROOT.exists():
        print(f"\n❌ No encuentro: {BRONZE_ROOT.resolve()}")
        print("   Ajusta BRONZE_ROOT en el script.")
        sys.exit(1)

    # ── Recolectar archivos ────────────────────────────────────────────────
    files = collect_files(BRONZE_ROOT)
    if not files:
        print("\n❌ No se encontraron archivos JSON.")
        sys.exit(1)

    raids_found = sorted({raid_id for raid_id, _ in files})
    print(f"\n  Raids:   {len(raids_found)}")
    print(f"  Archivos:{len(files):>6}")
    print(f"  Esperado:~{len(files) * 500:,} eventos\n")

    if dry_run:
        print("  Muestra de claves S3 que se generarían:")
        for raid_id, fpath in files[:4]:
            print(f"    {build_s3_key(raid_id, fpath.name)}")
        print(f"    ... y {len(files) - 4} más")
        return

    # ── Conectar a MinIO ───────────────────────────────────────────────────
    storage = MinIOStorageClient()
    ensure_bucket(storage, BUCKET)

    # ── Subir archivos ─────────────────────────────────────────────────────
    success = 0
    failed  = 0
    total_bytes = 0

    with tqdm(total=len(files), desc="Subiendo a Bronze", unit="archivo") as pbar:
        for raid_id, fpath in files:
            s3_key = build_s3_key(raid_id, fpath.name)
            try:
                raw_bytes = fpath.read_bytes()
                storage.s3.put_object(
                    Bucket=BUCKET,
                    Key=s3_key,
                    Body=raw_bytes,
                    ContentType="application/json",
                    Metadata={
                        "raid-id":        raid_id,
                        "ingest-date":    INGEST_DATE,
                        "source-file":    fpath.name,
                        "source-system":  "ingest_bronze_production.py",
                    },
                )
                success     += 1
                total_bytes += len(raw_bytes)

            except Exception as e:
                failed += 1
                tqdm.write(f"  ❌ {fpath.name}: {e}")

            pbar.update(1)

    # ── Reporte final ──────────────────────────────────────────────────────
    print()
    print("=" * 65)
    print("  RESULTADO")
    print("=" * 65)
    print(f"  ✅ Subidos:  {success:>6} archivos")
    print(f"  ❌ Fallidos: {failed:>6} archivos")
    print(f"  📦 Total:    {total_bytes / 1024 / 1024:.1f} MB transferidos")
    print()

    if failed == 0:
        print("  ✅ Bronze completo. Siguiente paso:")
        print()
        print("     python src/etl/run_bronze_to_silver.py")
        print()
    else:
        print("  ⚠️  Revisa los errores antes de continuar.")
    print("=" * 65)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestión local → MinIO Bronze")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra qué se subiría sin ejecutar nada"
    )
    args = parser.parse_args()
    main(dry_run=args.dry_run)
