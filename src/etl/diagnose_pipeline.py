# src/etl/diagnose_pipeline.py
"""
Diagnóstico quirúrgico del pipeline Bronze → Silver → Gold.
Identifica exactamente dónde se pierde el dato de producción.
"""
import sys, io, json, re
from pathlib import Path
import boto3
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.storage.minio_client import MinIOStorageClient

s3 = MinIOStorageClient().s3

def count_bucket(bucket: str, prefix: str = "") -> int:
    total = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        total += len(page.get("Contents", []))
    return total

def list_bucket_sample(bucket: str, prefix: str = "", n: int = 6) -> list:
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append((obj["Key"], obj["Size"]))
            if len(keys) >= n:
                return keys
    return keys

# ─── 1. BRONZE: ¿Qué prefijos existen? ────────────────────────────────────
print("=" * 65)
print("  [1] BRONZE — Inventario de prefijos")
print("=" * 65)

old_prefix  = "wow_raid_events/v1/raidid="   # Flask (sin guión bajo)
new_prefix  = "wow_raid_events/v1/raid_id="  # ingest_bronze_production.py

old_count = count_bucket("bronze", old_prefix)
new_count = count_bucket("bronze", new_prefix)

print(f"  Prefijo VIEJO (raidid=):   {old_count:>6} archivos   ← test data Flask")
print(f"  Prefijo NUEVO (raid_id=):  {new_count:>6} archivos   ← producción")
print(f"  TOTAL Bronze:              {old_count + new_count:>6} archivos")

# ─── 2. BRONZE: Contenido real de un batch de producción ──────────────────
print("\n" + "=" * 65)
print("  [2] BRONZE — Estructura interna de un batch de producción")
print("=" * 65)

prod_samples = list_bucket_sample("bronze", new_prefix, n=3)
if prod_samples:
    key, size = prod_samples[0]
    print(f"  Leyendo: {key}  ({size/1024:.1f} KB)")
    raw = s3.get_object(Bucket="bronze", Key=key)["Body"].read()
    data = json.loads(raw)

    # Detectar formato
    if isinstance(data, list):
        events = data
        fmt = "array directo"
    elif isinstance(data, dict) and "events" in data:
        events = data["events"]
        fmt = "envelope (batch_id + events)"
    else:
        events = [data]
        fmt = "dict único"

    print(f"  Formato detectado: {fmt}")
    print(f"  Número de eventos en este batch: {len(events)}")

    if events:
        first = events[0]
        print(f"\n  Claves del primer evento ({len(first)} campos):")
        for k, v in list(first.items())[:12]:
            print(f"    {k:<40} = {str(v)[:45]}")

        # Verificar campo raid_id y event_type
        print(f"\n  raid_id  en eventos: {first.get('raid_id', '❌ NO EXISTE')}")
        print(f"  event_type:          {first.get('event_type', '❌ NO EXISTE')}")
        print(f"  timestamp:           {first.get('timestamp', '❌ NO EXISTE')}")

        # Contar unique raid_ids en el batch
        raid_ids  = {e.get("raid_id")  for e in events}
        etypes    = {e.get("event_type") for e in events}
        players   = {e.get("source_player_id") for e in events if e.get("source_player_id")}
        print(f"\n  Unique raid_ids en batch:    {raid_ids}")
        print(f"  Unique event_types:          {etypes}")
        print(f"  Unique source_player_ids:    {len(players)} jugadores")
else:
    print("  ❌ No se encontraron archivos con prefijo NUEVO.")

# ─── 3. SILVER: ¿Qué hay? ─────────────────────────────────────────────────
print("\n" + "=" * 65)
print("  [3] SILVER — Inventario y conteo de filas")
print("=" * 65)

silver_total = count_bucket("silver")
print(f"  Total archivos en Silver: {silver_total}")

# Por partición (raid_id + event_date)
paginator = s3.get_paginator("list_objects_v2")
partitions: dict = {}
for page in paginator.paginate(Bucket="silver"):
    for obj in page.get("Contents", []):
        key = obj["Key"]
        parts = key.split("/")
        raid_seg = next((s for s in parts if s.startswith("raid_id=")), None)
        date_seg = next((s for s in parts if s.startswith("event_date=")), None)
        if raid_seg and date_seg:
            p = (raid_seg.split("=",1)[1], date_seg.split("=",1)[1])
            partitions[p] = partitions.get(p, 0) + 1

print(f"  Particiones encontradas: {len(partitions)}")
print(f"\n  {'raid_id':<12} {'event_date':<14} {'nº parquets':>12}")
print(f"  {'─'*12} {'─'*14} {'─'*12}")
for (r, d), cnt in sorted(partitions.items()):
    print(f"  {r:<12} {d:<14} {cnt:>12}")

# ─── 4. SILVER: Muestra de filas de raid001 ───────────────────────────────
print("\n" + "=" * 65)
print("  [4] SILVER — Muestra de contenido (raid001, primer parquet)")
print("=" * 65)

silver_samples = list_bucket_sample("silver", "wow_raid_events/v1/raid_id=raid001", n=1)
if silver_samples:
    key, size = silver_samples[0]
    print(f"  Leyendo: {key}  ({size/1024:.1f} KB)")
    raw  = s3.get_object(Bucket="silver", Key=key)["Body"].read()
    df   = pd.read_parquet(io.BytesIO(raw))
    print(f"  Filas en este parquet:  {len(df)}")
    print(f"  Columnas: {list(df.columns)}")
    print(f"\n  Unique event_type:  {df['event_type'].unique().tolist() if 'event_type' in df.columns else '❌'}")
    print(f"  Unique players:     {df['source_player_id'].nunique() if 'source_player_id' in df.columns else '❌'}")
    if "damage_amount" in df.columns:
        print(f"  damage_amount sum:  {df['damage_amount'].sum():,.0f}")
else:
    print("  ❌ No se encontraron archivos Silver para raid001.")

print("\n" + "=" * 65)
print("  FIN DIAGNÓSTICO")
print("=" * 65)
