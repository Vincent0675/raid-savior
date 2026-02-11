from datetime import datetime, timezone
from typing import List
import argparse
import time
import requests
import os
import json
from typing import Sequence, Dict, Any
import uuid

from src.generators.raid_event_generator import WoWEventGenerator, RaidSession

def build_raid_events(
        generator: WoWEventGenerator,
        raid_id: str,
        num_players: int,
        duration_s: int,
        num_events:int,
) -> List["WoWRaidEvent"]:
    """
    Crea una RaidSession y genera num_events eventos para esa raid.
    """
    session: RaidSession = generator.generate_raid_session(
        raid_id=raid_id,
        num_players=num_players,
        duration_s=duration_s,
        start_time=None, # Por ahora usa el default (ahora-1h) ya definido
        boss_name="Ragnaros",
    )
    events = generator.generate_events(session=session, num_events=num_events)
    print(f"Raid {raid_id}: eventos generados = {len(events)}; "
      f"raid_ids únicos en esos eventos = "
      f"{set(e.raid_id for e in events)}")
    return events # Esto solo orquesta el generate_raid_session y generate_events que ya se tienen


def chunk_events(events, batch_size: int):
    """
    Generador que devuelve lisas de eventos de tamaño <= batch_size.
    """
    for i in range(0, len(events), batch_size):
        yield events[i:i + batch_size] # Más adelante se añadirá controles de RAM, por ahora simplemente troceamos la lista.

def send_batch_http(
        events_batch,
        receiver_url: str,
        batch_source: str = "massive-generator-v1",
):
    """
    Envía un batch al receptor HTTP /events y devuelve (status_code, json_response).
    """
    payload = [e.model_dump(mode="json") for e in events_batch]
    resp = requests.post(receiver_url, json=payload, timeout=10)
    data = None
    try:
        data = resp.json()
    except Exception:
        pass
    return resp.status_code, data

def write_batch_file(
    events_batch: Sequence,
    output_dir: str,
    raid_id: str,
    batch_index: int,
    batch_source: str = "massive-generator-v1",
):
    """
    Escribe un batch en disco como JSON.
    Un archivo por batch, agrupados por raid.
    """
    raid_dir = os.path.join(output_dir, raid_id)
    os.makedirs(raid_dir, exist_ok=True)

    filename = f"batch_{batch_index:04d}.json"
    path = os.path.join(raid_dir, filename)

    payload = {
        "batch_source": batch_source,
        "events": [e.model_dump(mode="json") for e in events_batch],
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    return path

def append_run_record(record: Dict[str, Any], catalog_path: str = "data/run_catalog.jsonl") -> None:
    """
    Añade un registro de ejecución (run) al catálogo en formato JSONL.
    Crea el directorio si no existe.
    """
    catalog_dir = os.path.dirname(catalog_path) or "."
    os.makedirs(catalog_dir, exist_ok=True)

    with open(catalog_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-raids", type=int, default=1)
    parser.add_argument("--num-events-per-raid", type=int, default=2000)
    parser.add_argument("--num-players", type=int, default=20)
    parser.add_argument("--duration-s", type=int, default=300)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--receiver-url", type=str, default="http://localhost:5000/events")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--output-mode",
        type=str,
        choices=["http", "offline"],
        default="http",
        help="¿A dónde quieres destinar los batches? http (Flask Receptor) u offline (JSON local)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/bronze",
        help="Directorio base para modo offline",
    )
    args = parser.parse_args()

    generator = WoWEventGenerator(seed=args.seed)

    total_events_sent = 0
    t0 = time.time()

    for raid_index in range(args.num_raids):
        raid_id = f"raid{raid_index+1:03d}"
        events = build_raid_events(
            generator=generator,
            raid_id=raid_id,
            num_players=args.num_players,
            duration_s=args.duration_s,
            num_events=args.num_events_per_raid,
        )

        batch_index = 0

        for batch in chunk_events(events, args.batch_size):
            batch_index += 1

            if args.output_mode == "http":
                status, data = send_batch_http(batch, args.receiver_url)

                if not (200 <= status < 300):
                    print(f"[ERROR] HTTP {status} para raid {raid_id}")
                    print(data)
                    return

                # Ya verificamos que status HTTP es 2xx, así que podemos confiar en eso
                if not data:
                    print(f"[WARNING] Respuesta vacía para raid {raid_id}")
                    # No hacemos return aquí, porque el HTTP fue exitoso

                # Mostrar el status del JSON para trazabilidad
                response_status = data.get('status', 'N/A') if data else 'N/A'
                batch_id = data.get('batch_id', 'N/A') if data else 'N/A'
                
                print(f"[OK][HTTP] raid={raid_id} batch_event_count={len(batch)} http_status={status} response_status={response_status} batch_id={batch_id}")

            elif args.output_mode == "offline":
                path = write_batch_file(
                    events_batch=batch,
                    output_dir=args.output_dir,
                    raid_id=raid_id,
                    batch_index=batch_index,
                )
                print(f"[OK][FILE] raid={raid_id} batch_event_count={len(batch)} path={path}")

            total_events_sent += len(batch)

    elapsed = time.time() - t0
    print(f"Eventos enviados/generados: {total_events_sent}")
    print(f"Tiempo total: {elapsed:.2f}s  ->  {total_events_sent/elapsed:.1f} ev/s")

    # ===== Registro en el catálogo de runs =====
    t_start_utc = datetime.fromtimestamp(t0, tz=timezone.utc).isoformat()
    t_end_utc = datetime.fromtimestamp(t0 + elapsed, tz=timezone.utc).isoformat()

    run_id = f"run_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid.uuid4().hex[:8]}"

    record: Dict[str, Any] = {
        "run_id": run_id,
        "timestamp_start": t_start_utc,
        "timestamp_end": t_end_utc,
        "mode": args.output_mode,
        "num_raids": args.num_raids,
        "num_events_per_raid": args.num_events_per_raid,
        "batch_size": args.batch_size,
        "num_players": args.num_players,
        "duration_s": args.duration_s,
        "seed": args.seed,
        "output_dir": args.output_dir,
        "events_generated": total_events_sent,
        "elapsed_seconds": elapsed,
        "events_per_second": total_events_sent / elapsed if elapsed > 0 else None,
    }

    if args.output_mode == "http":
        record["receiver_url"] = args.receiver_url

    append_run_record(record)
    print(f"[CATALOG] Run registrado con run_id={run_id}")
    
if __name__ == "__main__":
    print(">>> generate_massive_http: main() iniciado")
    main()