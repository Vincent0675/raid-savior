import os
import json
import argparse
import time
from typing import List

import requests


def find_batch_files(base_dir: str) -> List[str]:
    """
    Devuelve la lista de rutas a batch_*.json dentro de base_dir, recursivamente.
    Ejemplo de layout esperado:
      base_dir/raid001/batch_0001.json
      base_dir/raid001/batch_0002.json
      base_dir/raid002/batch_0001.json
    """
    batch_files = []
    for root, dirs, files in os.walk(base_dir):
        for name in files:
            if name.startswith("batch_") and name.endswith(".json"):
                batch_files.append(os.path.join(root, name))
    batch_files.sort()
    return batch_files


def send_batch_file(path: str, receiver_url: str, timeout: float = 10.0):
    """
    Lee un batch offline y lo envía a /events.
    """
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    resp = requests.post(receiver_url, json=payload, timeout=timeout)
    data = None
    try:
        data = resp.json()
    except Exception:
        pass
    return resp.status_code, data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-dir",
        type=str,
        default="data/bronze",
        help="Directorio base con batches offline (raidXXX/batch_YYYY.json)",
    )
    parser.add_argument(
        "--receiver-url",
        type=str,
        default="http://localhost:5000/events",
        help="URL del endpoint /events del receptor Flask",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=0,
        help="Máximo número de ficheros a reenviar (0 = todos)",
    )
    args = parser.parse_args()

    print(">>> replay_offline_to_http: inicio")

    batch_files = find_batch_files(args.input_dir)
    if args.max_batches > 0:
        batch_files = batch_files[: args.max_batches]

    print(f"Encontrados {len(batch_files)} ficheros batch para reenvío")

    t0 = time.time()
    total_events = 0

    for i, path in enumerate(batch_files, start=1):
        status, data = send_batch_file(path, args.receiver_url)

        # Log mínimo por batch
        if not (200 <= status < 300):
            print(f"[ERROR] HTTP {status} para batch {path}")
            print(data)
            return

        eventcount = data.get("eventcount") if isinstance(data, dict) else None
        total_events += eventcount or 0

        print(f"[OK] {i}/{len(batch_files)} status={status} "
              f"batch_id={data.get('batchid')} eventcount={eventcount} path={path}")

    elapsed = time.time() - t0
    print(f"Total eventos reenviados: {total_events}")
    print(f"Tiempo total: {elapsed:.2f}s -> {total_events/elapsed:.1f} ev/s")


if __name__ == "__main__":
    main()
