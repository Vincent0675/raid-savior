"""
run_silver_to_gold.py — Runner CLI para el pipeline Silver → Gold.

Entrypoint de producción/desarrollo. Ejecuta GoldLayerETL.run_for_partition()
para una partición concreta sin pasar por la suite de tests.

Uso:
    python -m src.etl.run_silver_to_gold --raid-id raid001 --event-date 2026-02-19
    python -m src.etl.run_silver_to_gold --raid-id raid001 --event-date 2026-02-19 --log-level DEBUG
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="run_silver_to_gold",
        description="Pipeline ETL Silver → Gold — WoW Raid Telemetry.",
    )
    parser.add_argument(
        "--raid-id",
        required=True,
        help="Identificador de la raid (ej. raid001).",
    )
    parser.add_argument(
        "--event-date",
        required=True,
        help="Fecha de la partición Silver en formato YYYY-MM-DD.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logging (default: INFO).",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("run_silver_to_gold")

    # Validar formato de fecha antes de arrancar el pipeline
    try:
        datetime.strptime(args.event_date, "%Y-%m-%d")
    except ValueError:
        logger.error(
            "--event-date debe estar en formato YYYY-MM-DD. Recibido: '%s'",
            args.event_date,
        )
        return 1

    logger.info("=" * 60)
    logger.info("Pipeline Silver → Gold")
    logger.info("  raid_id    : %s", args.raid_id)
    logger.info("  event_date : %s", args.event_date)
    logger.info("=" * 60)

    try:
        from src.analytics.gold_layer import GoldLayerETL

        etl = GoldLayerETL()
        result = etl.run_for_partition(
            raid_id=args.raid_id,
            event_date=args.event_date,
        )

        logger.info("Pipeline completado exitosamente.")
        print(json.dumps(result, indent=2, default=str))
        return 0

    except (ValueError, RuntimeError) as exc:
        logger.error("FALLO en el pipeline: %s", exc)
        return 1

    except Exception as exc:
        logger.error("ERROR INESPERADO: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
