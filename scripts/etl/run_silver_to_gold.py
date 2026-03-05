"""
run_silver_to_gold.py — Runner CLI para el pipeline Silver → Gold.

Modos de ejecución:
  --all                              → procesa TODAS las particiones Silver
  --raid-id raid001 --event-date ... → procesa UNA partición concreta

Uso:
    python -m src.etl.run_silver_to_gold --all
    python -m src.etl.run_silver_to_gold --raid-id raid001 --event-date 2026-02-25
    python -m src.etl.run_silver_to_gold --all --log-level DEBUG
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

    # Modo: --all o (--raid-id + --event-date)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--all",
        action="store_true",
        help="Descubre y procesa TODAS las particiones disponibles en Silver.",
    )
    mode.add_argument(
        "--raid-id",
        metavar="RAID_ID",
        help="Identificador de la raid (ej. raid001). Requiere --event-date.",
    )

    parser.add_argument(
        "--event-date",
        metavar="YYYY-MM-DD",
        help="Fecha de la partición Silver. Requerido si se usa --raid-id.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Nivel de logging (default: INFO).",
    )
    return parser.parse_args()


def _validate_args(args: argparse.Namespace) -> bool:
    """Valida coherencia entre argumentos."""
    if args.raid_id and not args.event_date:
        print("❌ --event-date es obligatorio cuando se usa --raid-id.")
        return False
    if args.event_date and not args.raid_id:
        print("❌ --raid-id es obligatorio cuando se usa --event-date.")
        return False
    if args.raid_id:
        try:
            datetime.strptime(args.event_date, "%Y-%m-%d")
        except ValueError:
            print(f"❌ --event-date debe ser YYYY-MM-DD. Recibido: '{args.event_date}'")
            return False
    return True


def main() -> int:
    args = _parse_args()

    if not _validate_args(args):
        return 1

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger("run_silver_to_gold")

    try:
        from src.analytics.gold_layer import GoldLayerETL
        etl = GoldLayerETL()

        # ── Modo --all ──────────────────────────────────────────────────
        if args.all:
            logger.info("=" * 60)
            logger.info("Pipeline Silver → Gold  [MODO BATCH — todas las particiones]")
            logger.info("=" * 60)

            summary = etl.run_all()

            print("\n" + "=" * 60)
            print(f"  RESULTADO BATCH")
            print("=" * 60)
            print(f"  Total particiones:  {summary['total_partitions']}")
            print(f"  ✅ Exitosas:        {summary['successful']}")
            print(f"  ❌ Fallidas:        {summary['failed']}")

            if summary["errors"]:
                print("\n  Particiones con error:")
                for err in summary["errors"]:
                    print(f"    • {err['raid_id']} / {err['event_date']}: {err['error']}")

            print()
            print(json.dumps(summary["results"], indent=2, default=str))
            return 0 if summary["failed"] == 0 else 1

        # ── Modo --raid-id + --event-date ───────────────────────────────
        logger.info("=" * 60)
        logger.info("Pipeline Silver → Gold  [MODO PARTICIÓN ÚNICA]")
        logger.info("  raid_id    : %s", args.raid_id)
        logger.info("  event_date : %s", args.event_date)
        logger.info("=" * 60)

        result = etl.run_for_partition(
            raid_id=args.raid_id,
            event_date=args.event_date,
        )
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
