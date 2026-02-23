# Contexto del Proyecto: WoW Raid Telemetry Pipeline

- Repositorio: github.com/Vincent0675/raid-savior   
- Fecha del último commit: 23 de febrero de 2026   
- Estado: Arquitectura Medallion completa y operativa (Bronze → Silver → Gold)   

# Stack tecnológico

    Python 3.10 + Conda/Mamba, entorno wow-telemetry

    Pydantic v2 — validación Schema-on-Write

    NumPy — generación de distribuciones estadísticas (Normal, Bernoulli)

    Pandas + PyArrow — ETL Silver y Gold, formato Parquet (compresión Snappy)

    Flask — receptor HTTP (POST /events, SSE en /stream/events)

    MinIO — object storage compatible S3 (3 buckets: bronze, silver, gold)

    Docker — contenedorización de MinIO

    OS: Pop OS! 22.04, GPU RTX 3050 Laptop (4GB VRAM, CUDA disponible)

# Arquitectura Medallion — Estado actual

```text
Bronze (JSON raw validado)
  → Silver (Parquet, particionado Hive-style por raid_id/event_date)
      → Gold (modelo semidimensional Parquet)
```

## Capa Bronze

    Receptor Flask valida con Pydantic v2 (Schema-on-Write) antes de guardar

    Almacena JSON en MinIO bajo wow-raid-events/raid_id=.../ingest_date=.../

    6 tipos de eventos: combat_damage, heal, player_death, spell_cast, boss_phase, mana_regeneration

## Capa Silver

    ETL bronze_to_silver.py: limpieza, deduplicación, conversión de tipos, enriquecimiento

    Salida Parquet (Snappy) particionado: wow_raid_events/v1/raid_id=.../event_date=.../

    ~50.000 eventos por raid tras filtros de calidad

## Capa Gold — Modelo semidimensional

| Tabla                  | Path                                                | Descripción                                |
| ---------------------- | --------------------------------------------------- | ------------------------------------------ |
| dim_player             | dim_player/player_id=all/dim_player.parquet         | Global, acumula todos los jugadores vistos |
| dim_raid               | dim_raid/raid_id={id}/dim_raid.parquet              | Una fila por raid                          |
| fact_raid_summary      | fact_raid_summary/raid_id={id}/event_date={d}/      | KPIs agregados por raid                    |
| fact_player_raid_stats | fact_player_raid_stats/raid_id={id}/event_date={d}/ | Métricas por jugador por raid              |

- dim_player implementa SCD Tipo 1 con upsert: merge outer por player_id, actualiza last_seen_date (max) y total_raids (+N), preserva first_seen_date (min) y campos estables.   
- Validación de schemas Pydantic v2 pre-escritura (fail-fast antes de tocar MinIO).   

## Generador de datos sintéticos

    Clase WoWEventGenerator(seed=42) en src/generators/raid_event_generator.py

    Genera jugadores con player_id aleatorio (uuid4().hex[:8]) por cada raid → comportamiento "PUG" (jugadores distintos por raid, intencionado)

    Script de ingesta masiva: src/scripts/generate_massive_http.py

    Parámetros: --num-raids, --num-events-per-raid, --num-players, --batch-size, --output-mode http|offline

## Comandos principales

```bash
# Ingesta Bronze (modo HTTP, Flask levantado)
python -m src.scripts.generate_massive_http --num-raids 2 --num-events-per-raid 50000

# ETL Bronze → Silver
python -m src.etl.run_bronze_to_silver --raid-id raid001 --ingest-date 2026-02-19

# ETL Silver → Gold
python -m src.etl.run_silver_to_gold --raid-id raid001 --event-date 2026-02-19
```

## Historial de fases completadas

| Fase  | Descripción                                              | Commit clave |
| ----- | -------------------------------------------------------- | ------------ |
| 1     | Schema Pydantic v2 + generador sintético NumPy           | 80ce21a      |
| 2     | Receptor HTTP Flask + ingesta MinIO Bronze               | e89bbfe      |
| 3     | ETL Bronze → Silver (Parquet, limpieza, enriquecimiento) | bf8c072      |
| 4     | ETL Silver → Gold (modelo semidimensional, KPIs)         | 1d95bc5      |
| 4 fix | Upsert SCD Tipo 1 en dim_player + fix TypeError fechas   | 666e8e3      |

## Próximo objetivo
Refactorizar el generador para soportar un roster fijo de jugadores (misma guild en múltiples raids), de modo que dim_player acumule total_raids > 1 para los mismos jugadores. Actualmente genera player_id UUID aleatorio por raid (comportamiento PUG intencionado, pero no realista para análisis de progresión de jugadores a largo plazo).