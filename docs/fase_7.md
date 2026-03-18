# Fase 7 — Table Formats con Apache Iceberg (ACID y Time Travel)

## Objetivo

La Fase 7 introduce Apache Iceberg en las capas Silver y Gold del pipeline para evolucionar desde directorios Parquet tradicionales hacia tablas gestionadas con metadatos transaccionales. El objetivo es añadir capacidades de ACID, time travel, mutaciones controladas y evolución de schema, manteniendo la arquitectura Medallion sobre MinIO.

## Estado actual

La Fase 7 está **completada** a fecha 2026-03-18. Todas las subfases (7.1 → 7.5)
han sido ejecutadas y verificadas sobre el entorno local con PySpark 3.5 y MinIO.

## Qué problema resuelve

Hasta la Fase 6, Silver y Gold funcionaban como directorios de archivos Parquet. Ese enfoque era válido para lectura y escritura por lotes, pero limitaba la evolución del sistema al no ofrecer transacciones, historial de versiones ni una base sólida para cambios de schema y uso multi-motor.

## Alcance de la fase

Incluye:

- Catálogo Iceberg sobre MinIO
- Migración progresiva de Silver a Iceberg
- Migración progresiva de Gold a Iceberg
- Preparación para time travel y correcciones de negocio
- Base técnica para mutaciones controladas y evolución de schema

No incluye:

- Cambios estructurales en Bronze
- Dashboards finales
- Serving por API
- Modelado de machine learning
- Integración con datos reales externos

## Subfases

- 7.1 Catálogo Iceberg sobre MinIO
- 7.2 Silver ACID (eventos limpios)
- 7.3 Gold ACID: tablas de hechos
- 7.4 Gold ACID: dimensiones
- 7.5 Time travel y correcciones de negocio

## Decisiones técnicas

- PySpark 3.5 como motor principal
- Catálogo tipo Hadoop catalog sobre MinIO
- Namespaces `wow.silver` y `wow.gold`
- Mantener compatibilidad con buenas prácticas de Python 3.10.x
- Posible soporte adicional con Pytest, Ruff y MyPy

## Resultados obtenidos

La Fase 7 ha convertido Silver y Gold en capas ACID con historial consultable.
Las tablas Iceberg están operativas sobre MinIO con soporte de time travel
demostrado, mutaciones versionadas aplicadas y continuidad total de la
arquitectura Medallion.

Tablas resultantes:

- `wow.silver.raid_events` — ~600 000 eventos, ACID, particionada
- `wow.gold.fact_raid_summary` — 12 filas
- `wow.gold.fact_player_raid_stats` — operativa
- `wow.gold.dim_player` — 312 jugadores, MERGE INTO
- `wow.gold.dim_raid` — 12 raids, MERGE INTO, 2 snapshots verificados con time travel

## Estado

**Estado de fase:** completada — 2026-03-18  
**Subfase cerrada:** 7.5 Time travel y correcciones de negocio  
**Documentación extendida:** `docs/fase_7-5_resultados.md` y `docs/architecture/fase_7_table_formats_apache_iceberg.md`