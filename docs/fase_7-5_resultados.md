# Subfase 7.5 — Time Travel y Correcciones de Negocio sobre Iceberg Gold

**Estado**: ✅ COMPLETADA — 2026-03-18  
**Fase padre**: Fase 7 — Migración Silver/Gold a Apache Iceberg

---

## Objetivo

Demostrar las capacidades de time travel y mutación controlada de Apache
Iceberg sobre tablas Gold ya operativas, cerrando los dos últimos criterios
de la Fase 7.

---

## Capacidades demostradas

### Time Travel

- `VERSION AS OF <snapshot_id>`: consulta determinista por ID de snapshot
- `TIMESTAMP AS OF '<ts>'`: consulta por marca de tiempo

Ambas modalidades confirmadas sobre `wow.gold.dim_raid`.

### Corrección de negocio (UPDATE + verificación)

Ciclo completo ejecutado:
1. Detección del placeholder `boss_name = 'Unknown Boss'` en los 12 raids
2. Captura del `snapshot_id` PRE-corrección: `5290609065853948740`
3. `UPDATE` aplicado → genera snapshot nuevo: `4677864922372387292` (operation: `overwrite`)
4. Verificación: tabla actual muestra boss real; `VERSION AS OF` snapshot anterior
   devuelve el estado original intacto

---

## Snapshots registrados en wow.gold.dim_raid

| snapshot_id          | committed_at                | operation |
|----------------------|-----------------------------|-----------|
| 5290609065853948740  | 2026-03-12 17:36:33.781     | append    |
| 4677864922372387292  | 2026-03-18 12:18:18.982     | overwrite |

---

## Decisiones técnicas

| Decisión | Motivo |
|---|---|
| Script de demostración separado (`gold_iceberg_time_travel.py`) | No contamina el ETL de producción |
| UPDATE con `CASE WHEN` sobre `boss_name = 'Unknown Boss'` | Corrección selectiva, no reescritura total |
| Verificación con `VERSION AS OF` al final de Parte B | Prueba explícita de inmutabilidad del snapshot previo |

---

## Script

`src/etl/gold_iceberg_time_travel.py` — script de demostración, no ETL de producción.

Salida obtenida:

```text
26/03/18 12:18:09 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

============================================================
PARTE 0 — Estado actual de dim_raid
============================================================

>>> Filas actuales con boss_name = 'Unknown Boss':
26/03/18 12:18:12 WARN MetricsConfig: Cannot locate configuration: tried hadoop-metrics2-s3a-file-system.properties,hadoop-metrics2.properties
+-------+------------+----------+
|raid_id|boss_name   |event_date|
+-------+------------+----------+
|raid001|Unknown Boss|2026-02-25|
|raid002|Unknown Boss|2026-02-25|
|raid003|Unknown Boss|2026-02-25|
|raid004|Unknown Boss|2026-02-25|
|raid005|Unknown Boss|2026-02-25|
|raid006|Unknown Boss|2026-02-25|
|raid007|Unknown Boss|2026-02-25|
|raid008|Unknown Boss|2026-02-25|
|raid009|Unknown Boss|2026-02-25|
|raid010|Unknown Boss|2026-02-25|
|raid666|Unknown Boss|2026-03-05|
|raid999|Unknown Boss|2026-03-05|
+-------+------------+----------+


>>> HISTORIAL DE SNAPSHOTS — wow.gold.dim_raid
+-------------------+-----------------------+---------+
|snapshot_id        |committed_at           |operation|
+-------------------+-----------------------+---------+
|5290609065853948740|2026-03-12 17:36:33.781|append   |
+-------------------+-----------------------+---------+


============================================================
PARTE A — Time Travel: VERSION AS OF / TIMESTAMP AS OF
============================================================

>>> A1: VERSION AS OF 5290609065853948740
    (snapshot creado en: 2026-03-12 17:36:33.781000)
+-------+------------+----------+
|raid_id|boss_name   |event_date|
+-------+------------+----------+
|raid001|Unknown Boss|2026-02-25|
|raid002|Unknown Boss|2026-02-25|
|raid003|Unknown Boss|2026-02-25|
|raid004|Unknown Boss|2026-02-25|
|raid005|Unknown Boss|2026-02-25|
|raid006|Unknown Boss|2026-02-25|
|raid007|Unknown Boss|2026-02-25|
|raid008|Unknown Boss|2026-02-25|
|raid009|Unknown Boss|2026-02-25|
|raid010|Unknown Boss|2026-02-25|
|raid666|Unknown Boss|2026-03-05|
|raid999|Unknown Boss|2026-03-05|
+-------+------------+----------+


>>> A2: TIMESTAMP AS OF '2026-03-12 17:36:33.781000'
+-------+------------+----------+
|raid_id|boss_name   |event_date|
+-------+------------+----------+
|raid001|Unknown Boss|2026-02-25|
|raid002|Unknown Boss|2026-02-25|
|raid003|Unknown Boss|2026-02-25|
|raid004|Unknown Boss|2026-02-25|
|raid005|Unknown Boss|2026-02-25|
|raid006|Unknown Boss|2026-02-25|
|raid007|Unknown Boss|2026-02-25|
|raid008|Unknown Boss|2026-02-25|
|raid009|Unknown Boss|2026-02-25|
|raid010|Unknown Boss|2026-02-25|
|raid666|Unknown Boss|2026-03-05|
|raid999|Unknown Boss|2026-03-05|
+-------+------------+----------+


============================================================
PARTE B — Corrección de negocio: UPDATE boss_name
============================================================

>>> HISTORIAL DE SNAPSHOTS — wow.gold.dim_raid
+-------------------+-----------------------+---------+
|snapshot_id        |committed_at           |operation|
+-------------------+-----------------------+---------+
|5290609065853948740|2026-03-12 17:36:33.781|append   |
+-------------------+-----------------------+---------+


>>> Snapshot PRE-corrección capturado: 5290609065853948740

>>> Aplicando UPDATE en dim_raid (boss_name Unknown Boss → nombre real)...
    UPDATE aplicado.

>>> Estado POST-corrección (tabla actual):
+-------+----------+----------+
|raid_id|boss_name |event_date|
+-------+----------+----------+
|raid001|Onyxia    |2026-02-25|
|raid002|Ragnaros  |2026-02-25|
|raid003|Nefarian  |2026-02-25|
|raid004|CThun     |2026-02-25|
|raid005|KelThuzad |2026-02-25|
|raid006|Illidan   |2026-02-25|
|raid007|Arthas    |2026-02-25|
|raid008|Archimonde|2026-02-25|
|raid009|Kiljaeden |2026-02-25|
|raid010|Deathwing |2026-02-25|
|raid666|Sargeras  |2026-03-05|
|raid999|The Jailer|2026-03-05|
+-------+----------+----------+


>>> Estado PRE-corrección (VERSION AS OF 5290609065853948740):
+-------+------------+----------+
|raid_id|boss_name   |event_date|
+-------+------------+----------+
|raid001|Unknown Boss|2026-02-25|
|raid002|Unknown Boss|2026-02-25|
|raid003|Unknown Boss|2026-02-25|
|raid004|Unknown Boss|2026-02-25|
|raid005|Unknown Boss|2026-02-25|
|raid006|Unknown Boss|2026-02-25|
|raid007|Unknown Boss|2026-02-25|
|raid008|Unknown Boss|2026-02-25|
|raid009|Unknown Boss|2026-02-25|
|raid010|Unknown Boss|2026-02-25|
|raid666|Unknown Boss|2026-03-05|
|raid999|Unknown Boss|2026-03-05|
+-------+------------+----------+


>>> HISTORIAL DE SNAPSHOTS — wow.gold.dim_raid
+-------------------+-----------------------+---------+
|snapshot_id        |committed_at           |operation|
+-------------------+-----------------------+---------+
|5290609065853948740|2026-03-12 17:36:33.781|append   |
|4677864922372387292|2026-03-18 12:18:18.982|overwrite|
+-------------------+-----------------------+---------+

[SparkSession] Sesión detenida correctamente.
```
---

## Criterios de cierre de Fase 7

| Criterio | Estado |
|---|---|
| Catálogo Iceberg operativo sobre MinIO | ✅ |
| Silver materializada como tabla Iceberg ACID | ✅ |
| Gold materializada en tablas Iceberg (hechos y dimensiones) | ✅ |
| Soporte funcional de time travel | ✅ |
| Correcciones de negocio sobre base versionada | ✅ |
| Continuidad de la arquitectura Medallion sin ruptura de capas | ✅ |
