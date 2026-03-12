# Subfase 7.4 — Dimensiones Gold Iceberg: `dim_player` vs `dim_raid`

Documento de referencia para entender las diferencias de diseño entre las dos
dimensiones implementadas en la Subfase 7.4. Útil para revisión de código,
onboarding y como plantilla para futuras dimensiones.

---

## 1. ¿Qué es una dimensión en este modelo?

En la arquitectura Medallion de este proyecto, las tablas **Gold** se dividen en:

- **Facts** (`fact_raid_summary`, `fact_player_raid_stats`): métricas numéricas,
  cambian en cada ejecución.
- **Dimensions** (`dim_player`, `dim_raid`): describen **quién** o **qué** participó.
  Son de baja volatilidad — sus atributos cambian poco o nada.

Ambas dimensiones usan **MERGE INTO (upsert)** para ser idempotentes: se pueden
re-ejecutar sin duplicar filas.

---

## 2. Comparativa de diseño

| Aspecto | `dim_player` | `dim_raid` |
|---|---|---|
| **Script** | `src/etl/gold_iceberg_dim_player.py` | `src/etl/gold_iceberg_dim_raid.py` |
| **Tabla Iceberg** | `wow.gold.dim_player` | `wow.gold.dim_raid` |
| **Fuente** | `wow.silver.raid_events` | `wow.silver.raid_events` |
| **Clave natural (MERGE)** | `player_id` | `raid_id` |
| **Grain** | 1 fila por jugador único | 1 fila por raid único |
| **`groupBy`** | `source_player_id` | `raid_id` |
| **Columna inmutable en MERGE** | `first_seen_date` | `event_date` |
| **Columnas calculadas** | `player_name`, `player_class`, `total_raids`... | `raid_size` (countDistinct) |
| **Columnas literales** | ninguna | `boss_name`, `difficulty`, `duration_target_ms` |
| **`withColumnRenamed`** | sí: `source_player_id` → `player_id` | no necesario |
| **Partición Iceberg** | `player_class` | `event_date` |
| **Vista staging** | `staging_dim_player` | `staging_dim_raid` |

---

## 3. Grain: la diferencia fundamental

El **grain** es la pregunta que responde cada fila de la tabla.

- `dim_player` → *"¿Quién es este jugador y cuándo apareció por primera vez?"*
  Un jugador puede participar en muchos raids. La dimensión lo consolida en
  **una sola fila** con sus atributos estables (clase, rol) y estadísticas de
  presencia histórica (`total_raids`, `first_seen_date`, `last_seen_date`).

- `dim_raid` → *"¿Qué características tiene este raid?"*
  Un raid es un evento único e irrepetible. La dimensión lo describe con
  **una sola fila** con sus metadatos (boss, dificultad, tamaño, fecha).

---

## 4. Por qué `event_date` es inmutable en `dim_raid`

En `dim_player`, `first_seen_date` se excluye del `UPDATE SET` del MERGE porque
representa **cuándo el jugador apareció por primera vez** — ese hecho histórico no
debe sobreescribirse en re-ejecuciones futuras.

En `dim_raid`, `event_date` cumple el mismo rol: es la fecha en que ocurrió el raid.
Un raid no "cambia de fecha" si el pipeline se re-ejecuta. Excluirla del UPDATE
garantiza que el historial temporal del catálogo Iceberg permanece correcto.

```sql
-- dim_player: first_seen_date NO aparece en UPDATE SET
WHEN MATCHED THEN UPDATE SET
    player_name    = source.player_name,
    total_raids    = source.total_raids,
    last_seen_date = source.last_seen_date,   -- sí se actualiza (puede crecer)
    ingested_at    = source.ingested_at
    -- first_seen_date: ausente → inmutable

-- dim_raid: event_date NO aparece en UPDATE SET
WHEN MATCHED THEN UPDATE SET
    raid_size          = source.raid_size,
    boss_name          = source.boss_name,
    difficulty         = source.difficulty,
    duration_target_ms = source.duration_target_ms,
    ingested_at        = source.ingested_at
    -- event_date: ausente → inmutable
```

---

## 5. Por qué se eliminó el segundo argumento de `compute_dim_raid`

La función original en `spark_silver_to_gold.py` tenía esta firma:

```python
def compute_dim_raid(df: DataFrame, fact_raid_summary: DataFrame) -> DataFrame:
```

Necesitaba `fact_raid_summary` únicamente para extraer `n_players` (que era
`countDistinct("source_player_id")` calculado previamente). En el script
independiente Iceberg, ese cálculo se hace **directamente desde Silver**:

```python
# Versión original (pipeline monolítico):
fact_raid_summary.select("raid_id", "n_players")   # n_players ya calculado

# Versión Iceberg (script independiente):
df_raw.groupBy("raid_id").agg(
    F.countDistinct("source_player_id").alias("raid_size"),  # mismo cálculo
    F.min(F.to_date("timestamp")).alias("event_date"),
)
```

Esto hace el script **autocontenido**: una sola fuente de datos, sin dependencia
en otras tablas Gold que podrían no existir aún.

---

## 6. Estrategia de particionado

| Tabla | Columna de partición | Razón |
|---|---|---|
| `dim_player` | `player_class` | Baja cardinalidad (~8 clases WoW), agrupa jugadores similares |
| `dim_raid` | `event_date` | Baja cardinalidad, acceso temporal natural, evita small files de UUID |

`raid_id` es un UUID de alta cardinalidad — particionarlo generaría miles de
directorios en MinIO con una sola fila cada uno (antipatrón "small files").
`event_date` agrupa múltiples raids por día, cardinalidad manejable y los queries
de análisis temporal se benefician del partition pruning.

---

## 7. Columnas literales en `dim_raid`

`dim_raid` es la única dimensión Gold que usa `F.lit()` en su construcción:

```python
.withColumn("boss_name",          F.lit("Unknown Boss"))  # placeholder Fase D
.withColumn("difficulty",         F.lit("Normal"))         # placeholder Fase D
.withColumn("duration_target_ms", F.lit(360_000.0))        # regla de negocio: 6 min
```

- `boss_name` y `difficulty` son **placeholders**: se actualizarán en una fase
  posterior cuando se integre la Warcraft Logs API. El schema Iceberg ya los
  define como `STRING` para admitir ese cambio sin schema evolution.
- `duration_target_ms` es una **regla de negocio fija** (6 minutos = 360.000 ms),
  no derivada de los datos Silver.

`dim_player` no tiene columnas literales — todos sus atributos se calculan
directamente desde los eventos Silver.

---

*Documento generado: Subfase 7.4 — Fase 7 (Migración Silver/Gold a Apache Iceberg)*
