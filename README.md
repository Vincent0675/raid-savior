# WoW Raid Telemetry Pipeline

**Proyecto de Curso:** Big Data e Inteligencia Artificial  
**Autor:** Byron V. Blatch Rodriguez  
**Profesor:** Francisco Javier Ortega  
**Repositorio:** [github.com/Vincent0675/raid-savior](https://github.com/Vincent0675/raid-savior)  
**Estado del Proyecto:** Fase 3 de 5 completada (60%)  
**Última Actualización:** 21 de enero de 2026

---

## Descripción del Proyecto

Pipeline de datos event-driven que simula la telemetría de raids en el videojuego World of Warcraft. El proyecto implementa una arquitectura Medallion completa (Bronze → Silver → Gold) para demostrar el dominio de herramientas modernas de Big Data, desde la ingesta de eventos en tiempo real hasta la generación de métricas analíticas de negocio.

El caso de uso simula un sistema de monitorización de rendimiento de jugadores en raids de alto nivel, donde cada habilidad lanzada, daño recibido y curación aplicada genera un evento de telemetría que debe ser procesado, validado, transformado y analizado.

---

## Objetivos de Aprendizaje

Este proyecto demuestra competencias en:

1. **Arquitectura de datos escalable**: Implementación de la arquitectura Medallion para refinamiento progresivo de datos.
2. **Ingesta event-driven**: Receptor HTTP con validación schema-on-write usando Pydantic v2.
3. **Almacenamiento distribuido**: MinIO (compatible S3) con particionamiento Hive-style para optimización de queries.
4. **Transformación ETL**: Pipeline de limpieza, tipado y enriquecimiento con Pandas/PyArrow.
5. **Formatos columnar**: Migración de JSON a Parquet con compresión Snappy (reducción 80-85%).
6. **Testing automatizado**: Pipeline reproducible con tests de integración end-to-end.
7. **Escalabilidad**: Diseño preparado para procesar millones de eventos mediante particionamiento inteligente.

---

## Arquitectura: Medallion (Bronze → Silver → Gold)

La arquitectura Medallion es un patrón de diseño moderno para data lakes que organiza los datos en tres capas de refinamiento progresivo, permitiendo escalabilidad y trazabilidad.

### Capa Bronze (Raw/Ingesta)

**Responsabilidad:** Almacenamiento de datos crudos tal cual llegan desde la fuente.

- **Formato:** JSON (texto plano)
- **Particionamiento:** `wow_raid_events/v1/raidid={raid_id}/ingest_date={YYYY-MM-DD}/`
- **Validación:** Schema-on-write con Pydantic (rechazo de eventos inválidos en origen)
- **Granularidad:** Evento individual
- **Inmutabilidad:** Los datos nunca se modifican, solo se añaden (append-only)

**Ventaja:** Auditoría completa. Si un error ocurre en capas superiores, siempre se puede reprocessar desde Bronze.

### Capa Silver (Clean/Refinada)

**Responsabilidad:** Datos limpios, tipados y enriquecidos, listos para análisis.

- **Formato:** Apache Parquet (columnar + compresión Snappy)
- **Particionamiento:** `wow_raid_events/v1/raid_id={raid_id}/event_date={YYYY-MM-DD}/`
- **Transformaciones aplicadas:**
  - Conversión de tipos (string → datetime64, float64, bool)
  - Deduplicación (eliminación de eventos duplicados)
  - Validación de rangos lógicos (ej. health 0-100%)
  - Enriquecimiento a nivel de evento (`is_massive_hit`, `ingest_latency_ms`)
- **Granularidad:** Evento individual (mantiene detalle máximo)
- **Compresión:** 80-85% reducción de tamaño vs JSON

**Ventaja:** Velocidad de lectura 10-100x superior gracias a formato columnar y predicate pushdown.

### Capa Gold (Business/Analítica) - PLANIFICADA

**Responsabilidad:** Métricas agregadas y datasets optimizados para dashboards.

- **Formato:** Parquet o tablas denormalizadas
- **Transformaciones aplicadas:**
  - Agregaciones (GROUP BY jugador, clase, raid, fase del boss)
  - Métricas de negocio (DPS, HPS, rankings, percentiles)
  - Joins entre dimensiones (player, encounter, ability)
- **Granularidad:** Agregada (múltiples eventos → una métrica)

**Ejemplo de tabla Gold:**  
`player_performance_by_raid`: DPS/HPS promedio por jugador y raid.

---

## Decisiones de Diseño Clave

### 1. Particionamiento Hive-Style

El particionamiento físico de archivos siguiendo el patrón `columna=valor/` permite que motores analíticos (Spark, Athena, DuckDB) omitan la lectura de particiones irrelevantes.

**Ejemplo de optimización:**
```
Query: SELECT * FROM events WHERE raid_id = 'raid001' AND event_date = '2026-01-21'

Sin particionamiento:
- Lee TODOS los archivos del bucket
- Filtra en memoria
- Tiempo: O(N) donde N = total de eventos

Con particionamiento:
- Lee SOLO la carpeta raid_id=raid001/event_date=2026-01-21/
- Sin filtrado adicional necesario
- Tiempo: O(M) donde M = eventos de esa partición (reducción 90-99%)
```

**Particiones implementadas:**
- `raid_id`: Separa raids independientes (paralelización por raid)
- `event_date`: Permite queries temporales eficientes (ej. "últimos 7 días")

**Escalabilidad:** Con 100 raids y datos de 365 días, se generan 36,500 particiones independientes, permitiendo lectura masivamente paralela.

### 2. Formato Parquet Columnar

A diferencia de JSON (orientado a filas), Parquet almacena datos por columna. Esto permite:

- **Compresión extrema:** Columnas con valores repetidos (ej. `raid_id`, `event_type`) se comprimen con Dictionary Encoding (reducción 90-95%)
- **Lectura selectiva:** Una query que necesita solo 3 columnas de 50 totales lee solo esas 3 columnas del disco
- **Pushdown de predicados:** Metadatos (min/max por bloque) permiten saltar bloques que no cumplen filtros

**Benchmark interno (10 eventos):**
- JSON crudo: 100-150 KB
- Parquet + Snappy: 20-25 KB (reducción 80-83%)

### 3. Schema-on-Write vs Schema-on-Read

**Decisión:** Validación estricta con Pydantic en ingesta (schema-on-write).

**Alternativa rechazada:** Permitir cualquier JSON y validar al leer (schema-on-read).

**Justificación:**
- Detecta errores en el momento de ingesta (feedback inmediato al cliente)
- Evita acumulación de datos corruptos en Bronze
- Garantiza tipado fuerte en todo el pipeline

---

## Estado Actual del Proyecto

### Fase 1: Generación de Datos Sintéticos (COMPLETADA)

**Objetivo:** Crear eventos de telemetría realistas con distribuciones estadísticas.

**Componentes:**
- `src/schemas/eventos_schema.py`: Modelo Pydantic con 6 tipos de eventos (combat_damage, heal, player_death, spell_cast, boss_phase, mana_regeneration)
- `src/generators/raid_event_generator.py`: Generador con distribuciones NumPy (Normal para daño, Bernoulli para críticos)

**Características:**
- Eventos con timestamps ordenados temporalmente
- Distribución realista de roles (8% tank, 20% healer, 72% dps)
- Validación estricta (ej. `raid_id` debe cumplir patrón `^raid\d{3}$`)

**Test de verificación:**
```bash
python scripts/generate_dataset.py
```

### Fase 2: Receptor HTTP + Ingesta Bronze (COMPLETADA)

**Objetivo:** API HTTP que recibe eventos, valida y persiste en MinIO (capa Bronze).

**Componentes:**
- `src/api/receiver.py`: API Flask con endpoint POST `/events`
- `src/storage/minio_client.py`: Cliente wrapper para MinIO/S3
- `infra/minio/docker-compose.yml`: Infraestructura de almacenamiento

**Flujo de ingesta:**
1. Cliente envía array JSON de eventos → `POST /events`
2. Validación con Pydantic (rechazo con 400 si falla)
3. Generación de `batch_id` (UUID único)
4. Persistencia en `s3://bronze/wow_raid_events/v1/raidid=X/ingest_date=Y/batch_{uuid}.json`
5. Respuesta 201 con `batch_id`

**Test de verificación:**
```bash
# Terminal 1: Levantar receptor
python src/api/receiver.py

# Terminal 2: Enviar eventos
curl -X POST http://localhost:5000/events   -H "Content-Type: application/json"   -d @data/eventos_ejemplo.json
```

### Fase 3: Transformación ETL Bronze → Silver (COMPLETADA)

**Objetivo:** Pipeline ETL que transforma JSON crudo a Parquet optimizado.

**Componentes:**
- `src/etl/transformers.py`: Lógica de limpieza, tipado y enriquecimiento
- `src/etl/bronze_to_silver.py`: Orquestador ETL (lectura → transformación → escritura)

**Transformaciones aplicadas:**

1. **Limpieza:**
   - Eliminación de duplicados exactos (columna `event_id`)
   - Filtrado de valores inválidos (ej. `health_pct` fuera de rango 0-100)

2. **Conversión de tipos:**
   - `timestamp`: string ISO 8601 → datetime64[ns, UTC]
   - `damage_amount`, `healing_amount`: string/int → float64
   - `is_critical_hit`: string → bool

3. **Enriquecimiento:**
   - `ingest_latency_ms`: Diferencia temporal entre evento y su ingesta
   - `is_massive_hit`: Flag booleano para daño > 10,000
   - `event_date`: Fecha extraída del timestamp para particionamiento

**Test de verificación:**
```bash
python scripts/test_pipeline_integration.py
```

**Resultado esperado:** Mensaje "TEST COMPLETO: TODOS LOS PASOS EXITOSOS" con métricas de compresión.

---

## Fases Planificadas

### Fase 4: Capa Gold - Agregaciones y Métricas de Negocio (PLANIFICADA)

**Objetivo:** Calcular métricas agregadas de rendimiento y rankings.

**Componentes a desarrollar:**
- `src/analytics/aggregators.py`: Funciones de agregación (DPS, HPS, rankings)
- `src/analytics/gold_layer.py`: Orquestador de cálculos Gold

**Métricas planificadas:**

1. **DPS (Damage Per Second) por Jugador:**
   - Fórmula: `SUM(damage_amount) / (encounter_duration_ms / 1000)`
   - Agrupado por: `player_name`, `player_class`, `raid_id`
   - Output: `gold/player_dps/raid_id={X}/player_dps.parquet`

2. **HPS (Healing Per Second) por Healer:**
   - Similar a DPS pero filtrando `event_type = 'heal'`

3. **Timeline de Encounter:**
   - Daño y curación acumulados segundo a segundo
   - Agrupado por: `timestamp` redondeado a 1s, `raid_id`
   - Output: `gold/encounter_timeline/raid_id={X}/timeline.parquet`

4. **Rankings:**
   - Top 10 jugadores por DPS/HPS
   - Percentiles de rendimiento (P50, P90, P99)

5. **Detección de Anomalías:**
   - Jugadores con DPS estadísticamente improbable (>3 desviaciones estándar)
   - Uso para detección de trampas

**Tecnologías:**
- Pandas para agregaciones en memoria (datasets pequeños)
- Transición a DuckDB si el volumen crece (procesamiento analítico eficiente)

### Fase 5: Visualización y Machine Learning (PLANIFICADA)

**Objetivo:** Dashboard interactivo y modelos predictivos.

**Componentes a desarrollar:**

1. **Dashboard Interactivo:**
   - Tecnología: Streamlit o Plotly Dash
   - Gráficas:
     - Línea temporal: Daño vs Curación a lo largo del encuentro
     - Barras: Top 10 DPS por clase
     - Heatmap: Muertes de jugadores por fase del boss
     - Scatter: Correlación entre críticos y DPS total

2. **Modelos Predictivos:**
   - **Clasificación:** Predecir si una raid tendrá éxito basándose en métricas de los primeros 60 segundos
   - **Regresión:** Estimar tiempo de kill del boss según composición de raid
   - **Clustering:** Agrupar jugadores por estilo de juego (agresivo, conservador, eficiente)

**Tecnologías:**
- scikit-learn para modelos baseline
- XGBoost para modelos avanzados
- PyTorch si se requiere deep learning (ej. LSTM para secuencias temporales)

**Optimización GPU:**
- Uso de NVIDIA RTX 3050 (4GB VRAM) para entrenamiento acelerado
- CUDA 12.x para operaciones tensoriales

---

## Reproducción del Proyecto

### Requisitos de Hardware

**Configuración de desarrollo:**
- **Portátil:** ASUS TUF Gaming A15
- **GPU:** NVIDIA RTX 3050 Laptop (4GB VRAM, soporte CUDA)
- **Sistema Operativo:** GNU/Linux, Pop OS! 22.04
- **RAM:** 16 GB recomendados (mínimo 8 GB)
- **Almacenamiento:** 10 GB libres (datasets + contenedores Docker)

### Requisitos de Software

**Versiones exactas utilizadas:**
- Python: 3.10.19
- Mamba: 2.3.3
- Docker: 20.10+ (para MinIO)
- Git: 2.34+

### Instalación y Configuración

#### 1. Clonar el Repositorio

```bash
git clone https://github.com/Vincent0675/raid-savior.git
cd raid-savior
```

#### 4. Levantar Infraestructura (MinIO)

```bash
cd infra/minio
docker compose up -d
```

**Verificar que MinIO está corriendo:**
```bash
docker ps | grep minio
```

**Acceder a la consola web:**
- URL: http://localhost:9001
- Credenciales: `minio` / `minio123`

#### 5. Crear Buckets en MinIO

Desde la consola web (http://localhost:9001):
1. Crear bucket `bronze`
2. Crear bucket `silver`

#### 6. Configurar Variables de Env

Crear archivo `.env` en la raíz del proyecto:

```ini
# Configuración MinIO
S3_ENDPOINT_URL=http://localhost:9000
S3_ACCESS_KEY=minio
S3_SECRET_KEY=minio123

# Buckets
S3_BUCKET_BRONZE=bronze
S3_BUCKET_SILVER=silver
```

#### 7. Ejecutar Test de Integración

**Terminal 1 - Levantar receptor HTTP:**
```bash
python src/api/receiver.py
```

**Terminal 2 - Ejecutar test end-to-end:**
```bash
python scripts/test_pipeline_integration.py
```

---

## Stack Tecnológico

### Core
- **Python 3.10:** Lenguaje principal
- **Pydantic v2:** Validación de schemas con tipado estricto
- **NumPy:** Generación de distribuciones estadísticas (Normal, Bernoulli)

### API y Web
- **Flask 3.1:** Framework HTTP ligero para receptor de eventos
- **python-dotenv:** Gestión de configuración mediante variables de entorno

### Almacenamiento
- **MinIO:** Object storage compatible S3 (desarrollo local)
- **boto3:** Cliente Python para S3/MinIO
- **Apache Parquet:** Formato columnar con compresión Snappy

### Procesamiento de Datos
- **Pandas 2.3:** Manipulación de DataFrames
- **PyArrow 22.0:** Motor nativo para lectura/escritura de Parquet
- **fsspec/s3fs:** Sistemas de archivos abstractos para Pandas

---

## Autor

**Byron V. Blatch Rodriguez**  
Estudiante de Big Data e Inteligencia Artificial en IES Rafael Alberti, Cádiz.
GitHub: [@Vincent0675](https://github.com/Vincent0675)

**Profesor:** Francisco Javier Ortega  
**Fecha de Inicio:** Diciembre 2025  
**Estado:** Fase 3 de 5 completada
