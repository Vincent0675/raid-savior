# WoW Raid Telemetry Pipeline

**Proyecto de Curso:** Big Data e Inteligencia Artificial  
**Autor:** Byron V. Blatch Rodriguez  
**Profesor:** Francisco Javier Ortega  
**Repositorio:** [github.com/Vincent0675/raid-savior](https://github.com/Vincent0675/raid-savior)  
**Estado del Proyecto (implementación):** Fase 3 completada, Fase 4 en desarrollo avanzado.  
**Contexto académico (documentación):** Roadmap maestro con Fase 4 considerada operativa a nivel Gold.  
**Última Actualización:** 12 de febrero de 2026.

---

## 1. Visión general del proyecto

Este repositorio implementa un **pipeline de telemetría event-driven** que simula raids de World of Warcraft usando una arquitectura **Medallion** completa (Bronze → Silver → Gold) sobre MinIO (estructura S3), con validación estricta mediante Pydantic v2 y transformación ETL a formato Parquet columnar.  

Cada habilidad lanzada, golpe recibido o curación realizada se modela como un **evento estructurado** que atraviesa las capas Bronze (datos crudos), Silver (datos limpios y enriquecidos) y Gold (tablas agregadas analíticas), sirviendo de base a dashboards y modelos de IA para evaluar el rendimiento de raids y jugadores.

---

## 2. Objetivos de aprendizaje y contexto académico

El proyecto es el núcleo técnico donde convergen varias asignaturas de la especialización en Big Data e Inteligencia Artificial, todas apoyadas en la misma arquitectura Medallion.

### 2.1 Competencias técnicas clave

**Arquitectura de datos escalable**: Diseño e implementación de la arquitectura Medallion (Bronze / Silver / Gold) sobre object storage compatible S3 (MinIO).  

**Ingesta event-driven con validación fuerte**: Receptor HTTP Flask que aplica validación *schema-on-write* con Pydantic v2 antes de persistir en Bronze.  

**Streaming y observabilidad básica**: Generación masiva de eventos a través de HTTP y visualización en tiempo real con un cliente SSE HTML/JS.  

**Almacenamiento distribuido y formatos columnar**: MinIO como capa de almacenamiento distribuido y conversión de JSON a Parquet con compresión Snappy en Silver.  

**Transformación ETL profesional**: Limpieza, tipado, deduplicación, validación de rangos y enriquecimiento de eventos mediante Pandas y PyArrow.  

**Capa Gold orientada a negocio y a IA**: Definición e implementación progresiva de `gold.raid_summary` y `gold.player_raid_stats` como “fuente única de verdad” para analítica y ML.  

**Testing automatizado e integración end-to-end**: Suite de tests unitarios e integración (incluyendo `tests/test_pipeline_integration.py` y `tests/test_gold_pipeline.py`) que ejercitan el pipeline completo.

### 2.2 Integración con las asignaturas

Según el documento de contexto maestro, el proyecto se integra así en el plan académico:

##### **Big Data Aplicado (BDA)**  
  - Implementación de capas Bronze, Silver y Gold en MinIO.  
  - Ingesta masiva event-driven con validación *schema-on-write* y adopción de Parquet para eficiencia de coste y tiempo.  

##### **Sistemas de Big Data**  
  - Observabilidad del pipeline y telemetría de eventos.  
  - Uso previsto de Grafana/Kibana sobre Silver y Gold para monitorizar salud técnica y KPIs.  
  - Infraestructura basada en contenedores Docker para servicios como MinIO y el receptor.  

##### **Programación de Inteligencia Artificial**  
  - Diseño de APIs de consumo (FastAPI planificada) para exponer métricas de Gold a otros servicios.  
  - Dashboards ligeros con Streamlit para visualización rápida de métricas por raid y por jugador.  
  - Uso de AutoML (PyCaret) sobre `gold.raid_summary` como base de experimentación.  

##### **Modelos de Inteligencia Artificial**  
  - Clasificación predictiva del éxito de la raid (`raid_outcome`) usando features de Gold.  
  - Clustering de estilos de juego a partir de `gold.player_raid_stats` (agresivo, eficiente, conservador, etc.).  
  - Entrenamiento acelerado mediante GPU (RTX 3050 + CUDA) para modelos de mayor complejidad.  

---

## 3. Arquitectura Medallion: Bronze → Silver → Gold

La arquitectura Medallion organiza los datos en tres capas de refinamiento progresivo que mejoran calidad, estructura y utilidad analítica.

### 3.1 Capa Bronze – Raw / Ingesta

**Responsabilidad:** almacenar datos crudos tal y como llegan desde el receptor HTTP, pero ya validados por schema-on-write.

- **Formato:** JSON (array de eventos validados).  
- **Destino:** bucket `bronze` en MinIO.  
- **Key pattern (contrato de escritura):**  
  `wow_raid_events/v1/raidid={raid_id}/ingestdate={YYYY-MM-DD}/batch={uuid}.json`.  
- **Validación:**  
  - Pydantic v2 en el propio receptor (schema-on-write).  
  - Rechazo con HTTP 400 ante eventos inválidos, sin permitir su escritura en Bronze.  
- **Propiedades clave:**  
  - Inmutabilidad (append-only).  
  - Metadatos con `batch-id`, `event-count`, `ingest-timestamp` y `batch-source` para trazabilidad.

### 3.2 Capa Silver – Clean / Refinada

**Responsabilidad:** ofrecer eventos limpios, tipados y enriquecidos, listos para uso analítico masivo.

- **Formato:** Apache Parquet + compresión Snappy.  
- **Destino:** bucket `silver` en MinIO.  
- **Particionamiento lógico:**  
  `wow_raid_events/v1/raidid={raid_id}/eventdate={YYYY-MM-DD}/...`.  

**Transformaciones principales (módulo `SilverTransformer`):**

1. **Cast de tipos**  
   - `timestamp`, `ingest_timestamp`: string ISO 8601 → `datetime64[ns, UTC]`.  
   - Numéricos (`damage_amount`, `healing_amount`, health_pct, resources) → `float64`.  

2. **Deduplicación**  
   - Eliminación de duplicados por `event_id`.  
   - Conteo de duplicados eliminados en metadata.  

3. **Validación de rangos**  
   - `health_pct` en rango [0, 100].  
   - Daños y curaciones no negativos.  

4. **Enriquecimiento**  
   - `ingest_latency_ms`: diferencia temporal entre `timestamp` e `ingest_timestamp`.  
   - `is_massive_hit`: flag para golpes de daño mayor a un umbral (ej. 10.000).  
   - `event_date`: fecha derivada para particionamiento y reporting.  

### 3.3 Capa Gold – Business / Analítica

**Responsabilidad:** proporcionar tablas analíticas agregadas que sirvan de base a decisiones y modelos de IA.

Tablas Gold definidas en el documento maestro:

1. **`gold.raid_summary` (macroscópica)**  
   - 1 fila por raid/encuentro.  
   - Incluye KPIs globales: duración, daño total, curación total, muertes, etc.  
  - Campo `raid_outcome` (Success/Wipe) calculado según reglas de negocio sobre vida del boss y muertes permitidas.  

2. **`gold.player_raid_stats` (microscópica)**  
   - 1 fila por jugador y raid.  
   - KPIs: DPS, HPS, ratio de críticos, share de daño, muertes, entre otros.  

Gold es el punto de partida para:

- Dashboards live (Streamlit / Chart.js / Grafana / Kibana).  
- Modelos de clasificación (éxito de raid) y clustering (perfiles de jugador).  
- Análisis de rendimiento individual y de composición de raid.

---

## 4. Flujo práctico: ejecutar el pipeline end-to-end

A continuación se describe el flujo que puedes ejecutar hoy para recorrer el pipeline desde Bronze hasta Silver y ejercer la lógica de Gold a través de tests.

### Paso 1 – Levantar MinIO

Desde la carpeta `infra/minio`:

```bash
cd infra/minio
docker compose up -d
```

- **Consola web:** `http://localhost:9001`  
- **Credenciales por defecto (ejemplo local):** usuario `minio`, contraseña `minio123`.   
- Verifica que existen los buckets `bronze` y `silver` (y opcionalmente `gold`) en MinIO.

### Paso 2 – Levantar el receptor Flask

Desde la raíz del repositorio:

```bash
python src/api/receiver.py
```

- Expone un endpoint `POST /events` que valida eventos con Pydantic v2 y persiste batches válidos en Bronze siguiendo el contrato de escritura.  

### Paso 3 – Abrir el cliente SSE

Abre en el navegador:

- El archivo `tests/cliente_sse.html`.

Este cliente se conecta al endpoint SSE configurado dentro del HTML y muestra en la consola de JS el flujo de eventos en tiempo real.

### Paso 4 – Lanzar el generador masivo de eventos HTTP

En otra terminal, desde la raíz del repositorio:

```bash
python src/generators/generate_massive_http.py \
  --num-raids 5 \
  --num-events-per-raid 50 \
  --batch-size 25 \
  --output-mode http
```

- Genera eventos sintéticos distribuidos en varias raids, aplicando distribuciones realistas (Normal para daño, Bernoulli para críticos) definidas en la lógica de Fase 1.  
- Envía micro-batches al receptor Flask via HTTP, que los valida y guarda en Bronze.

### Paso 5 – Observar el streaming SSE y Bronze

- En el navegador, abre la consola de JS del cliente SSE y verifica que los eventos se reciben y muestran sin errores.  
- En la terminal del receptor Flask se verán logs de `POST /events`.  
- En MinIO, inspecciona el bucket `bronze` para ver los nuevos objetos JSON creados según el patrón de key.

### Paso 6 – Ejecutar ETL Bronze → Silver

Una vez confirmada la existencia de datos en Bronze, lanza el ETL:

```bash
python src/etl/run_bronze_to_silver.py
```

- Lee batches de Bronze vía cliente MinIO.  
- Aplica las transformaciones definidas en `SilverTransformer`.  
- Escribe los resultados en formato Parquet comprimido en el bucket `silver` siguiendo el layout de Silver.

### Paso 7 – Inspeccionar Bronze vs Silver

Para comparar una muestra de ambos buckets:

```bash
python src/etl/inspect_bronze_vs_silver.py
```

- Analiza diferencias de tamaño, número de filas y estructura entre JSON (Bronze) y Parquet (Silver).  
- Nota: con volúmenes pequeños, es posible que Parquet parezca más pesado debido al overhead inicial; el diseño está pensado para ganar de forma clara a mayor escala.

### Paso 8 – Ejercer la lógica Gold mediante tests

Actualmente, la transformación hacia Gold se verifica y recorre principalmente a través de la suite de tests:

```bash
pytest tests/test_gold_pipeline.py -q
```

- Este test utiliza la lógica de `src/analytics/gold_layers.py` y `src/analytics/aggregators.py` para construir y validar las tablas Gold a partir de Silver.  
- Parte de esta capa está marcada explícitamente como **pendiente de revisión y consolidación**, pero ya cubre el flujo conceptual hacia `gold.raid_summary` y `gold.player_raid_stats`.

---

## 5. Estado por fases y roadmap

### 5.1 Fases implementadas (pipeline técnico)

- **Fase 1 – Schema y Generador Sintético (COMPLETADA)**  
  - Definición del schema de eventos en JSON Schema y modelos Pydantic v2 para WoW raid events.  
  - Generación de eventos sintéticos con distribuciones realistas y validación centralizada.  

- **Fase 2 – Receptor HTTP + Ingesta Bronze (COMPLETADA)**  
  - Servidor Flask con endpoint `/events`, validación Pydantic y escritura en MinIO Bronze siguiendo un contrato de escritura determinista.  

- **Fase 3 – ETL Bronze → Silver (COMPLETADA A NIVEL FUNCIONAL)**  
  - Módulo `src/etl` con `SilverTransformer` y `BronzeToSilverETL` implementando limpieza, tipado, enriquecimiento y escritura a Parquet.  
  - Scripts y tests de integración que verifican la presencia de archivos `.parquet` en Silver.  

- **Fase 4 – Gold / Analytics (EN DESARROLLO AVANZADO)**  
  - Documento maestro define tablas `gold.raid_summary` y `gold.player_raid_stats` y su lógica de negocio.  
  - Implementaciones iniciales en `src/analytics/` más tests como `tests/test_gold_pipeline.py` en ejecución, con partes marcadas como “pendientes de revisión”.

### 5.2 Roadmap de continuación (vista académica)


- **Fase A – Escalabilidad y Volumen**  
  - Generación de millones de eventos mediante un `WoWEventGenerator` optimizado por batches.  
  - Refinar streaming SSE para dashboards en tiempo real.  

- **Fase B – Interfaz y APIs**  
  - Dashboard live (Streamlit / Chart.js) mostrando métricas en tiempo real.  
  - API robusta (FastAPI) para consulta de históricos en Gold.  

- **Fase C – Modelado de IA**  
  - Preparación de datasets de entrenamiento a partir de Gold.  
  - Pipeline de ML (entrenamiento, validación, exportación de modelos de clasificación y clustering).  

- **Fase D – Realidad y Externalización**  
  - Integración con Warcraft Logs API para validar el pipeline con datos reales de jugadores.

---

## 6. Stack tecnológico

### Core

- **Python 3.10** – Lenguaje principal del proyecto.   
- **Pydantic v2** – Validación estricta de schemas y generación de JSON Schema.   
- **NumPy** – Generación de distribuciones estadísticas (Normal, Bernoulli) para eventos sintéticos.   

### API e ingesta

- **Flask 3.x** – Framework HTTP para el receptor de eventos.   
- **python-dotenv** – Gestión de configuración a través de variables de entorno.  

### Almacenamiento

- **MinIO** – Object storage compatible S3 para capas Bronze/Silver/Gold.   
- **boto3** – Cliente S3/MinIO para Python.  
- **Apache Parquet + Snappy** – Formato columnar comprimido para Silver y Gold.   

### Procesamiento de datos

- **Pandas 2.x** – Manipulación de DataFrames y agregaciones.  
- **PyArrow** – Motor Parquet y soporte columnar en Python.   

### Testing y herramientas

- **pytest** – Framework de testing para unitarios e integración.  
- **Docker** – Orquestación de MinIO y otros servicios de infraestructura.   

---

## 7. Reproducción del entorno

### 7.1 Requisitos de hardware (entorno de desarrollo)

- Portátil ASUS TUF Gaming A15.  
- GPU Nvidia RTX 3050 Laptop (4GB VRAM, soporte CUDA).  
- GNU/Linux Pop!_OS 22.04.  
- Memoria recomendada: 8–16 GB.  
- Espacio en disco sugerido: ~10 GB (datasets + contenedores Docker).

### 7.2 Pasos básicos de instalación

1. **Clonar el repositorio**

```bash
git clone https://github.com/Vincent0675/raid-savior.git
cd raid-savior
```

2. **Crear entorno e instalar dependencias**

```bash
mamba create -n wow-telemetry python=3.10
mamba activate wow-telemetry
pip install -r requirements.txt
```

3. **Configurar `.env` (ejemplo mínimo)**

```ini
S3_ENDPOINT_URL=http://localhost:9000
S3_ACCESS_KEY=minio
S3_SECRET_KEY=minio123

S3_BUCKET_BRONZE=bronze
S3_BUCKET_SILVER=silver
S3_BUCKET_GOLD=gold

FLASK_HOST=0.0.0.0
FLASK_PORT=5000
```

4. **Levantar MinIO**

```bash
cd infra/minio
docker compose up -d
```

5. **Crear buckets en MinIO**

Desde `http://localhost:9001`:

- Crear `bronze`, `silver` y opcionalmente `gold` con usuario `minio` y contraseña `minio123`.

6. **Ejecutar el flujo descrito en la sección 4**

- Levantar `src/api/receiver.py`.  
- Abrir `tests/cliente_sse.html`.  
- Ejecutar `src/generators/generate_massive_http.py`.  
- Correr `src/etl/run_bronze_to_silver.py` y `src/etl/inspect_bronze_vs_silver.py`.  
- Ejecutar `pytest tests/test_gold_pipeline.py -q` para ejercitar Gold.

---

## 8. Autoría

**Autor:** Byron V. Blatch Rodriguez  
Estudiante de Big Data e Inteligencia Artificial.  
GitHub: [@Vincent0675](https://github.com/Vincent0675)  

**Profesor:** Francisco Javier Ortega  

Este README consolidado combina el estado real de la implementación en el repositorio con el contexto académico del documento maestro y la guía práctica para ejecutar el pipeline end-to-end desde Bronze hasta las primeras capas de Gold.
```