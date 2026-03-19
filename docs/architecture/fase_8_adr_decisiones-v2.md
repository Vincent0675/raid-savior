# Fase 8 — Architecture Decision Records (ADR)

**Documento:** Justificación de Decisiones de Diseño para Producción Sostenible  
**Fecha:** 2026-03-18  
**Versión:** 1.0

---

## Resumen Ejecutivo

Esta fase implementa la capa de consumo del pipeline Medallion con 5 decisiones críticas de arquitectura que definen:

- **Sostenibilidad operativa:** Bajo overhead de infraestructura, bajo acoplamiento
- **Escalabilidad:** Capaz de crecer desde desarrollo a producción sin rediseño
- **Presupuesto:** Estimado en ~€40–50/mes en cloud (t3.medium), 0 servicios adicionales

Las decisiones están documentadas siguiendo el formato **ADR (Architecture Decision Record)** para facilitar auditoría, onboarding y justificación presupuestaria ante terceros.

---

## ADR-8-01 REV.1 — REST Catalog en lugar de HadoopCatalog directo

**Fecha**: 2026-03-18 · **Estado**: APROBADO · **Sustituye**: ADR-8-01 v1 (PyIceberg + HadoopCatalog)

### Por qué cambia la decisión

PyIceberg 0.11 no implementa `HadoopCatalog` como `CatalogType` válido — el tipo `hadoop` existe exclusivamente en la JVM de Spark. `CatalogType` en Python soporta únicamente `rest`, `sql`, `hive`, `glue` y `dynamodb`. Lo descubrimos empíricamente en este proyecto.

***

### Arquitectura resultante

```
Spark (Fase 7)          PyIceberg 0.11 (Fase 8)
     │                          │
     └──► REST Catalog ◄────────┘
          tabulario/iceberg-rest:0.11.0
          puerto 8181
               │
               ▼
          s3://warehouse/   (MinIO)
          gold/dim_player/
          gold/dim_raid/
          gold/fact_raid_summary/
          gold/fact_player_raid_stats/
```

Tanto Spark como PyIceberg apuntan al mismo servidor REST. El servidor REST es quien habla con MinIO directamente. **Un único punto de verdad para los metadatos**.[^1][^2]

***

### Imagen Docker elegida

`tabulario/iceberg-rest:0.11.0` — publicada el 10 de marzo de 2026. Es la imagen de referencia oficial con más de 500K pulls, backed por `JdbcCatalog` con SQLite embebido. Sin base de datos externa requerida.[^3][^1]

***

### Cambios sobre el `docker-compose.yml` actual

**Nuevo servicio** a añadir:

```yaml
iceberg-rest:
  image: tabulario/iceberg-rest:0.11.0
  container_name: raid-savior-iceberg-rest
  ports:
    - "8181:8181"
  environment:
    - AWS_ACCESS_KEY_ID=${MINIO_ROOT_USER:-minioadmin}
    - AWS_SECRET_ACCESS_KEY=${MINIO_ROOT_PASSWORD:-minioadmin}
    - AWS_REGION=us-east-1
    - CATALOG_WAREHOUSE=s3://warehouse/
    - CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO
    - CATALOG_S3_ENDPOINT=http://minio:9000
    - CATALOG_S3_PATH_STYLE_ACCESS=true
  networks:
    - raid-network
  depends_on:
    minio:
      condition: service_healthy
  restart: unless-stopped
```

**Spark** — cambio en `spark_session.py`, dos líneas:

```python
# ANTES (HadoopCatalog — eliminar)
.config("spark.sql.catalog.wow.type", "hadoop")
.config("spark.sql.catalog.wow.warehouse", "s3a://warehouse/")

# DESPUÉS (REST Catalog)
.config("spark.sql.catalog.wow.type", "rest")
.config("spark.sql.catalog.wow.uri", "http://localhost:8181")
```

**PyIceberg** — `iceberg_service.py` (Subfase 8.2):

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("wow", **{
    "type": "rest",
    "uri": "http://localhost:8181",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
})
```

***

### Impacto en tablas Gold existentes

⚠️ **Deuda técnica crítica**: las tablas creadas por Spark con `HadoopCatalog` en `s3://warehouse/gold/` **no son visibles automáticamente** para el REST Catalog — su SQLite embebido está vacío. Hay que registrarlas. Esto es la **primera tarea de Subfase 8.2** antes de cualquier query.

El procedimiento de registro lo definimos al arrancar la subfase.

***

### Tabla de impacto

| Componente | Antes | Después |
| :-- | :-- | :-- |
| `spark_session.py` | `type=hadoop` | `type=rest`, `uri=http://localhost:8181` |
| `docker-compose.yml` | sin REST | +1 servicio `iceberg-rest` |
| `iceberg_service.py` | no existe | `type=rest` en `load_catalog` |
| Tablas Gold existentes | visibles en Hadoop | requieren registro en REST |
| Coste operativo | sin cambio | +~200 MB imagen, 1 contenedor |


---

## ADR-8-02: Gestión de Configuración — BaseSettings Propio

### Status
✅ **ACEPTADA**

### Contexto
FastAPI necesita configuración independiente de Flask (credenciales MinIO, puerto API, variables de entorno por deployment). Existen dos enfoques:

1. **Extender `src/config.py`**: Reutilizar clase existente diseñada para Flask
2. **`BaseSettings` propio en `src/api/settings.py`**: Configuración independiente con validación Pydantic

### Decisión
**Crear `src/api/settings.py` con `pydantic_settings.BaseSettings` completamente independiente de Flask.**

### Justificación

**Comparativa:**

| Aspecto | Extender src/config.py | BaseSettings propio |
|---|---|---|
| Fail-fast | ❌ Valores por defecto silenciosos | ✅ ValidationError al arrancar |
| Multi-entorno | ❌ Sin soporte nativo | ✅ `.env` por entorno |
| Type-safety | ⚠️ Clase plana sin validación | ✅ Pydantic v2 con type hints |
| Acoplamiento Flask-API | ✅ Alto (problema) | ❌ Bajo (solución) |
| Versionabilidad API | ❌ Cambios Flash → API | ✅ Independiente |

**Razones operativas:**

**Fail-fast en producción:**
```python
# Sin BaseSettings:
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minio123")  # Default silencioso ❌

# Con BaseSettings:
class Settings(BaseSettings):
    s3_secret_key: str  # Requerido, lanza ValidationError ✅
```

Si `S3_SECRET_KEY` no está definida en producción, el contenedor **falla al arrancar** (`docker compose up` devuelve error). Esto es correcto — previene bugs de seguridad silenciosos.

**Multi-entorno sin rebuilding:**
```
.env.dev    → DEBUG=true, WORKERS=2, ENVIRONMENT=development
.env.staging → DEBUG=false, WORKERS=4, ENVIRONMENT=staging
.env.prod   → DEBUG=false, WORKERS=8, ENVIRONMENT=production
```

Cambiar workers de 2 a 8 no requiere `docker build` — solo cargar `.env.prod` diferente.

**Presupuesto:**
- **Dependencia nueva:** `pydantic-settings` (incluido en `pydantic[settings]`)
- **Código nuevo:** ~50 líneas
- **Operación:** 0 impacto — validación automática

### Consecuencias
- ✅ Fail-fast en deployments → bugs detectados temprano
- ✅ Multi-entorno nativo sin duplicación de código
- ✅ Type-safety completa (MyPy pasa)
- ✅ Auditoría de cambios de configuración por entorno
- ⚠️ Separación lógica entre Flask y FastAPI (requiere gestión manual de variables compartidas)

---

## ADR-8-03: Modelo de Despliegue — Gunicorn + Uvicorn Workers

### Status
✅ **ACEPTADA**

### Contexto
FastAPI corre sobre uvicorn (ASGI server). Existen 3 modelos de despliegue:

1. **uvicorn single-process**: `uvicorn src.api.main:app --port 8000` (1 core)
2. **gunicorn + uvicorn workers**: Process manager con N workers paralelos
3. **Load balancer + múltiples containers**: Kubernetes / orquestación compleja

### Decisión
**Usar `gunicorn` como process manager con `uvicorn.workers.UvicornWorker` workers. Variable `WEB_CONCURRENCY` controla workers sin reconstruir imagen.**

### Justificación

**Cargas de concurrencia esperadas:**

Grafana hace polling cada 30 s a 4 paneles → ~480+ requests/hora. Con DuckDB single-thread:
- 1 request pesado (query compleja) tarda ~50 ms
- 2 requests simultáneos se encolan → latencia se dispara a 100 ms
- 3+ requests sin workers → timeouts

**Solución con gunicorn:**
```bash
gunicorn src.api.main:app \
  --workers=2 \
  --worker-class uvicorn.workers.UvicornWorker \
  --bind 0.0.0.0:8000
```

**Comparativa por modelo:**

| Métrica | uvicorn single | gunicorn (2w) | gunicorn (4w) |
|---|---|---|---|
| vCPU requerido | 1 | 2 | 4 |
| Latencia p99 (Grafana) | 200–500 ms | 50–100 ms | 10–50 ms |
| Coste AWS | t3.micro (~€10) | t3.small (~€25) | t3.medium (~€40) |
| Escalabilidad | No (cuello) | Sí (hasta 8w) | Sí (hasta N) |

**Presupuesto:**
- **Desarrollo:** Ninguno (uvicorn --reload sin workers)
- **Producción:** t3.small (2 workers) → ~€25/mes; t3.medium (4 workers) → ~€40/mes
- **Escalabilidad:** Crecer de 2→4→8 workers ajustando `WEB_CONCURRENCY` sin rebuilding

**Implementación:**

```dockerfile
# Dockerfile (producción)
ENV WEB_CONCURRENCY=2

CMD ["gunicorn", "src.api.main:app", \
     "--workers=${WEB_CONCURRENCY}", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8000", \
     "--timeout=120"]
```

```yaml
# docker-compose.yml
environment:
  WEB_CONCURRENCY: ${WEB_CONCURRENCY:-2}
```

### Consecuencias
- ✅ Concurrencia escalable sin bottleneck de 1 core
- ✅ Crecimiento horizontal (2→4→8 workers) con cambio de variable, no código
- ✅ Latencia predecible incluso con picos de Grafana
- ⚠️ Memoria overhead por worker (~50 MB extra por worker)
- ⚠️ Timeout settings crítico (defecto 30 s puede ser insuficiente para queries pesadas)

---

## ADR-8-04: Datasource de Grafana — Infinity Plugin

### Status
✅ **ACEPTADA**

### Contexto
Grafana necesita consumir datos de FastAPI. Existen tres enfoques:

1. **Infinity Plugin**: HTTP JSON directamente desde FastAPI
2. **PostgreSQL + sync job**: Materialización en base relacional
3. **Iceberg + cloud (Athena/BigQuery)**: Query directa sin materialización

### Decisión
**Usar Infinity Plugin de Grafana como datasource. FastAPI expone endpoints JSON que Infinity consume via HTTP.**

### Justificación

**Comparativa de infraestructura:**

| Aspecto | Infinity | PostgreSQL | Athena |
|---|---|---|---|
| Servicios nuevos | 0 | 1 (RDS) | 1 (cloud service) |
| Coste mensual | €0 | €50–150 | €30–100 |
| Latencia | 100–500 ms | 50–100 ms | 5–10 s |
| Alertas complejas | ⚠️ Limitadas | ✅ Completas | ✅ Completas |
| Mantenimiento | Mínimo | Alto (schema) | Medio (queries) |

**Razones operativas:**

**Coste cero adicional:**
- Infinity Plugin es open-source
- FastAPI ya está presupuestada
- MinIO ya está presupuestada
- No requiere sincronización, no requiere base de datos, no requiere job adicional

**Latencia aceptable:**
Para observabilidad (refresh 30 s), <500 ms por panel es tolerable. No es OLTP.

**Presupuesto:**
- **Infraestructura:** €0 (consumo sobre FastAPI existente)
- **Desarrollo:** ~8 horas (configurar plugin, diseñar dashboards)
- **Operación:** Mínima (solo mantener FastAPI)

**Limitación conocida:**
Infinity no soporta alertas complejas tipo "si wipe_rate > 50% durante 2+ ciclos". Si esto es crítico en futuro, se evalúa PostgreSQL como **breaking change a Fase 8.4**.

### Consecuencias
- ✅ Coste cero adicional de infraestructura
- ✅ Bajo acoplamiento (Grafana depende solo de HTTP)
- ✅ Sin sincronización de datos (siempre frescos)
- ⚠️ Alertas limitadas (requiere postgre si necesarias)
- ⚠️ Infinity plugin es de comunidad (no soporte oficial Grafana)

---

## ADR-8-05: Modelos de Respuesta — Independence de Schemas de Ingesta

### Status
✅ **ACEPTADA**

### Contexto
FastAPI necesita devolver JSON con KPIs. Existen dos enfoques:

1. **Reutilizar `src/schemas/`**: Modelos Pydantic de ingesta como modelos de respuesta
2. **`src/api/schemas/responses.py`**: Modelos independientes para API

### Decisión
**Crear `src/api/schemas/responses.py` completamente independiente de `src/schemas/`.**

### Justificación

**El problema de acoplamiento:**

```python
# Enfoque problemático: reutilizar ingesta
from src.schemas import RaidEventSchema

@app.get("/raids/{raid_id}")
def get_raid(raid_id: UUID):
    # Si alguien en Fase 1 añade un campo opcional a RaidEventSchema,
    # el contrato de la API cambia silenciosamente
    return RaidEventSchema(...)  # ❌ Frágil
```

**La solución: modelos independientes:**

```python
# Enfoque correcto: modelos propios para API
from src.api.schemas.responses import RaidSummaryResponse

@app.get("/raids/{raid_id}")
def get_raid(raid_id: UUID):
    # El contrato es explícito, independiente de cambios de ingesta
    return RaidSummaryResponse(...)  # ✅ Estable
```

**Comparativa:**

| Aspecto | Reutilizar schemas | responses.py independiente |
|---|---|---|
| Contrato de API | ❌ Frágil (evoluciona con ingesta) | ✅ Estable (controlable) |
| OpenAPI docs | ⚠️ Mezcla ingesta + salida | ✅ Salida clara |
| Versionado | ❌ No (1 versión) | ✅ Sí (/v1/, /v2/) |
| Consumidores externos | ❌ Frágil (breaking changes) | ✅ Protegido |
| Código nuevo | 0 líneas (reutilizo) | ~50 líneas | 

**Presupuesto:**
- **Dependencia:** Ninguna (Pydantic ya existe)
- **Código nuevo:** ~50–80 líneas de modelos
- **Mantenimiento:** Bajo (cambios de API no impactan ingesta)

**Riesgo mitigado:**
Si en Fase 2 añades un nuevo tipo de evento (`buff_cast`), o cambias el schema de `combat_damage`, **la API no se rompe**.

### Consecuencias
- ✅ Contrato de API independiente del pipeline de ingesta
- ✅ Versionado de API futuro sin fricción
- ✅ Documentación OpenAPI clara y pura
- ✅ Evita breaking changes silenciosos
- ⚠️ Duplicación lógica de algunos tipos base (UUID, float, etc.)

---

## Matriz de Decisiones vs. Impacto

```
┌─────────────────────────────────────────────────────────────────┐
│ Decisión         │ Coste nuevo │ Latencia │ Escalabilidad │ Riesgo │
├──────────────────┼─────────────┼──────────┼───────────────┼────────┤
│ ADR-8-01: PyICE  │ €0          │ <100ms   │ Alta          │ Bajo   │
│ ADR-8-02: Config │ €0 (pip)    │ N/A      │ Alta          │ Bajo   │
│ ADR-8-03: Gunic  │ €15–25      │ <100ms   │ Alta          │ Bajo   │
│ ADR-8-04: Grafana│ €0          │ 100–500m │ Media         │ Medio  │
│ ADR-8-05: Schema │ €0          │ N/A      │ Alta          │ Bajo   │
└─────────────────────────────────────────────────────────────────┘
```

**Coste total estimado en producción:** €40–50/mes en cloud (AWS t3.medium)

---

## Recomendaciones para Auditoría de Presupuesto

Cuando presentes estas decisiones a stakeholders o junta directiva:

### Presentación Ejecutiva (5 min)
> "Fase 8 implementa la visualización de KPIs del pipeline con **coste operativo fijo en ~€40/mes** y **sin servicios adicionales**. Las decisiones priorizan *bajo acoplamiento* y *escalabilidad horizontal* para crecimiento futuro sin rediseño arquitectónico."

### Argumento de Sostenibilidad
1. **Bajo overhead:** Single-node con DuckDB, no cluster distribuido
2. **Multi-entorno:** Mismo código, diferentes `.env` por environment
3. **Sin lock-in:** DuckDB → Spark si futuro lo requiere; Infinity → PostgreSQL si alertas complejas necesarias
4. **Presupuesto predecible:** €40–50/mes fijo, escalable a €80–100/mes con 4–8 workers

### Comparativa vs. Alternativas
- **Alternativa A (PySpark en cada request):** ~€200/mes (necesita cluster mínimo)
- **Alternativa B (PostgreSQL + sync):** ~€100/mes (RDS + mantenimiento)
- **Alternativa C (nuestra opción):** ~€40/mes ✅

---

## Changelog de Decisiones

| Fecha | ADR | Decisión | Razón |
|---|---|---|---|
| 2026-03-18 | 8-01 | PyIceberg + DuckDB | Latencia + escalabilidad |
| 2026-03-18 | 8-02 | BaseSettings propio | Fail-fast + multi-env |
| 2026-03-18 | 8-03 | Gunicorn + workers | Concurrencia Grafana |
| 2026-03-18 | 8-04 | Infinity Plugin | Coste cero + independencia |
| 2026-03-18 | 8-05 | responses.py independiente | Contrato API estable |

---

**Documento generado:** 2026-03-18  
**Responsable:** Tutor Senior Pedagógico + Equipo Técnico  
**Revisión próxima:** Al cierre de Fase 8 (estimado 2026-04-15)

