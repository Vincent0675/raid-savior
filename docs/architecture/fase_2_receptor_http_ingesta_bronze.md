# Fase 2 — Receptor HTTP e Ingesta en MinIO Bronze

## 1. Propósito de la fase

La Fase 2 convierte el contrato de datos definido en Fase 1 en una entrada operativa del pipeline. Su finalidad es recibir eventos por HTTP, validarlos en tiempo real y persistir únicamente datos válidos en la capa Bronze sobre MinIO.

En términos de arquitectura, esta fase implementa el punto de entrada del sistema. Si en Fase 1 se definió la forma de la señal, en Fase 2 se construye el circuito de adquisición que la recibe, la filtra y la almacena sin contaminar etapas posteriores.

---

## 2. Objetivos técnicos

Los objetivos principales de la fase son:

- Exponer un servicio HTTP para recibir eventos
- Reutilizar el schema de Fase 1 como contrato de entrada
- Aplicar validación schema-on-write en tiempo de ingesta
- Rechazar batches inválidos sin persistencia parcial
- Agrupar eventos válidos en micro-batches
- Persistir los batches en MinIO como JSON raw
- Definir un layout Bronze estable, versionado y particionado
- Dejar preparada la base para la transformación Bronze → Silver

---

## 3. Estado y alcance real

### Incluido en la fase

- Servicio Flask para ingesta
- Endpoint `GET /health`
- Endpoint `POST /events`
- Validación con Pydantic
- Respuestas HTTP de éxito y error
- Construcción de batch payload con metadata de ingesta
- Persistencia en bucket Bronze de MinIO
- Particionado por `raidid` e `ingest_date`
- Configuración centralizada por variables de entorno
- Testing básico con pytest y pruebas manuales

### Fuera de alcance en esta fase

- Transformación a Parquet
- Deduplicación
- Limpieza y enriquecimiento de datos
- Silver layer
- Gold layer
- DLQ productiva
- Reintentos avanzados y colas asíncronas
- Orquestación con scheduler

---

## 4. Relación con Fase 1

La Fase 2 no redefine el schema: lo reutiliza. El modelo Pydantic creado en Fase 1 actúa como contrato de entrada para la API, de modo que la validación deja de ser solo una comprobación local y pasa a convertirse en una barrera real de ingesta.

Esto es importante porque impide que Bronze se convierta en un almacén de JSON arbitrario. La fase consolida el cambio desde “generar datos válidos” hacia “aceptar solo datos válidos”.

---

## 5. Arquitectura de la fase

La arquitectura implementada puede resumirse así:

1. Cliente HTTP envía un payload JSON con `batchsource` y `events`.
2. Flask parsea el request.
3. Se valida el tamaño y la forma general del payload.
4. Cada evento se valida con Pydantic.
5. Si existe cualquier error, el batch se rechaza completo con HTTP 400.
6. Si todo valida, se construye un batch con metadata de ingesta.
7. Se calcula el key de escritura según el contrato Bronze.
8. Se persiste el batch en MinIO mediante API S3-compatible.
9. El servicio devuelve metadata de persistencia al cliente.

---

## 6. Componentes implementados

### 6.1. Flask

Flask actúa como receptor HTTP del pipeline. Su responsabilidad es aceptar requests, parsear JSON, delegar validación y devolver respuestas claras al productor.

### 6.2. Pydantic

Pydantic reutiliza el modelo de Fase 1 y aplica validación inmediata sobre cada evento recibido. El servicio no persiste payloads que no superen este filtro.

### 6.3. MinIO

MinIO funciona como object storage compatible con S3 para la capa Bronze. El sistema escribe batches completos como objetos JSON con key determinista en estructura y metadata asociada.

### 6.4. Cliente de almacenamiento

La lógica de escritura no queda incrustada en la vista Flask, sino abstraída en un cliente de almacenamiento. Esto separa claramente orquestación HTTP y persistencia.

### 6.5. Configuración centralizada

La fase incluye un módulo de configuración con variables de entorno para host, puerto, endpoint S3, credenciales, bucket y parámetros de batching.

---

## 7. Endpoints del servicio

### `GET /health`

Endpoint de comprobación básica del servicio. Se utiliza para verificar que el receptor está levantado y respondiendo correctamente.

### `POST /events`

Endpoint principal de ingesta. Recibe batches de eventos en JSON y devuelve:

- `200 OK` si el batch es válido y se persiste correctamente
- `400 Bad Request` si el payload está vacío, mal formado o contiene eventos inválidos
- `500 Internal Server Error` si falla la persistencia en almacenamiento

---

## 8. Contrato de entrada

El request esperado para `POST /events` contiene:

- `batchsource`: identificador del origen del batch
- `events`: lista de eventos

La fase también introduce controles de seguridad operativa básicos:

- límite de eventos por request
- rechazo de payload vacío
- rechazo de JSON inválido
- rechazo de batches con raids mezcladas

Este último punto es importante porque el layout Bronze se particiona por `raidid`, así que un batch debe pertenecer a una única raid.

---

## 9. Validación schema-on-write

La validación se produce antes de cualquier escritura. Cada evento del batch se intenta instanciar como modelo Pydantic y, si al menos uno falla, el batch entero se rechaza.

Este comportamiento implementa una estrategia de todo-o-nada a nivel de request. Aún no hay persistencia parcial ni desvío productivo a DLQ, por lo que el criterio es mantener Bronze limpio aunque eso implique rechazar el lote completo.

### Reglas operativas relevantes

- JSON mal formado → `400`
- payload vacío → `400`
- demasiados eventos en una petición → `400`
- evento inválido según schema → `400`
- mezcla de múltiples raids en un batch → `400`
- fallo de escritura en MinIO → `500`

---

## 10. Micro-batching

La fase no se orienta a guardar un objeto por evento, sino batches de eventos. Esto reduce overhead de objetos pequeños y mejora la eficiencia de listados y lectura posterior.

### Parámetros documentados

- `BATCH_MAX_EVENTS = 500`
- `BATCH_MAX_TIMEOUT_SECONDS = 60`
- `MAX_EVENTS_PER_REQUEST = 10000`

Aunque la implementación presentada construye el batch en memoria a partir del payload recibido, la fase ya deja fijado el criterio de micro-batch como contrato operativo de diseño.

---

## 11. Construcción del batch

Una vez validados los eventos, el servicio construye un payload enriquecido con metadata de ingesta. El batch incluye, como mínimo:

- `batchid`
- `batchsource`
- `ingesttimestamp`
- `ingest_date`
- `eventcount`
- `events`

Esto convierte el objeto Bronze en una unidad de almacenamiento trazable, no solo en una lista cruda de eventos.

---

## 12. Contrato de escritura Bronze

La fase define explícitamente el contrato de escritura en MinIO. No basta con “guardar un JSON”; hay que guardar con una estructura estable, versionada y fácil de recorrer.

### Bucket

- `bronze`

### Layout lógico

- namespace del tipo de dato
- versión del layout
- partición por `raidid`
- partición por `ingest_date`
- fichero batch con UUID

### Ejemplo conceptual

```text
s3://bronze/wowraidevents/v1/raidid=raid001/ingest_date=2026-01-15/batch-550e8400-e29b-41d4-a716-446655440000.json
```

**Principios del contrato:**

- Inmutabilidad del objeto

- Versionado del layout

- Cálculo determinista de la estructura del key

- Particionado por dimensiones de acceso natural

- Ingest date en UTC

- Batch id único

## 13. Metadata de objeto

Además del body JSON, la escritura añade metadata útil para trazabilidad y debugging, por ejemplo:

- batch-id

- event-count

- ingest-timestamp

- raid-id

Esto mejora la inspección del objeto sin necesidad de parsear siempre el contenido completo.

## 14. Configuración por entorno

La fase centraliza configuración mediante variables de entorno. Entre las más relevantes:

- FLASK_HOST

- FLASK_PORT

- FLASK_DEBUG

- S3_ENDPOINT_URL

- S3_ACCESS_KEY

- S3_SECRET_KEY

- S3_BUCKET_BRONZE

- BATCH_MAX_EVENTS

- BATCH_MAX_TIMEOUT_SECONDS

- MAX_EVENTS_PER_REQUEST

Este enfoque reduce hardcoding y facilita ejecución local, pruebas y futura containerización.

## 15. Estructura lógica del código

La organización de la fase separa responsabilidades en módulos:

`app.py`

**Responsable de:**

- Inicializar Flask

- Exponer endpoints

- Parsear requests

- Coordinar validación y persistencia

- Devolver respuestas HTTP

`storage/minioclient.py`

**Responsable de:**

- Construir el cliente S3-compatible

- Calcular keys Bronze

- Persistir batches

- Listar objetos por prefijo cuando sea necesario

`config.py`

**Responsable de:**

- Centralizar parámetros de ejecución

- Exponer configuración tipada

`tests/`

**Responsable de:**

- Verificar endpoint de salud

- Comprobar ingesta válida

- Comprobar rechazo de ingesta inválida

## 16. Estrategia de errores

La fase adopta una estrategia simple y robusta:

- **Errores del cliente → 400**

- **Errores de almacenamiento → 500**

No se implementa todavía una Dead Letter Queue productiva, pero la documentación ya deja ese camino preparado como evolución natural. En el estado actual, la prioridad es garantizar que Bronze solo contenga objetos válidos.

## 17. Testing

La fase contempla al menos tres niveles de comprobación.

### 17.1. Unit tests

Validan comportamiento del receptor y respuestas esperadas ante payloads válidos e inválidos.

### 17.2. Smoke tests manuales

Permiten lanzar el servidor y enviar un batch manual para confirmar persistencia y respuesta.

### 17.3. Verificación en MinIO

Confirma que el objeto realmente existe en Bronze, que su key respeta el contrato y que el JSON interno puede inspeccionarse correctamente.

## 18. Resultado funcional de la fase

La Fase 2 deja resuelto el primer tramo operativo del pipeline:

- Existe un punto de entrada por red

- El sistema recibe JSON de telemetría

- El contrato de Fase 1 se aplica en tiempo real

- Los eventos válidos se almacenan en Bronze

- la organización física del almacenamiento ya prepara el trabajo de Fase 3

Esto convierte el proyecto en un pipeline real de ingesta, aunque todavía no haya transformación analítica aguas abajo.

## 19. Limitaciones actuales

Aun siendo funcional, la fase mantiene limitaciones explícitas:

- Bronze almacena JSON raw, no Parquet

- No hay deduplicación todavía

- No existe DLQ operativa

- El flujo sigue siendo síncrono

- No hay cola intermedia ni broker

- La recuperación de batches por API no forma parte del contrato productivo

- El particionado está diseñado manualmente, no mediante table formats

Estas limitaciones son coherentes con el objetivo de la fase y se resuelven en etapas posteriores.

## 20. Relación con Fase 3

La Fase 3 toma como entrada los batches JSON persistidos en Bronze y realiza:

- Lectura por prefijos

- Parseo estructurado

- Limpieza

- Deduplicación

- Enriquecimiento

- Escritura a Silver en formato columnar

Dicho de otra forma: Fase 2 asegura entrada válida y trazable; Fase 3 convierte esa materia prima en datos procesables y eficientes.

## 21. Resumen técnico

| **Elemento** | **Decisión** |
| --- | --- |
| Entrada |	HTTP POST /events |
| Framework | Flask |
| Validación | Pydantic reutilizando Fase 1 |
|Estrategia | Schema-on-write |
| Almacenamiento | MinIO S3-compatible |
| Capa destino | Bronze |
| Formato | JSON raw por batch |
| Particiones | raidid, ingest_date |
| Batch | micro-batch |
| Configuración | Variables de entorno |
| Estado | Completa |

## 22. Criterio de cierre

La fase puede considerarse cerrada cuando se cumplen estas condiciones:

- El servicio responde en GET /health

- Acepta batches válidos por POST /events

- Rechaza batches inválidos con 400

- No persiste datos inválidos

- Escribe batches válidos en MinIO Bronze

- El key sigue el layout definido

- El contenido almacenado es legible y trazable

- La salida deja preparado el salto a Silver

---

**Estado:** Completa  
**Rol de la fase:** entrada operativa y persistencia raw en Bronze  
**Siguiente fase:** ETL Bronze → Silver  
