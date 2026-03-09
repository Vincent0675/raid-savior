# Fase 3 — Transformación Bronze a Silver con Pandas y Parquet

## 1. Propósito de la fase

La Fase 3 implementa la primera capa de refinamiento real del pipeline Medallion. Su objetivo es transformar los batches JSON crudos almacenados en Bronze en datasets limpios, tipados, enriquecidos y optimizados para lectura analítica en Silver.

Si Fase 2 resolvió la entrada y la persistencia raw, Fase 3 resuelve la calidad operativa del dato. En una analogía industrial, Bronze es el material recibido de planta; Silver es el material ya inspeccionado, clasificado y preparado para procesos de mayor valor.

---

## 2. Objetivos técnicos

Los objetivos principales de la fase son:

- Leer batches JSON desde MinIO Bronze
- Convertir eventos a estructura tabular
- Aplicar limpieza y normalización
- Corregir tipos de datos
- Eliminar duplicados
- Validar rangos lógicos básicos
- Enriquecer eventos con columnas derivadas
- Persistir la salida en Parquet con compresión Snappy
- Organizar la capa Silver con particionado útil para consultas posteriores

---

## 3. Estado y alcance real

### Incluido en la fase

- Lectura de objetos JSON desde Bronze
- Transformación de batches a `pandas.DataFrame`
- Pipeline de casteo de tipos
- Deduplicación por `eventid`
- Validación básica de rangos
- Enriquecimiento de columnas
- Escritura a Parquet con PyArrow
- Compresión Snappy
- Persistencia en bucket Silver
- Script de prueba de extremo a extremo Bronze → Silver

### Fuera de alcance en esta fase

- Agregaciones analíticas Gold
- Modelo dimensional
- Serving de KPIs
- Procesamiento distribuido con Spark
- Iceberg / Delta / ACID
- Schema evolution gestionado por table formats
- Orquestación formal del pipeline

---

## 4. Papel de Silver en la arquitectura Medallion

La capa Silver representa datos refinados. Aquí los datos ya no se almacenan “tal como llegaron”, sino que pasan por una etapa controlada de transformación para mejorar:

- calidad
- consistencia
- eficiencia de almacenamiento
- rendimiento de lectura
- utilidad analítica

Bronze conserva trazabilidad raw. Silver introduce disciplina estructural para que las fases analíticas posteriores no tengan que cargar con ruido innecesario.

---

## 5. Motivación de diseño

JSON es válido como formato de ingesta y trazabilidad, pero no es el formato adecuado para analítica intensiva. La Fase 3 cambia el centro de gravedad desde almacenamiento row-based a almacenamiento columnar.

Este cambio responde a tres necesidades:

1. Reducir tamaño en disco
2. Leer solo las columnas necesarias
3. Preparar datos para consultas, agregaciones y motores posteriores

---

## 6. Decisión tecnológica: Pandas + PyArrow

La fase adopta Pandas y PyArrow como stack principal de transformación.

### Razones de esta decisión

- El volumen de datos objetivo en esta fase es manejable localmente
- El entorno del proyecto favorece ejecución simple y rápida en portátil
- Pandas ofrece velocidad suficiente para lotes pequeños y medianos
- PyArrow proporciona soporte nativo sólido para Parquet

No se eligió Spark en esta etapa porque su overhead sería desproporcionado para el tamaño y complejidad actual del pipeline.

---

## 7. Flujo general del pipeline

El flujo implementado en Fase 3 puede resumirse así:

1. Leer un batch JSON desde Bronze
2. Parsear el contenido y extraer `events`
3. Convertir eventos a `DataFrame`
4. Aplicar pipeline de transformación
5. Construir metadata del proceso
6. Calcular la ruta destino en Silver
7. Serializar a Parquet con compresión Snappy
8. Persistir en MinIO Silver

---

## 8. Componentes implementados

### 8.1. `transformers.py`

Encapsula la lógica de limpieza, validación y enriquecimiento. Se separa del orquestador para que la lógica de transformación sea reutilizable y testeable.

### 8.2. `bronzetosilver.py`

Actúa como orquestador principal del ETL. Se encarga de leer, transformar y guardar.

### 8.3. Script de prueba

Se incluye un script para generar o emular un batch de prueba, persistirlo en Bronze y ejecutar el flujo completo hasta Silver.

---

## 9. Lectura desde Bronze

La entrada del ETL es un batch JSON previamente persistido en Bronze. El orquestador obtiene el objeto desde MinIO usando su key y parsea el body para recuperar:

- metadata del batch
- lista de eventos
- contexto de ingesta

El batch debe contener eventos. Si la lista está vacía, el proceso falla explícitamente.

---

## 10. Conversión a DataFrame

Una vez recuperada la lista de eventos, se convierte en un `pandas.DataFrame`. Este paso es importante porque Silver trabaja sobre estructura tabular, no sobre listas de diccionarios sueltos.

La conversión a DataFrame permite aplicar transformaciones vectorizadas y preparar una salida columnar coherente.

---

## 11. Transformaciones principales

La fase organiza las transformaciones como un pipeline secuencial.

### 11.1. Casteo de tipos

Se convierten las columnas a tipos adecuados:

- timestamps a `datetime` con UTC
- columnas numéricas a tipos numéricos reales
- identificadores y nombres a strings consistentes

Este paso corrige problemas típicos de datos raw, como números serializados como texto o fechas tratadas como cadenas.

### 11.2. Deduplicación

Se eliminan duplicados exactos usando `eventid` como clave principal. La deduplicación conserva la primera ocurrencia y registra cuántos duplicados fueron eliminados.

### 11.3. Validación de rangos

Se aplica validación básica sobre columnas sensibles, por ejemplo:

- porcentajes de vida entre 0 y 100
- daño y sanación no negativos

Las filas fuera de rango se eliminan del dataset final y se registran mensajes de validación.

### 11.4. Enriquecimiento

Se añaden columnas derivadas útiles para análisis posterior. Entre ellas:

- `ingestlatencyms`
- `ismassivehit`
- `ingest_date`

Esto convierte Silver en una capa no solo limpia, sino también informativamente más rica.

---

## 12. Columnas derivadas relevantes

### `ingestlatencyms`

Calcula la diferencia entre `ingesttimestamp` y `timestamp`, expresada en milisegundos. Esta métrica ayuda a cuantificar latencia entre ocurrencia del evento e ingesta.

### `ismassivehit`

Marca eventos de daño especialmente altos. Sirve como primera señal derivada para futuras analíticas o detección de anomalías.

### `ingest_date`

Extrae la fecha lógica del evento a partir del timestamp. Esta columna se usa para particionamiento y facilita procesamiento por ventanas diarias.

---

## 13. Metadata del proceso ETL

Además del DataFrame transformado, la fase construye metadata útil sobre la ejecución, por ejemplo:

- `batchid`
- `originaleventcount`
- `finaleventcount`
- `duplicatesremoved`
- `validationerrors`
- `rowsaftervalidation`
- `transformationtimestamp`

Esta metadata es clave para trazabilidad operativa y debugging.

---

## 14. Escritura en Silver

La salida se persiste en MinIO Silver en formato Parquet. El proceso de escritura utiliza:

- motor `pyarrow`
- compresión `snappy`
- `index=False`

### Condiciones de guardado

- si el DataFrame final está vacío, no se escribe
- la ruta Silver se calcula antes de persistir
- se añade metadata básica al objeto almacenado

---

## 15. Contrato de escritura Silver

La fase define un layout estable para la capa Silver.

### Bucket

- `silver`

### Particiones

- `raidid`
- `ingest_date`

### Patrón conceptual

```text
s3://silver/wowraidevents/v1/raidid=raid001/ingest_date=2026-01-21/part-<batchid>.parquet
```

**Principios del layout**

- Separación por dimensión de negocio (raidid)

- Separación temporal (ingest_date)

- Trazabilidad por batchid

- Estructura versionada

- Compatibilidad con lecturas posteriores por prefijo

## 16. Justificación de Parquet

Parquet se adopta porque mejora de forma clara la capa analítica respecto a JSON.

### Ventajas principales

- formato columnar
- mejor compresión
- lectura selectiva de columnas
- mejor rendimiento en consultas analíticas
- schema explícito
- mejor base para evolución futura

### Compresión elegida

Se utiliza Snappy por su equilibrio entre velocidad de escritura y compresión razonable, apropiado para una fase de procesamiento frecuente.

---

## 17. Validaciones operativas

La fase incluye controles simples pero importantes:

- batch sin eventos → error
- DataFrame vacío tras transformación → no se guarda
- error de lectura desde Bronze → fallo explícito
- error de escritura en Silver → fallo explícito

La estrategia sigue siendo conservadora: si el lote no queda en estado coherente, no se publica en Silver.

---

## 18. Testing

La fase contempla validación funcional del flujo completo Bronze → Silver.

### Cobertura mínima esperada

- creación de un batch de prueba
- persistencia del batch en Bronze
- ejecución del ETL
- verificación de escritura `.parquet` en Silver
- comprobación de metadata del resultado

Esto confirma que la transición entre capas funciona y que la salida ya está disponible en el formato esperado.

---

## 19. Resultado funcional de la fase

La Fase 3 deja resuelta la primera transformación real del lakehouse:

- Bronze contiene JSON raw validados
- Silver contiene Parquet limpios y enriquecidos
- el dato ya está listo para consumo analítico intermedio
- la siguiente fase puede centrarse en modelado y agregación, no en saneamiento básico

---

## 20. Limitaciones actuales

Aunque Silver ya es funcional, la fase mantiene límites explícitos:

- no hay agregación de negocio final
- no existe todavía modelo de hechos y dimensiones
- la validación es básica, no semánticamente profunda
- el procesamiento sigue siendo local e in-memory
- no se ha introducido todavía Spark ni table formats
- no existe time travel ni gestión ACID

Estas limitaciones son coherentes con el objetivo de la fase: limpiar y estructurar, no todavía explotar analíticamente a gran escala.

---

## 21. Relación con Fase 4

La Fase 4 toma como entrada los datos ya refinados de Silver y construye la capa Gold. Gracias a Fase 3, Fase 4 no necesita trabajar directamente sobre JSON raw ni repetir tareas de limpieza y casteo.

Dicho de otro modo:

- Fase 2 asegura ingesta válida
- Fase 3 asegura calidad estructural y eficiencia
- Fase 4 construye valor analítico

---

## 22. Resumen técnico

| Elemento | Decisión |
|---|---|
| Entrada | JSON batch desde Bronze |
| Transformación | Pandas |
| Motor Parquet | PyArrow |
| Formato salida | Parquet |
| Compresión | Snappy |
| Bucket destino | Silver |
| Particiones | `raidid`, `ingest_date` |
| Limpieza | Sí |
| Deduplicación | Sí, por `eventid` |
| Enriquecimiento | Sí |
| Estado | completa |

---

## 23. Criterio de cierre

La Fase 3 puede darse por cerrada cuando se cumplen estas condiciones:

- se puede leer un batch real desde Bronze
- los eventos se convierten a DataFrame sin errores
- se aplican casteo, deduplicación y validación básica
- se generan columnas derivadas útiles
- el resultado se escribe en Silver como Parquet
- el archivo aparece en MinIO con la ruta esperada
- la salida queda lista para consumo por la capa Gold

---

**Estado:** completa  
**Rol de la fase:** refinamiento estructural y optimización analítica de Bronze a Silver  
**Siguiente fase:** construcción de Gold y agregaciones analíticas
