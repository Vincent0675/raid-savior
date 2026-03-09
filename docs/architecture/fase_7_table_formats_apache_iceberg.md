# Fase 7 — Migración de Silver y Gold a Apache Iceberg sobre MinIO

## 1. Propósito de la fase

La Fase 7 introduce table formats modernos en el pipeline Medallion mediante Apache Iceberg. Su objetivo es transformar las capas Silver y Gold, que hasta ahora funcionan como directorios Parquet, en tablas gestionadas con metadatos transaccionales sobre MinIO.

Esta fase no cambia la lógica conceptual de Bronze, Silver y Gold, sino la forma en la que Silver y Gold se materializan, versionan y evolucionan. El propósito es preparar el proyecto para una arquitectura más cercana a producción real, con mejores garantías de consistencia y mayor flexibilidad futura.

---

## 2. Estado actual de la fase

La Fase 7 es la fase vigente del proyecto. Dentro de ella, la subfase actualmente en ejecución es la 7.2, centrada en Silver ACID para eventos limpios.

Esto implica que la fase debe documentarse como una transición en curso, no como una migración ya cerrada por completo. La arquitectura objetivo está definida, pero el foco operativo actual está en consolidar Silver como tabla Iceberg.

---

## 3. Objetivo técnico global

El objetivo técnico de la fase es migrar Silver y Gold a Apache Iceberg sobre MinIO para obtener:

- ACID
- time travel
- mutaciones controladas
- evolución de schema
- desacoplamiento entre almacenamiento y motor de consulta

En términos de ingeniería, se pasa de almacenar solo “archivos correctos” a gestionar “tablas con contrato transaccional y memoria histórica”.

---

## 4. Por qué Iceberg entra ahora

Hasta la Fase 6, las capas Silver y Gold ya son funcionales, pero descansan sobre directorios de archivos Parquet. Ese diseño funciona para lotes y prototipado, pero introduce limitaciones estructurales cuando el proyecto busca escalabilidad real y evolución operativa.

Los problemas identificados son:

- ausencia de ACID
- ausencia de historial consultable
- rigidez ante cambios de schema
- dependencia práctica de Spark como motor dominante de acceso

Apache Iceberg entra en Fase 7 precisamente para resolver ese cuello de botella arquitectónico.

---

## 5. Problemas concretos del estado anterior

### Sin ACID

Si una escritura falla a mitad del proceso, el estado físico del directorio puede quedar inconsistente. Esto es especialmente delicado en Gold, donde una materialización parcial puede mezclar datos antiguos y nuevos.

### Sin historial

Cada rematerialización pisa el estado anterior. Sin una capa transaccional de snapshots, no existe una forma limpia de consultar cómo estaba la tabla en un momento previo.

### Sin evolución de schema robusta

Añadir columnas o modificar estructuras implica mayor fricción operativa y reescrituras más agresivas sobre archivos existentes.

### Acceso menos portable

Trabajar solo con Parquet sin capa de catálogo/metadata fuerte complica la interoperabilidad con motores adicionales como Athena, Trino, Flink o entornos cloud posteriores.

---

## 6. Encaje con la visión del proyecto

Apache Iceberg encaja con el objetivo de “producción + escalabilidad real” por tres motivos principales:

- el pipeline se mantiene sobre object storage y puede evolucionar desde MinIO hacia S3 sin rediseño estructural completo
- el proyecto aspira a trabajar con volúmenes grandes de datos
- la arquitectura busca mantener abierta la puerta a más motores y servicios analíticos en el futuro

Iceberg es adecuado aquí porque se apoya en un catálogo independiente del motor y conserva el almacenamiento en Parquet, añadiendo por encima una capa de metadatos y snapshots.

---

## 7. Qué es Iceberg en este proyecto

En el contexto del proyecto, Iceberg debe entenderse como una capa de metadatos que organiza y gobierna tablas apoyadas en archivos Parquet. Añade snapshots inmutables, catálogo de tablas y semántica transaccional sin exigir cambiar de paradigma de almacenamiento.

No sustituye MinIO ni la arquitectura Medallion. Lo que hace es volver mucho más robusta la forma en que Silver y Gold se escriben, evolucionan y consultan.

---

## 8. Impacto por capa Medallion

| Capa | Estado previo | Estado objetivo con Iceberg |
|---|---|---|
| Bronze | JSON particionado en MinIO | Sin cambios; continúa como zona raw e inmutable |
| Silver | Parquet + Snappy con particionado tipo Hive | Tabla Iceberg para eventos limpios, con ACID y time travel |
| Gold | Parquet particionado para consumo analítico | Tablas Iceberg para hechos y dimensiones, con soporte de mutaciones y evolución de schema |

El cambio, por tanto, se concentra en Silver y Gold. Bronze permanece estable porque su función sigue siendo preservar el dato raw sin necesidad de semántica transaccional avanzada.

---

## 9. Subfases definidas

La fase se divide en cinco subfases ya definidas:

- 7.1 Catálogo Iceberg sobre MinIO
- 7.2 Silver ACID (eventos limpios)
- 7.3 Gold ACID: tablas de hechos
- 7.4 Gold ACID: dimensiones
- 7.5 Time travel y correcciones de negocio

Esta secuencia es correcta desde un punto de vista de ingeniería: primero se valida el plano de control y catálogo, luego se consolida Silver, y después se asciende a Gold con tablas más sensibles a lógica de negocio.

---

## 10. Estado real de avance

### 7.1 ya aterrizada

La documentación indica que ya existe la subfase de catálogo Iceberg sobre MinIO, apoyada por `inspect_iceberg_connection.py`. También consta un ping-test funcional en `wow.gold.ping_test` con time travel.

### 7.2 en proceso

El foco activo de desarrollo está en Silver ACID. Esto significa que la prioridad actual es transformar la capa de eventos limpios desde directorios Parquet particionados hacia una tabla Iceberg transaccional y consultable con historial.

### 7.3, 7.4 y 7.5 como continuación

Las tablas Gold ACID de hechos, las dimensiones y las correcciones apoyadas en time travel forman parte de la arquitectura objetivo de la fase, pero no deben documentarse como cierre ya completado.

---

## 11. Decisiones técnicas confirmadas

Las decisiones técnicas explícitas para esta fase son:

- usar PySpark 3.5 como motor principal
- usar catálogo tipo Hadoop catalog sobre MinIO
- trabajar con namespaces `wow.silver` y `wow.gold`
- mantener buenas prácticas compatibles con Python 3.10.x
- contemplar Pytest cuando sea útil
- sostener disciplina de calidad con Ruff, MyPy y Pytest

Estas decisiones son coherentes con una fase de transición hacia tablas gestionadas y con el stack ya consolidado en fases anteriores.

---

## 12. Papel de PySpark 3.5

PySpark 3.5 se mantiene como motor principal de ejecución en esta fase. Su papel ya no es solo procesar datasets, sino operar sobre tablas Iceberg dentro de un flujo más cercano a lakehouse que a data lake basado únicamente en ficheros.

En otras palabras, Spark pasa de ser solo un procesador de lotes a convertirse en uno de los motores principales de manipulación sobre tablas transaccionales.

---

## 13. Papel del catálogo

El catálogo tipo Hadoop sobre MinIO es una pieza central de esta fase. Su función es dar un nombre lógico estable a las tablas y conectar ese nombre con sus metadatos físicos y snapshots.

Eso permite separar mejor tres planos que antes estaban demasiado pegados:

- almacenamiento físico
- metadatos de tabla
- motor de ejecución

Esta separación es una mejora arquitectónica importante porque reduce el acoplamiento del sistema.

---

## 14. Subfase 7.2: Silver ACID

La subfase activa del proyecto es 7.2, dedicada a Silver ACID para eventos limpios. Su misión es convertir Silver en una tabla Iceberg que conserve el valor de la capa refined, pero añadiendo garantías transaccionales e historial.

Desde la lógica Medallion, esto es una mejora del “filtro” de calidad de Silver. La señal ya estaba limpia; ahora además queda encapsulada en una estructura con memoria, rollback lógico y mejor capacidad de evolución.

---

## 15. Qué aporta Silver Iceberg

La migración de Silver a Iceberg aporta varios beneficios prácticos:

- escrituras más seguras
- snapshots consultables
- base para time travel
- mejor soporte para evolución de schema
- mejor interoperabilidad futura con otros motores

Silver sigue siendo la capa de eventos limpios, tipados y refinados, pero deja de depender solo de convenciones de carpetas y archivos para su integridad operativa.

---

## 16. Gold dentro de la Fase 7

Aunque el trabajo actual esté en Silver, la Fase 7 ya define el destino natural de Gold dentro del mismo modelo. Gold deberá evolucionar hacia tablas Iceberg separadas entre hechos y dimensiones, manteniendo la lógica semidimensional ya introducida previamente.

El cambio aquí no es rehacer el modelo analítico, sino materializarlo sobre una base con mutaciones controladas, time travel y mejor gobernanza.

---

## 17. Relación con fases anteriores

La Fase 7 reutiliza todo lo construido antes:

- Bronze ya existe como raw validado
- Silver ya existe como refined en Parquet
- Gold ya existe como capa analítica y semidimensional
- Spark ya se introdujo como motor distribuido
- Dagster ya dio automatización operacional al pipeline

Lo que hace ahora Iceberg es elevar la madurez del sistema sin romper esa evolución previa.

---

## 18. Relación con fases posteriores

La Fase 7 prepara el terreno para las fases siguientes. Una vez Silver y Gold estén soportadas sobre Iceberg, servir métricas por API, conectar dashboards, entrenar ML o integrar datos reales tendrá una base más robusta.

Esto es importante porque una capa Gold consumible gana mucho valor cuando está respaldada por snapshots, evolución de schema y mutaciones controladas.

---

## 19. Resumen técnico

| Elemento | Decisión |
|---|---|
| Fase actual | Fase 7 |
| Subfase activa | 7.2 Silver ACID |
| Objetivo global | migrar Silver/Gold a Apache Iceberg |
| Storage base | MinIO |
| Motor principal | PySpark 3.5 |
| Catálogo | Hadoop catalog |
| Namespaces | `wow.silver`, `wow.gold` |
| Beneficios buscados | ACID, time travel, schema evolution, mutaciones controladas |
| Estado | en proceso |

---

## 20. Criterio de cierre de la fase

La Fase 7 podrá darse por cerrada cuando se cumplan estas condiciones:

- catálogo Iceberg operativo sobre MinIO
- Silver materializada como tabla Iceberg ACID
- Gold materializada en tablas Iceberg para hechos y dimensiones
- soporte funcional de time travel
- capacidad de aplicar correcciones de negocio sobre una base versionada
- continuidad de la arquitectura Medallion sin ruptura de capas

---

**Estado de la fase:** actual  
**Rol de la fase:** transición de Silver/Gold desde Parquet gestionado por carpetas hacia tablas Iceberg con metadatos transaccionales
