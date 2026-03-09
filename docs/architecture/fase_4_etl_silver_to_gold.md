# Fase 4 — Transformación Silver a Gold y Modelo Semidimensional Inicial

## 1. Propósito de la fase

La Fase 4 implementa la capa Gold del pipeline Medallion. Su objetivo es transformar los datos refinados de Silver en entidades analíticas de alto valor, orientadas a métricas, reporting y explotación posterior.

Si Silver resuelve limpieza, tipado y enriquecimiento técnico, Gold resuelve semántica de negocio. En términos industriales, Silver equivale a material procesado y clasificado; Gold equivale al producto ya ensamblado y listo para inspección funcional y consumo operativo.

---

## 2. Objetivos técnicos

Los objetivos principales de la fase son:

- leer datasets refinados desde Silver
- agregar información a nivel raid y a nivel jugador
- construir la versión inicial del modelo semidimensional de Gold
- consolidar tablas de consumo analítico
- definir KPIs de rendimiento y resultado
- dejar una base estable para visualización, serving y machine learning

---

## 3. Estado y alcance real

### Incluido en la fase

- ETL Silver → Gold
- procesamiento analítico en memoria
- uso de Pandas como motor principal de transformación
- soporte de modelado y consulta con DuckDB
- creación inicial del modelo semidimensional de hechos y dimensiones
- generación de tablas Gold orientadas a negocio
- cálculo de métricas globales de raid
- cálculo de métricas individuales por jugador y raid

### Fuera de alcance en esta fase

- serving por API en producción
- dashboards finales de explotación
- Gold distribuido sobre Spark
- table formats con ACID y time travel
- integración con datos reales externos
- capa de consumo operacional definitiva

---

## 4. Papel de Gold en la arquitectura

La capa Gold representa la vista de negocio del pipeline. Aquí los datos dejan de estar organizados principalmente para ingeniería y pasan a organizarse para responder preguntas analíticas.

Gold actúa como fuente única de verdad para métricas finales. Esto evita recalcular KPIs complejos cada vez que un consumidor necesita consultar rendimiento, resultado de raid o comportamiento de jugadores.

---

## 5. Relación con las fases anteriores

La Fase 1 definió el contrato del evento.  
La Fase 2 aseguró la ingesta válida en Bronze.  
La Fase 3 limpió, tipó y optimizó los datos en Silver.  
La Fase 4 aprovecha esa base para construir métricas consolidadas y un modelo semidimensional inicial orientado a consumo.

Dicho de otro modo:

- Bronze conserva raw validado
- Silver refina la señal
- Gold entrega información útil para decisión y análisis

---

## 6. Decisión tecnológica

La fase se apoya en procesamiento en memoria con Pandas y soporte analítico con DuckDB.

### Justificación

- el roadmap del proyecto describe la Fase 4 como ETL Silver → Gold con analítica en Pandas
- la misma documentación indica que aquí se crea la versión inicial del modelo semidimensional con hechos y dimensiones
- DuckDB encaja bien como capa de consulta y estructuración local del modelo Gold

Esta decisión minimiza complejidad y coste computacional en una etapa donde lo más importante es consolidar semántica analítica, no escalar horizontalmente.

---

## 7. Naturaleza del modelo Gold

La Fase 4 no se limita a hacer agregaciones sueltas. Según el roadmap del proyecto, aquí se construye la versión inicial del modelo semidimensional de Gold.

Eso significa que la semidimensionalidad no queda relegada a una fase posterior, sino que nace ya en esta etapa como base funcional. Las fases posteriores amplían escalabilidad, serving, table formats o integración externa, pero no “inventan desde cero” la estructura semidimensional.

---

## 8. Tablas Gold consolidadas

La documentación del proyecto identifica dos tablas principales en Gold.

### 8.1. `gold.raidsummary`

Representa el nivel global del encuentro. Su granularidad es una fila por raid o encounter.

Su función es resumir el resultado y comportamiento agregado de la sesión. Aquí vive la lectura ejecutiva del combate.

### 8.2. `gold.playerraidstats`

Representa el nivel individual. Su granularidad es una fila por jugador y raid.

Su función es describir el rendimiento de cada participante dentro del contexto del encounter, sin perder comparabilidad entre jugadores.

---

## 9. Granularidad y diseño analítico

La clave de una capa Gold sana es controlar bien la granularidad.

### `gold.raidsummary`

- 1 fila por raid o encounter
- nivel de resumen global
- útil para outcome, rendimiento total y análisis longitudinal de raids

### `gold.playerraidstats`

- 1 fila por jugador y raid
- nivel de rendimiento individual
- útil para comparar DPS, HPS, muertes y participación relativa

Esta separación es coherente con una modelización semidimensional inicial: una entidad resume el comportamiento global del encounter y otra expone el detalle analítico por jugador.

---

## 10. KPIs de raid

La tabla de resumen global incorpora el KPI crítico `raidoutcome`, clasificado como `Success` o `Wipe`.

Según el contexto del proyecto, este resultado se deriva mediante lógica de negocio aplicada sobre variables del encounter, como umbral de vida del boss y límite de muertes. Esto convierte un conjunto de eventos técnicos en una señal de negocio interpretable.

Otros campos esperables en este nivel incluyen métricas agregadas del encuentro, duración, volumen total de daño, curación o indicadores equivalentes de cierre del combate.

---

## 11. KPIs de jugador

La tabla `gold.playerraidstats` concentra métricas individuales relevantes para análisis comparativo.

Entre los KPIs documentados del proyecto se incluyen:

- DPS
- HPS
- `critrate`
- participación relativa en daño
- muertes individuales

Esta tabla es especialmente valiosa porque sirve tanto para análisis de rendimiento como para futuros modelos de clasificación o clustering de estilos de juego.

---

## 12. Lógica de negocio

La Fase 4 es el punto donde la lógica de negocio entra de forma explícita. Antes de Gold, el pipeline trabaja sobre estructura, validez y calidad técnica; en Gold aparece la interpretación.

Ejemplo claro: `raidoutcome` no existe como campo bruto en el evento. Es una variable derivada que condensa múltiples señales del combate en una etiqueta operativa.

Esto es análogo a un sistema de control industrial que no solo mide sensores por separado, sino que emite un estado consolidado como “proceso estable”, “alarma” o “fallo”.

---

## 13. Flujo general Silver → Gold

El flujo lógico de la fase puede resumirse así:

1. leer datasets refinados desde Silver
2. seleccionar las columnas relevantes para analítica
3. agrupar por granularidad de raid o de jugador
4. calcular métricas agregadas y ratios
5. aplicar reglas de negocio
6. materializar entidades Gold finales

Este flujo convierte datos refinados en producto analítico.

---

## 14. Papel de Pandas

Pandas actúa como motor principal de agregación y transformación analítica. Su rol en esta fase no es limpiar datos raw, sino construir métricas, consolidar granularidades y preparar tablas consumibles.

Esto encaja bien con el estado del proyecto porque el dataset ya viene limpio desde Silver. La CPU local se dedica aquí a agregación, cálculo de KPIs y construcción del modelo Gold inicial.

---

## 15. Papel de DuckDB

DuckDB aporta una capa muy útil para consulta analítica local y manejo eficiente de estructuras tabulares. En esta fase complementa a Pandas en tareas de exploración, agregación o estructuración del modelo semidimensional inicial.

Su valor está en ofrecer una experiencia de análisis tipo SQL sobre datos locales, con muy bajo overhead operativo. Es una decisión coherente antes de migrar Gold a un motor distribuido en fases posteriores.

---

## 16. Contrato conceptual de Gold

La documentación disponible no detalla un layout físico completo de la misma forma que Bronze o Silver, pero sí deja clara la función del contrato de Gold: exponer entidades estables, legibles y orientadas a consumo.

### Entidades principales

- `gold.raidsummary`
- `gold.playerraidstats`

### Principios del contrato

- granularidad explícita
- KPIs definidos por lógica de negocio
- nombres estables y semánticos
- separación entre visión global e individual
- base reutilizable para dashboards, APIs y ML
- semidimensionalidad inicial ya materializada en esta fase

---

## 17. Resultado funcional de la fase

El contexto maestro del proyecto indica que las agregaciones Gold están operativas y que ambas tablas principales ya se consideran la fuente única de verdad analítica del sistema.

Eso significa que el pipeline alcanza aquí un hito importante: deja de ser solo una tubería de ingestión y refinamiento y pasa a generar un modelo analítico inicial con semidimensionalidad funcional.

---

## 18. Valor para fases posteriores

La capa Gold desbloquea varias líneas de trabajo posteriores:

- visualización técnica y de negocio
- endpoints de consulta vía API
- dashboards ligeros
- predicción de éxito o wipe
- clustering de estilos de juego
- serving de históricos y KPIs

Esto es importante porque fases como visualización, serving o IA no deberían apoyarse directamente sobre Silver si Gold ya encapsula las métricas consolidadas y la estructura analítica base.

---

## 19. Limitaciones actuales

Aunque la Fase 4 está completa a nivel de objetivo inicial, mantiene limitaciones naturales:

- el procesamiento sigue siendo local
- el modelo semidimensional es inicial, no definitivo
- no hay serving productivo todavía
- no hay motor distribuido para Gold en esta fase
- no se usan aún formatos de tabla con mutabilidad controlada
- la integración con datos reales aún queda pendiente

Estas limitaciones son coherentes con el roadmap, que mueve el Gold distribuido a la fase siguiente y deja serving e integración real para etapas posteriores.

---

## 20. Relación con Fase 5

El roadmap del proyecto sitúa la siguiente evolución como una migración del procesamiento Gold a un motor distribuido usando PySpark 3.5 y conectores S3A.

Por tanto, la Fase 4 debe entenderse como la consolidación funcional y semántica del modelo Gold inicial, mientras que la Fase 5 resuelve escalabilidad y ejecución distribuida.

En una analogía electrónica, Fase 4 calibra el circuito y demuestra que la lógica funciona; Fase 5 rediseña la etapa de potencia para soportar más carga.

---

## 21. Resumen técnico

| Elemento | Decisión |
|---|---|
| Entrada | datos refinados desde Silver |
| Tipo de procesamiento | analítica y agregación |
| Motor principal | Pandas |
| Soporte analítico | DuckDB |
| Capa destino | Gold |
| Modelo | semidimensional inicial |
| Tabla global | `gold.raidsummary` |
| Tabla individual | `gold.playerraidstats` |
| KPI clave de raid | `raidoutcome` |
| Estado | completa |

---

## 22. Criterio de cierre

La Fase 4 puede darse por cerrada cuando se cumplen estas condiciones:

- existe una capa Gold operativa
- se ha construido la versión inicial del modelo semidimensional
- se han consolidado tablas analíticas estables
- la granularidad global e individual está separada
- los KPIs principales de raid y jugador están calculados
- `raidoutcome` se obtiene mediante lógica de negocio explícita
- Gold puede actuar como fuente para visualización, APIs y ML

---

**Estado:** completa  
**Rol de la fase:** consolidación de métricas de negocio y modelo semidimensional inicial en la capa Gold  
**Siguiente fase:** escalado del procesamiento Gold a motor distribuido
