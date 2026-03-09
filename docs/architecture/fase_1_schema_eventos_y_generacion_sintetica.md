# Fase 1 — Schema del Evento y Generación de Datos Sintéticos

## 1. Propósito de la fase

La Fase 1 establece la base contractual del pipeline de telemetría de raids. Su objetivo es definir de forma explícita qué es un evento válido, cómo se representa, qué restricciones debe cumplir y cómo generar datos sintéticos consistentes para desarrollar el resto de la arquitectura sin depender todavía de una fuente real.

En términos de ingeniería, esta fase fija la especificación de la señal antes de conectar la tubería completa. En una analogía de electrónica, equivale a definir el rango, formato y tolerancias de una señal antes de acoplarla al sistema de adquisición.

---

## 2. Objetivos técnicos

Los objetivos principales de la Fase 1 son:

- Diseñar un schema formal para los eventos de telemetría
- Implementar ese schema con Pydantic v2
- Exportar el contrato a JSON Schema
- Validar eventos con reglas estructurales y de negocio
- Generar datos sintéticos plausibles con NumPy
- Garantizar reproducibilidad mediante semillas
- Dejar lista la base para la ingesta de Fase 2

---

## 3. Estado y alcance real

### Incluido en la fase

- Modelado del evento de telemetría
- Validación con Pydantic v2
- Exportación a JSON Schema
- Generación sintética de sesiones y eventos
- Serialización a JSON
- Testing básico de validación y consistencia estadística

### Fuera de alcance en esta fase

- Receptor HTTP
- Persistencia en MinIO
- Particionado físico Bronze
- Conversión a Parquet
- ETL Silver
- Modelo Gold
- Serving o visualización

### Aclaración sobre `buff`

Aunque el proyecto contempla que en el futuro exista soporte específico para eventos de tipo `buff`, ese tipo aún no se considera implementado como parte funcional cerrada de la Fase 1. Debe entenderse como una ampliación prevista del schema, no como una capacidad completada en el estado actual.

---

## 4. Decisiones de diseño

### 4.1. Contrato explícito antes de propagar datos

La primera decisión importante fue no tratar los eventos como JSON arbitrario. Se definió un contrato claro para evitar que datos ambiguos o inconsistentes contaminen el pipeline desde su origen.

Esto responde a una lógica industrial simple: si la entrada ya llega mal calibrada, cada etapa posterior tiene que gastar más coste en corregir, interpretar o rechazar señal defectuosa.

### 4.2. Pydantic v2 como núcleo de validación

Se eligió Pydantic v2 porque permite:

- Definir modelos con type hints de Python
- Añadir restricciones declarativas con `Field`
- Aplicar validaciones personalizadas
- Obtener errores claros
- Exportar JSON Schema automáticamente

La ventaja principal frente a validación manual dispersa es que el contrato queda centralizado, tipado y mantenible.

### 4.3. JSON Schema como contrato interoperable

Pydantic se usa para validación operativa, pero también se exporta el schema a JSON Schema para:

- Documentación formal
- Trazabilidad
- Interoperabilidad futura
- Versionado del contrato

### 4.4. Generación sintética basada en distribuciones

No se generan datos uniformes sin criterio. Se modelan valores con distribuciones estadísticas plausibles para que el dataset sintético sirva realmente para probar validación, volumen, lógica temporal y futuras transformaciones.

### 4.5. Reproducibilidad obligatoria

El generador usa semilla fija para garantizar que una ejecución pueda repetirse. Esto es crítico para testing, depuración y comparación entre versiones.

---

## 5. Stack de Fase 1

| Tecnología | Función |
|---|---|
| Python | Lenguaje principal |
| Pydantic v2 | Validación y definición del modelo |
| JSON Schema | Contrato formal exportable |
| NumPy | Distribuciones estadísticas |
| `uuid` | Identificación única de eventos |
| `random` | Selección discreta de entidades |

---

## 6. Definición de evento

Un evento es la unidad atómica de telemetría del sistema. Representa un hecho inmutable ocurrido durante una raid y contiene suficiente contexto para poder analizarlo más tarde sin ambigüedad.

Un evento bien definido debe responder a estas preguntas:

- Cuándo ocurrió
- Dónde ocurrió
- Quién lo originó
- Qué sucedió
- A quién o qué afectó
- Cuánto impactó
- Con qué contexto técnico fue registrado

---

## 7. Estructura general del schema

El schema de Fase 1 se organizó en bloques de información para mantener separación de responsabilidades dentro del propio evento.

### 7.1. Identificación

- `event_id`
- `event_type`
- `timestamp`

### 7.2. Contexto de raid

- `raid_id`
- `encounter_id`
- `encounter_duration_ms`

### 7.3. Actor origen

- `source_player_id`
- `source_player_name`
- `source_player_role`
- `source_player_class`
- `source_player_level`

### 7.4. Acción

- `ability_id`
- `ability_name`
- `ability_school`

### 7.5. Magnitud cuantitativa

- `damage_amount`
- `healing_amount`
- `is_critical_hit`
- `critical_multiplier`
- `is_resisted`
- `is_blocked`
- `is_absorbed`

### 7.6. Target

- `target_entity_id`
- `target_entity_name`
- `target_entity_type`
- `target_entity_health_pct_before`
- `target_entity_health_pct_after`

### 7.7. Metadata técnica

- `ingestion_timestamp`
- `source_system`
- `data_quality_flags`
- `server_latency_ms`
- `client_latency_ms`

---

## 8. Tipos de evento

### Núcleo actual

Los tipos de evento que forman el núcleo actual documentado de Fase 1 son:

- `combat_damage`
- `heal`
- `player_death`
- `spell_cast`
- `boss_phase`

### Extensión prevista

- `buff`

El evento `buff` se mantiene como evolución natural del modelo, útil para enriquecer analítica de estados temporales, sin considerarlo todavía parte cerrada del alcance implementado.

---

## 9. Validación con Pydantic v2

La validación de Fase 1 no se limita al tipado básico. También incorpora reglas de negocio. Es decir, no basta con que el JSON “tenga forma”; debe representar una señal válida dentro de los límites del sistema.

### 9.1. Reglas estructurales

- `event_id` debe ser un UUID válido
- `timestamp` debe ser un `datetime` válido
- `event_type` debe pertenecer a un enum controlado
- No se permiten campos extra
- Las cadenas se normalizan eliminando espacios sobrantes

### 9.2. Reglas de negocio

- `timestamp` no puede estar en el futuro
- `damage_amount` debe existir en eventos de daño
- `healing_amount` debe existir en eventos de curación
- Los porcentajes de salud deben permanecer en el rango 0–100
- Los niveles de jugador deben respetar límites válidos

### 9.3. Configuración del modelo

Se adopta una configuración estricta del modelo:

- `extra = "forbid"`
- `str_strip_whitespace = True`

Esto convierte el schema en una barrera de entrada, no en una simple guía documental.

---

## 10. Filosofía de validación

La fase se apoya en una lógica de validación previa a la propagación de datos. En la práctica, esto equivale a que un evento debe superar el control de calidad antes de considerarse apto para entrar en el flujo del pipeline.

Esta decisión prepara el terreno para una ingesta de tipo schema-on-write en fases posteriores, donde el receptor no aceptará payloads ambiguos o mal formados sin dejar constancia del error.

---

## 11. Generación de datos sintéticos

### 11.1. Motivación

Como aún no existe una fuente real conectada al pipeline, el proyecto necesita una forma controlada de producir eventos. Sin datos sintéticos, no se puede probar validación, serialización, volumen, secuencias temporales ni etapas posteriores.

### 11.2. Enfoque

El generador crea primero una sesión de raid consistente y después distribuye eventos a lo largo de esa ventana temporal. Esto evita registros aislados sin contexto y permite simular comportamiento de encounter.

### 11.3. Información modelada por sesión

- `raid_id`
- `start_time`
- `duration_ms`
- `players`
- `boss_id`
- `boss_name`

---

## 12. Distribuciones estadísticas utilizadas

La generación se apoya en distribuciones simples pero suficientes para simular datos plausibles de desarrollo.

### 12.1. `damage_amount`

Se modela con distribución Normal y clipping a un rango realista. La idea es representar variabilidad continua alrededor de una media esperada, igual que una señal analógica con ruido natural.

### 12.2. `healing_amount`

Se modela también con distribución Normal, con media y dispersión distintas al daño. Esto permite aproximar patrones de curación sin caer en valores rígidos o uniformes.

### 12.3. `is_critical_hit`

Se modela con Bernoulli porque es una variable binaria: ocurre o no ocurre. Esta es la analogía directa con una señal digital de dos estados.

### 12.4. `critical_multiplier`

Se calcula de forma condicionada: si el evento es crítico, toma un valor dentro de un rango plausible; si no lo es, permanece en 1.0.

### 12.5. Roles y tipos de evento

La composición de jugadores y la aparición de tipos de evento se resuelven mediante selección categórica ponderada. Esto evita composiciones absurdas y mejora el realismo del dataset.

### 12.6. Distribución temporal

Los timestamps se reparten a lo largo de la duración de la sesión, generando una secuencia temporal coherente y útil para análisis posteriores.

---

## 13. Reproducibilidad

El generador usa semilla fija para asegurar ejecuciones reproducibles. Esto permite comparar cambios de implementación sin introducir ruido aleatorio innecesario.

La reproducibilidad es especialmente importante para:

- Testing
- Debugging
- Benchmarks
- Comparación de versiones
- Demostraciones controladas

---

## 14. Componentes esperados de la fase

### `eventos_schema.py`

Responsable de:

- Definir enums
- Declarar el modelo principal del evento
- Aplicar restricciones con `Field`
- Implementar validadores personalizados
- Exportar JSON Schema

### `raid_event_generator.py`

Responsable de:

- Crear sesiones sintéticas
- Generar jugadores y bosses
- Distribuir timestamps
- Construir eventos por tipo
- Instanciar objetos validados con Pydantic

### Validación y exportación

Responsable de:

- Verificar lotes generados
- Detectar errores
- Serializar salida JSON
- Preparar datos para la siguiente fase

---

## 15. Testing esperado

La fase debe validarse desde tres ángulos:

### 15.1. Validación individual

Un evento correcto debe pasar y uno incorrecto debe fallar con error claro.

### 15.2. Generación masiva

Un lote generado por el sistema debe validarse de forma consistente sin introducir eventos inválidos por defecto.

### 15.3. Consistencia estadística

La distribución observada de variables como daño y curación debe aproximarse a los parámetros esperados.

### 15.4. Reproducibilidad

Dos ejecuciones con la misma semilla deben producir resultados equivalentes o suficientemente consistentes para testing.

---

## 16. Entregables de Fase 1

La fase se considera completada cuando existen al menos los siguientes entregables:

- Modelo de evento en Pydantic v2
- JSON Schema exportable
- Generador sintético funcional
- Dataset JSON válido
- Base conceptual lista para conectar Fase 2

---

## 17. Limitaciones actuales

Aunque la fase deja una base sólida, aún existen limitaciones claras:

- No hay integración con fuente real
- No existe todavía receptor HTTP funcional dentro de esta fase
- No hay persistencia real en object storage
- `buff` no está completado
- El modelo puede seguir evolucionando conforme maduren las necesidades analíticas

---

## 18. Relación con Fase 2

La salida natural de esta fase es la ingesta operativa. Fase 2 tomará este contrato y lo conectará con el almacenamiento Bronze mediante un receptor HTTP y persistencia en MinIO.

En términos de pipeline industrial, Fase 1 define la especificación de la señal; Fase 2 conecta esa señal con la línea de entrada del sistema.

---

## 19. Resumen técnico

| Elemento | Decisión |
|---|---|
| Contrato de datos | Definido explícitamente |
| Librería de validación | Pydantic v2 |
| Contrato interoperable | JSON Schema |
| Fuente de datos | Sintética |
| Motor estadístico | NumPy |
| Identificación | UUID v4 |
| Reproducibilidad | Seed fija |
| Tipos núcleo | damage, heal, death, spell, phase |
| Tipo futuro | buff |
| Siguiente fase | Ingesta HTTP + Bronze |

---

## 20. Criterio de cierre

La Fase 1 puede darse por cerrada cuando el proyecto responde afirmativamente a estas preguntas:

- ¿Existe un contrato formal del evento?
- ¿Ese contrato valida correctamente?
- ¿Puede exportarse a JSON Schema?
- ¿Puede generarse un volumen útil de eventos sintéticos?
- ¿La generación es reproducible?
- ¿La Fase 2 puede construirse sin redefinir el modelo?

Si todas esas condiciones se cumplen, la fase ha cumplido su función arquitectónica.

---

**Estado:** completa  
**Rol de la fase:** base contractual y sintética del pipeline  
**Nota de evolución:** `buff` queda reservado para ampliación futura del schema
