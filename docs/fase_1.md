# Fase 1 — Schema y Generación de Datos Sintéticos

## Objetivo

La Fase 1 define el contrato de datos del pipeline de telemetría de raids de World of Warcraft. En esta etapa se diseñó el schema de eventos, se implementó la validación con Pydantic v2 y se construyó un generador sintético reproducible con NumPy para producir eventos plausibles y listos para fases posteriores.

## Alcance

Incluye:

- Diseño del modelo de evento
- Validación estricta con Pydantic v2
- Exportación a JSON Schema
- Generación de datos sintéticos reproducibles
- Testing básico de validación y consistencia estadística

No incluye todavía:

- Ingesta HTTP
- Persistencia en MinIO
- ETL Bronze → Silver
- Modelo analítico Gold

## Componentes principales

### Schema de eventos

Se definió un modelo tipado para representar eventos de raid con información de:

- Identificación del evento
- Contexto de raid y encounter
- Jugador origen
- Entidad objetivo
- Habilidad utilizada
- Métricas cuantitativas
- Metadata técnica de ingesta y latencia

### Validación

La validación se implementó con Pydantic v2 bajo una filosofía schema-on-write, asegurando que los eventos cumplan restricciones estructurales y lógicas antes de propagarse por el pipeline.

### Generación sintética

Se construyó un generador de sesiones y eventos que utiliza distribuciones estadísticas realistas:

- Normal para daño y curación
- Bernoulli para críticos
- Selección categórica para roles y tipos de evento
- Semilla fija para reproducibilidad

## Tipos de evento actuales

La base funcional actual contempla como núcleo:

- `combat_damage`
- `heal`
- `player_death`
- `spell_cast`
- `boss_phase`

> `buff` queda reservado como extensión futura del schema, pero no forma parte de la implementación cerrada de esta fase.

## Resultado de la fase

La Fase 1 deja preparado un contrato de datos robusto y un generador sintético funcional, listos para alimentar la siguiente etapa del proyecto: la ingesta HTTP y el almacenamiento en Bronze.

## Stack

- Python
- Pydantic v2
- JSON Schema
- NumPy
- UUID v4
