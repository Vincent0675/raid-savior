# Fase 2 — Receptor HTTP e Ingesta en MinIO Bronze

## Objetivo

La Fase 2 traslada el proyecto desde la generación local de eventos hacia una ingesta orientada a flujo real. En esta etapa se implementó un receptor HTTP con Flask que acepta batches de eventos, valida cada payload con Pydantic y persiste los datos crudos validados en MinIO como capa Bronze.

## Alcance

Incluye:

- Receptor HTTP con Flask
- Endpoint de salud del servicio
- Endpoint `POST /events` para ingesta de batches
- Validación schema-on-write reutilizando el modelo de Fase 1
- Rechazo inmediato de eventos inválidos con respuesta HTTP 400
- Persistencia de batches válidos en MinIO
- Layout Bronze particionado por `raidid` e `ingest_date`
- Configuración por variables de entorno
- Tests básicos de receptor y almacenamiento

No incluye todavía:

- Transformación a Parquet
- Limpieza o enriquecimiento Silver
- Dead Letter Queue productiva
- Orquestación del pipeline
- Serving analítico

## Arquitectura implementada

El flujo de la fase es:

1. Un cliente envía un batch JSON por HTTP.
2. Flask recibe el payload.
3. Pydantic valida cada evento antes de guardarlo.
4. Si hay errores, el batch se rechaza completo.
5. Si el batch es válido, se construye un objeto JSON con metadata de ingesta.
6. El batch se persiste en MinIO dentro del bucket Bronze.

## Contrato de escritura Bronze

La escritura en Bronze sigue un layout estable y versionado, con particionado por dimensiones de acceso natural:

- `raidid`
- `ingest_date`

Ejemplo conceptual de key:

`wowraidevents/v1/raidid=raid001/ingest_date=2026-01-15/batch-<uuid>.json`

Este diseño facilita listados por prefijo, trazabilidad y transición posterior a Silver.

## Validación

La fase adopta un enfoque schema-on-write:

- Solo se almacenan eventos válidos
- Los errores se reportan en la respuesta HTTP
- No se guarda información inválida en Bronze
- Se valida que un batch no mezcle múltiples raids

## Resultado de la fase

La Fase 2 deja operativo el punto de entrada del pipeline. Desde este momento, el proyecto ya no depende exclusivamente de ficheros locales, sino que puede recibir eventos por red y almacenarlos en una capa Bronze estructurada y trazable.

## Stack

- Python
- Flask
- Pydantic v2
- boto3 / API S3-compatible
- MinIO
- JSON
