# Fase 5 — Migración del procesamiento Gold a motor distribuido

## 1. Propósito de la fase

La Fase 5 representa la evolución del procesamiento de la capa Gold hacia un modelo distribuido. Su objetivo es sustituir el enfoque anterior de ejecución local por un motor capaz de soportar mayor escala de procesamiento dentro del flujo Silver → Gold.

Según el roadmap del proyecto, esta fase se materializa mediante la migración del procesamiento Gold a PySpark 3.5 y el uso de conectores S3A para el acceso al almacenamiento.

---

## 2. Objetivos técnicos

Los objetivos principales de la fase son:

- migrar el procesamiento Silver → Gold a un motor distribuido
- adoptar PySpark 3.5 como framework de ejecución
- conectar el procesamiento distribuido al almacenamiento mediante S3A
- mantener la capa Gold como destino del flujo analítico
- preparar el pipeline para mayor volumen y paralelismo

---

## 3. Estado y alcance real

### Incluido en la fase

- ETL Silver → Gold distribuido
- migración del procesamiento de Gold a Spark
- uso de PySpark 3.5
- utilización de conectores S3A
- continuidad funcional de la capa Gold en una arquitectura de mayor escalabilidad

### Fuera de alcance en esta fase

- orquestación formal del pipeline
- incorporación de table formats con ACID y time travel
- serving por API
- visualización final
- modelado de machine learning
- integración con datos reales

---

## 4. Relación con Fase 4

La Fase 4 dejó una capa Gold funcional con procesamiento analítico previo. La Fase 5 no redefine el objetivo de Gold, sino que migra su procesamiento a una base técnica distribuida.

Dicho de otra forma, Fase 4 consolida la lógica analítica inicial de Gold y Fase 5 traslada esa lógica a un motor más adecuado para escalabilidad.

---

## 5. Decisión tecnológica

El roadmap del proyecto define de forma explícita dos decisiones técnicas para esta fase:

- PySpark 3.5 como motor distribuido
- conectores S3A para acceso al almacenamiento

Estas dos piezas sitúan la fase en el terreno de procesamiento distribuido sobre object storage, alineando Gold con una arquitectura más preparada para crecer en volumen y carga de trabajo.

---

## 6. Papel de PySpark 3.5

PySpark 3.5 actúa como núcleo de ejecución distribuida del ETL Silver → Gold. Su incorporación marca el paso desde transformaciones ejecutadas en un contexto local hacia una estrategia basada en paralelismo y reparto de trabajo.

En términos de arquitectura, esto equivale a pasar de un sistema que procesa en una sola línea a uno que puede repartir carga entre múltiples unidades de ejecución.

---

## 7. Papel de S3A

Los conectores S3A cumplen la función de enlazar el motor distribuido con el almacenamiento del proyecto. En esta fase, su papel es permitir que Spark trabaje sobre las capas del pipeline utilizando la interfaz compatible con S3.

Esto es importante porque evita un rediseño completo del almacenamiento y permite mantener la integración con la arquitectura ya construida en fases anteriores.

---

## 8. Qué cambia realmente en Fase 5

La novedad principal de la fase no es la aparición de una nueva capa del pipeline, sino el cambio del motor de procesamiento de Gold. El destino sigue siendo la capa Gold, pero la forma de ejecutar el ETL pasa a un enfoque distribuido.

Por tanto, la fase debe entenderse como una migración de ejecución y escalabilidad, no como una redefinición del modelo analítico ya introducido previamente.

---

## 9. Valor arquitectónico

Esta fase aporta una mejora clara de arquitectura: desacopla la validez del modelo analítico de las limitaciones del procesamiento local. El pipeline mantiene la misma intención funcional, pero gana una base más sólida para crecer.

En una analogía de ingeniería, es como mantener el mismo diseño de control pero sustituir un banco de pruebas de laboratorio por una línea preparada para operar con mayor carga.

---

## 10. Relación con fases posteriores

El propio roadmap separa claramente esta fase de las siguientes:

- la orquestación del pipeline se mueve a Fase 6
- los table formats con ACID y time travel se reservan para Fase 7
- visualización y serving aparecen en Fase 8
- machine learning aparece en Fase 9
- la integración con datos reales aparece en Fase 10

Esto confirma que Fase 5 se centra en el motor distribuido de Gold y no en ampliar el sistema hacia capacidades de gobierno, serving o consumo externo.

---

## 11. Resumen técnico

| Elemento | Decisión |
|---|---|
| Flujo | Silver → Gold |
| Tipo de procesamiento | distribuido |
| Motor | PySpark 3.5 |
| Conectividad storage | S3A |
| Capa objetivo | Gold |
| Naturaleza de la fase | migración de procesamiento |
| Estado | completa |

---

## 12. Criterio de cierre

La Fase 5 puede darse por cerrada cuando se cumplen estas condiciones:

- el procesamiento de Gold ha sido migrado a un motor distribuido
- PySpark 3.5 actúa como base de ejecución
- el acceso al almacenamiento se realiza mediante S3A
- el flujo Silver → Gold se mantiene operativo en la nueva arquitectura
- el roadmap del proyecto reconoce la fase como completada

---

**Estado:** completa  
**Rol de la fase:** migración del procesamiento Gold a ejecución distribuida  
**Siguiente fase:** orquestación del pipeline
