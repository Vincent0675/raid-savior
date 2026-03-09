# Fase 6 — Orquestación automática del pipeline con Dagster

## 1. Propósito de la fase

La Fase 6 añade una capa de orquestación al pipeline de telemetría. Su objetivo no es redefinir las transformaciones de Bronze, Silver o Gold, sino coordinar su ejecución de forma automática, ordenada y mantenible.

En esta etapa se incorpora Dagster como herramienta de orquestación, utilizando el paradigma de `assets` y automatizando dependencias y `schedules`.

---

## 2. Objetivos técnicos

Los objetivos principales de la fase son:

- introducir una capa formal de orquestación
- representar los procesos ETL como `assets`
- automatizar dependencias entre etapas del pipeline
- programar ejecuciones automáticas mediante `schedules`
- reducir la dependencia de ejecución manual

---

## 3. Estado y alcance real

### Incluido en la fase

- implementación de Dagster como orquestador
- definición de procesos ETL como `assets`
- automatización de dependencias
- automatización de ejecuciones mediante `schedules`
- consolidación de la ejecución del pipeline como flujo controlado

### Fuera de alcance en esta fase

- incorporación de table formats con ACID y time travel
- serving por API
- visualización final
- modelado de machine learning
- integración con datos reales externos

---

## 4. Relación con las fases anteriores

Las fases previas construyen las capacidades funcionales del pipeline:

- Fase 2 establece la ingesta en Bronze
- Fase 3 consolida la transformación a Silver
- Fase 4 construye la capa Gold inicial
- Fase 5 migra el procesamiento Gold a un motor distribuido

La Fase 6 actúa sobre la operación del sistema, no sobre una nueva capa de datos. Su valor está en gobernar la ejecución de lo ya construido.

---

## 5. Papel de Dagster

Dagster se introduce como mecanismo de orquestación del data pipeline. En esta fase su función es convertir scripts ETL en componentes operables dentro de un flujo con dependencias explícitas y ejecución programada.

En términos de ingeniería, Dagster pasa a ser el sistema de control del proceso: no cambia la materia prima ni el producto final, pero sí decide cuándo arranca cada etapa y en qué secuencia debe ejecutarse.

---

## 6. Uso de `assets`

El roadmap de la fase especifica que los scripts ETL se orquestan como `assets`. Esto implica que las transformaciones dejan de verse solo como scripts sueltos y pasan a integrarse dentro de un modelo de pipeline con relaciones explícitas.

Ese cambio mejora la legibilidad operativa del sistema y prepara el proyecto para una evolución más ordenada en fases posteriores.

---

## 7. Dependencias automáticas

Uno de los aportes clave de la fase es la automatización de dependencias. Esto permite que la ejecución del pipeline responda a la relación lógica entre etapas y no a disparos manuales aislados.

Dicho de forma simple, el pipeline deja de comportarse como varios scripts independientes y empieza a funcionar como una cadena de producción con secuencia controlada.

---

## 8. `Schedules`

La inclusión de `schedules` aporta automatización temporal a la ejecución del pipeline. Con ello, el sistema puede programar corridas periódicas sin depender de intervención manual constante.

Este punto es pequeño en implementación, pero importante en madurez operativa: convierte un flujo reproducible en un flujo también automatizable.

---

## 9. Qué cambia realmente en Fase 6

La Fase 6 no crea nuevas capas Medallion ni introduce nuevos datasets de negocio. Lo que cambia es el plano de operación del sistema.

Antes de esta fase, el pipeline podía existir como conjunto de procesos funcionales. Después de esta fase, esos procesos quedan integrados bajo una lógica de orquestación explícita.

---

## 10. Valor arquitectónico

Esta fase aporta orden, repetibilidad y control sobre la ejecución. Su valor no está tanto en complejidad algorítmica como en profesionalizar la operación del pipeline.

En una analogía industrial, si las fases anteriores construyen tuberías, depósitos y estaciones de transformación, la Fase 6 instala el PLC que coordina cuándo se abre cada válvula y en qué orden circula el flujo.

---

## 11. Relación con fases posteriores

El roadmap separa esta fase de las siguientes evoluciones del proyecto:

- Fase 7 introduce table formats con ACID y time travel
- Fase 8 incorpora visualización y serving
- Fase 9 abre la línea de machine learning
- Fase 10 conecta con datos reales externos

Eso significa que Fase 6 debe entenderse como cierre de la automatización operativa básica, no como fase de consumo, gobierno avanzado o explotación analítica externa.

---

## 12. Resumen técnico

| Elemento | Decisión |
|---|---|
| Naturaleza de la fase | Orquestación |
| Herramienta principal | Dagster |
| Unidad lógica | `assets` |
| Automatización | dependencias y `schedules` |
| Capa afectada | ejecución del pipeline |
| Tipo de mejora | operacional |
| Estado | completa |

---

## 13. Criterio de cierre

La Fase 6 puede darse por cerrada cuando se cumplen estas condiciones:

- el pipeline está orquestado con Dagster
- los ETL se integran como `assets`
- las dependencias quedan automatizadas
- existen `schedules` para ejecución automática
- la operación del pipeline deja de depender exclusivamente de lanzamiento manual

---

**Estado:** completa  
**Rol de la fase:** automatización y orquestación operativa del pipeline  
**Siguiente fase:** table formats con ACID y time travel
