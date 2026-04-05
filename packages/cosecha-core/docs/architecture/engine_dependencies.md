# Dependencias y composición entre engines

## Propósito

Este documento describe cómo conviven y se componen varios engines sin
importarse entre sí ni compartir objetos vivos del dominio.

## Principios

- ningún engine depende de clases internas de otro,
- la composición se declara sobre contratos y capabilities,
- planning, explain y runtime trabajan sobre artefactos serializables,
- el scheduler no usa imports cruzados ad hoc.

## Formas válidas de dependencia

### Conocimiento

Un engine puede consumir snapshots o catálogos publicados por otro si
el contrato es serializable y estable.

### Planificación

Un engine puede declarar que un nodo requiere capacidades o nodos de
otro engine. La relación vive en `Planning Semantics`.

### Ejecución

Un nodo puede activar otro dominio solo si el plan ya lo declara y el
runtime mantiene correlación y compatibilidad explícitas.

## Resolución

La composición debe dejar fijados:

- orden de evaluación,
- política de fallo cruzado,
- contexto permitido,
- aislamiento entre dominios.

## Política de fallos cruzados

- un fallo funcional de un engine no degrada automáticamente a otro,
- un fallo de infraestructura compartida puede proyectarse a varios
  engines si la política declara causalidad común,
- toda cancelación o degradación cruzada debe quedar explicada en
  `Plan Explanation` o en eventos correlados.
