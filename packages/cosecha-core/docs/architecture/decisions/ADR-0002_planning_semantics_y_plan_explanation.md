# ADR-0002: Planning Semantics y Plan Explanation como contratos formales

## Estado

Accepted

## Decisión

La planificación produce contratos formales inspeccionables:

- `Planning Semantics`,
- `PlanningIssue`,
- `Plan Explanation`.

El framework soporta al menos dos modos, `strict` y `relaxed`, sin
degradar explain a logging informal.

## Consecuencias

- CLI, LSP, MCP y agentes pueden inspeccionar planes parciales,
- los fallos parciales se representan sin excepciones opacas,
- aumenta el peso de los tests de serialización e invariantes.
