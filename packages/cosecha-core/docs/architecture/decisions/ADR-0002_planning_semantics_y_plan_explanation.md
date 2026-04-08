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

## Nota de evolucion

La formulacion de este ADR es historicamente correcta pero demasiado
breve para el contrato actual.

- Los shapes serializables reales de `PlanningIssue`,
  `Planning Semantics` y `Plan Explanation` viven hoy en
  `cosecha.core.execution_ir`.
- `strict` y `relaxed` son modos semanticos del IR y de la explicacion
  del plan, no flags de UI o logging.
- La superficie publica que expone planning desde los engines se
  describe hoy en el catalogo canonico formalizado por ADR-0010.
