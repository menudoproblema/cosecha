# Engines en Cosecha

## Qué es un engine

Un engine es el adaptador que traduce un dominio de tests concreto al
vocabulario transversal del framework.

Su responsabilidad es:

- descubrir artefactos ejecutables de su dominio,
- construir nodos con identidad estable,
- publicar conocimiento incremental útil para planning y tooling,
- ejecutar o degradar su ejecución conforme a las capabilities del
  runtime,
- proyectar resultados al modelo común del core.

No es responsable de:

- definir la configuración global del proyecto,
- imponer un formato de salida final,
- coordinar la sesión completa,
- almacenar por su cuenta la fuente de verdad del sistema.

## Contrato con el core

El contrato base del engine vive en:

- `src/cosecha/core/engines/base.py`
- `src/cosecha/core/capabilities.py`
- `src/cosecha/core/operations.py`
- `src/cosecha/core/execution_ir.py`

Un engine publica capabilities y límites explícitos. El core usa ese
contrato para:

- construir planes,
- validar compatibilidad con runtime profiles,
- decidir degradaciones diagnósticas,
- coordinar reporting y artefactos.

## Qué aporta cada engine actual

- `cosecha-engine-gherkin`: descubre `.feature`, indexa definiciones y
  ejecuta escenarios paso a paso.
- `cosecha-engine-pytest`: descubre tests y fixtures Pytest y adapta su
  ejecución al modelo común del framework.

## Reglas de diseño

- un engine no debe importar internals de otro engine,
- toda composición entre engines debe expresarse por contratos del core,
- la UX de consola no pertenece al engine,
- el conocimiento persistido del engine es una proyección sobre la
  knowledge base del core, no una autoridad paralela.

## Documentos relacionados

- [Arquitectura de Cosecha](./architecture.md)
- [Composición entre engines](./architecture/engine_dependencies.md)
- [Engine Gherkin](../../cosecha-engine-gherkin/docs/index.md)
- [Engine Pytest](../../cosecha-engine-pytest/docs/index.md)
