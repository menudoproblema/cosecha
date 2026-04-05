# Engine Pytest

## Objetivo

`cosecha-engine-pytest` adapta suites escritas para `pytest` a los
contratos de ejecución de Cosecha.

Su responsabilidad es:

- descubrir unidades ejecutables de Pytest,
- construir nodos con identidad estable,
- traducir fixtures, runtime y resultados al modelo común,
- exponer explicación y conocimiento incremental del engine.

No es responsable de:

- redefinir la semántica propia de `pytest`,
- imponer conceptos de Gherkin u otros engines,
- renderizar resultados finales,
- decidir políticas globales de runtime o reporting.

## Conceptos

- `PytestCollector`
- `PytestEngine`
- `PytestExecutionAdapter`
- fixtures visibles y `pytest_plugins`
- bridge de recursos a fixtures

## Mapa local

- [Arquitectura](./architecture.md)
- Flujos:
  - [Discovery y conocimiento](./flows/discovery-and-knowledge.md)
  - [Planificación y ejecución](./flows/planning-and-execution.md)
  - [Fixtures y recursos](./flows/fixtures-and-resources.md)
- [Maintenance](./maintenance.md)
- [Known Issues](./known_issues.md)
- [Pending Improvements](./pending_improvements.md)

## Referencias transversales

- [Arquitectura del framework](../../cosecha-core/docs/architecture.md)
- [Dependencias entre engines](../../cosecha-core/docs/architecture/engine_dependencies.md)
- [Modelo de errores](../../cosecha-core/docs/architecture/error_model.md)
