# Engine Gherkin

## Objetivo

`cosecha-engine-gherkin` traduce artefactos `.feature` a unidades
ejecutables dentro de Cosecha.

Su responsabilidad es:

- descubrir y parsear features,
- construir tests estables desde escenarios y ejemplos,
- resolver definiciones ejecutables bajo demanda,
- publicar conocimiento incremental del proyecto,
- adaptar el dominio Gherkin a los contratos del core.

No es responsable de:

- decidir políticas globales de runtime,
- renderizar resultados finales,
- definir el modelo transversal de errores o capacidades,
- sustituir la autoridad del core.

## Conceptos

- `GherkinCollector`
- `StepCatalog`
- `LazyStepResolver`
- `definition_paths`
- `GherkinEngine`

## Mapa local

- [Arquitectura](./architecture.md)
- [Rutas y `definition_paths`](./paths.md)
- Flujos:
  - [Discovery y conocimiento](./flows/discovery-and-knowledge.md)
  - [Planificación y materialización lazy](./flows/planning-and-materialization.md)
  - [Ejecución](./flows/execution.md)
- [Maintenance](./maintenance.md)
- [Known Issues](./known_issues.md)
- [Pending Improvements](./pending_improvements.md)

## Referencias transversales

- [Arquitectura del framework](../../cosecha-core/docs/architecture.md)
- [Modelo de configuración](../../cosecha-core/docs/architecture/configuration_model.md)
- [Flujos end-to-end](../../cosecha-core/docs/architecture/end_to_end_flows.md)
