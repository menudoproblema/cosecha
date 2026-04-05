# Cobertura de migración documental

## Criterio

Esta matriz registra el destino de cada documento de
`../gdynamics/cosecha-framework/docs/**`.

Estados usados:

- `migrado`
- `fusionado`
- `dividido`
- `omitido`

## Matriz

| Origen | Destino | Estado | Nota |
| --- | --- | --- | --- |
| `docs/architecture.md` | `cosecha-core/docs/architecture.md` | migrado | Adaptado al monorepo |
| `docs/architecture/configuration_model.md` | `cosecha-core/docs/architecture/configuration_model.md` | migrado | Ajustado al manifiesto y ownership actuales |
| `docs/architecture/end_to_end_flows.md` | `cosecha-core/docs/architecture/end_to_end_flows.md` | migrado | Reescrito con paquetes del monorepo |
| `docs/architecture/error_model.md` | `cosecha-core/docs/architecture/error_model.md` | migrado | Mantiene taxonomía de errores |
| `docs/architecture/engine_dependencies.md` | `cosecha-core/docs/architecture/engine_dependencies.md` | migrado | Mantiene composición entre engines |
| `docs/architecture/type_and_coercion_system.md` | `cosecha-core/docs/architecture/type_and_coercion_system.md` | migrado | Mantiene contrato transversal |
| `docs/architecture/documentation_conventions.md` | `cosecha-core/docs/architecture/documentation_conventions.md` | migrado | Reescrito para taxonomía del monorepo |
| `docs/architecture/pending_improvements.md` | `cosecha-core/docs/architecture/pending_improvements.md` | migrado | Consolidado como trabajo futuro global |
| `docs/architecture/decisions/README.md` | `cosecha-core/docs/architecture/decisions/README.md` | migrado | Índice de ADR actualizado |
| `docs/architecture/decisions/ADR-0001_knowledge_snapshot_static_discovery_y_materializacion_lazy.md` | `cosecha-core/docs/architecture/decisions/ADR-0001_knowledge_snapshot_static_discovery_y_materializacion_lazy.md` | migrado | Resumen adaptado |
| `docs/architecture/decisions/ADR-0002_planning_semantics_y_plan_explanation.md` | `cosecha-core/docs/architecture/decisions/ADR-0002_planning_semantics_y_plan_explanation.md` | migrado | Resumen adaptado |
| `docs/architecture/decisions/ADR-0003_identidad_estable_y_bootstrap_preparado_de_workers.md` | `cosecha-core/docs/architecture/decisions/ADR-0003_identidad_estable_y_bootstrap_preparado_de_workers.md` | migrado | Resumen adaptado |
| `docs/architecture/decisions/ADR-0004_matriz_de_capacidades_y_limites_explicitos.md` | `cosecha-core/docs/architecture/decisions/ADR-0004_matriz_de_capacidades_y_limites_explicitos.md` | migrado | Resumen adaptado |
| `docs/architecture/decisions/ADR-0005_event_stream_tipado_y_reporting_desacoplado.md` | `cosecha-core/docs/architecture/decisions/ADR-0005_event_stream_tipado_y_reporting_desacoplado.md` | migrado | Resumen adaptado |
| `docs/modules/gherkin/index.md` | `cosecha-engine-gherkin/docs/index.md` | migrado | Ownership del engine |
| `docs/modules/gherkin/architecture.md` | `cosecha-engine-gherkin/docs/architecture.md` | migrado | Ownership del engine |
| `docs/modules/gherkin/flows/discovery-and-knowledge.md` | `cosecha-engine-gherkin/docs/flows/discovery-and-knowledge.md` | migrado | Ownership del engine |
| `docs/modules/gherkin/flows/planning-and-materialization.md` | `cosecha-engine-gherkin/docs/flows/planning-and-materialization.md` | migrado | Ownership del engine |
| `docs/modules/gherkin/flows/execution.md` | `cosecha-engine-gherkin/docs/flows/execution.md` | migrado | Ownership del engine |
| `docs/modules/gherkin/known_issues.md` | `cosecha-engine-gherkin/docs/known_issues.md` | migrado | Sin incidencias locales abiertas |
| `docs/modules/gherkin/maintenance.md` | `cosecha-engine-gherkin/docs/maintenance.md` | migrado | Actualizado a rutas del paquete |
| `docs/modules/gherkin/pending_improvements.md` | `cosecha-engine-gherkin/docs/pending_improvements.md` | migrado | Mantiene backlog local |
| `docs/modules/pytest/index.md` | `cosecha-engine-pytest/docs/index.md` | migrado | Ownership del engine |
| `docs/modules/pytest/architecture.md` | `cosecha-engine-pytest/docs/architecture.md` | migrado | Ownership del engine |
| `docs/modules/pytest/flows/discovery-and-knowledge.md` | `cosecha-engine-pytest/docs/flows/discovery-and-knowledge.md` | migrado | Ownership del engine |
| `docs/modules/pytest/flows/planning-and-execution.md` | `cosecha-engine-pytest/docs/flows/planning-and-execution.md` | migrado | Ownership del engine |
| `docs/modules/pytest/flows/fixtures-and-resources.md` | `cosecha-engine-pytest/docs/flows/fixtures-and-resources.md` | migrado | Ownership del engine |
| `docs/modules/pytest/known_issues.md` | `cosecha-engine-pytest/docs/known_issues.md` | migrado | Sin incidencias locales abiertas |
| `docs/modules/pytest/maintenance.md` | `cosecha-engine-pytest/docs/maintenance.md` | migrado | Actualizado al paquete actual |
| `docs/modules/pytest/pending_improvements.md` | `cosecha-engine-pytest/docs/pending_improvements.md` | migrado | Mantiene backlog local |
| `docs/paths.md` | `cosecha-engine-gherkin/docs/paths.md` | migrado | Pasa a ownership de Gherkin |
| `docs/reporters.md` | `cosecha/docs/reporting.md`, `cosecha-reporter-console/docs/index.md`, `cosecha-reporter-json/docs/index.md`, `cosecha-reporter-junit/docs/index.md`, `cosecha-plugin-coverage/docs/index.md` | dividido | Repartido por ownership real |
| `docs/roadmap.md` | `cosecha-core/docs/roadmap.md` | migrado | Reescrito y reubicado como documentación del framework |
| `docs/software-requirements/index.md` | No aplica | omitido | Decisión explícita de no migrarlo |
