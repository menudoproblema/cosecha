# Catalogo canonico CXP de Cosecha

## Autoridad

`cosecha` define su propio catalogo interoperable. `cxp` hospeda ese
catalogo, pero no decide la semantica del sistema.

La secuencia normativa es:

1. `cosecha` fija interfaces, capabilities, metadata, operaciones,
   telemetria y tiers.
2. `cxp` publica exactamente ese contrato.
3. `cosecha` integra y valida contra el catalogo publicado.

## Capas del sistema

El catalogo de Cosecha se organiza por capas con ownership explicito:

- `cosecha/engine`
  Contrato de engines. Incluye discovery, knowledge, plan/explain,
  validacion de draft, lifecycle de sesion y dependencias entre engines.
- `cosecha/runtime`
  Contrato de runtime providers. Incluye ejecucion, aislamiento,
  workers, recursos y observabilidad live.
- `cosecha/reporter`
  Contrato de reporters. Incluye lifecycle, proyeccion de resultados y
  escritura de artefactos humanos o portables.
- `cosecha/plugin`
  Contrato de plugins del core. Incluye lifecycle, surfaces publicadas,
  requisitos y capacidades transversales declaradas de forma explicita.
- `cosecha/instrumentation`
  Contrato de instrumentaciones de sesion que no forman parte del
  lifecycle de plugin, como coverage.

`launcher`, `CLI`, `MCP` y otros consumidores no definen semantica del
catalogo. Solo consumen y presentan capacidades publicadas por las capas
anteriores.

## Interfaces y capabilities

### `cosecha/engine`

Capabilities canonicas:

- `engine_lifecycle`
- `test_lifecycle`
- `draft_validation`
- `selection_labels`
- `project_definition_knowledge`
- `library_definition_knowledge`
- `project_registry_knowledge`
- `plan_explanation`
- `static_definition_discovery`
- `on_demand_definition_materialization`
- `engine_dependency_knowledge`

Decisiones cerradas:

- `selection_labels` incluye `run` como operacion publica porque los
  engines reales la usan para filtrado y seleccion, no solo para
  planning.
- `definition_knowledge` se separa por origen real en proyecto,
  libreria y registro.
- `live_execution_observability` no pertenece al interface de engine.
  Los engines pueden alimentar la proyeccion live, pero la capacidad
  publica vive en `cosecha/runtime`.

Tiers recomendados:

- `core`
- `knowledge`
- `planning`
- `integrated`

### `cosecha/runtime`

Capabilities canonicas:

- `injected_execution_plans`
- `isolated_processes`
- `persistent_workers`
- `run_scoped_resources`
- `worker_scoped_resources`
- `live_execution_observability`

Decisiones cerradas:

- la observabilidad live publica pertenece al runtime;
- `local` y `process` son tiers reales del sistema actual;
- no se publica `managed` mientras no exista control operativo estable.

Tiers recomendados:

- `local`
- `process`
- `observable`

### `cosecha/reporter`

Capabilities canonicas:

- `report_lifecycle`
- `result_projection`
- `artifact_output`
- `structured_output`
- `human_output`

Decisiones cerradas:

- `structured_output` y `human_output` se modelan como capabilities
  distintas para no esconder la diferencia en un simple metadata field;
- `artifact_output` expresa persistencia o escritura final de salida.
- `artifact_output`, `structured_output` y `human_output` son ejes
  ortogonales y combinables;
- los tiers de reporter son umbrales minimos de conformidad, no clases
  mutuamente excluyentes; un mismo reporter puede satisfacer varios
  tiers a la vez.

Tiers recomendados:

- `core`
- `artifact`
- `structured`
- `human`

### `cosecha/plugin`

Capabilities canonicas:

- `plugin_lifecycle`
- `surface_publication`
- `capability_requirements`
- `timing_summary`
- `telemetry_export`

Decisiones cerradas:

- `coverage_summary` deja de pertenecer al interface de plugin;
- ninguna capability opcional puede inferirse por nombre de clase;
- las capabilities opcionales deben declararse explicitamente por el
  plugin.

Tiers recomendados:

- `core`
- `constrained`
- `timing_sidecar`
- `telemetry_sidecar`
- `reporting_sidecar`

### `cosecha/instrumentation`

Capabilities canonicas:

- `instrumentation_bootstrap`
- `session_summary`
- `structured_summary`

Decisiones cerradas:

- coverage pertenece a instrumentation, no a plugin;
- la instrumentacion publica prepara bootstrap y recolecta un resumen
  estructurado de sesion;
- la persistencia del resumen dentro de `SessionArtifact` pertenece al
  core de `cosecha`, no al instrumenter;
- `structured_summary` expresa que el payload recolectado es
  serializable y consumible por CLI, MCP o persistencia de artefactos
  sin mezclar esa responsabilidad con la instrumentacion.

Tiers recomendados:

- `summary`
- `structured`

## Matriz de componentes reales

### Engines

- `gherkin`
  Capabilities reales: `draft_validation`, `selection_labels`,
  `project_definition_knowledge`, `project_registry_knowledge`,
  `plan_explanation`, `lazy_project_definition_loading`,
  `static_project_definition_discovery`,
  `library_definition_knowledge`.
  Operaciones reales: `run`, `draft.validate`, `definition.resolve`,
  `knowledge.query_tests`, `knowledge.query_definitions`,
  `knowledge.query_registry_items`, `plan.analyze`, `plan.explain`,
  `plan.simulate`.
  Eventos/spans relevantes: snapshots y domain events de ejecucion por
  step; contribuye a observabilidad live del runtime.

- `pytest`
  Capabilities reales: `selection_labels`,
  `static_project_definition_discovery`,
  `project_definition_knowledge`, `draft_validation`,
  `library_definition_knowledge`, `plan_explanation`.
  Operaciones reales: `run`, `draft.validate`, `definition.resolve`,
  `knowledge.query_tests`, `knowledge.query_definitions`,
  `plan.analyze`, `plan.explain`, `plan.simulate`.

### Runtime providers

- `LocalRuntimeProvider`
  Capabilities reales: `injected_execution_plans`,
  `run_scoped_resources`, `worker_scoped_resources` como
  `accepted_noop`, `live_execution_observability`.

- `ProcessRuntimeProvider`
  Capabilities reales: `isolated_processes`, `persistent_workers`,
  `injected_execution_plans`, `run_scoped_resources`,
  `worker_scoped_resources`, `live_execution_observability`.

### Reporters

- `console`
  Capabilities reales: lifecycle, result projection y salida humana.
- `json`
  Capabilities reales: lifecycle, result projection, artifact output y
  structured output.
- `junit`
  Capabilities reales: lifecycle, result projection, artifact output y
  structured output.

### Plugins

- `TimingPlugin`
  Capability real: `timing_summary`.
- `TelemetryPlugin`
  Capability real: `telemetry_export`.

### Instrumentation

- `CoverageInstrumenter`
  Capability real: bootstrap de instrumentacion, recoleccion de resumen
  de sesion y payload estructurado de coverage.

## Telemetria normativa

Los campos y spans normativos deben salir del comportamiento real del
sistema:

- engines: `engine.collect`, `engine.session.start`,
  `engine.session.finish`, `engine.test.start`, `engine.test.finish`,
  `engine.test.execute`, `engine.test.phase`, `engine.draft.validate`,
  `engine.definition.resolve`, `engine.plan.analyze`,
  `engine.plan.explain`, `engine.plan.simulate`,
  `engine.dependencies.describe`;
- runtime: `execution.subscribe`, `execution.live_status`,
  `execution.live_tail` como operaciones expuestas por la capability de
  observabilidad live;
- reporters: `reporter.start`, `reporter.add_test`,
  `reporter.add_test_result`, `reporter.print_report`,
  `reporter.output.write`;
- plugins: `plugin.initialize`, `plugin.start`, `plugin.finish`,
  `plugin.after_session_closed`, `plugin.timing.print_report`,
  `plugin.telemetry.sink.start`;
- instrumentation: `instrumentation.prepare`,
  `instrumentation.collect`.

## Compatibilidad y migracion

- `coverage_summary` queda obsoleta en `cosecha/plugin`.
- el adapter de `cosecha` deja de sintetizar capabilities por nombre;
- `cosecha` debe publicar snapshots fieles al contrato final de `cxp`;
- cualquier capability nueva de Cosecha debe especificarse aqui antes de
  publicarse en `cxp`.
