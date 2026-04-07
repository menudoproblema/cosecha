# Cosecha

## Objetivo

`cosecha-core/docs` es la entrada documental canónica del framework.

Aquí vive la documentación concerniente al proyecto en sí:

- arquitectura general de Cosecha,
- modelo de configuración, errores, capabilities y ADR,
- definición de qué es un engine y cómo se compone con el core,
- roadmap y trazabilidad de la migración documental,
- arquitectura local de `cosecha-core`.

La documentación específica de cada pieza vive en el paquete que la
implementa: CLI en `cosecha/docs`, Gherkin en
`cosecha-engine-gherkin/docs`, Pytest en `cosecha-engine-pytest/docs`,
reporters y plugins en sus propios paquetes.

## Mapa del framework

- [Arquitectura de Cosecha](./architecture.md)
- [Qué es un engine y cómo encaja en el framework](./engines.md)
- [Modelo de configuración](./architecture/configuration_model.md)
- [Flujos end-to-end](./architecture/end_to_end_flows.md)
- [Modelo de errores](./architecture/error_model.md)
- [Composición entre engines](./architecture/engine_dependencies.md)
- [Tipos y coerciones](./architecture/type_and_coercion_system.md)
- [ADR](./architecture/decisions/README.md)
- [Roadmap](./roadmap.md)
- [Cobertura de migración documental](./migration_coverage.md)

## `cosecha-core` como paquete

`cosecha-core` es dueño de:

- `Config` y `CosechaManifest`,
- `Runner`, scheduler y runtime providers,
- contratos de engine, hook, plugin y reporter,
- `Knowledge Base`, artefactos y eventos de dominio,
- `Reporting IR`, `SessionTiming` y coordinación de sesión,
- la semántica propia del framework y su integración explícita con
  `cxp`.

No es dueño de:

- el CLI humano y LSP,
- la semántica local de Gherkin o Pytest,
- formatos concretos de salida estructurada,
- integración con recursos, catálogos o tooling externo que vivan en
  paquetes separados.

## Documentación local de `cosecha-core`

- [Arquitectura del paquete](./package_architecture.md)
- [Maintenance](./maintenance.md)

## Documentación por paquete

- [CLI](../../cosecha/docs/index.md)
- [Engine Gherkin](../../cosecha-engine-gherkin/docs/index.md)
- [Engine Pytest](../../cosecha-engine-pytest/docs/index.md)
- [Reporter consola](../../cosecha-reporter-console/docs/index.md)
- [Reporter JSON](../../cosecha-reporter-json/docs/index.md)
- [Reporter JUnit](../../cosecha-reporter-junit/docs/index.md)
- [Coverage instrumentation](../../cosecha-instrumentation-coverage/docs/index.md)
