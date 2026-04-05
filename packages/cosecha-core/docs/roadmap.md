# Roadmap de Cosecha

## Propósito

Este roadmap describe la implantación de la arquitectura actual del
monorepo sin reintroducir acoplamientos eliminados.

Cada fase debe dejar:

- un camino frío correcto,
- contratos más estables que la fase anterior,
- una separación más clara entre autoridad semántica, ejecución y
  proyecciones persistidas.

## Fase 1. Núcleo semántico y contratos base

Ownership principal: `cosecha-core`

- autoridad semántica sobre fuentes y configuración efectiva,
- `Typed Operation`,
- `Knowledge Snapshot`,
- `Planning Semantics`,
- `Execution IR`,
- `Plan Explanation`,
- identidades estables.

## Fase 2. Ejecución local y recursos

Ownership principal: `cosecha-core`

- runtime local,
- `Context Bundle`,
- `ResourceManager`,
- scopes de recursos,
- taxonomía inicial de eventos y separación de reporting.

## Fase 3. Reporting y observabilidad

Ownership principal: `cosecha-core` + reporters

- stream tipado de eventos,
- `Reporting IR`,
- reporter de consola,
- reporters estructurados desacoplados,
- lectura post-mortem desde artefactos y KB.

## Fase 4. Scheduler y runtime multiproceso

Ownership principal: `cosecha-core`

- scheduler explícito,
- protocolo master-worker versionado,
- workers persistentes,
- políticas de timeout, retry y afinidad,
- matriz de capacidades engine/runtime.

## Fase 5. Knowledge Base persistente

Ownership principal: `cosecha-core`

- snapshot versionado,
- índices persistidos,
- invalidación y reconstrucción,
- retención acotada de artefactos,
- mejoras de latencia para CLI, LSP y agentes.

## Fase 6. Interfaces inteligentes y operaciones parciales

Ownership principal: `cosecha-core` + `cosecha`

- explain, simulación y validación parcial,
- observabilidad viva read-only,
- operaciones tipadas para tooling externo,
- separación explícita entre proyección viva y artefactos finales.

## Fase 7. Optimización y extensibilidad

Ownership repartido:

- `cosecha-core`: scheduler, runtime, KB y observabilidad,
- engines: discovery, knowledge y ejecución local de su dominio,
- reporters/plugins: nuevas superficies de salida y tooling.

## Estado de migración documental

Este roadmap usa ownership por paquete del monorepo actual. No describe
layouts legacy ni rutas eliminadas.
