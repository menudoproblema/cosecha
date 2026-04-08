# Arquitectura de Cosecha

## Resumen

Cosecha se organiza como un monorepo de paquetes especializados con una
arquitectura transversal orientada a contratos:

- las fuentes del workspace y la configuración efectiva son la
  autoridad semántica,
- `cosecha-core` compila, planifica, ejecuta y persiste conocimiento,
- `cosecha-core` usa `cxp` directamente como base de interoperabilidad
  compartida,
- los engines adaptan dominios concretos al vocabulario común del core,
- la CLI compone operaciones humanas sobre ese núcleo,
- reporters y plugins consumen hechos del sistema sin acoplarse a un
  engine concreto.

La arquitectura sigue separando:

- autoridad semántica,
- proyecciones persistidas,
- vistas ejecutables,
- presentación humana o estructurada.

## Capas del sistema

### Entrada y composición

La entrada pública humana vive en `cosecha`:

- `run`
- `plan`
- `manifest`
- `gherkin`
- `pytest`
- `knowledge`
- `session`
- `doctor`

La CLI no contiene planificación ni ejecución. Su responsabilidad es
resolver configuración, activar plugins y construir operaciones tipadas.

### Núcleo semántico

`cosecha-core` es dueño de:

- configuración efectiva,
- manifiesto y materialización,
- knowledge base y artefactos de sesión,
- `Planning Semantics`,
- `Execution IR`,
- `Plan Explanation`,
- runtime providers y `ResourceManager`,
- coordinación de reporting,
- timing, eventos y observabilidad.

### Engines

Cada engine adapta su dominio a capabilities transversales del core.

Estado actual:

- `cosecha-engine-gherkin`: discovery de `.feature`, catálogo de
  definiciones y ejecución secuencial de steps.
- `cosecha-engine-pytest`: discovery de tests/fixtures Pytest y
  adaptación de ejecución al modelo común del framework.

### Reporters y plugins

Los reporters y plugins ya no se documentan ni se materializan como
ramas hardcodeadas del core:

- reporters por entry points `cosecha.shell.reporting`,
- plugins por entry points `cosecha.plugins`.

Esto incluye reporter de consola, JSON, JUnit, coverage y timing.

## Modelo de autoridad

La arquitectura distingue explícitamente entre:

- fuentes del workspace,
- configuración efectiva,
- contratos propios de Cosecha,
- tipos ecosistémicos de interoperabilidad resueltos por `cxp`,
- conocimiento persistido,
- artefactos de planificación,
- artefactos de ejecución y reporting.

La knowledge base es una proyección reconstruible del proyecto, no una
fuente de verdad alternativa.

Una operación debe seguir siendo semánticamente correcta aunque la KB
se invalide, se vacíe o no exista.

## Documentos relacionados

- [Qué es un engine](./engines.md)
- [Catalogo canonico CXP de Cosecha](./architecture/canonical_catalog.md)
- [Modelo de configuración](./architecture/configuration_model.md)
- [Flujos end-to-end](./architecture/end_to_end_flows.md)
- [Modelo de errores](./architecture/error_model.md)
- [Composición entre engines](./architecture/engine_dependencies.md)
- [Tipos y coerciones](./architecture/type_and_coercion_system.md)
- [ADR](./architecture/decisions/README.md)
- [Roadmap](./roadmap.md)
