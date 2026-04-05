# Arquitectura local de `cosecha-core`

## Alcance

`cosecha-core` concentra la autoridad semántica compartida del
framework. Aquí se decide cómo se representa un plan, cómo se valida un
runtime profile, cómo se materializa un recurso, cómo se publica
conocimiento y cómo se coordina una sesión de ejecución.

## Componentes principales

### Configuración y manifiesto

- `src/cosecha/core/config.py`
- `src/cosecha/core/cosecha_manifest.py`
- `src/cosecha/core/runtime_profiles.py`

Definen el workspace efectivo, los engines activos, los recursos, los
bindings y las restricciones del runtime sin depender de un engine
concreto.

### Contratos e interoperabilidad

- `src/cosecha/core/engines/base.py`
- `src/cosecha/core/hooks.py`
- `src/cosecha/core/plugins/base.py`
- `src/cosecha/core/runtime_interop.py`

Aquí viven los contratos públicos que consumen los paquetes satélite y
los helpers mínimos de interoperabilidad con `cxp`. `cosecha-core` ya
no reexporta la superficie de `cxp` bajo paths propios.

### Planning y ejecución

- `src/cosecha/core/operations.py`
- `src/cosecha/core/execution_ir.py`
- `src/cosecha/core/runner.py`
- `src/cosecha/core/scheduler.py`
- `src/cosecha/core/runtime.py`
- `src/cosecha/core/runtime_worker.py`

El core transforma configuración y selección en operaciones tipadas,
construye nodos ejecutables, decide placement y coordina la ejecución
real con el runtime activo.

### Knowledge y artefactos

- `src/cosecha/core/knowledge_base.py`
- `src/cosecha/core/definition_knowledge.py`
- `src/cosecha/core/registry_knowledge.py`
- `src/cosecha/core/session_artifacts.py`

El conocimiento persistido y los artefactos de sesión pertenecen al
core. Los engines publican su proyección local, pero la semántica del
almacenamiento y de consulta es transversal.

### Reporting y observabilidad

- `src/cosecha/core/reporter.py`
- `src/cosecha/core/reporting_ir.py`
- `src/cosecha/core/reporting_coordinator.py`
- `src/cosecha/core/session_timing.py`
- `src/cosecha/core/domain_event_stream.py`

El core coordina reporters y plugins sin importar formatos concretos.
Los reporters estructurados se resuelven por discovery y entry points.

## Reglas de diseño

- El core no debe introducir semántica local de un engine.
- El core no debe acoplarse a reporters concretos por import directo.
- El vocabulario público de capacidades, errores y artefactos se define
  aquí antes que en cualquier paquete periférico.
- Los cambios transversales deben reflejarse primero en
  `./architecture.md` y en las ADR, no solo en esta documentación
  local.
