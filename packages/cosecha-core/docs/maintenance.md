# Maintenance de `cosecha-core`

## Puntos de entrada

- `src/cosecha/core/runner.py`
- `src/cosecha/core/discovery.py`
- `src/cosecha/core/cosecha_manifest.py`
- `src/cosecha/core/runtime.py`
- `src/cosecha/core/runtime_worker.py`
- `src/cosecha/core/resources.py`
- `src/cosecha/core/operations.py`
- `src/cosecha/core/catalogs/`

## Operativa

- revisar timings de `collect`, `plan`, `session` y `worker bootstrap`
  cuando cambien runner, scheduler o runtime,
- validar que discovery sigue resolviendo engines, reporters y plugins
  por entry points sin imports hardcoded,
- comprobar compatibilidad entre manifiesto, runtime profiles y
  bindings cuando cambie `catalogs`,
- revisar artefactos y knowledge persistida si cambia la semántica de
  invalidación,
- evitar que lógica de CLI, engine o reporter concreto vuelva al core.

## Tests relevantes

- `tests/test_runner.py`
- `tests/test_runtime_profiles.py`
- `tests/test_catalogs.py`
- `tests/test_workspace_discovery.py`
- `tests/test_import_path_resolution.py`

## Notas

- La autoridad documental global del framework vive en este mismo
  directorio, empezando por `./index.md` y `./architecture.md`.
- Los cambios en error model, capabilities o planning deben actualizar
  también las ADR correspondientes cuando afecten a varios paquetes.
