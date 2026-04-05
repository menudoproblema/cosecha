# Maintenance de Gherkin

## Puntos de entrada

- `src/cosecha/engine/gherkin/collector.py`
- `src/cosecha/engine/gherkin/engine.py`
- `src/cosecha/engine/gherkin/discovery.py`
- `src/cosecha/engine/gherkin/step_ast_discovery.py`
- `src/cosecha/engine/gherkin/step_catalog.py`
- `src/cosecha/engine/gherkin/step_materialization.py`
- `src/cosecha/engine/gherkin/steps/registry.py`

## Operativa

- revisar timings de `collect`, `session` y fases de pasos cuando se
  toque discovery o materialización,
- validar la cache de features y el conocimiento persistido de steps si
  aparecen regresiones de arranque,
- comprobar que planning no reintroduce import eager de steps ni de
  librerías compartidas,
- revisar `definition_paths` cuando cambie la forma de publicar
  definiciones externas,
- verificar compatibilidad de LSP solo como contribución del engine, no
  como ownership del paquete CLI.

## Tests relevantes

- `tests/test_gherkin_step_catalog.py`
- `tests/test_gherkin_step_loading.py`
- `tests/test_gherkin_hooks.py`
- `tests/test_architecture.py`

## Notas

- La cache persistida de conocimiento es una optimización descartable.
- Los cambios globales deben documentarse primero en
  `../../cosecha-core/docs/architecture.md`.
