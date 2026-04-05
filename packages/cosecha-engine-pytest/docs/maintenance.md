# Maintenance de Pytest

## Puntos de entrada

- `src/cosecha/engine/pytest/collector.py`
- `src/cosecha/engine/pytest/engine.py`
- `src/cosecha/engine/pytest/runtime_adapter.py`
- `src/cosecha/engine/pytest/discovery.py`
- `src/cosecha/engine/pytest/context.py`

## Operativa

- revisar timings de discovery, bootstrap y ejecución cuando cambie el
  adaptador real de `pytest`,
- comprobar estabilidad de `stable_id` al tocar naming, selección o
  agrupación,
- validar que el engine no introduce dependencias de presentación,
- revisar precedencia entre fixtures locales, `conftest.py`,
  `pytest_plugins` y bridge de recursos,
- comprobar degradaciones a runtime real cuando entren plugins o marks
  fuera del subconjunto soportado.

## Tests relevantes

- tests de arquitectura para capabilities del engine,
- tests de contratos del adaptador de ejecución,
- tests de serialización de identidad estable,
- tests de integración con runtime local y multiproceso.

## Notas

- Este paquete define el engine Pytest, no la estrategia global del
  producto.
- Las decisiones transversales siguen en
  `../../cosecha-core/docs/architecture.md`.
