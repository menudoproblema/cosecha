# Plugin de coverage

## Objetivo

`cosecha-plugin-coverage` mide cobertura de código como plugin
transversal del runner, no como feature de un engine concreto.

## Punto de entrada

- `src/cosecha/plugin/coverage/__init__.py`

## Superficie pública

- `--cov`
- `--cov-branch`
- `--cov-report`

## Alcance actual

El resumen de coverage se publica dentro del `report_summary` de sesión
e incluye:

- porcentaje total,
- engines observados,
- `source_targets`,
- tipo de reporte,
- si la medición incluye subprocess Python,
- si la medición incluye procesos worker de Cosecha.

La implementación actual publica explícitamente:

- `measurement_scope = controller_process`
- `includes_python_subprocesses = true`
- `includes_worker_processes = false`

Eso significa que hoy la cobertura multi-engine agrega correctamente
entre engines que ejecutan código en el proceso controlador, instrumenta
subprocess Python lanzados desde ese proceso y no pretende describir
todavía workers persistentes o runtimes remotos como garantizados.
