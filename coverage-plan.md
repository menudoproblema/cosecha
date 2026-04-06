# Coverage v1 mínimo y robusto en Cosecha

## Resumen
Resolver coverage dentro de Cosecha con el menor número de conceptos nuevos. La shell detecta `cosecha run --cov ...`, prepara un workdir temporal, relanza el mismo comando bajo Coverage.py y, al terminar, recoge el resultado, actualiza el artifact de sesión y renderiza el bloque final de coverage. No se introduce un framework genérico de instrumentación; solo se deja un envelope común para summaries futuros.

## Cambios principales
- Mantener la orquestación en shell y simplificar `launcher.py`:
  - eliminar `ExecutionLauncher` y `_iter_execution_launchers()`
  - `main()` pasa a ser un bootstrap específico de coverage
  - si aplica coverage, ejecuta el relanzamiento instrumentado
  - si no aplica, delega directamente en `runner_cli.main()`
- Sustituir `CoveragePlugin + coverage_summary` por un `CoverageInstrumenter` interno con:
  - `prepare(args) -> Contribution`
  - `collect(workdir) -> InstrumentationSummary`
- Añadir dos tipos mínimos:
  - `Contribution`: `env`, `argv_prefix`, `workdir_files`, `warnings`
  - `InstrumentationSummary`: `instrumentation_name`, `summary_kind`, `payload`
- Generalizar `SessionReportState` y `SessionReportSummary` a:
  - `instrumentation_summaries: dict[str, InstrumentationSummary]`
- Mantener `coverage_total` solo como valor derivado de `instrumentation_summaries["coverage"]`.
- No crear aún `cosecha.instrumentation` ni otro group de entry points. Mientras N=1, la shell hace import condicional directo del package de coverage.

## Flujo de ejecución
- El bootstrap de shell detecta `run` + `--cov`. Si el extra de coverage no está instalado, devuelve un error claro.
- Prevención de recursión:
  - el parent marca el child con `COSECHA_COVERAGE_ACTIVE=1`
  - si la shell ve esa env var, no vuelve a bootstrappear coverage
  - `--cov` se mantiene en el argv del child; el guard de env evita el bucle
- El parent crea un workdir temporal y pasa al child `COSECHA_INSTRUMENTATION_METADATA_FILE` con la ruta del JSON de metadata.
- `prepare()` de coverage:
  - crea la configuración efectiva de coverage dentro del workdir
  - devuelve el `argv_prefix` para ejecutar el child bajo coverage
  - devuelve el `env` necesario para que los datos de coverage caigan en el workdir
  - parte de la configuración descubierta por Coverage.py y overridea solo lo imprescindible:
    - `parallel = true`
    - `data_file` dentro del workdir
    - `source` derivado de `--cov`
  - deja intacto el resto de la configuración del usuario
  - si detecta un conflicto relevante con config existente, añade un warning explícito
- El parent materializa `workdir_files` antes de lanzar el child.
- El child ejecuta el flujo normal de `cosecha run`, persiste su artifact de sesión y, si existe `COSECHA_INSTRUMENTATION_METADATA_FILE`, escribe ahí un JSON mínimo con:
  - `session_id`
  - referencia suficiente para reabrir o localizar el artifact de sesión
- La metadata debe escribirse en cuanto exista un `session_id` estable y el artifact sea direccionable. La escritura debe ser atómica:
  - escribir a `*.tmp`
  - `rename` al path final
- Esa metadata debe escribirse también cuando hay tests fallidos. Solo faltará en crashes reales o abortos muy tempranos.
- El parent siempre intenta `collect()`, aunque el child termine con exit code distinto de cero.
- Si hay metadata:
  - ejecuta `collect()`
  - actualiza el artifact de sesión con `instrumentation_summaries["coverage"]`
  - renderiza coverage después de la salida normal del child
- Si no hay metadata:
  - no intenta persistir coverage
  - emite un warning explícito
- Orden de salida garantizado:
  - primero tests + summary normal del child
  - después el bloque de coverage del parent
- Política del workdir:
  - éxito: cleanup
  - fallo de `collect()` o `combine`: conservar y mostrar ruta

## Render y persistencia
- En v1 el render humano de coverage lo hace la shell directamente desde `InstrumentationSummary`.
- El payload de coverage debe incluir:
  - `total_coverage`
  - `report_type`
  - `measurement_scope`
  - `branch`
  - `engine_names`
  - `source_targets`
  - `includes_worker_processes`
- `runner_cli` y queries de sesión exponen `instrumentation_summaries`.
- `coverage_total` sigue derivándose desde el summary de coverage para compatibilidad de salida.

## Tests
- `prepare()` genera rcfile/env/argv correctos y respeta config existente salvo overrides necesarios.
- El guard `COSECHA_COVERAGE_ACTIVE` evita recursión al relanzar.
- `launcher.py` queda cubierto como bootstrap coverage-only y delegación directa a `runner_cli`.
- La metadata se escribe de forma atómica y se produce también con tests rojos.
- `collect()` combina `.coverage*` y construye el summary esperado.
- `run --cov` exitoso:
  - relanza el child correctamente
  - persiste `instrumentation_summaries["coverage"]`
  - muestra coverage al final
- `run --cov` con tests rojos:
  - mantiene el exit code de fallo
  - sigue recogiendo coverage
  - persiste el summary si existe metadata
- Crash temprano del child:
  - no hay metadata
  - se emite warning
  - no se corrompen artifacts
- Serialización:
  - `SessionReportSummary` y artifacts exponen `instrumentation_summaries`
  - `coverage_total` sigue saliendo como derivado
- Regresión:
  - los tests actuales de CLI/artifacts siguen verdes
  - los tests del package de coverage pasan a validar el instrumenter
  - los tests del launcher actual se sustituyen por tests del bootstrap coverage-only

## Suposiciones y defaults
- Esta v1 sigue optimizada para un solo instrumenter real: coverage.
- No hay composición entre instrumenters, conflictos generales, IPC, `sitecustomize` ni package externo de supervisor.
- El package `cosecha-plugin-coverage` no se renombra aún.
- Si aparece un segundo instrumenter real, entonces se revisa con datos si conviene introducir discovery específico o extraer infraestructura común.
