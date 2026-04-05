# Arquitectura local de Gherkin

## Alcance

El engine Gherkin implementa un engine concreto sobre los contratos
generales de Cosecha.

Sus piezas principales son:

- parsing de `.feature`,
- construcción de `GherkinTestItem`,
- consumo de wiring declarativo desde el manifiesto,
- indexación incremental de definiciones ejecutables,
- resolución lazy de archivos de steps,
- creación de contexto y ejecución secuencial de pasos.

## Componentes

### Collector

`src/cosecha/engine/gherkin/collector.py`

Descubre ficheros `.feature`, construye tests, localiza directorios
`steps/`, incorpora `definition_paths` y publica conocimiento
incremental del engine.

### Catálogo y conocimiento

- `src/cosecha/engine/gherkin/step_catalog.py`
- `src/cosecha/engine/gherkin/step_ast_discovery.py`

El engine mantiene un índice operativo para planning y materialización.
Ese índice no sustituye a la knowledge base global; es su proyección
local para el dominio Gherkin.

### Materialización lazy

- `src/cosecha/engine/gherkin/step_materialization.py`
- `src/cosecha/engine/gherkin/steps/registry.py`

Las definiciones ejecutables no se importan eager en planning. El
resolver consulta el catálogo, importa solo módulos candidatos y
registra únicamente las definiciones necesarias para la query.

### Engine y reporter local

- `src/cosecha/engine/gherkin/engine.py`
- `src/cosecha/engine/gherkin/reporter.py`
- `src/cosecha/engine/gherkin/discovery.py`

El engine publica capabilities, diagnósticos y proyección local de
reporting, pero la coordinación de sesión y la salida final pertenecen
al core y a los reporters desacoplados.

## Límites

- Gherkin no define el runtime global.
- Gherkin no debe depender de una shell concreta.
- La salida humana final no vive aquí.
- Las decisiones sobre knowledge transversal, artifacts o error model
  siguen siendo del core.
