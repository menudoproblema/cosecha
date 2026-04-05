# Reporter JSON

## Objetivo

`cosecha-reporter-json` serializa resultados de ejecución a un documento
JSON estable para consumo por herramientas, CI o agentes.

## Punto de entrada

- `src/cosecha/reporter/json/__init__.py`

## Contenido del reporte

El reporte incluye, como mínimo:

- versión de esquema,
- reporter activo,
- engine asociado cuando aplica,
- raíz efectiva del workspace,
- duración total,
- resumen por `status`,
- lista de tests con mensaje, error y metadata útil.

En Gherkin puede añadir además nombre de feature, escenario y línea.

## Casos de uso

- análisis post-mortem sin parsear la consola,
- integración con tooling propio,
- consumo por agentes que necesiten un artefacto portable y legible.
