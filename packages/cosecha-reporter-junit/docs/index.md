# Reporter JUnit

## Objetivo

`cosecha-reporter-junit` genera XML JUnit para integrarse con CI,
agentes y herramientas que ya entienden ese formato.

## Punto de entrada

- `src/cosecha/reporter/junit/__init__.py`

## Características

- genera `<testsuites>` y `<testcase>` con tiempos agregados,
- publica `file` y `line` para localizar el fallo,
- refleja `failure`, `error` y `skipped`,
- en Gherkin añade una traza de pasos en `system-out`.

## Uso recomendado

- pipelines de CI que ya consumen JUnit,
- agentes que necesiten localizar rápido archivo, línea y contexto del
  fallo,
- exportes portables cuando la consola humana no es suficiente.
