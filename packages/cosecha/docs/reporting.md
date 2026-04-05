# Reporting desde el CLI

## Uso desde `cosecha run`

La superficie canónica del CLI usa subcomandos. Los reportes
estructurados se generan desde `cosecha run` mediante:

- `--report TYPE:PATH`

Tipos soportados por instalación:

- `console`
- `json`
- `junit`

Cada tipo de reporte puede aparecer como mucho una vez por comando.

## Ejemplos

```bash
cosecha run --report junit:results.xml
cosecha run --output summary --report junit:ci.xml
cosecha run --report json:report.json
```

## Resumen humano de sesión

Al cerrar una sesión, el runner proyecta en consola un resumen
transversal con:

- total de tests,
- breakdown por `status`,
- breakdown por `failure_kind`,
- desglose por engine cuando hay varios activos,
- bloques adicionales como timings o coverage cuando sus plugins están
  activos.

Ese resumen se basa en artefactos y `Reporting IR` del core; el CLI no
mantiene un contrato paralelo.

## Relación con artefactos persistidos

`cosecha session summary` y las consultas de artefactos leen la
proyección final persistida por el core.

Cuando se habilita observabilidad adicional, el CLI puede mostrar un
resumen compacto de snapshots vivos sin convertir el artefacto final en
un stream completo de diagnóstico.

## Uso por agentes y CI

La salida renderizada del CLI está optimizada para humanos. Cuando un
consumidor necesita un artefacto portable debe usar reporters
estructurados como JSON o JUnit y no depender de scraping de consola.
