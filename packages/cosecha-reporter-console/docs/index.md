# Reporter de consola

## Objetivo

`cosecha-reporter-console` proyecta a salida humana el `Reporting IR`
agregado por el core.

Su ownership cubre:

- resumen de sesión legible,
- detalle de fallos para humanos,
- progreso y bloques compactos de observabilidad compatibles con la
  consola del framework.

No es dueño de:

- la coordinación de sesión,
- el modelo de artefactos,
- formatos de intercambio máquina.

## Punto de entrada

- `src/cosecha/reporter/console/__init__.py`

## Notas

- Se registra por entry point en `cosecha.shell.reporting`.
- Es el reporter instalado por defecto junto con el CLI.
