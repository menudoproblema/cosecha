# Ejecución en Gherkin

## Entrada

- `GherkinTestItem`,
- `Context`,
- `StepRegistry`,
- coerciones efectivas,
- recursos ya gestionados por el core.

## Flujo

1. El engine crea un contexto nuevo para el test.
2. El test pre-resuelve el matching de sus pasos.
3. Los pasos se ejecutan en orden secuencial.
4. Cada paso valida restricciones de tabla y argumentos.
5. El resultado final del escenario se proyecta a `Reporting IR`.
6. El core agrega timings, artefactos y resumen de sesión.

## Errores

- paso sin implementación,
- definición ambigua,
- fallo funcional de la implementación,
- error inesperado durante la ejecución del escenario.
