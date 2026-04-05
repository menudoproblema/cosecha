# Fixtures y recursos en Pytest

## Entrada

- requisitos de recursos del plan,
- fixtures descubiertas por el engine,
- `Context Bundle` efectivo del runtime,
- políticas de aislamiento del worker.

## Flujo

1. El plan declara recursos y constraints.
2. El runtime materializa el `Context Bundle`.
3. El engine traduce recursos de Cosecha a fixtures o adaptación
   equivalente.
4. La ejecución consume esa traducción dentro del entorno del worker.
5. Los resultados y errores se reflejan como hechos del dominio común.

## Reglas

- no toda fixture equivale a un recurso de Cosecha,
- la traducción fixture-recurso debe ser explícita y trazable,
- `conftest.py` y las fixtures locales mantienen precedencia sobre el
  bridge de recursos,
- un fallo de recurso no debe confundirse con un fallo funcional del
  test.
