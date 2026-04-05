# Discovery y conocimiento en Pytest

## Entrada

- rutas de colección,
- workspace efectivo,
- archivos Python con tests y fixtures,
- configuración efectiva del engine,
- `definition_paths` cuando proceda,
- imports locales y `pytest_plugins` resolubles estáticamente.

## Flujo

1. El collector normaliza las rutas de colección.
2. Descubre artefactos compatibles con `pytest`.
3. Indexa tests, clases y fixtures sin ejecutar código como camino
   principal.
4. Construye identidades estables.
5. Incorpora fixtures visibles desde imports, `pytest_plugins` y
   `definition_paths`.
6. Publica eventos de conocimiento del engine.
7. Persiste o refresca su proyección local en la knowledge base.

## Salida

- catálogo de tests ejecutables,
- catálogo de fixtures,
- relaciones test-fixture,
- conocimiento consultable por tooling y planning,
- diagnósticos de degradación cuando el engine requiere runtime real.

## Errores

- símbolo no resoluble de forma estable,
- archivo inconsistente o no indexable,
- conflicto entre identidades estables,
- conocimiento persistido obsoleto o inválido.
