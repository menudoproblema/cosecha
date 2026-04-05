# Discovery y conocimiento en Gherkin

## Entrada

- rutas de colección,
- workspace efectivo,
- ficheros `.feature`,
- directorios `steps/` visibles por jerarquía,
- `definition_paths` configuradas.

## Flujo

1. `GherkinCollector` normaliza las rutas de colección.
2. Descubre `.feature` aplicables.
3. Busca `steps/` subiendo por la jerarquía e incorpora
   `definition_paths`.
4. Parsea features y construye `GherkinTestItem`.
5. Construye o refresca `StepCatalog`.
6. Reutiliza conocimiento persistido para archivos no modificados.
7. AST-discover solo los archivos nuevos o cambiados.
8. Persiste el conocimiento incremental resultante.

## Salida

- tests recolectados,
- índice de definiciones para planning,
- conocimiento persistido actualizado,
- timings y diagnósticos asociados.

## Errores

- parser errors de `.feature`,
- archivos de steps no resolubles por AST,
- conocimiento persistido inválido u obsoleto,
- invalidación incompleta de entradas antiguas.
