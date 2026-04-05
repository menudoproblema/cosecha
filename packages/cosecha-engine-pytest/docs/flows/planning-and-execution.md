# Planificación y ejecución en Pytest

## Entrada

- tests Pytest descubiertos,
- fixtures indexadas,
- runtime profiles efectivos,
- `Execution IR` construido por el core.

## Flujo

1. El runner consulta el conocimiento del engine.
2. Construye nodos con identidad estable y metadata del dominio.
3. El scheduler decide placement según políticas y recursos.
4. El runtime entrega al worker el nodo y el contexto.
5. El engine decide si puede ejecutar por el camino soportado o si debe
   degradar al adaptador real agrupado de `pytest`.
6. El resultado se traduce a eventos y `Reporting IR` comunes.

## Validaciones

- cada nodo debe mapearse a una selección ejecutable estable,
- `requires`, `requires_capability` y `disallow_mode` deben validarse
  antes de ejecutar,
- los fallos de preparación se representan como explicación o issue
  estructurada.

## Errores

- nodo no seleccionable de forma estable,
- incompatibilidad entre nodo y runtime,
- fallo interno de `pytest`,
- error al traducir la salida al modelo común.
