# Planificación y materialización lazy en Gherkin

## Entrada

- tests Gherkin recolectados,
- índice de definiciones del proyecto,
- textos de pasos requeridos,
- plan construido por el core.

## Flujo

1. Cada test publica sus textos requeridos.
2. El runner consulta el índice del engine.
3. El nodo queda enriquecido con archivos candidatos.
4. El runtime prepara el nodo con esa metadata.
5. Cuando un paso necesita matching, `LazyStepResolver` consulta los
   candidatos.
6. Solo se importan los archivos aún no cargados.
7. `StepRegistry` resuelve el matching efectivo.

## Validaciones

- un archivo se materializa como mucho una vez por proceso,
- la ausencia de candidatos no produce carga global eager,
- la ambigüedad se detecta tras materialización real,
- los fallos de import se reflejan como diagnósticos.

## Salida

- `StepRegistry` poblado bajo demanda,
- matching resuelto para los pasos necesarios,
- menor coste de arranque frente a la carga global.
