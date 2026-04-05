# Arquitectura local de Pytest

## Alcance

El engine Pytest implementa un engine concreto sobre los contratos
generales de Cosecha sin reimplementar `pytest` como framework.

Sus piezas principales son:

- discovery estático de tests y fixtures,
- construcción de nodos con identidad estable,
- consumo de wiring declarativo desde el manifiesto,
- adaptación entre recursos del runtime y fixtures,
- traducción de resultados y eventos al modelo común.

## Componentes

### Collector

- `src/cosecha/engine/pytest/collector.py`
- `src/cosecha/engine/pytest/items.py`

Descubre tests y fixtures sin depender del import eager como camino
principal. Publica conocimiento incremental del engine y conserva
identidad estable por nodo.

### Knowledge local

La proyección Pytest sobre la knowledge base global representa:

- tests descubiertos,
- fixtures visibles,
- procedencia de definiciones externas,
- metadatos suficientes para explain y tooling.

No constituye una fuente de verdad separada del core.

### Engine y runtime adapter

- `src/cosecha/engine/pytest/engine.py`
- `src/cosecha/engine/pytest/runtime_adapter.py`
- `src/cosecha/engine/pytest/discovery.py`

El engine decide si un nodo puede mantenerse en el camino soportado por
análisis estático o si debe degradar al adaptador real de `pytest`.
Siempre publica resultados en vocabulario común del core.

## Límites

- Pytest no define el modelo transversal de recursos ni de reporting.
- El engine no debe introducir una UX paralela a la del CLI.
- Las decisiones sobre capabilities, runtime profiles y artefactos
  finales siguen perteneciendo al core.
