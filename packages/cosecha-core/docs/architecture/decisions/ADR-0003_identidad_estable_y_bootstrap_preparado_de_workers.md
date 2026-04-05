# ADR-0003: Identidad estable de nodos y bootstrap preparado de workers

## Estado

Accepted

## Decisión

Cada nodo de ejecución tiene identidad estable independiente de una
sesión concreta y el runtime trabaja sobre snapshots serializables ya
preparados por el master.

Reglas:

- `stable_id` por nodo,
- bootstrap serializable antes de ejecutar,
- workers inicializados con bundles ya compilados,
- correlación estable entre request, worker, nodo y reporting.

## Consecuencias

- mejor reproducibilidad entre explain, scheduling y ejecución,
- menor coste por nodo en runtimes persistentes,
- necesidad de protocolo master-worker versionado.
