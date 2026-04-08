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

## Nota de evolucion

Este ADR sigue siendo el dueño de `stable_id` y del bootstrap
serializable, pero la taxonomia de ids operativos del sistema se reparte
ya entre varios ADRs:

- `stable_id`: identidad estable del nodo de ejecucion.
- `workspace_fingerprint`: identidad semantica del workspace, definida
  en ADR-0006.
- `invocation_id`: identidad operativa del `ExecutionContext`, definida
  en ADR-0006.
- `session_id`: identidad operativa de la sesion de runtime y del
  `ShadowExecutionContext`, usada por ADR-0008 y ADR-0009.
- `component_id`: identidad declarativa de los componentes con
  namespaces propios, definida por ADR-0009.

Este ADR debe leerse junto con ADR-0006 y ADR-0009 cuando se razone
sobre correlacion, persistencia o transporte entre controller y
workers.

La taxonomia completa, la relacion jerarquica entre ids y el algoritmo
vigente de `stable_id` quedan formalizados en ADR-0013.
