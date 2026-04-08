# ADR-0001: Knowledge Snapshot, discovery estático y materialización lazy

## Estado

Accepted

## Decisión

El framework descubre y persiste conocimiento reutilizable antes de
importar o materializar código del proyecto siempre que el dominio lo
permita.

Reglas:

- el sistema publica un `Knowledge Snapshot` reconstruible,
- el discovery estático es el camino principal cuando es suficiente,
- el resultado del discovery alimenta planning, draft validation y
  tooling,
- la importación real se difiere a la sesión o a la resolución
  efectiva,
- la KB acelera el camino caliente, pero no sustituye la compilación
  correcta desde fuentes.

## Consecuencias

- menos trabajo repetido en collection y validación reactiva,
- mejor soporte para explain y tooling,
- necesidad de versionado e invalidación explícitos del snapshot.

## Nota de evolucion

Este ADR fija la direccion, pero no es ya la unica autoridad del
contrato.

- La identidad operativa del workspace y la invalidacion de artefactos
  persistentes se apoyan hoy en `workspace_fingerprint`, tal y como
  desarrolla ADR-0006.
- El discovery estatico y las capabilities publicas que alimentan
  planning, validacion y tooling se publican hoy a traves del catalogo
  canonico de `cosecha`, formalizado en ADR-0010.
- Este ADR debe leerse como decision fundacional de "knowledge primero";
  los tipos, snapshots y reglas operativas concretas viven ya en los
  ADR posteriores y en el codigo.
