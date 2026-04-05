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
