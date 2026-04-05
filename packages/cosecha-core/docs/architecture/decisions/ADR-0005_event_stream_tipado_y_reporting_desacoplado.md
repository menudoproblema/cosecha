# ADR-0005: Event stream tipado y reporting desacoplado

## Estado

Accepted

## Decisión

Reporting, telemetría y diagnósticos consumen un stream tipado de
hechos del dominio y no callbacks directos del engine.

Reglas:

- el sistema emite eventos tipados,
- reporting y telemetría procesan esos eventos mediante sinks o
  coordinadores explícitos,
- la inicialización y el lifecycle de reporting pertenecen a
  composición,
- el engine no controla la UI ni la presentación final.

## Consecuencias

- consola, JUnit, JSON y tooling comparten la misma base de hechos,
- baja el acoplamiento entre ejecución y presentación,
- el sistema necesita una taxonomía de eventos estable.
