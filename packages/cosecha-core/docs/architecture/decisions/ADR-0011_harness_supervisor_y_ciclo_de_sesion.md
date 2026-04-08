# ADR-0011: Harness, supervisor y ciclo de sesion

## Estado

Accepted

## Contexto

La arquitectura operativa de Cosecha se apoya hoy en varias piezas que
cooperan estrechamente:

- launcher y CLI como entry points;
- `Runner` como orquestador de sesion;
- `ProcessRuntimeProvider` o el runtime activo como ejecutor;
- reporters, plugins e instrumentadores como sidecars de sesion.

Ese conjunto ya existe en codigo, pero hasta ahora no tenia un nombre
arquitectonico unico ni un ADR que fijara sus fronteras. Eso complica
razonar sobre ownership del lifecycle, del shadow, de la telemetria y
de los contratos entre controller y workers.

## Decision

El sistema reconoce explicitamente una capa de **Harness** o
**Supervisor de sesion**.

El Harness no es una clase unica obligatoria. Es una responsabilidad
arquitectonica compuesta por las piezas que controlan una sesion de
ejecucion de extremo a extremo.

## Responsabilidades del Harness

El Harness es dueño de:

- abrir y cerrar la sesion operativa;
- materializar `ExecutionContext` y `ShadowExecutionContext`;
- bindear el shadow y el registro activo de capabilities efimeras en el
  proceso controlador;
- construir y arrancar el runtime provider;
- coordinar reporters, plugins e instrumentacion de sesion;
- publicar eventos, snapshots y artefactos de sesion;
- decidir cleanup, preservacion y diagnosticos finales.

El Harness no es dueño de:

- la semantica de cada engine;
- la implementacion interna de cada runtime provider;
- el contrato publico `cosecha/*` hospedado en `cxp`.

## Composicion actual

En la implementacion actual, el Harness se materializa asi:

- launcher/CLI: entrada y composicion inicial;
- `Runner`: coordinador principal de sesion;
- runtime provider activo: ejecucion efectiva y transporte a workers;
- reporters/plugins/instrumentadores: sidecars coordinados por la
  sesion.

Consecuencia: cuando un ADR hable de "supervisor", "harness" o
"controller de sesion", se refiere a esta responsabilidad compuesta,
aunque la implementacion concreta se reparta entre varias clases.

## Reglas normativas

- Ningun componente fuera del Harness decide por si solo el lifecycle de
  la sesion completa.
- El bootstrap de workers y subprocesses cuelga del Harness, aunque lo
  ejecute tecnicamente el runtime provider.
- El Harness es el unico lugar legitimo para componer shadow,
  observabilidad live, reporting e instrumentacion sin contaminar los
  engines.

## Relacion con ADRs anteriores

- ADR-0006 fija el contexto operativo base que el Harness materializa.
- ADR-0007 fija la frontera de observabilidad live que el Harness
  transporta y publica.
- ADR-0008 y ADR-0009 fijan el shadow y los permisos efimeros que el
  Harness debe bindear y transportar.
- ADR-0010 fija el catalogo publico que el Harness consume, pero no
  redefine.

## Consecuencias

Positivas:

- desaparece la figura implicita del "supervisor misterioso";
- las responsabilidades transversales quedan localizadas;
- futuros cambios como ejecucion remota, cloud runners o clusters tienen
  un punto arquitectonico claro donde engancharse.

Costes:

- obliga a distinguir mejor entre responsabilidades de sesion y
  responsabilidades de componente;
- hace mas visible la necesidad de mantener `Runner`, launcher y runtime
  alineados como parte de una sola capa.
