# ADR-0014: Envelopes tipados y payloads extensibles

## Estado

Accepted

## Contexto

ADR-0005 fijaba un stream tipado de hechos del dominio. ADR-0007
introdujo payloads opacos para snapshots live de engine. Ambas cosas son
compatibles, pero esa compatibilidad no estaba documentada de forma
explicita y se leia como contradiccion.

## Decision

El sistema distingue entre:

- **envelope tipado y estable**
- **payload extensible del productor**

La politica normativa es:

- los eventos del dominio, sus `event_type`, metadata comun e ids
  operativos son tipados y estables;
- los comandos y respuestas del runtime usan envelopes tipados;
- los snapshots live y otros payloads especificos de engine pueden ser
  extensibles y parcialmente opacos si el envelope sigue siendo
  estable.

## Envelope tipado

Se consideran parte del envelope tipado:

- tipo de evento o comando;
- metadata comun;
- ids de correlacion;
- campos contractuales comunes del canal;
- `snapshot_kind` y `phase` cuando apliquen.

Estos campos deben mantenerse versionables y con semantica publica
estable para que tooling transversal, KB, CLI, MCP y reporting puedan
consumirlos sin conocer cada engine.

## Payload extensible

Se considera payload extensible:

- contenido especifico de engine dentro de snapshots live;
- campos ricos que el core persiste o transporta sin validacion
  semantica completa;
- metadata puntual que evoluciona mas rapido que el envelope.

Regla normativa: la opacidad solo se permite en el payload especifico
del productor. No se permite en el envelope comun del sistema.

## Canales del sistema

- **Domain events**: tipados como clases/dataclasses con metadata
  estable; algunos campos internos pueden ser ricos pero el evento como
  tal no es opaco.
- **Runtime protocol**: comandos, respuestas y envelopes tipados.
- **Live snapshots de engine**: envelope tipado + payload extensible.
- **Capability metadata**: preferencia por metadata declarativa,
  validable por schema en `cxp`.

## Consecuencias

Positivas:

- se preserva tooling transversal sin congelar demasiado pronto los
  payloads de engine;
- desaparece la falsa contradiccion entre ADR-0005 y ADR-0007;
- se aclara donde debe vivir la tipificacion estricta y donde se permite
  evolucion rapida.

Costes:

- obliga a vigilar mejor la frontera entre envelope y payload;
- exige no esconder informacion estructural comun dentro de payloads
  supuestamente opacos.

## Relacion con ADRs anteriores

- Extiende ADR-0005.
- Aclara ADR-0007.
- Es coherente con ADR-0010 y ADR-0012, donde las capabilities publicas
  prefieren metadata validable frente a blobs opacos.
