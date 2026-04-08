# ADR-0010: Catalogo canonico de Cosecha publicado en CXP

## Estado

Accepted

## Contexto

La evolucion reciente de `cosecha` ha dejado corto el marco original de
ADR-0004. El sistema real ya no trabaja solo con una lista plana de
capabilities y tres niveles de soporte. Hoy existen:

- interfaces publicas diferenciadas por capa del sistema;
- capabilities con metadata estructurada y operaciones normativas;
- `tiers` y `profiles` con semantica de conformidad;
- validacion cruzada contra catalogos first-party publicados en `cxp`.

Ese contrato ya existe en codigo y se usa en la integracion entre
`cosecha` y `cxp`, pero faltaba un ADR que lo formalizara.

## Decision

`cosecha` es la autoridad semantica de su dominio interoperable.
`cxp` hospeda ese contrato como catalogo canonico, pero no define su
modelo.

La secuencia normativa queda fijada asi:

1. `cosecha` define interfaces, capabilities, metadata, operaciones,
   telemetria, `tiers` y `profiles`.
2. `cxp` publica exactamente ese contrato bajo interfaces
   `cosecha/*`.
3. `cosecha` integra y valida sus snapshots contra ese catalogo ya
   fijado.

El objetivo no es "hacer que pase un adapter", sino publicar un
contrato explicito y completo que permita validar el sistema sin
heuristicas.

## Capas canonicas

El catalogo de Cosecha se organiza por capas con ownership explicito:

- `cosecha/engine`
- `cosecha/runtime`
- `cosecha/reporter`
- `cosecha/plugin`
- `cosecha/instrumentation`

`launcher`, CLI, MCP, LSP y otros consumidores no definen semantica del
catalogo. Solo consumen y presentan capacidades publicadas por estas
capas.

## Reglas de modelado

- Las capabilities publicas se declaran explicitamente. No se infieren
  por nombre de clase, por nombre de paquete ni por heuristicas de
  adapter.
- Cada capability publica puede definir operaciones normativas,
  `metadata_schema`, `summary`, atributos y reglas de entrega.
- `tiers` y `profiles` forman parte del contrato publico. Son umbrales
  de conformidad y no sustituyen a la lista explicita de capabilities.
- La documentacion canonica de este contrato vive en
  [`canonical_catalog.md`](../canonical_catalog.md).
- Las capabilities internas de infraestructura, como
  `produces_ephemeral_artifacts`, no forman parte del catalogo publico
  interoperable de `cxp`.

## Consecuencias

Positivas:

- `cosecha` deja de depender de snapshots heurísticos o de compatibilidad
  encubierta.
- `cxp` hospeda un contrato estable, validable y alineado con el
  sistema real.
- Las evoluciones de interfaces y capabilities tienen una fuente de
  verdad explicita.

Costes:

- cualquier cambio publico en `cosecha/*` debe actualizar primero la
  especificacion canonica;
- `cosecha` y `cxp` deben evolucionar coordinadamente;
- aumenta el coste de tests de conformidad y de compatibilidad.

## Relacion con ADRs anteriores

- Extiende ADR-0004, que fijaba la necesidad de una matriz de
  capabilities explicita pero no describia todavia el shape completo del
  contrato.
- Se apoya en ADR-0007 para capacidades de observabilidad live y en
  ADR-0009 para capacidades internas de infraestructura que no forman
  parte del catalogo interoperable.

## Regla de evolucion

Cualquier capability nueva o cambio de shape en interfaces `cosecha/*`
debe:

1. especificarse primero en `canonical_catalog.md`;
2. actualizar este ADR si cambia el modelo general;
3. publicarse despues en `cxp`;
4. integrarse finalmente en `cosecha`.
