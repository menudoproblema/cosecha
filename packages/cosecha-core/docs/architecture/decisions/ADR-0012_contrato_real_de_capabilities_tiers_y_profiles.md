# ADR-0012: Contrato real de capabilities, tiers y profiles

## Estado

Accepted

## Contexto

ADR-0004 fijo correctamente la necesidad de una matriz de capabilities
explicita, pero no describia el shape real que el sistema ha ido
adoptando. Hoy Cosecha y `cxp` operan con un contrato mas rico que ya
existe en codigo:

- `CapabilityDescriptor` en `cosecha-core`;
- catalogos `cosecha/*` en `cxp`;
- validacion por interfaces, `tiers`, `profiles`, metadata schemas y
  telemetria.

Faltaba un ADR que cerrara ese contrato real para que ADR-0004 deje de
ser una base demasiado abstracta.

## Decision

La matriz de capabilities de Cosecha tiene dos niveles de contrato
coordinados:

1. **Descripcion publicada por componentes** en `cosecha-core`.
2. **Contrato interoperable validado** en `cxp`.

El primer nivel describe lo que un componente publica. El segundo nivel
declara contra que interfaz, schema y perfil se valida esa publicacion.

## Shape base en `cosecha-core`

La unidad minima publicada por un componente es `CapabilityDescriptor`.
Su shape vigente incluye:

- `name`
- `level`
- `api_version`
- `stability`
- `summary`
- `attributes`
- `operations`
- `delivery_mode`
- `granularity`

Reglas:

- `level` conserva la semantica fundacional de ADR-0004:
  `supported`, `accepted_noop`, `unsupported`.
- `attributes` describe metadata publica del componente y no debe
  duplicar flags internos de infraestructura salvo que formen parte de un
  contrato visible.
- `operations` publica operaciones normativas y su `result_type`
  esperado cuando aplique.
- `delivery_mode` y `granularity` forman parte del contrato observable y
  no son comentarios informales.

## Shape interoperable en `cxp`

Los catalogos `cosecha/*` publicados en `cxp` anaden contrato
interoperable sobre esa base:

- interfaz (`cosecha/engine`, `cosecha/runtime`, etc.);
- `metadata_schema` por capability cuando aplique;
- `tiers`;
- `profiles`;
- telemetria normativa por capability;
- validacion de metadata, operaciones y conformidad de interfaz.

`tiers` y `profiles` no sustituyen a las capabilities publicadas. Son
umbrales de conformidad definidos sobre ellas.

## Distincion entre capabilities publicas e internas

No toda capability declarada en `cosecha-core` pertenece al catalogo
publico interoperable.

- Publicas: las que describen funciones del sistema visibles en
  `cosecha/*` y se validan en `cxp`.
- Internas: las que modelan infraestructura o enforcement local del
  core, por ejemplo `produces_ephemeral_artifacts`.

Regla normativa: una capability interna puede participar en bootstrap,
shadow, runtime o enforcement, pero no se publica en `cxp` salvo que se
decida expresamente elevarla a contrato interoperable.

## Fuente de verdad y evolucion

- La semantica nace en `cosecha`.
- La forma canonica del contrato se documenta en
  `canonical_catalog.md` y ADR-0010.
- `cxp` publica y valida exactamente ese contrato.
- Ninguna capability nueva debe publicarse por heuristica o deduccion
  del adapter.

## Consecuencias

Positivas:

- ADR-0004 queda aterrado en un modelo real y versionable.
- `tiers` y `profiles` dejan de ser "magia del catalogo" sin respaldo
  documental.
- la frontera entre contrato publico y mecanismo interno queda
  explicitada.

Costes:

- aumenta el coste documental de cada capability nueva;
- `cosecha` y `cxp` deben seguir evolucionando coordinadamente;
- exige disciplina para no colar detalles internos en el catalogo
  publico.

## Relacion con ADRs anteriores

- Extiende ADR-0004.
- Se coordina con ADR-0010 para el catalogo `cosecha/*`.
- Soporta ADR-0007 y ADR-0009, que dependen de la existencia de una
  matriz de capabilities robusta.
