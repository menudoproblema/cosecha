# ADR-0004: Matriz de capacidades y límites explícitos de engine y runtime

## Estado

Accepted, extendido por ADR-0010 y ADR-0012

## Decisión

Engines, runtimes y adapters publican capacidades explícitas y límites
contractuales.

Cada capability se clasifica como:

- `supported`
- `accepted_noop`
- `unsupported`

Los nombres de capability son transversales y se usan en planning,
placement, explain e interfaces de control.

## Consecuencias

- las incompatibilidades dejan de ser sorpresas de runtime,
- explain puede justificar fallbacks y noops con base contractual,
- cada capability nueva debe declararse y testearse.

## Nota de evolucion

Este ADR conserva la decision fundacional de "capabilities explicitas",
pero no pretende ya describir por si solo el shape completo del modelo.

- Los niveles `supported`, `accepted_noop` y `unsupported` siguen
  vigentes como semantica base.
- El contrato real actual anade metadata estructurada, operaciones,
  `metadata_schema`, `tiers`, `profiles`, `freshness`,
  `delivery_mode` y reglas de validacion que se formalizan en ADR-0010
  y en el catalogo canonico de `cosecha`.
- ADR-0007 y ADR-0009 dependen de esta matriz. Por eso la autoridad
  vigente para nuevas capabilities ya no es este resumen aislado, sino
  el contrato canonico posterior.
- ADR-0012 formaliza el shape real del contrato de capabilities,
  incluidos `summary`, `attributes`, `operations`, `delivery_mode`,
  `granularity`, `metadata_schema`, `tiers` y `profiles`.
