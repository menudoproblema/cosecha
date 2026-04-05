# ADR-0004: Matriz de capacidades y límites explícitos de engine y runtime

## Estado

Accepted

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
