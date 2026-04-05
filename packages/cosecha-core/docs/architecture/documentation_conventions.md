# Convenciones de documentación

## Objetivo

La documentación del monorepo se reparte por ownership real del
paquete, evitando duplicar estructuras legacy que ya no forman parte
del diseño vigente.

## Reglas

- la arquitectura transversal del framework vive en `cosecha-core/docs/`,
- la documentación específica de un paquete vive en su `docs/`,
- el documento de entrada canónico es `index.md`,
- las ADR viven en `cosecha-core/docs/architecture/decisions/`,
- no se mantiene documentación de compatibilidad solo para recordar
  rutas o módulos legacy.

## Estructura recomendada

### Framework

- `cosecha-core/docs/index.md`
- `cosecha-core/docs/architecture.md`
- `cosecha-core/docs/engines.md`
- `cosecha-core/docs/architecture/**`
- `cosecha-core/docs/roadmap.md`
- `cosecha-core/docs/migration_coverage.md`

### Paquetes

- `docs/index.md`
- `docs/architecture.md` cuando haya arquitectura local relevante,
- `docs/flows/**` cuando el paquete tenga flujos propios,
- `docs/maintenance.md`,
- `docs/known_issues.md`,
- `docs/pending_improvements.md`.

## Criterio editorial

- mantener el idioma en español,
- enlazar a la documentación transversal en vez de duplicarla,
- registrar en la matriz de migración los documentos fusionados,
  divididos u omitidos,
- actualizar referencias técnicas al estado vigente del monorepo.
