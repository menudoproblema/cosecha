# Modelo de configuración

## Propósito

La configuración efectiva es una de las dos fuentes de verdad del
sistema junto con el workspace.

En el estado actual del monorepo, el wiring declarativo del proyecto
vive en `tests/cosecha.toml` o `cosecha.toml`, y se combina con la
entrada CLI y con los defaults del framework.

## Principios

- la configuración se resuelve antes de planificar o ejecutar,
- se expresa mediante contratos tipados y serializables,
- el runtime y la KB consumen el mismo snapshot efectivo,
- un cambio de configuración puede invalidar conocimiento persistido y
  planes previos.

## Categorías

### Entrada

- paths
- filtros
- límites
- selección de engines
- modo de planificación

### Engines

- engines activos y sus `runtime_profile_ids`
- `definition_paths`
- configuración específica de discovery, validation, explain o
  ejecución

### Runtime

- tipo de runtime
- concurrencia
- timeouts y retry
- captura de logs
- reporting estructurado

### Recursos

- recursos compartidos declarados en el manifiesto
- bindings por engine
- lifecycle y modo de provisión

### Observabilidad y reporting

- `output_mode`
- `output_detail`
- exportadores
- política de persistencia de artefactos de sesión

## Resolución

La precedencia efectiva del sistema es:

1. entrada directa de la operación,
2. configuración declarada del proyecto o workspace,
3. defaults del sistema.

Los invariantes mínimos son:

- precedencia determinista,
- snapshot serializable,
- explicación de qué valor quedó activo y por qué,
- fingerprint estable para invalidar conocimiento derivado.

## Runtime profiles e interfaces

Los `runtime_profiles.services` usan interfaces canónicas de runtime.

El core mantiene ownership sobre materialización, scheduling y runtime,
pero el vocabulario público de interfaces y capabilities se valida
contra la capa de catálogos interoperables del framework actual.

## Persistencia de artefactos

La sesión persiste una versión compacta de:

- snapshot de configuración,
- capabilities activas,
- plan explanation,
- timing,
- resumen de reporting,
- resumen de telemetría,
- fallos y ficheros relevantes dentro de budgets explícitos.

La compactación no cambia la autoridad del sistema. Solo limita el
coste de persistencia y consulta post-mortem.
