# Modelo de errores

## Propósito

El framework clasifica los errores por familia y conserva la frontera
entre compilación, runtime, infraestructura y presentación.

## Familias

### Error de compilación

Deriva de:

- fuentes inválidas,
- definiciones incompatibles,
- problemas de tipos o coerciones,
- conocimiento persistido inconsistente.

Se publica como `PlanningIssue` o artefacto equivalente, no como fallo
de runtime.

### Error de infraestructura

Deriva de:

- worker roto,
- IPC inválido,
- timeout,
- recurso no disponible,
- fallo de health-check o provisión.

No se proyecta como fallo funcional sin una traducción explícita.

### Error funcional de nodo

Deriva de:

- aserciones fallidas,
- excepciones de negocio,
- incoherencia funcional del dominio ya materializado.

### Error de control plane

Deriva de:

- comando inválido,
- operación no autorizada,
- protocolo incompatible,
- simulación no soportada.

## `strict` frente a `relaxed`

### `strict`

- bloquea planes no ejecutables,
- exige compatibilidad completa antes del runtime.

### `relaxed`

- conserva issues y explain,
- permite inspección parcial sin degradar a excepciones opacas.

## `failure_kind`

Además de `status`, el sistema publica:

- `test`
- `runtime`
- `infrastructure`
- `hook`
- `bootstrap`
- `collection`

Esto complementa el estado final del nodo y mejora el diagnóstico
post-mortem.
