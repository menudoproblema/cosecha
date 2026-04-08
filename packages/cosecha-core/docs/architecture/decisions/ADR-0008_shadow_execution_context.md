# ADR-0008: Shadow execution context y aislamiento de artefactos efímeros

## Estado

Accepted, extendido por ADR-0009

## Contexto

El modelo introducido por ADR-0006 separa identidad semántica del
workspace y contexto operativo de invocación, pero todavía deja sin
formalizar dónde viven los artefactos efímeros de una sesión concreta.
Hoy esos artefactos aparecen repartidos entre:

- `.cosecha/runtime` para estado de workers,
- directorios temporales ad hoc para instrumentación,
- paths predecibles calculados por cada componente.

Esta dispersión impide un aislamiento fuerte entre sesiones
concurrentes, complica el cleanup, dificulta la depuración cuando falla
la instrumentación y deja sin punto estable la futura inyección de
subprocess bootstrap.

Este ADR formaliza el subcontexto efímero de sesión como
`ShadowExecutionContext`.

## Modelo

`ExecutionContext` sigue describiendo el contexto operativo persistente
de una invocación: directorio de ejecución, storage físico y huella del
workspace.

`ShadowExecutionContext` describe el árbol efímero y exclusivo de una
sesión concreta dentro de ese contexto operativo. Es el lugar único
donde viven:

- estado de runtime de workers,
- metadata de instrumentación,
- workdirs temporales de instrumentadores,
- artefactos efímeros que deben poder preservarse o limpiarse como una
  sola unidad.

El shadow context es operativo, no semántico. Nunca participa en el
fingerprint del workspace.

## Decisiones

### Decisión 1 — Ubicación normativa

`ShadowExecutionContext` vive bajo:

```python
knowledge_storage_root / "shadow" / <session_id>
```

No vive en `/tmp` ni en una ruta opaca al workspace. El storage efímero
de sesión pertenece al storage operativo del proyecto.

### Decisión 2 — Rutas derivadas normativas

El shadow context expone, como mínimo, estas rutas:

- `root_path`
- `runtime_state_dir`
- `instrumentation_dir`
- `metadata_file`
- `coverage_dir`

Estas rutas son derivadas determinísticas del `root_path`. No se
resuelven mediante convenciones distribuidas.

### Decisión 3 — Transporte entre controller y workers

El shadow context debe llegar a workers persistentes sin depender de
inferencia local. Puede transportarse serializado o como raíz derivable
dentro de `ExecutionContext`, pero el contrato debe viajar por el
bootstrap de runtime.

Consecuencia: un worker no debe reconstruir el shadow dir a partir de
`root_path`, `cwd` o nombres hardcodeados.

### Decisión 4 — Estado runtime dentro del shadow dir

`ProcessRuntimeProvider` usa siempre `shadow.runtime_state_dir` para el
estado de workers y para cualquier fichero efímero de fencing o
recuperación.

El path `.cosecha/runtime/<session_id>` deja de ser la autoridad
primaria.

### Decisión 5 — Instrumentación integrada

El launcher de coverage y cualquier instrumentador de proceso reutilizan
ese mismo shadow dir. El workdir de coverage vive bajo
`shadow.instrumentation_dir`, no en un tempdir independiente.

Consecuencia: runtime e instrumentación comparten el mismo árbol
efímero de sesión.

### Decisión 6 — Política de cleanup

La política de lifecycle queda fijada así:

- en éxito: el shadow dir completo se borra,
- en fallo de runtime o de instrumentación: el shadow dir completo se
  preserva.

No se permiten limpiezas parciales que dejen un árbol a medio estado y
rompan la inspección posterior.

### Decisión 7 — Diagnóstico visible

Cuando el shadow dir se preserva, la ruta exacta debe quedar visible en
warning, log o diagnóstico equivalente.

Rationale:

- la preservación solo es útil si el operador sabe qué directorio abrir,
- evita tener que reconstruir la ruta a partir de varios ids.

## Consecuencias

`ShadowExecutionContext` se convierte en la unidad de aislamiento
efímero por sesión.

La arquitectura queda preparada para evoluciones posteriores como
`sitecustomize.py`, bootstrap de subprocesses Python o sandboxes
externos sin renegociar de nuevo la semántica del storage efímero.

## Nota de evolucion

ADR-0009 extiende este ADR en tres puntos sustantivos:

- el namespacing por componente dentro del shadow;
- el transporte del shadow y de los permisos efimeros mediante
  bootstrap;
- el cleanup granular por namespace.

La implementacion actual mantiene este ADR como autoridad sobre la
existencia y ubicacion del shadow de sesion, pero el layout operativo
vigente ya distingue:

- instrumentacion bajo `shadow.instrumentation_dir / <component_id>`;
- estado de runtime bajo `shadow.runtime_state_dir / <component_id>`.

`shadow.coverage_dir` permanece como alias de compatibilidad hacia el
namespace oficial de `cosecha.instrumentation.coverage`.
