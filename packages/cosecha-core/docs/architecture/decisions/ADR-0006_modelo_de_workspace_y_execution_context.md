# ADR-0006: Modelo de workspace y separación de execution context

## Estado

Accepted (decisiones 1–6) · Deferred (sub-decisiones de implementación)

## Contexto

Cosecha necesita un contrato explícito sobre qué es un workspace, qué es
una invocación, y cómo se descubre uno desde el filesystem. En el estado
actual del sistema esa información no está modelada como un concepto
único: vive distribuida en varias funciones de discovery, en heurísticas
sobre el nombre `tests/`, en lógica de cwd del runtime y en inferencia
implícita de `sys.path`. Cada componente que necesita responder a la
pregunta "¿qué es el proyecto y desde dónde se ejecuta?" lo resuelve por
su cuenta, con reglas similares pero no idénticas, a través de
implementaciones duplicadas en core, runtime, pytest engine, MCP, LSP y
shell.

Esta dispersión tiene tres efectos concretos:

- cualquier cambio de comportamiento exige auditar varios sitios y
  arriesga divergencias entre ellos,
- la KB y los planes cacheados quedan ligados a paths absolutos por
  defecto, lo que impide portabilidad real,
- arquitecturas posteriores que necesiten redirigir la ejecución
  (supervisor con shadow execution context, sandboxes, runners remotos)
  no tienen un punto limpio donde insertarse.

Este ADR establece el modelo de workspace y la separación entre
identidad estable del proyecto y contexto operativo de invocación, con
todas las decisiones de contrato cerradas para que el resto del sistema
pueda construir sobre él sin renegociarlo.

## Modelo

El modelo se articula en tres planos disjuntos.

**Identidad estable del proyecto** se representa como `EffectiveWorkspace`.
Es lo que define qué es el proyecto: dónde está su raíz semántica, qué
locations de código contiene, contra qué ancla la KB sus paths. Es
serializable, fingerprintable y estable: cambia solo cuando cambia la
semántica del proyecto.

**Contexto operativo de invocación** se representa como
`ExecutionContext`. Es lo que define cómo se ejecuta una invocación
concreta: desde qué directorio, con qué storage para la KB, con qué
identificador. Puede ser efímero (shadow exec, sandbox, runner remoto)
sin que eso afecte a la identidad del workspace.

**Mecanismos de descubrimiento e inferencia** se representan como
`WorkspaceResolutionPolicy`, una política de scope de proceso que
define cómo se localiza el manifest, qué adapters de layout están
activos y qué límites estructurales se aplican a la resolución del
workspace.

La separación de estos tres planos es la propiedad central del modelo.
Identidad y operación nunca se mezclan: lo que invalida la KB y los
planes cae en `EffectiveWorkspace`; lo que es per-invocación cae en
`ExecutionContext`. La política de resolución vive fuera de ambos y se
configura una vez por proceso.

## Conceptos del dominio

El modelo expone cinco entidades nombradas con responsabilidad disjunta:

| Concepto              | Pertenece a         | Pregunta que responde                              |
| --------------------- | ------------------- | -------------------------------------------------- |
| `manifest_path`       | EffectiveWorkspace  | ¿Dónde vive la declaración?                        |
| `workspace_root`      | EffectiveWorkspace  | ¿Cuál es el ancla semántica del proyecto?          |
| `knowledge_anchor`    | EffectiveWorkspace  | ¿Contra qué ancla la KB sus paths persistidos?     |
| `import_environment`  | EffectiveWorkspace  | ¿Cuál es el entorno de resolución del código?      |
| `execution_root`      | ExecutionContext    | ¿Desde dónde se ejecutan los procesos hijo?        |
| `knowledge_storage_root` | ExecutionContext | ¿Dónde viven físicamente los archivos de la KB?    |

Tres notas sobre estas entidades:

- En proyectos típicos, `workspace_root`, `execution_root` y
  `knowledge_anchor` coinciden. Esa coincidencia es el caso particular,
  no la definición. Tener nombres separados permite que diverjan cuando
  importa sin pedir cambios estructurales.
- `import_environment` no es una lista de strings que van a `sys.path`.
  Es un conjunto ordenado de `CodeLocation`, cada una con un `role`
  (source, tests, vendored, generated) y una bandera `importable`.
  Esto permite que el runtime, la KB y el analizador de impacto
  consuman la misma estructura interpretándola según su propósito sin
  reinferir el rol del nombre del directorio.
- `knowledge_anchor` y `knowledge_storage_root` son cosas distintas. El
  ancla es semántica y estable: contra ella se persisten los paths
  relativos de la KB. El storage es físico y operativo: dónde viven los
  archivos. Mover el storage no debe invalidar la KB porque los paths
  persistidos están anclados al `knowledge_anchor`, no al storage.

## Decisiones

### Decisión 1 — Límites normativos de `workspace_root`

`workspace_root` debe ser un ancestro de `manifest_path` a una distancia
de como máximo `max_ancestor_distance` niveles. El default es `1`.
Cualquier intento de escapar más allá es `WorkspaceResolutionError`.

```python
def validate_workspace_root(
    workspace_root: Path,
    manifest_path: Path,
    *,
    max_distance: int,
) -> None:
    rel = manifest_path.parent.resolve().relative_to(workspace_root.resolve())
    if len(rel.parts) > max_distance:
        raise WorkspaceResolutionError(...)
```

`max_ancestor_distance` pertenece a `WorkspaceResolutionPolicy`
(decisión 6), no a `ManifestSearchStrategy`. La estrategia de búsqueda
se limita a patrones de localización del manifest; los límites
estructurales del workspace son política del proceso.

Rationale:

- Permite el caso de un manifest ubicado en un subdirectorio cuyo
  workspace efectivo es el directorio padre (distancia 1).
- Bloquea `workspace_root = "/"` o ancestros lejanos arbitrarios, que
  son síntoma de configuración rota.
- Bloquea `workspace_root` *descendiente* del `manifest_path`: si el
  workspace debe ser más pequeño que el directorio del manifest, se
  mueve el manifest. No hay subworkspace.

Consecuencia: la pregunta "¿el manifest puede describir un workspace
mayor que su árbol?" tiene respuesta única: sí, hasta N niveles arriba,
con N pequeño y declarado.

### Decisión 2 — Fingerprint del `EffectiveWorkspace`

El fingerprint es semántico, no sensible a path absoluto. Reglas:

- no entran paths absolutos al hash,
- solo entran campos semánticos normalizados relativos a `workspace_root`,
- `workspace_root` y `manifest_path` no entran al hash en ninguna forma.

```python
def compute_workspace_fingerprint(ws: EffectiveWorkspace) -> str:
    payload = {
        "knowledge_anchor": str(
            ws.knowledge_anchor.relative_to(ws.workspace_root)
        ),
        "import_environment": [
            {
                "path": str(loc.path.relative_to(ws.workspace_root)),
                "role": loc.role,
                "importable": loc.importable,
            }
            for loc in ws.import_environment.locations
        ],
    }
    return sha256(canonical_json(payload).encode()).hexdigest()
```

Rationale:

- Dos clones del mismo proyecto en directorios distintos producen el
  mismo fingerprint. La KB es portable de verdad.
- Cualquier "fingerprint sensible a máquina" (path absoluto, hostname,
  user) sigue siendo posible como **identidad de instalación**, pero es
  un concepto separado y debe tener nombre distinto. No se mezcla con
  la identidad del workspace.

Consecuencia: el fingerprint que invalida planes y KB cambia solo
cuando cambia la semántica del workspace. Mover el repositorio de
directorio o de máquina no invalida nada.

### Decisión 3 — Orden semántico en `ImportEnvironment`

`ImportEnvironment.locations` es una tupla ordenada con orden
contractual: la primera location gana en resolución de imports. No hay
campo de prioridad separado. El orden es el contrato.

```python
@dataclass(slots=True, frozen=True)
class ImportEnvironment:
    """
    Locations en orden de precedencia. La primera entrada gana en
    resolución de imports. Equivalente a la posición en sys.path.

    Los productores (adapters, declaración del usuario) deben emitir
    locations en el orden semántico deseado.
    """
    locations: tuple[CodeLocation, ...]
```

Construcción del orden cuando hay múltiples fuentes:

1. Locations declaradas en `[workspace]` van primero, en su orden de
   declaración.
2. Locations contribuidas por adapters van después, ordenadas por
   prioridad de adapter (mayor primero) y dentro de cada adapter por
   su orden de emisión.
3. Deduplicación por `path`: gana la primera ocurrencia. Las posteriores
   se descartan y aparecen como `shadowed` en `provenance`.

Rationale:

- Un campo `priority` aparte del orden duplica significado y abre la
  posibilidad de inconsistencia.
- El orden como contrato hace que dos productores con mismo conjunto
  pero distinto orden produzcan estados detectablemente distintos, en
  vez de comportamientos divergentes con estados aparentemente iguales.
- Documentar shadowing en `provenance` evita que la deduplicación
  silenciosa sea sorpresa.

### Decisión 4 — Exclusividad vs composición de adapters

Exclusividad para roots, composición para locations:

- **Roots** (`workspace_root`, `knowledge_anchor`): los contribuye un
  único adapter "ganador" según prioridad. Si varios adapters proponen
  roots, gana el de mayor prioridad. Los demás quedan registrados en
  `provenance` como `ignored_by_higher_priority` con su `adapter_name`
  y los roots que habrían propuesto.
- **Locations**: composables. Múltiples adapters pueden contribuir
  locations. Se unen por concatenación en orden de prioridad
  (decisión 3), con dedup por path.

Política de conflicto: **dominant**, no strict. Un adapter de menor
prioridad que propone roots distintos no es error: queda registrado
como ignorado. La trazabilidad la garantiza `provenance`. La razón
para elegir `dominant` sobre `strict`:

- `strict` hace frágil la autoría de adapters: añadir un adapter nuevo
  puede romper proyectos existentes que combinan adapters preexistentes
  perfectamente coherentes hasta ese momento.
- `dominant` permite extensibilidad incremental sin riesgo de romper
  configuraciones que funcionan, y la auditabilidad se cubre con
  `provenance`.
- El caso de empate en la misma prioridad sí es error: si dos adapters
  compiten en el mismo nivel y proponen roots distintos, el sistema no
  puede elegir y exige al usuario declarar `[workspace]` explícitamente.

```python
@dataclass(slots=True, frozen=True)
class LayoutAdaptation:
    workspace_root: Path | None = None      # exclusivo
    knowledge_anchor: Path | None = None    # exclusivo
    code_locations: tuple[CodeLocation, ...] = ()   # composable
```

Resolución:

```python
def merge_adaptations(matches: tuple[LayoutMatch, ...]) -> LayoutAdaptation:
    sorted_matches = sorted(matches, key=lambda m: -m.priority)

    # Roots: ganador único por prioridad. Empate en máxima prioridad
    # con valores divergentes => error. Empate con valores idénticos =>
    # se tolera.
    root_winner = select_root_winner(sorted_matches)

    # Locations: union ordered, dedup preserving order, shadowing
    # registrado en provenance.
    all_locations = []
    for m in sorted_matches:
        all_locations.extend(m.adaptation.code_locations)
    deduped = dedup_preserving_order(all_locations)

    return LayoutAdaptation(
        workspace_root=root_winner.workspace_root if root_winner else None,
        knowledge_anchor=root_winner.knowledge_anchor if root_winner else None,
        code_locations=tuple(deduped),
    )
```

Rationale:

- Los roots tienen un valor verdadero por proyecto. La composición no
  tiene sentido semántico.
- Las locations son inherentemente aditivas. Negar composición fuerza
  adapters monolíticos que no escalan.
- La división evita la peor opción: merge model formal y completo para
  todo. Solo lo que necesita componer compone.

Consecuencia: los paquetes de adapters pueden ofrecer unidades pequeñas
y composables (`SrcLayoutAdapter`, `GeneratedDirectoryAdapter`,
`VendoredDirectoryAdapter`) sin que cada uno conozca a los demás.

### Decisión 5 — Autoridad sobre `ExecutionContext`

El `ExecutionContext` lo construye el `RuntimeProfile`, con overrides
explícitos de la CLI. Ningún adapter ni componente del workspace puede
tocarlo.

```python
def build_execution_context(
    workspace: EffectiveWorkspace,
    runtime_profile: RuntimeProfileSpec,
    *,
    cli_overrides: ExecutionOverrides | None = None,
    invocation_id: str,
) -> ExecutionContext:
    base = runtime_profile.default_execution_context(workspace)
    if cli_overrides is not None:
        base = apply_overrides(base, cli_overrides)
    return base
```

Cadena de autoridad:

1. **Default estructural**: si no hay runtime profile,
   `execution_root = workspace.workspace_root`,
   `knowledge_storage_root = workspace.workspace_root / ".cosecha"`.
   Vive en `cosecha-workspace`, no en runtime.
   `.cosecha` es, por tanto, el storage operativo por defecto consumido
   por shadow execution y preservacion de artefactos en ADR-0008 y
   ADR-0009.
2. **Runtime profile**: puede sustituir cualquiera de los dos. Es el
   lugar natural para que un perfil "supervisor" declare un shadow
   execution context apuntando a un directorio temporal.
3. **CLI overrides**: última palabra.
4. **Adapters**: explícitamente sin acceso. Trabajan sobre identidad
   (`EffectiveWorkspace`), no sobre operación.

Rationale:

- `RuntimeProfileSpec` ya existe en el manifest y es el lugar natural
  para declarar política de ejecución.
- La línea entre "qué es el proyecto" (workspace, adapters) y "cómo lo
  ejecuto esta vez" (profile, cli) queda nítida.
- Cualquier futuro mecanismo de redirección de ejecución entra como un
  runtime profile más, sin tocar el modelo.

Consecuencia: un componente que necesita `execution_root` no puede
tomarlo del workspace; tiene que recibir un `ExecutionContext`. Esto
fuerza la separación en las firmas y elimina lecturas accidentales del
lado equivocado.

### Decisión 6 — Scope contractual de `WorkspaceResolutionPolicy`

La estrategia de búsqueda y los parámetros estructurales del workspace
son una `WorkspaceResolutionPolicy` con scope de proceso. Se establece
una vez en el bootstrap del entry point y `discover_cosecha_manifest`
la lee desde allí. No es un parámetro que viaje por las firmas.

La policy se almacena en un `ContextVar`, no en una global mutable
clásica. Esto da el mismo contrato de proceso sin contaminación entre
tests, async tasks y servidores largos.

```python
from contextvars import ContextVar
from contextlib import contextmanager

@dataclass(slots=True, frozen=True)
class WorkspaceResolutionPolicy:
    search_strategy: ManifestSearchStrategy
    max_ancestor_distance: int = 1
    layout_adapters: tuple[LayoutAdapter, ...] = ()

_active_policy: ContextVar[WorkspaceResolutionPolicy] = ContextVar(
    "cosecha_workspace_resolution_policy",
    default=DEFAULT_POLICY,
)

def get_active_policy() -> WorkspaceResolutionPolicy:
    return _active_policy.get()

@contextmanager
def using_policy(policy: WorkspaceResolutionPolicy):
    token = _active_policy.set(policy)
    try:
        yield
    finally:
        _active_policy.reset(token)
```

Bootstrap por entry point:

- `cosecha` CLI: policy default + adapters cargados de entry points.
- `cosecha-lsp`: misma policy.
- `cosecha-mcp`: misma policy.
- Cualquier entry point futuro (extensiones de editor, harnesses
  externos, herramientas integradas): se adhiere a la misma policy del
  proceso.
- Tests: usan `using_policy()` para swap controlado.

Rationale:

- Si la estrategia viaja por firmas, antes o después dos consumidores
  pasan estrategias distintas y la herramienta se vuelve inconsistente.
- `ContextVar` evita los problemas clásicos de globals mutables: aísla
  por contexto async, no se contamina entre tests, no requiere locks.
- La policy de proceso garantiza que todos los consumidores ven el
  mismo proyecto, siempre.

Consecuencia: `discover_cosecha_manifest` no toma `strategy` como
parámetro. Lo lee de la policy activa. Esto simplifica las firmas de
todos los consumidores actuales y futuros.

## Sub-decisiones diferidas

No son del modelo base. Son detalles de implementación que se cierran
cuando lleguen, no antes:

1. **Default de `knowledge_storage_root`**: por defecto
   `workspace_root / ".cosecha"`. Trade-off futuro entre obviedad y
   limpieza del repo (p. ej. migrar a `$XDG_CACHE_HOME/cosecha/<project_id>`).
   Decisión cuando haya feedback real.
2. **Política de validación de `knowledge_storage_root`**: ¿debe
   existir? ¿debe ser escribible? ¿se crea on-demand? Probablemente
   on-demand con error explícito si falla, pero se decide al
   implementar la KB portable.
3. **Naming**: `LayoutAdapter` vs `WorkspaceLayoutAdapter` vs
   `WorkspaceAdapter`. Pendiente de bikeshed durante PR review.
4. **Versionado del paquete `cosecha-workspace`**: ¿semver
   independiente o lockstep con `cosecha-core`? Diferido hasta tener al
   menos un consumer externo del paquete.
5. **Nombre del entry point group** para layout adapters. Pendiente de
   cruce con la convención del resto del proyecto.
6. **Comportamiento ante `manifest_file` explícito vs policy**: si se
   pasa `--manifest-file foo/cosecha.toml`, ¿la policy sigue
   aplicándose para validar `max_ancestor_distance`? Inclinación: sí,
   pero diferido hasta tener tests.

## Plan de ejecución

Tres etapas. Las dos primeras son compromiso firme. La tercera es
condicional.

### Etapa A — Unificación de discovery (refactor puro)

- Crear `cosecha-workspace` como paquete con `discover_cosecha_manifest`
  mínima y `WorkspaceResolutionPolicy` cuyo default reproduce el
  comportamiento actual byte a byte.
- Migrar todos los entry points y consumidores actuales a leer de la
  policy: core, runtime, pytest engine, MCP, LSP, shell, y cualquier
  otro punto que hoy reimplemente discovery.
- Sin tipos nuevos del modelo en esta etapa.
- Tests existentes intactos.

### Etapa B — Modelo y resolución

- Añadir a `cosecha-workspace`: `EffectiveWorkspace`, `ExecutionContext`,
  `WorkspaceDeclaration`, `LayoutAdapter`, `LayoutMatch`,
  `LayoutAdaptation`, `ImportEnvironment`, `CodeLocation`,
  `WorkspaceProvenance`.
- Crear `cosecha-workspace-python` con adapters para layouts Python.
  Registrarlos vía entry points.
- Implementar `resolve_workspace`, `build_execution_context`,
  validación de `max_ancestor_distance`, fingerprint semántico.
- Refactorizar consumidores para recibir `EffectiveWorkspace` o
  `ExecutionContext` según corresponda. Eliminar las funciones de
  inferencia heurística que vivían en runtime y utils.
- `cosecha config explain` muestra `EffectiveWorkspace` con
  `provenance` y, si aplica, `ExecutionContext` activo con su
  procedencia.
- Comportamiento default reproduce el actual; tests existentes verdes
  sin tocarse.

### Etapa C — Endurecimiento (condicional)

Solo se aborda si:

- aparecen proyectos reales con layouts no estándar que fuercen a
  declarar `[workspace]` explícitamente,
- o un futuro mecanismo de supervisión de la ejecución entra en
  implementación y necesita invariantes más fuertes,
- o se descubre que la inferencia tiene casos límite imposibles de
  mantener.

Sin fecha. Sin compromiso.

## Consecuencias

Positivas:

- El modelo separa identidad (workspace) de operación (execution
  context). Mecanismos futuros de redirección de ejecución se
  construyen encima sin tocarlo.
- La KB es portable por diseño, no por accidente.
- La estrategia de búsqueda es contrato de proceso, no parámetro
  errante.
- Los adapters son composables donde tiene sentido y exclusivos donde
  no, sin merge model genérico.
- Ningún componente fuera de los paquetes de adapters específicos
  conoce nombres de directorio convencionales (`src`, `tests`,
  `packages`).

Negativas / costes:

- Aparece un paquete nuevo (`cosecha-workspace`) y otro upstream
  (`cosecha-workspace-python`). Trabajo de packaging y de actualizar
  dependencias en `pyproject.toml` de varios paquetes.
- La separación `EffectiveWorkspace` / `ExecutionContext` significa que
  muchas firmas que hoy reciben "el contexto" pasan a recibir uno u
  otro explícitamente. Refactorización amplia pero mecánica.
- La policy de proceso introduce un `ContextVar` activo. Hay que ser
  disciplinado con `using_policy()` en tests.

Neutrales:

- El usuario final no nota nada. Los proyectos existentes siguen
  funcionando sin tocar configuración.
