# ADR-0009: Transporte del shadow y capability de escritura efímera

## Estado

Accepted (decisiones 1–9) · Deferred (sub-decisiones de implementación)

## Contexto

ADR-0008 estableció el `ShadowExecutionContext` como unidad de aislamiento efímero por sesión y fijó su ubicación, sus rutas derivadas y su política de cleanup. Esa norma ya está operativa en el launcher de coverage y en `ProcessRuntimeProvider`, pero no resuelve todavía:

- ¿cómo llega el shadow a componentes que no son runtime provider ni launcher, concretamente providers de recursos (SSL, MongoDB) y engines (Gherkin, pytest runtime adapter)?
- ¿qué contrato define qué componentes están autorizados a escribir artefactos efímeros, y qué ocurre si un componente escribe sin declararlo?

Sin ese contrato, varios componentes siguen escribiendo fuera del shadow: SSL en `tempfile.mkdtemp` o `.cosecha/runtime/ssl`, MongoDB en tempdirs aleatorios, Gherkin mediante `TempPathManager` y el adapter de pytest en directorios ad-hoc. Todos son artefactos de sesión que escapan a la auditoría y al cleanup unificado de ADR-0008.

Este ADR cierra el transporte del shadow hacia cualquier componente que lo necesite, define la capability que lo autoriza, y establece el invariante que garantiza que ninguna escritura efímera escape al shadow sin dejar rastro.

## Modelo

La producción de artefactos efímeros es una capacidad explícita del componente, no una libertad implícita. El modelo se articula en cuatro piezas.

**El shadow como servicio de proceso**. El shadow activo de una invocación es accesible desde cualquier componente mediante un accessor tipado. No viaja por las firmas; se resuelve desde un `ContextVar` establecido por el bootstrap. Esto es coherente con la política de resolución de workspace (ADR-0006 decisión 6) y evita contaminar APIs que no producen artefactos efímeros.

**La capability como contrato**. Un componente que necesite escribir artefactos efímeros declara la capability `produces_ephemeral_artifacts` en su matriz (ADR-0004). Sin esa declaración, el accessor falla. La capability condiciona el acceso en runtime; no es solo documentación.

**El namespace como aislamiento**. Cada componente recibe un subdirectorio propio dentro del shadow, derivado de su identidad estable. No hay acceso directo a `shadow.root_path`; solo a los namespaces derivados del `component_id` a través de `ShadowHandle`. Esto evita colisiones y permite cleanup granular.

**La dualidad efímero/persistente**. Un componente puede producir tanto artefactos efímeros como persistentes. La capability distingue ambos casos y enruta cada tipo a su destino correcto: efímero al shadow, persistente al `knowledge_storage_root`.

## Conceptos del dominio

| Concepto                         | Responsabilidad                                        |
| -------------------------------- | ------------------------------------------------------ |
| `ShadowHandle`                    | Accessor tipado que entrega a un componente sus namespaces efímero y persistente |
| `EphemeralArtifactCapability`     | Declaración en la capability matrix del componente que autoriza acceso al shadow |
| `active_ephemeral_capability_registry` | Snapshot inmutable de las capabilities efímeras autorizadas para una invocación concreta, publicado por el bootstrap |
| `component_id`                    | Identificador estable y declarativo del componente (independiente del nombre del paquete) |
| `session_id`                      | Identificador operativo con el que se materializa `ShadowExecutionContext`; en el runtime actual lo provee el runtime bootstrap |
| `ephemeral_namespace`             | Se resuelve por dominio: `instrumentation` y `runtime` reutilizan sus raices dedicadas; cualquier otro dominio usa `shadow.root_path / <ephemeral_domain> / <component_id>` |
| `persistent_namespace`            | `knowledge_storage_root / "components" / <component_id>` |
| `preserved_artifacts_namespace`   | `knowledge_storage_root / "preserved_artifacts" / <session_id> / <component_id>` |

## Decisiones

### Decisión 1 — Transporte mediante `ContextVar` de invocación

El shadow activo de una invocación y el registro activo de capabilities efímeras se publican en `ContextVar` en el bootstrap del runtime, y se consumen mediante accessors públicos del paquete `cosecha-core`.

```python
from collections.abc import Mapping
from contextvars import ContextVar
from contextlib import contextmanager
from types import MappingProxyType

_active_shadow: ContextVar[ShadowExecutionContext | None] = ContextVar(
    "cosecha_active_shadow",
    default=None,
)
_active_ephemeral_capabilities: ContextVar[
    Mapping[str, EphemeralArtifactCapability] | None
] = ContextVar(
    "cosecha_active_ephemeral_capabilities",
    default=None,
)

def get_active_shadow() -> ShadowExecutionContext:
    shadow = _active_shadow.get()
    if shadow is None:
        raise ShadowNotBoundError(
            "No active ShadowExecutionContext. This component is being "
            "invoked outside the runtime bootstrap."
        )
    return shadow

def get_active_ephemeral_capabilities() -> Mapping[str, EphemeralArtifactCapability]:
    capabilities = _active_ephemeral_capabilities.get()
    if capabilities is None:
        raise ShadowCapabilityRegistryNotBoundError(
            "No active ephemeral capability registry. This component is being "
            "invoked outside the runtime bootstrap."
        )
    return capabilities

@contextmanager
def binding_shadow(
    shadow: ShadowExecutionContext,
    *,
    ephemeral_capabilities: Mapping[str, EphemeralArtifactCapability],
):
    shadow_token = _active_shadow.set(shadow)
    capability_token = _active_ephemeral_capabilities.set(
        MappingProxyType(dict(ephemeral_capabilities)),
    )
    try:
        yield
    finally:
        _active_ephemeral_capabilities.reset(capability_token)
        _active_shadow.reset(shadow_token)
```

`get_active_ephemeral_capabilities()` falla con `ShadowCapabilityRegistryNotBoundError` cuando el componente intenta acceder fuera de un bootstrap que haya publicado un registro activo de capabilities efímeras. `binding_shadow()` publica ese registro envuelto en `types.MappingProxyType` para garantizar inmutabilidad real del snapshot durante toda la invocación.

Bootstrap obligatorio: `ProcessRuntimeProvider`, el launcher de
coverage y cualquier otro entry point que inicie una invocacion deben
envolver su ejecucion con
`binding_shadow(shadow, ephemeral_capabilities=...)` despues de
resolver el shadow y materializar la capability matrix activa para esa
invocacion. Cuando el bootstrap cree workers o subprocesses, debe
transportar tambien el subconjunto exacto de
`ephemeral_capabilities` concedidas a esa ejecucion concreta. El
proceso hijo las rebindea localmente; no las reconstruye desde env vars
ni desde el filesystem.

Cuando el bootstrap lance subprocesses de Cosecha o procesos anidados, debe limpiar primero las variables de entorno heredadas de Cosecha y después inyectar el shadow actual. El hijo nunca debe reutilizar por accidente el shadow del padre.

Rationale:
- `ContextVar` es coherente con ADR-0006 y con mecanismos ya existentes en el sistema.
- Evita contaminar firmas de componentes que no producen artefactos efímeros.

Consecuencia: ningún componente puede leer el shadow del filesystem ni reconstruir la autorización desde env vars. El acceso pasa por `get_active_shadow()`, `get_active_ephemeral_capabilities()` o por la API de conveniencia, y en todos los casos se valida la autorización.

### Decisión 2 — Capability `produces_ephemeral_artifacts`

Un componente que necesite escribir artefactos efímeros declara en su capability matrix la capacidad `produces_ephemeral_artifacts` con información estructurada:

```python
@dataclass(slots=True, frozen=True)
class EphemeralArtifactCapability:
    component_id: str                    # identidad estable (ej. "cosecha.provider.ssl")
    ephemeral_domain: str                # dominio efimero; `instrumentation` y `runtime` son los built-ins actuales
    produces_persistent: bool = False    # ¿también produce persistentes?
    cleanup_on_success: bool = True      # default: limpiar en éxito
    preserve_on_failure: bool = True     # default: preservar en fallo
    description: str = ""                # para diagnósticos
```

Los defaults `(cleanup_on_success=True, preserve_on_failure=True)` reproducen exactamente la política de ADR-0008 a nivel de namespace. Los flags pertenecen al tipo de componente, no a una sesión concreta; no se overridean por invocación. Escenarios como "en CI limpiar siempre" se resuelven fuera del ciclo del shadow, no con flags dinámicos.

### Decisión 3 — `ShadowHandle`: accessor tipado por componente

Los componentes no acceden a `ShadowExecutionContext` directamente. Acceden a un `ShadowHandle` tipado que el core entrega según la capability declarada.

```python
@dataclass(slots=True, frozen=True)
class ShadowHandle:
    component_id: str
    ephemeral_root: Path
    persistent_root: Path | None

    def ephemeral_dir(self, *parts: str) -> Path:
        target = self.ephemeral_root.joinpath(*parts)
        target.mkdir(parents=True, exist_ok=True)
        return target

    def ephemeral_file(self, name: str) -> Path:
        self.ephemeral_root.mkdir(parents=True, exist_ok=True)
        return self.ephemeral_root / name

    def persistent_dir(self, *parts: str) -> Path:
        if self.persistent_root is None:
            raise PersistentArtifactsNotEnabledError(self.component_id)
        target = self.persistent_root.joinpath(*parts)
        target.mkdir(parents=True, exist_ok=True)
        return target

    def persistent_file(self, name: str) -> Path:
        if self.persistent_root is None:
            raise PersistentArtifactsNotEnabledError(self.component_id)
        self.persistent_root.mkdir(parents=True, exist_ok=True)
        return self.persistent_root / name
```

`persistent_dir()` y `persistent_file()` fallan con `PersistentArtifactsNotEnabledError` cuando la capability del componente no autoriza persistencia.

La API pública de bajo nivel para obtener este handle es explícita:

```python
def acquire_shadow_handle(component_id: str) -> ShadowHandle:
    """Lee el shadow y la capability activa del componente, los valida y construye el handle."""
```

`acquire_shadow_handle(component_id)` lee el shadow activo y el registro activo de capabilities efímeras desde los `ContextVar` de la Decisión 1, valida que `component_id` tenga `EphemeralArtifactCapability` declarada para la invocación actual y construye el `ShadowHandle`.

Rutas de fallo normativas:

- `ShadowNotBoundError` si no hay shadow activo.
- `ShadowCapabilityRegistryNotBoundError` si no hay registro activo de capabilities efímeras.
- `EphemeralCapabilityNotGrantedError` si `component_id` no figura en el registro activo de la invocación.

Esta función es la base no mágica del modelo. El sugar de la Decisión 9 solo resuelve el `component_id` del caller y delega en ella. Debe existir para código que no pueda o no deba depender de frame inspection.

### Decisión 4 — Namespacing por `component_id` declarativo

Cada componente escribe bajo un subdirectorio dedicado derivado determinísticamente de su `component_id`. `component_id` es un string declarativo estable (ej: `"cosecha.provider.ssl"`) definido por el componente. **No se deriva del nombre del paquete Python**. Renombrar el paquete no cambia el `component_id`, así que el acceso a datos persistentes sobrevive a refactorizaciones.

La fuente de verdad es la constante `COSECHA_COMPONENT_ID` declarada en el paquete del componente. El campo `EphemeralArtifactCapability.component_id` repite ese valor para hacerlo visible en la capability matrix.

Hay dos registros distintos:

- El **registro de discovery** de ADR-0004 contiene todas las capabilities declaradas por los paquetes descubiertos. La validación de igualdad exacta `COSECHA_COMPONENT_ID` ↔ `EphemeralArtifactCapability.component_id` ocurre aquí, una vez al cargar el sistema. Una discrepancia es un error de configuración y debe fallar en registro o bootstrap.
- El **registro activo por invocación** de la Decisión 1 contiene solo el subconjunto ya validado y autorizado para la invocación actual. `acquire_shadow_handle()` consulta únicamente este registro activo; nunca reejecuta la validación de discovery.

El layout normativo queda fijado asi:

- Namespace efimero de instrumentacion:
  `shadow.instrumentation_dir / <component_id>`.
- Namespace efimero de runtime/recursos:
  `shadow.runtime_state_dir / <component_id>`.
- Namespace efimero de otros dominios:
  `shadow.root_path / <ephemeral_domain> / <component_id>`.
- Namespace persistente: `knowledge_storage_root / "components" / <component_id>`.
- Área de preservación por sesión: `knowledge_storage_root / "preserved_artifacts" / <session_id> / <component_id>`.

Compatibilidad con ADR-0008:

- ADR-0008 Decision 2 sigue vigente para `coverage_dir` mientras la
  sub-decision diferida sobre migracion de coverage no se cierre.
  Durante la transicion, `shadow.coverage_dir` y
  `shadow.instrumentation_dir / "cosecha.instrumentation.coverage"`
  pueden coexistir como alias.
- `metadata_file` sigue siendo un artefacto normativo del bootstrap y permanece en `shadow.instrumentation_dir / "run-metadata.json"`. No se considera un namespace de componente.

### Decisión 5 — Dualidad efímero/persistente

Un componente enruta sus escrituras así: material ad-hoc de sesión a `handle.ephemeral_dir()` y material que el usuario quiere preservar entre sesiones a `handle.persistent_dir()`.
Si la capability declara `produces_persistent = False`, `persistent_root` es `None` y `handle.persistent_dir()` falla con un error semántico explícito.
Regla normativa: un componente nunca escribe persistentes bajo el shadow ni efímeros bajo `knowledge_storage_root`, salvo cuando una política de preservación de sesión los mueve a `preserved_artifacts`.

`knowledge_storage_root` debe viajar de forma explicita con el
`ShadowExecutionContext` o con el bootstrap. No se deriva por subir
directorios desde `shadow.root_path`.

### Decisión 6 — Cleanup por namespace

La política de cleanup de ADR-0008 se mantiene para el shadow completo, pero este ADR añade control granular por namespace mediante `cleanup_on_success` y `preserve_on_failure`. El comportamiento normativo por componente queda así:

- Éxito + `cleanup_on_success = True`: el namespace efímero se elimina junto con el shadow de la sesión.
- Éxito + `cleanup_on_success = False`: el namespace efímero se mueve a `knowledge_storage_root / "preserved_artifacts" / <session_id> / <component_id>` antes de borrar el shadow.
- Fallo + `preserve_on_failure = True`: el namespace efímero permanece dentro del shadow preservado por ADR-0008.
- Fallo + `preserve_on_failure = False`: el namespace efímero se elimina antes de exponer el shadow preservado al operador.

Los flags no aplicables al resultado de la sesión no alteran ese branch. Este ADR no redefine el lifecycle global del shadow; solo fija qué ocurre con cada namespace del componente dentro de él.

### Decisión 7 — Invariante de verificación

El cumplimiento se verifica con un test de invariante en la suite del
core. El test materializa un workspace limpio, ejecuta una invocacion
minima y verifica por diferencia que no aparecen archivos ni
directorios nuevos fuera de los destinos declarados
(`knowledge_storage_root`, `shadow.root_path` u outputs CLI
explicitos). Dentro de `shadow.root_path`, el test debe reconocer como
destinos bootstrap declarados `metadata_file` y, mientras dure la
transicion, `coverage_dir`. Para componentes preservados, el test debe
verificar tambien que el namespace preservado cae exactamente bajo
`preserved_artifacts/<session_id>/<component_id>`. Cualquier escritura
fuera de esos destinos rompe CI.

Cuando un componente necesite inputs no triviales para activarse
minimamente en este leak test, el propio componente debe aportar un
fixture o harness minimo de activacion. El test de invariante no
autoriza saltarse componentes declarados; autoriza encapsular su
bootstrap minimo.

### Decisión 8 — Modo Detach para Tests y Scripts (`use_detached_shadow`)

Para permitir el uso de componentes fuera del bootstrap normal (tests unitarios, utilidades rápidas), se expone una API pública `use_detached_shadow()` como context manager.

```python
@contextmanager
def use_detached_shadow(
    *,
    granted_capabilities: tuple[EphemeralArtifactCapability, ...] = (),
):
    # Crea un shadow efímero en tempfile.mkdtemp(), materializa un registro
    # in-memory con las capabilities concedidas y lo bindea al ContextVar
    ...
```

Este modo ofrece garantías degradadas: no hay invariante de verificación, no hay integración con el storage del workspace y el cleanup ocurre al salir del context manager. El tempdir no sobrevive al bloque `with`.

Este modo no desactiva la validación de capabilities. El llamador debe conceder explícitamente las `EphemeralArtifactCapability` permitidas para ese bloque mediante `granted_capabilities`; el context manager construye con ellas un registro efímero in-memory keyed por `component_id` y delega en `binding_shadow()` para publicar ese registro y el shadow en los `ContextVar`. Si `granted_capabilities` contiene múltiples entradas con el mismo `component_id`, `use_detached_shadow()` falla con `DuplicateCapabilityGrantError` antes de publicar el registro. Un componente sin grant explícito falla igual que en producción.

Su uso debe ser explícito.

### Decisión 9 — API de conveniencia (Syntactic Sugar)

Para minimizar la fricción frente a `tempfile`, el core expone funciones de acceso directo que infieren la identidad del componente:

```python
# En el componente:
from cosecha.shadow import ephemeral_file

path = ephemeral_file("ca.pem")
```

`ephemeral_file` e `ephemeral_dir` inspeccionan el frame del caller en su primera invocación desde cada módulo, localizan `COSECHA_COMPONENT_ID`, cachean el resultado en un registro interno `{module_name: component_id}` y reutilizan ese valor en llamadas posteriores. La resolución no ocurre en el `import` de `cosecha.shadow`, sino en la primera llamada efectiva desde el módulo consumidor.

La API de conveniencia delega siempre en `acquire_shadow_handle(component_id)`. Si el módulo no define `COSECHA_COMPONENT_ID` o la capability no está declarada para ese `component_id`, la primera invocación falla con un error semántico claro.

La inferencia por frame es azúcar de conveniencia, no el contrato base. Si un componente usa wrappers, decorators o helpers genéricos que puedan oscurecer el módulo caller, debe usar `acquire_shadow_handle()` explícitamente en lugar del sugar.

El cache `{module_name: component_id}` es una optimizacion local al
proceso, sin TTL ni invalidacion explicita. La expectativa contractual
es que `COSECHA_COMPONENT_ID` no cambie durante la vida del proceso. Los
escenarios de `importlib.reload`, generacion dinamica de modulos o
reescritura del caller quedan fuera del caso nominal y deben usar la API
explicita en lugar del sugar.

## Consecuencias

Positivas:
- Cierra operativamente ADR-0008: ya no hay componentes que escriban fuera del shadow.
- Convierte el workspace limpio en un invariante verificado automáticamente.
- El `component_id` estable protege los datos persistentes frente a refactorizaciones.

Negativas / costes:
- El test de invariante es de integración y más lento que los tests unitarios convencionales.
- El sugar paga un coste de frame inspection en la primera llamada de cada módulo y puede hacer menos obvio el origen del error en algunos tracebacks.
- El sugar puede atribuir peor el `component_id` cuando el acceso a disco se encapsula detrás de wrappers o decorators genéricos; en esos casos hay que usar la API explícita.
- La migración afecta a varios paquetes y es trabajo mecánico amplio, aunque simple conceptualmente.
- `use_detached_shadow()` introduce un modo degradado que puede usarse indebidamente para esquivar el bootstrap real si no se limita a tests y utilidades controladas.
- Los namespaces preservados con `cleanup_on_success = False` pueden acumularse en `knowledge_storage_root` hasta que exista una política de GC explícita.
- Requiere disciplina en la declaración y validación de `COSECHA_COMPONENT_ID` en cada paquete que escriba a disco.

Nota sobre persistencia: los artefactos persistentes de un componente están ligados a la identidad semántica del workspace (ADR-0006). Si el fingerprint del workspace cambia, estos artefactos pueden quedar huérfanos.

Nota sobre enforcement: el `ShadowHandle` y el test de invariante ofrecen protección contractual y de CI a nivel de aplicación. No sustituyen un sandboxing de sistema operativo ni impiden escrituras arbitrarias fuera del shadow por parte de código malicioso.

## Sub-decisiones diferidas

- Plugins externos sin capability declarada: hoy fallan si intentan usar `acquire_shadow_handle()` o el sugar. Queda diferido si el host ofrecerá adapters o registro externo para terceros.
- Migración del namespace `coverage`: durante PR 2 puede mantenerse `shadow.coverage_dir` como alias transitorio hacia el namespace del componente que migre coverage. Su permanencia o eliminación se decide al cerrar la migración.
- Política respecto a `tempfile` del código bajo test: este ADR no prohíbe que el código del usuario use `tempfile`; la restricción aplica a componentes y bootstrap de Cosecha. Queda diferido si el leak test incorporará exclusiones o diagnósticos específicos para artefactos del user code.
- Política de GC para `preserved_artifacts`: este ADR fija cómo se preservan namespaces, pero no define TTL, cuotas ni comando de purga. Queda diferido si la limpieza se resuelve con `cosecha gc`, TTL automático o tooling equivalente.

## Plan de ejecución

Precondición: el registro de discovery descrito en ADR-0004 debe estar disponible. Si no lo está, el PR 1 incluye su implementación mínima para `EphemeralArtifactCapability` (discovery desde los paquetes instalados y validación `COSECHA_COMPONENT_ID` ↔ `capability.component_id`).

### PR 1 — Infraestructura y Sugar
- `ContextVar` para shadow activo y registro activo de capabilities efímeras, `ShadowHandle`, `EphemeralArtifactCapability`.
- `acquire_shadow_handle(component_id)` como API base pública.
- `use_detached_shadow(granted_capabilities=...)` y API de conveniencia (`ephemeral_file`, etc.).
- Bootstrap en `ProcessRuntimeProvider` y launcher.
- Limpiar variables de entorno heredadas de Cosecha antes de inyectar el shadow nuevo en subprocesses y nested runs.

### PR 2 — Migración de Base
- Migrar `coverage` y `runtime state` al uso del handle/sugar.
- Eliminar el fallback a `.cosecha/runtime` en `runtime_worker.py` para que `shadow.runtime_state_dir` sea la única autoridad.

### PR 3 — Migración de Providers y Engines
- SSL (persistente + efímero), MongoDB, Gherkin (TempPathManager).

### PR 4 — Test de Invariante
- Implementar el leak test del filesystem.
- Debe ejecutarse con cada `EphemeralArtifactCapability` declarada en el registro de discovery del monorepo, activando una configuración mínima por componente para asegurar cobertura total del catálogo.
- La serie no se considera cerrada ni mergeable mientras este test no pase con los componentes migrados.
