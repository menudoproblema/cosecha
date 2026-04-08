# ADR-0013: Taxonomia de identidades semanticas y operativas

## Estado

Accepted

## Contexto

La arquitectura de Cosecha usa varias identidades distintas, cada una
con ownership y estabilidad diferentes. Antes de este ADR, esas
identidades aparecian repartidas entre ADR-0003, ADR-0006, ADR-0008 y
ADR-0009 sin una taxonomia unificada.

## Decision

El sistema distingue cinco familias de identidad:

- identidad semantica de nodo: `stable_id`
- identidad semantica de workspace: `workspace_fingerprint`
- identidad operativa de invocacion: `invocation_id`
- identidad operativa de sesion de runtime: `session_id`
- identidad declarativa de componente: `component_id`

Ninguna de estas identidades sustituye a otra. Cada una responde a una
pregunta distinta del sistema.

## Taxonomia

### `stable_id`

Responde: "¿que nodo semantico del plan es este?"

Propiedades:

- estable entre sesiones mientras no cambie el ancla semantica del test;
- apto para correlacion entre planning, scheduling, reporting y KB;
- no representa intentos concretos ni workers concretos.

Algoritmo vigente:

- `test_path_label` relativo a `root_path` cuando es posible;
- ancla estable derivada del test;
- digest `sha256(anchor)[:12]`;
- formato final: `<engine_name>:<test_path_label>:<digest>`.

La ancla vigente se construye asi:

- para entidades con localizacion semantica (`feature`, `scenario`,
  `example`), se usan linea, columna y row anchor;
- si no existe esa localizacion, se cae a
  `test.__class__.__name__`, path y nombre visible del test.

Consecuencia: reordenaciones cosmeticas fuera de esa ancla no cambian
necesariamente el `stable_id`, pero cambios en la identidad semantica
del caso si pueden cambiarlo.

### `workspace_fingerprint`

Responde: "¿sigue siendo semanticamente el mismo workspace?"

Propiedades:

- definido por ADR-0006;
- sensible a la semantica del workspace, no al path absoluto;
- invalida planes y artefactos de conocimiento cuando cambia.

### `invocation_id`

Responde: "¿que invocacion operativa abrio este controller?"

Propiedades:

- pertenece a `ExecutionContext`;
- identifica una llamada concreta al sistema desde launcher, CLI o host;
- puede existir aunque no llegue a arrancar un runtime persistente.

### `session_id`

Responde: "¿que sesion de runtime/shadow esta viva?"

Propiedades:

- identifica la sesion operativa que materializa `ShadowExecutionContext`
  y workers asociados;
- la provee el bootstrap del runtime actual;
- puede coincidir con `invocation_id`, pero no esta obligado a ello.

### `component_id`

Responde: "¿quien es el dueño declarativo de estos namespaces o
capabilities?"

Propiedades:

- estable y declarativo;
- no depende del nombre del paquete Python;
- gobierna namespaces, grants y artefactos por componente.

## Relaciones jerarquicas

- un `workspace_fingerprint` puede agrupar muchas `invocation_id`;
- una `invocation_id` puede materializar una `session_id`;
- una `session_id` puede ejecutar muchos nodos con distintos
  `stable_id`;
- una `session_id` puede alojar muchos `component_id` activos;
- un mismo `component_id` puede participar en muchas sesiones.

## Reglas normativas

- no se usa `session_id` como sustituto de `stable_id`;
- no se usa `component_id` como sustituto de identidad operativa;
- la persistencia semantica en KB se ancla a `workspace_fingerprint` y a
  `stable_id`, no a ids operativos;
- los artefactos efimeros y namespaces se anclan a `session_id` y
  `component_id`.

## Consecuencias

Positivas:

- desaparece la confusion entre identidad del test, del workspace, de la
  invocacion y del storage efimero;
- se facilita la trazabilidad en runtimes remotos o distribuidos;
- la arquitectura queda lista para cloud runners y clusters sin
  renegociar ids.

Costes:

- obliga a nombrar mejor cada correlacion y cada indice persistido;
- incrementa el numero de ids visibles en eventos y diagnosticos.

## Relacion con ADRs anteriores

- Extiende ADR-0003.
- Se apoya en ADR-0006 para `workspace_fingerprint` e `invocation_id`.
- Se apoya en ADR-0008 y ADR-0009 para `session_id` y `component_id`.
