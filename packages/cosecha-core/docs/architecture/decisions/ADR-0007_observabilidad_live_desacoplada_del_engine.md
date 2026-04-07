# ADR-0007: Observabilidad live desacoplada del engine

## Estado

Accepted

## Contexto

El estado actual de la observabilidad live no está cerrado como contrato
de engine. `Runner` construye snapshots ricos de Pytest mediante una
función hardcodeada, mientras que Gherkin emite sus propios
`EngineSnapshotUpdatedEvent` desde dentro del engine. El resultado es un
modelo híbrido en el que el core conoce detalles de Pytest, pero no los
de otros motores, y en el que cada engine nuevo que quiera publicar
telemetría rica necesita negociar una excepción.

Ese acoplamiento tiene tres costes concretos:

- el core deja de ser agnóstico respecto al dominio de cada engine,
- Pytest y Gherkin siguen caminos distintos para un problema equivalente,
- cualquier engine futuro queda bloqueado o forzado a introducir lógica
  específica en `Runner`.

Este ADR fija una frontera estable: el core controla el ciclo de vida de
los snapshots live por fase, pero el contenido de esos snapshots es
responsabilidad del engine.

## Modelo

La observabilidad live de engine se divide en dos canales con
responsabilidad distinta.

**Canal estándar por fase**. `Runner` emite snapshots live en puntos
normativos del ciclo de ejecución (`setup`, `call`, `teardown`). El
payload lo produce el engine mediante un hook estándar.

**Canal adicional del engine**. Un engine puede emitir eventos live
propios fuera de esos puntos normativos cuando necesita granularidad más
fina, por ejemplo steps de Gherkin, hooks internos o estados
intermedios.

El core conserva la propiedad del *cuándo* y del *transporte* de los
snapshots estándar. El engine conserva la propiedad del *qué*.

## Decisiones

### Decisión 1 — Hook estándar de snapshot por fase

`Engine` expone el hook:

```python
def build_live_snapshot_payload(
    self,
    node: TestExecutionNode,
    phase: str,
) -> dict[str, object] | None:
    return None
```

El default es `None`. Un engine que no necesite snapshots por fase no
está obligado a publicar payloads.

### Decisión 2 — `Runner` deja de construir payloads específicos

`Runner` no conoce ya fixtures de Pytest, steps de Gherkin ni ninguna
otra estructura propia de engine. Su única responsabilidad es invocar el
hook y, si devuelve payload, emitir el evento live correspondiente.

Consecuencia: cualquier helper hardcodeado de engine dentro de `Runner`
debe desaparecer.

### Decisión 3 — El core mantiene la emisión del evento estándar

Los snapshots por fase siguen viajando como `EngineSnapshotUpdatedEvent`.
No se introduce un tipo de evento nuevo ni un modelo alternativo de
persistencia.

La KB, MCP, LSP, artifacts y queries siguen consumiendo el mismo evento
tipado con el mismo payload opaco.

### Decisión 4 — `snapshot_kind` estándar

El `snapshot_kind` de los snapshots por fase emitidos por `Runner` pasa
a ser `engine_runtime`.

Rationale:

- evita exponer un `snapshot_kind` distinto por engine cuando el contrato
  del canal es común,
- permite a los consumidores identificar de forma uniforme el snapshot
  estándar de runtime,
- deja libres los `snapshot_kind` específicos de engine para telemetría
  adicional.

### Decisión 5 — Los engines pueden emitir snapshots adicionales

Los engines pueden seguir emitiendo `EngineSnapshotUpdatedEvent`
adicionales con `snapshot_kind` propio. Este canal no se elimina ni se
subordina al hook estándar.

Ejemplo normativo: Gherkin mantiene `gherkin_execution` para snapshots
de step, porque `setup/call/teardown` no expresan el avance interno del
escenario.

### Decisión 6 — Fases normativas del canal estándar

El parámetro `phase` del hook queda normado a:

- `setup`
- `call`
- `teardown`

No se añaden fases libres por engine en el canal estándar. La
granularidad adicional se resuelve con eventos propios del engine.

### Decisión 7 — Opacidad del payload

`payload` sigue siendo un `dict[str, object]` opaco para el resto del
sistema. La KB y los consumers persistentes no validan semánticamente su
estructura más allá de serializarlo y devolverlo.

Consecuencia: la evolución del payload de un engine no exige migración
de schema siempre que siga siendo serializable.

## Consecuencias

`PytestEngine` pasa a implementar el hook estándar y reproduce el
payload que hoy construye el core.

`GherkinEngine` implementa el hook estándar con un snapshot mínimo por
fase, y conserva sus eventos `gherkin_execution` para observabilidad de
steps.

El sistema queda abierto a engines nuevos sin volver a introducir lógica
específica en `Runner`.
