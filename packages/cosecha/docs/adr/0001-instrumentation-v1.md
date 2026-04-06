# ADR 0001: Instrumentación v1 en Cosecha

## Estado

Aceptado.

## Contexto

La v1 de coverage ya resolvió el problema real: medir desde el arranque
del proceso sin dejar la cobertura partida entre launcher y plugin del
runner. Al mismo tiempo apareció una tentación clara: convertir esta
solución en un framework genérico de instrumentación antes de tener un
segundo caso real.

Este ADR fija por qué no se ha hecho eso y qué señales concretas deben
aparecer antes de abrir más arquitectura.

## Decisiones

### `ExecutionInstrumenter` es interno

El protocolo vive dentro de `cosecha-core` como contrato interno entre la
shell y los instrumenters actuales.

No se publica como API estable porque:

- hoy solo existe un instrumenter real,
- no hay evidencia todavía de qué partes del contrato son generales y
  cuáles son específicas de coverage,
- publicar ahora el protocolo haría más caro cambiarlo cuando llegue el
  segundo caso real.

### No hay discovery por entry points

La shell importa coverage de forma condicional cuando el usuario activa
`--cov`.

No existe todavía un group `cosecha.instrumentation` porque:

- el import hardcoded cuesta cero en el caso común,
- con un solo instrumenter no aporta modularidad real,
- un mecanismo de discovery sería infraestructura añadida sin presión de
  producto.

### No existe supervisor

No se introduce un `ExecutionSupervisor`, `InstrumentationComposer`,
`cxp-supervisor` ni package separado.

La shell sigue siendo dueña de un caso concreto de bootstrap y el core
solo expone un hook pequeño para escribir metadata de artefacto de
sesión.

Se descarta por ahora un supervisor porque:

- coverage ya funciona con un bootstrap pequeño,
- no hay composición real entre instrumenters todavía,
- no hay un segundo consumidor externo que justifique extraer la
  infraestructura.

## No hacer todavía

Hasta que aparezcan señales nuevas, no introducir:

- IPC o bus de instrumentación,
- capability matrix,
- package separado para supervisor o launchers,
- API pública de instrumentación,
- discovery por entry points para instrumenters.

## Señales que activarían refactors futuros

### Añadir un segundo instrumenter real

Se activa cuando exista una necesidad concreta de producto, no una
posibilidad abstracta.

La primera implementación debe intentar encajar en el protocolo actual
sin rediseñarlo. Solo después de convivir en `main` se revisa si hace
falta tocar:

- `Contribution`,
- el bootstrap de shell,
- checks explícitos de incompatibilidad con coverage.

### Introducir discovery por entry points

Solo cuando haya al menos dos instrumenters reales y el import directo ya
sea ruido mantenido en shell.

### Introducir composición interna

Solo cuando existan tres instrumenters reales o una fricción mantenida en
shell que no se resuelva con un `if` explícito y corto.

### Extraer infraestructura a otro package

Solo cuando se cumplan las tres:

- existe un consumidor externo real,
- el protocolo lleva varias releases estable,
- hay al menos tres casos de uso reales validados por código.

## Ejercicio de papel: profiler tipo `py-spy`

### Cómo se vería su `prepare()`

Un profiler externo tipo `py-spy` probablemente querría:

- añadir un `argv_prefix` que envuelva el comando real,
- reservar un archivo de salida dentro del `workdir`,
- quizá añadir warnings o metadata mínima de configuración.

La `Contribution` resultante encaja solo parcialmente en el shape actual.

### Colisiones con coverage

La colisión no estaría en `env` ni en `workdir`, sino en `argv_prefix`.

Coverage ya envuelve el child con `python -m coverage run ...`.
`py-spy` normalmente también necesita ser el wrapper de proceso.

### ¿Cabe en el mismo child?

No de forma limpia.

Coverage y un profiler externo de este tipo competirían por ser el
wrapper del subprocess. Ese es precisamente el primer dato real que
justificaría revisar la arquitectura si llega el segundo instrumenter.

### ¿Encaja su output en `InstrumentationSummary.payload`?

Sí, mientras se trate como resumen estructurado de rutas, métricas o
agregados. No hace falta otro envelope todavía.

## Conclusión

La v1 actual es suficiente hasta que exista un segundo instrumenter real.

El primer candidato que de verdad tensiona el diseño no es `timing`,
porque `timing` ya vive dentro del lifecycle del runner y no necesita ser
wrapper de proceso. El candidato útil para aprender es un profiler
externo tipo `py-spy`.

Si ese segundo caso llega y no encaja en el bootstrap actual, esa
incomodidad será la primera señal válida para abrir diseño adicional. No
antes.
