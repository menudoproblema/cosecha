# Flujos end-to-end

## Propósito

Este documento resume los flujos completos del framework ya repartido
en el monorepo actual.

## Flujo 1. Ejecución completa

1. La CLI normaliza la entrada a una operación tipada.
2. El manifiesto declara engines, runtime profiles y recursos.
3. `cosecha-core` materializa solo los componentes alcanzados por la
   operación.
4. La KB decide si puede reutilizar conocimiento caliente o debe
   reconstruirlo.
5. La compilación produce `Planning Semantics`, `Execution IR` y
   `Plan Explanation`.
6. El runtime ejecuta el plan y emite eventos y resultados.
7. Reporters, telemetría y persistencia consumen los mismos hechos.
8. La sesión cierra recursos, workers y artefactos.

## Flujo 2. Validación parcial

1. Un consumer invoca `draft.validate`, explain o simulación.
2. El sistema usa la KB como camino caliente si es seguro.
3. El core recompila solo lo necesario.
4. La operación devuelve issues, ejecutabilidad y explicación sin
   requerir una sesión completa de ejecución.

## Flujo 3. Reuso incremental de conocimiento

1. El sistema verifica frescura por manifest y fingerprints.
2. Si el snapshot es válido, reutiliza conocimiento persistido.
3. Si no lo es, invalida selectivamente y reconstruye solo lo afectado.

## Flujo 4. Ejecución multiproceso

1. El core prepara un bootstrap serializable por nodo.
2. El scheduler decide placement y afinidad.
3. Los workers ejecutan bundles ya preparados.
4. El sistema correlaciona `session_id`, `trace_id`, `worker_id` y
   `stable_id`.

## Flujo 5. Post-mortem

1. Una sesión cerrada deja artefactos compactos persistidos.
2. `cosecha session` y `cosecha knowledge` consultan esa información.
3. El usuario o un agente diagnostica sin depender de objetos vivos.
