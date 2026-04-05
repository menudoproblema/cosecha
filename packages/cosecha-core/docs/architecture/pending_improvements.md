# Pending Improvements

## Estado

No quedan guardas abiertas del roadmap histórico como condición de
migración al monorepo.

Las mejoras listadas aquí son evolución posterior y no bloquean la
corrección semántica del framework actual.

## Escala adicional

- seguir reduciendo costes residuales de import y matching en engines
  grandes,
- reevaluar fingerprints y caches si el profiling real vuelve a cambiar
  de cuello de botella,
- refinar políticas de agrupación o reutilización en runtimes masivos.

## Observabilidad avanzada

- enriquecer snapshots vivos si aparece un consumer real que necesite
  más contexto que el envelope actual,
- endurecer budgets y granularidad de streaming en multiproceso.

## Evolución de plataforma

- introducir runtimes remotos o multi-host cuando el producto lo
  justifique,
- endurecer cuotas, aislamiento y contratos de seguridad si aparecen
  consumers multi-tenant.

## Unificación runtime profiles -> ResourceManager

La arquitectura pública ya expone `runtime_profiles` como contrato
canónico de runtime y el core ya valida `depends_on`,
`initializes_from`, ciclos y alcance con la misma base de invariantes
que usa `ResourceManager` para recursos explícitos.

El margen pendiente queda acotado a bajar también la materialización y
el lifecycle operativo de servicios declarados a ese mismo carril, de
forma que las garantías de aislamiento y rehidratación no dependan de
hooks o adaptadores paralelos.

- servicios declarados en `runtime_profiles`.
