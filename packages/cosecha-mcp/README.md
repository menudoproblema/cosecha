# cosecha-mcp

Servidor MCP para explorar workspaces de Cosecha sin atacar `kb.db` a mano.

## Criterio de diseño

- usa las operaciones del core en lugar de consultas SQL ad-hoc
- diferencia entre `project_path` y `root_path` de Cosecha
- trata `tests/` como root efectivo cuando existe `tests/cosecha.toml`
- ejecuta las operaciones respaldadas por `Runner` en un subproceso efímero por petición
- reutiliza el Python del `venv` del workspace cuando coincide con la ABI del MCP y falla explícitamente si detecta un desajuste de versión
- limita y compacta respuestas grandes para no desbordar clientes MCP ni quemar contexto innecesario
- ofrece tools estructuradas y una tool de búsqueda rápida

## Tools

- `describe_workspace`
- `describe_knowledge_base`
- `describe_path_freshness`
- `search_catalog`
- `query_tests`
- `query_definitions`
- `query_registry_items`
- `query_resources`
- `read_session_artifacts`
- `list_recent_sessions`
- `inspect_test_plan`
- `get_execution_timeline`
- `list_test_execution_history`
- `list_engines_and_capabilities`
- `refresh_knowledge_base`
- `run_tests`

## Ejecución

```bash
cosecha-mcp
```

Opcionalmente se puede fijar un workspace por defecto con:

```bash
COSECHA_MCP_ROOT=/ruta/al/proyecto cosecha-mcp
```

La tool `run_tests` queda desactivada por defecto y requiere opt-in explícito:

```bash
COSECHA_MCP_ENABLE_RUN_TESTS=1 cosecha-mcp
```
