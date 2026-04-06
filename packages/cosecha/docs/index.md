# CLI de Cosecha

## Objetivo

`cosecha` es el paquete de entrada para usuario final.

Su ownership cubre:

- `cosecha run` y el resto de subcomandos humanos,
- contribuciones de shell como LSP y hooks de integración,
- composición por defecto de `cosecha-core` con engine de `pytest` y
  reporter de consola,
- documentación de la superficie pública del CLI.

No es dueño de:

- la semántica interna del runner,
- los engines concretos,
- los formatos estructurados de salida,
- la knowledge base o artefactos como contrato transversal.

## Puntos de entrada

- `src/cosecha/shell/runner_cli.py`
- `src/cosecha/shell/mochuelo_runtime.py`
- `packages/cosecha-lsp/src/cosecha_lsp/lsp_server.py` como paquete
  separado para tooling de LSP, expuesto con `cosecha-lsp`

## Mapa local

- [Reporting y artefactos desde el CLI](./reporting.md)

## Dependencias por defecto

La instalación base del paquete compone:

- `cosecha-core`
- `cosecha-engine-pytest`
- `cosecha-reporter-console`

Los engines, reporters estructurados, plugins, providers y herramientas
de desarrollo entran por extras o por instalación directa de sus
paquetes.

Extras agregados del metapaquete:

- `http`: `cosecha-provider-http`
- `http-uvicorn`: `cosecha-provider-http[uvicorn]`
- `http-hypercorn`: `cosecha-provider-http[hypercorn]`
- `ssl`: `cosecha-provider-ssl`
- `all`: engines + plugins + reporters + providers
- `devtools`: `cosecha-lsp`, `cosecha-mcp`
- `full`: `all` + `devtools`
