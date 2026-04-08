# VS Code Extension

Primera integracion funcional de VS Code para workspaces de Cosecha.

## Alcance

Este directorio queda fuera de `packages/` a proposito:

- `packages/` sigue significando paquete Python del `uv workspace`
- `editors/vscode/` pasa a ser el cliente editorial de VS Code
- la extension se apoya en `cosecha`, `cosecha-lsp` y `cosecha-mcp` en vez de duplicar logica

## Lo que ya puede aprovechar

### 1. LSP para `.feature`

La base ya existe en `packages/cosecha-lsp` y expone por stdio el script
`cosecha-lsp`.

Capacidades reales hoy:

- diagnosticos al abrir y guardar ficheros `.feature`
- completado de steps Gherkin
- `Go to Definition` hacia definiciones resueltas
- `Hover` sobre steps
- formateo de documentos `.feature`
- comandos LSP `getTemplates` y `gherkinCreateDataTable`

### 2. Editor y ejecucion

La CLI de `packages/cosecha` ya ofrece suficiente superficie para una
primera integracion de editor.

Capacidades realistas hoy:

- `cosecha run`
- `cosecha plan analyze`
- `cosecha plan explain`
- `cosecha plan simulate`
- `cosecha manifest show|explain|validate`
- `cosecha gherkin fmt|validate|pre-commit`
- `cosecha pytest validate`
- `cosecha knowledge query|reset|rebuild`
- `cosecha session artifacts|events|summary`
- `cosecha doctor`

La extension ya expone:

- activity bar `Cosecha` con una vista `Quick Actions`
- status bar para ejecutar Cosecha sobre el fichero activo o el
  workspace
- reinicio manual del LSP con `Cosecha: Restart Language Server`
- CodeLens en `.feature` y `.py`
- comando para insertar tablas Gherkin
- comandos para validar el fichero activo
- lanzamiento de scopes de test conocidos por la KB
- terminales por workspace en vez de un terminal global compartido

Nota: con la API actual de Cosecha, la seleccion operativa se hace por
`test_path`. Eso significa que la primera version ejecuta el scope de
fichero conocido por Cosecha, aunque el acceso se dispare desde una
linea de test concreta.

### 3. Knowledge Base y sesiones

La extension ya muestra en la sidebar:

- vista `Knowledge Base` con estado de la KB, counts y tests/definiciones
  del fichero activo
- contexto canonico de workspace con `workspace_root`,
  `knowledge_anchor` y `workspace_fingerprint`
- vista `Recent Sessions` con las ultimas sesiones persistidas y resumen
  rapido de estados
- apertura de payloads JSON de KB, definiciones, tests y sesiones en una
  vista HTML con exportacion a JSON
- vista de `Session Summary` con cobertura, conteos, instrumentation y
  acceso rapido al JSON crudo

La implementacion actual usa una bridge Python ligera en
`python/cosecha_bridge.py`, apoyada en `cosecha.workspace` y
`cosecha.core`, para no depender de un cliente MCP completo dentro de la
extension y al mismo tiempo reutilizar la resolucion canonica de
workspace.

## Roadmap sugerido

### Siguientes pasos

- Test Explorer nativo de VS Code con resultados integrados
- timeline y artefactos de sesion por nodo
- quick fixes conectados a validaciones Gherkin
- uso directo de MCP cuando interese enriquecer queries y vistas

### Futuro

- Test Explorer nativo de VS Code
- decoraciones de estado e historial por fichero
- asistentes para plantillas Gherkin y tablas
- vistas de conocimiento y recursos materializados

## Piezas principales

- `package.json`: manifiesto y comandos de la extension
- `tsconfig.json`: compilacion TypeScript
- `src/extension.ts`: activacion, arranque del LSP, TreeViews, CodeLens
  y comandos CLI
- `python/cosecha_bridge.py`: bridge Python compartida con resolucion
  canonica de workspace
- `media/cosecha.svg`: icono de la vista lateral

## Desarrollo local

```bash
cd editors/vscode
npm install
npm run compile
```

Despues, abrir este directorio en VS Code y lanzar la extension en modo
desarrollo con `F5`.

La carpeta `.vscode/` ya incluye:

- `launch.json` para abrir un `Extension Development Host`
- `tasks.json` para compilar TypeScript antes de arrancar

## Resolucion del backend

La extension intenta ejecutar Cosecha usando el entorno del workspace
con esta prioridad:

- interprete seleccionado por la extension de Python cuando este
  disponible
- `python.defaultInterpreterPath` y `python.pythonPath` si estan
  configurados
- entornos del root del workspace: `.venv`, `venv`, `env`, `.env` y
  cualquier carpeta con `pyvenv.cfg`
- `VIRTUAL_ENV` del proceso de VS Code si existe
- `uv run cosecha` y `uv run cosecha-lsp` como fallback final
- alias legacy `granjero` para compatibilidad del LSP

Si no encuentra backend ejecutable, muestra un error claro en lugar de
asumir que la extension trae su propio entorno Python.

La paleta incluye `Cosecha: Show Backend` para inspeccionar:

- interprete resuelto
- fuente de resolucion
- version de Python
- `sys.prefix`
- `workspace_root`
- `knowledge_anchor`
- `execution_root`
- `workspace_fingerprint`
- comando CLI y LSP que usara la extension

La seleccion manual del entorno se hace con
`Cosecha: Select Virtualenv Folder` y espera la carpeta del entorno
virtual, no el ejecutable `python`.

## Smoke Test

1. Abrir `editors/vscode` en VS Code.
2. Pulsar `F5` para abrir el `Extension Development Host`.
3. En la nueva ventana, abrir un workspace real de Cosecha.
4. Ejecutar `Cosecha: Show Backend` y comprobar en `Output > Cosecha`:
   - interprete resuelto
   - version de Python
   - comando CLI
   - comando LSP
5. Abrir un `.feature` y verificar que no falla el arranque del LSP.
6. Verificar que aparecen las vistas `Knowledge Base` y `Recent Sessions`
   en la activity bar de Cosecha.
7. Verificar que aparece `Quick Actions` y que sus items son clicables.
8. Ejecutar `Cosecha: Restart Language Server` y comprobar que el LSP
   vuelve a arrancar sin recargar la ventana.
9. Ejecutar `Cosecha: Manifest Validate`.
10. Ejecutar `Cosecha: Run` con un fichero activo y comprobar que usa ese
   path; repetir sin fichero activo y comprobar que cae al workspace.
11. Abrir un payload desde `Knowledge Base` o `Recent Sessions` y
    comprobar que se renderiza en la vista HTML con `Copy JSON` y
    `Export JSON`.
12. Abrir `Cosecha: Show Session Summary` desde `Recent Sessions` y
    comprobar que aparece el resumen HTML con `Open Raw JSON`.
13. En un `.feature`, usar `Cosecha: Insert Gherkin Data Table`.
