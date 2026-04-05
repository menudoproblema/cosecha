# Rutas y `definition_paths` en Gherkin

## Referencias básicas

El engine Gherkin trabaja sobre dos referencias:

- `root_path`: raíz efectiva del workspace,
- `base_path`: subruta del engine dentro de esa raíz.

## Resolución jerárquica de `steps/`

Para descubrir definiciones ejecutables del proyecto, el collector parte
de la ruta del test o del directorio de colección y sube por la
jerarquía hasta `root_path`, buscando carpetas `steps/` en cada nivel.

Esto implica:

- un test puede ver `steps/` locales de su módulo,
- también puede ver `steps/` globales definidos en ancestros,
- no se hace un barrido completo del árbol para cada test,
- el resultado depende de la jerarquía real del workspace.

## Papel de `definition_paths`

`definition_paths` publica definiciones externas al árbol natural del
escenario.

Su uso permite:

- incorporar librerías compartidas de steps,
- exponer definiciones fuera de la jerarquía local del feature,
- mantener el mismo contrato de knowledge y materialización lazy sin
  forzar discovery global.

Las definiciones externas de librería no dependen de la búsqueda
ascendente por `steps/`, aunque terminen integradas en el mismo catálogo
operativo del engine.
