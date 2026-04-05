# Type & Coercion System

## Propósito

Este documento recoge el contrato transversal de tipos y coerciones
para compilación, runtime, LSP, MCP, CLI y agentes.

## Qué publica el sistema

- tipos nominales conocidos,
- coerciones declaradas,
- esquema de parámetros de definiciones ejecutables,
- validación estática,
- compatibilidad entre productores y consumidores de valores tipados.

## Ownership del registry

- la compilación construye una vista inmutable para la operación,
- la KB conserva una proyección persistida e invalidable,
- el runtime consume snapshots serializados,
- los consumers externos consultan, pero no mutan el registry vivo.

## Tipos canónicos mínimos

- `string`
- `int`
- `float`
- `bool`
- `docstring`
- `table`
- `path`
- `json`

Cada engine puede publicar tipos propios si mantiene nombre nominal,
serialización y reglas mínimas de validación.

## Coerciones mínimas

- `string -> int`
- `string -> float`
- `string -> bool`
- `docstring -> string`
- `table -> list[dict[string, string]]`
- `string -> path`
- `string -> json`

Una coerción publicada debe declarar:

- tipo fuente,
- tipo destino,
- si puede fallar,
- razón estable del fallo.

## Invariantes

- la compilación puede validar tipos sin ejecutar dominio,
- una coerción no declarada no se asume implícitamente,
- `strict` y `relaxed` difieren en ejecutabilidad, no en el contrato de
  tipo,
- el registro de tipos forma parte de explain y del conocimiento
  persistido.
