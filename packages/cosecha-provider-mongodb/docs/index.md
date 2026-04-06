# Cosecha Provider MongoDB

## Objetivo

`cosecha-provider-mongodb` materializa el recurso `database/mongodb` para
Cosecha.

Soporta tres backends:

- `mock`: runtime embebido local basado en `mongoeco`
- `standalone`: proceso `mongod` efímero gestionado por el provider
- `live`: conexión a un MongoDB externo

## Configuración En `cosecha.toml`

Provider canónico:

```toml
provider = "cosecha.provider.mongodb:MongoResourceProvider"
```

### Elegir el nombre lógico del recurso

Las claves canónicas son `resource_name` y `resource_prefix`.

Compatibilidad:

- `database_name` sigue aceptándose como alias de `resource_name`
- `database_prefix` sigue aceptándose como alias de `resource_prefix`

La semántica interna del handle y del runtime sigue siendo de base de
datos, por eso `MongoResourceHandle` sigue exponiendo `database_name`.

Si defines `resource_name`, ese nombre se usa tal cual:

```toml
[[resources]]
name = "mongo"
provider = "cosecha.provider.mongodb:MongoResourceProvider"
scope = "test"
mode = "ephemeral"

[resources.config]
backend = "mock"
mongoeco_engine = "memory"
resource_name = "mi_suite_db"
cleanup_policy = "drop"
```

Si no defines `resource_name`, el provider genera uno a partir de
`resource_prefix` y del nombre del recurso:

```toml
[[resources]]
name = "mongo"
provider = "cosecha.provider.mongodb:MongoResourceProvider"
scope = "run"
mode = "ephemeral"

[resources.config]
backend = "mock"
mongoeco_engine = "memory"
resource_prefix = "suite"
cleanup_policy = "drop"
```

### Mock embebido con `mongoeco`

`backend = "mock"` no usa un servidor Mongo real. Usa `mongoeco` como runtime
local.

Mock en memoria:

```toml
[[resources]]
name = "mongo"
provider = "cosecha.provider.mongodb:MongoResourceProvider"
scope = "run"
mode = "ephemeral"

[resources.config]
backend = "mock"
mongoeco_engine = "memory"
database_name = "local_test_db"
cleanup_policy = "drop"
```

Mock con SQLite embebido:

```toml
[[resources]]
name = "mongo"
provider = "cosecha.provider.mongodb:MongoResourceProvider"
scope = "run"
mode = "ephemeral"

[resources.config]
backend = "mock"
mongoeco_engine = "sqlite"
mongoeco_sqlite_path = ".cosecha/runtime/mongoeco.db"
database_name = "contract_db"
cleanup_policy = "preserve"
```

Notas:

- `mongoeco_engine = "memory"` es el valor por defecto.
- si `mongoeco_engine = "sqlite"` y no defines `mongoeco_sqlite_path`, el provider
  crea almacenamiento temporal propio y lo limpia al liberar el recurso.
- si defines `mongoeco_sqlite_path`, el provider no borra ese fichero
  automáticamente.

### `standalone` con `mongod`

```toml
[[resources]]
name = "mongo"
provider = "cosecha.provider.mongodb:MongoResourceProvider"
scope = "run"
mode = "ephemeral"

[resources.config]
backend = "standalone"
database_name = "shared_suite"
cleanup_policy = "drop"
startup_timeout_seconds = 15
standalone_port = 27018
```

### `live` contra Mongo externo

```toml
[[resources]]
name = "mongo"
provider = "cosecha.provider.mongodb:MongoResourceProvider"
scope = "run"
mode = "live"

[resources.config]
backend = "live"
uri = "mongodb://127.0.0.1:27017/?directConnection=true"
database_name = "integration_db"
cleanup_policy = "preserve"
```

### Inicializar un recurso desde otro

```toml
[[resources]]
name = "seed"
provider = "cosecha.provider.mongodb:MongoResourceProvider"
scope = "run"
mode = "ephemeral"

[resources.config]
backend = "mock"
mongoeco_engine = "memory"
database_name = "seed_db"

[[resources]]
name = "mongo"
provider = "cosecha.provider.mongodb:MongoResourceProvider"
scope = "run"
mode = "ephemeral"
initializes_from = ["seed"]
initialization_mode = "state_snapshot"

[resources.config]
backend = "mock"
mongoeco_engine = "sqlite"
cleanup_policy = "drop"
```

## Overrides Desde Terminal

El provider soporta overrides por variables de entorno `COSECHA_MONGO_*`.

Precedencia:

- `resources.config` en `cosecha.toml`
- sobrescrito por las variables de entorno `COSECHA_MONGO_*`

Cambiar a backend `live` desde terminal:

```bash
COSECHA_MONGO_BACKEND=live \
COSECHA_MONGO_URI='mongodb://127.0.0.1:27017/?directConnection=true' \
COSECHA_MONGO_RESOURCE_NAME=integration_db \
cosecha run
```

Forzar mock en memoria:

```bash
COSECHA_MONGO_BACKEND=mock \
COSECHA_MONGO_RESOURCE_NAME=local_test_db \
cosecha run
```

Usar mock SQLite embebido:

```bash
COSECHA_MONGO_BACKEND=mock \
COSECHA_MONGO_MONGOECO_ENGINE=sqlite \
COSECHA_MONGO_MONGOECO_SQLITE_PATH=.cosecha/runtime/mongoeco.db \
COSECHA_MONGO_RESOURCE_NAME=sqlite_suite \
COSECHA_MONGO_CLEANUP_POLICY=preserve \
cosecha run
```

Configurar `standalone` desde terminal:

```bash
COSECHA_MONGO_BACKEND=standalone \
COSECHA_MONGO_RESOURCE_NAME=shared_suite \
COSECHA_MONGO_STARTUP_TIMEOUT_SECONDS=15 \
COSECHA_MONGO_STANDALONE_PORT=27018 \
cosecha run
```

Precedencia nominal:

- `COSECHA_MONGO_RESOURCE_NAME` pisa `COSECHA_MONGO_DATABASE_NAME`
- `COSECHA_MONGO_RESOURCE_PREFIX` pisa
  `COSECHA_MONGO_DATABASE_PREFIX`
- `resource_name` pisa `database_name`
- `resource_prefix` pisa `database_prefix`

Compatibilidad temporal:

- `backend_kind` sigue aceptándose como alias de `backend`
- `mock_engine` sigue aceptándose como alias de `mongoeco_engine`
- `mock_sqlite_path` sigue aceptándose como alias de `mongoeco_sqlite_path`
- `database_name` sigue aceptándose como alias de `resource_name`
- `database_prefix` sigue aceptándose como alias de `resource_prefix`
- `COSECHA_MONGO_MOCK_ENGINE` sigue aceptándose como alias de
  `COSECHA_MONGO_MONGOECO_ENGINE`
- `COSECHA_MONGO_MOCK_SQLITE_PATH` sigue aceptándose como alias de
  `COSECHA_MONGO_MONGOECO_SQLITE_PATH`
- `COSECHA_MONGO_DATABASE_NAME` sigue aceptándose como alias de
  `COSECHA_MONGO_RESOURCE_NAME`
- `COSECHA_MONGO_DATABASE_PREFIX` sigue aceptándose como alias de
  `COSECHA_MONGO_RESOURCE_PREFIX`

## Límites

- Esta iteración no añade flags dedicados del estilo
  `cosecha run --mongo-*`.
- `mock` es un runtime local embebido. No sustituye a un clúster Mongo real.
