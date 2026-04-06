# Cosecha Provider HTTP

## Objetivo

`cosecha-provider-http` materializa recursos HTTP para Cosecha.

El paquete expone dos providers coordinados:

- `HttpApplicationProvider` para aplicaciones `application/http`
- `HttpTransportProvider` para transporte `transport/http`

Soporta varias aplicaciones simultáneas en la misma ejecución. Cada
recurso materializa su propia app y, si aplica, su propio transporte.

Para `standalone` ASGI soporta dos servidores opcionales:

- `uvicorn`
- `hypercorn`

La selección por defecto es `uvicorn` cuando está instalado. Si no lo
está, intenta `hypercorn`. Si no hay ninguno, `standalone` falla con un
error claro.

La semántica de catálogos queda alineada con `cxp` sin introducir una
dependencia directa desde este paquete:

- `application/http` se usa como familia abstracta
- la concreción entra por `mode = "asgi" | "wsgi"`
- `transport/http` se usa como recurso separado
- la concreción entra por `mode = "inprocess" | "standalone" | "live"`

## Configuración En `cosecha.toml`

Providers canónicos:

```toml
provider = "cosecha.provider.http:HttpApplicationProvider"
provider = "cosecha.provider.http:HttpTransportProvider"
```

### Aplicación HTTP

```toml
[[resources]]
name = "api"
provider = "cosecha.provider.http:HttpApplicationProvider"
scope = "run"
mode = "ephemeral"

[resources.config]
backend = "asgi"
app = "apps.py:create_api"
resource_prefix = "suite"
```

Claves principales:

- `backend = "asgi" | "wsgi"`
- `app = "paquete.modulo:simbolo"` o `ruta.py:simbolo`
- `resource_name` opcional
- `resource_prefix` opcional

Si defines `resource_name`, se usa tal cual. Si no, el provider genera
uno a partir de `resource_prefix` y `resources.name`.

### Transporte HTTP inprocess

```toml
[[resources]]
name = "api-http"
provider = "cosecha.provider.http:HttpTransportProvider"
scope = "run"
mode = "ephemeral"
depends_on = ["api"]

[resources.config]
backend = "inprocess"
```

### Transporte HTTP standalone

```toml
[[resources]]
name = "api-server"
provider = "cosecha.provider.http:HttpTransportProvider"
scope = "run"
mode = "ephemeral"
depends_on = ["api"]

[resources.config]
backend = "standalone"
host = "127.0.0.1"
standalone_server = "auto"
```

Si no defines `port`, el provider elige uno libre automáticamente. Esto
permite levantar varias apps simultáneas sin colisiones.

Con puerto fijo:

```toml
[[resources]]
name = "api-server"
provider = "cosecha.provider.http:HttpTransportProvider"
scope = "run"
mode = "ephemeral"
depends_on = ["api"]

[resources.config]
backend = "standalone"
host = "127.0.0.1"
port = 8010
```

Con TLS directo:

```toml
[[resources]]
name = "api-server"
provider = "cosecha.provider.http:HttpTransportProvider"
scope = "run"
mode = "ephemeral"
depends_on = ["api"]

[resources.config]
backend = "standalone"
standalone_server = "hypercorn"
ssl_certfile = ".cosecha/runtime/tls/server.crt"
ssl_keyfile = ".cosecha/runtime/tls/server.key"
```

Las rutas TLS son explícitas. Si quieres generar certificados efímeros
desde Cosecha, usa `cosecha-provider-ssl` como recurso separado y pasa
sus rutas resultantes a la configuración HTTP correspondiente.

Con composición real de recursos:

```toml
[[resources]]
name = "tls"
provider = "cosecha.provider.ssl:SslMaterialProvider"
scope = "run"
mode = "ephemeral"

[[resources]]
name = "api-server"
provider = "cosecha.provider.http:HttpTransportProvider"
scope = "run"
mode = "ephemeral"
depends_on = ["api", "tls"]

[resources.config]
backend = "standalone"
ssl_resource = "tls"
```

En este caso el transport resuelve `cert_path`, `key_path` y
`ca_cert_path` desde el recurso SSL materializado.

Cuando usas `application_resource` o `ssl_resource`, Cosecha los trata
como dependencias efectivas del transport aunque no repitas esos nombres
en `depends_on`.

Restricción de capacidades:

- `standalone + ssl` requiere que la aplicación dependiente sea `asgi`
- `wsgi + ssl` falla durante la validación de composición del recurso

### Transporte HTTP live

```toml
[[resources]]
name = "api-live"
provider = "cosecha.provider.http:HttpTransportProvider"
scope = "run"
mode = "live"

[resources.config]
backend = "live"
base_url = "http://127.0.0.1:9000"
```

## Variables de entorno

El provider soporta overrides globales por `COSECHA_HTTP_*`.

- `COSECHA_HTTP_BACKEND`
- `COSECHA_HTTP_RESOURCE_NAME`
- `COSECHA_HTTP_RESOURCE_PREFIX`
- `COSECHA_HTTP_APP`
- `COSECHA_HTTP_HOST`
- `COSECHA_HTTP_PORT`
- `COSECHA_HTTP_STANDALONE_SERVER`
- `COSECHA_HTTP_BASE_URL`
- `COSECHA_HTTP_SSL_CERTFILE`
- `COSECHA_HTTP_SSL_KEYFILE`
- `COSECHA_HTTP_SSL_CA_CERTS`
- `COSECHA_HTTP_STARTUP_TIMEOUT_SECONDS`
- `COSECHA_HTTP_CLEANUP_POLICY`

Los overrides son globales al provider. El caso multiapp por defecto
depende del naming generado por recurso cuando no se fuerzan estos
valores.

## Ejemplo Multiapp

```toml
[[resources]]
name = "api-a"
provider = "cosecha.provider.http:HttpApplicationProvider"
scope = "run"
mode = "ephemeral"

[resources.config]
backend = "asgi"
app = "apps.py:create_api_a"
resource_prefix = "suite"

[[resources]]
name = "api-b"
provider = "cosecha.provider.http:HttpApplicationProvider"
scope = "run"
mode = "ephemeral"

[resources.config]
backend = "wsgi"
app = "apps.py:create_api_b"
resource_prefix = "suite"

[[resources]]
name = "api-a-http"
provider = "cosecha.provider.http:HttpTransportProvider"
scope = "run"
mode = "ephemeral"
depends_on = ["api-a"]

[resources.config]
backend = "standalone"

[[resources]]
name = "api-b-http"
provider = "cosecha.provider.http:HttpTransportProvider"
scope = "run"
mode = "ephemeral"
depends_on = ["api-b"]

[resources.config]
backend = "inprocess"
```

## Límites

Esta iteración cubre request/response HTTP.

Notas de empaquetado:

- el paquete base no arrastra servidor ASGI por defecto
- instala `cosecha-provider-http[uvicorn]` si quieres el backend por
  defecto
- instala `cosecha-provider-http[hypercorn]` si quieres HTTP/2 o HTTP/3
  desde `hypercorn`

## Tareas pendientes

- `WebSocket, TLS, early hints, server push ni file_wrapper`
