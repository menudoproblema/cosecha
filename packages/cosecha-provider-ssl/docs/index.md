# Cosecha Provider SSL

## Objetivo

`cosecha-provider-ssl` genera materiales SSL/TLS efímeros reutilizables
por otros recursos de runtime.

El provider está pensado para tests e integración local:

- certificado de servidor
- clave privada de servidor
- certificado de CA efímera para confiar en el servidor

Es un provider independiente. No arrastra dependencias de HTTP ni
inyecta automáticamente sus materiales en otros providers.

## Configuración en `cosecha.toml`

Provider canónico:

```toml
provider = "cosecha.provider.ssl:SslMaterialProvider"
```

Ejemplo básico:

```toml
[[resources]]
name = "tls"
provider = "cosecha.provider.ssl:SslMaterialProvider"
scope = "run"
mode = "ephemeral"

[resources.config]
common_name = "localhost"
dns_names = ["localhost"]
ip_addresses = ["127.0.0.1", "::1"]
```

Claves principales:

- `common_name`
- `dns_names`
- `ip_addresses`
- `validity_days`
- `cleanup_policy = "auto" | "preserve"`
- `cert_path`
- `key_path`
- `ca_cert_path`

Si no defines rutas, el provider crea ficheros temporales propios.

Un caso típico es generar estos materiales y reutilizar sus rutas en la
configuración TLS de `cosecha-provider-http`.

## Variables de entorno

- `COSECHA_SSL_COMMON_NAME`
- `COSECHA_SSL_DNS_NAMES`
- `COSECHA_SSL_IP_ADDRESSES`
- `COSECHA_SSL_VALIDITY_DAYS`
- `COSECHA_SSL_CLEANUP_POLICY`
- `COSECHA_SSL_CERT_PATH`
- `COSECHA_SSL_KEY_PATH`
- `COSECHA_SSL_CA_CERT_PATH`

## Handle público

El recurso materializado expone:

- `cert_path`
- `key_path`
- `ca_cert_path`
- `common_name`
- `dns_names`
- `ip_addresses`

## Límites

- Genera materiales efímeros para test/local, no PKI de producción.
- No gestiona revocación, rotación ni almacenamiento seguro externo.
