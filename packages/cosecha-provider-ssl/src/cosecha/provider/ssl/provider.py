from __future__ import annotations

import ipaddress
import json
import os
import tempfile

from collections.abc import Iterable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID


type SslCleanupPolicy = Literal['auto', 'preserve']

DEFAULT_DNS_NAMES = ('localhost',)
DEFAULT_IP_ADDRESSES = ('127.0.0.1', '::1')


@dataclass(slots=True, frozen=True)
class SslMaterialConfig:
    common_name: str = 'localhost'
    dns_names: tuple[str, ...] = DEFAULT_DNS_NAMES
    ip_addresses: tuple[str, ...] = DEFAULT_IP_ADDRESSES
    validity_days: int = 7
    cleanup_policy: SslCleanupPolicy = 'auto'
    cert_path: str | None = None
    key_path: str | None = None
    ca_cert_path: str | None = None

    @classmethod
    def from_requirement(cls, requirement) -> SslMaterialConfig:
        config = requirement.config
        return cls(
            common_name=_read_non_empty_str(
                _read_config_value(
                    config,
                    'common_name',
                    env_names=('COSECHA_SSL_COMMON_NAME',),
                    default='localhost',
                ),
                field_name='common_name',
            ),
            dns_names=_read_tuple_of_str(
                _read_config_value(
                    config,
                    'dns_names',
                    env_names=('COSECHA_SSL_DNS_NAMES',),
                    default=DEFAULT_DNS_NAMES,
                ),
                field_name='dns_names',
            ),
            ip_addresses=_read_tuple_of_str(
                _read_config_value(
                    config,
                    'ip_addresses',
                    env_names=('COSECHA_SSL_IP_ADDRESSES',),
                    default=DEFAULT_IP_ADDRESSES,
                ),
                field_name='ip_addresses',
            ),
            validity_days=_read_positive_int(
                _read_config_value(
                    config,
                    'validity_days',
                    env_names=('COSECHA_SSL_VALIDITY_DAYS',),
                    default=7,
                ),
                field_name='validity_days',
            ),
            cleanup_policy=_normalize_cleanup_policy(
                _read_config_value(
                    config,
                    'cleanup_policy',
                    env_names=('COSECHA_SSL_CLEANUP_POLICY',),
                    default='auto',
                ),
            ),
            cert_path=_read_optional_str(
                _read_config_value(
                    config,
                    'cert_path',
                    env_names=('COSECHA_SSL_CERT_PATH',),
                ),
            ),
            key_path=_read_optional_str(
                _read_config_value(
                    config,
                    'key_path',
                    env_names=('COSECHA_SSL_KEY_PATH',),
                ),
            ),
            ca_cert_path=_read_optional_str(
                _read_config_value(
                    config,
                    'ca_cert_path',
                    env_names=('COSECHA_SSL_CA_CERT_PATH',),
                ),
            ),
        )


@dataclass(slots=True)
class SslMaterialHandle:
    cert_path: str
    key_path: str
    ca_cert_path: str
    common_name: str
    dns_names: tuple[str, ...]
    ip_addresses: tuple[str, ...]
    cleanup_policy: SslCleanupPolicy
    tempdir: str | None = None
    managed_paths: tuple[str, ...] = ()

    def build_external_handle(self) -> str:
        return json.dumps(
            {
                'ca_cert_path': self.ca_cert_path,
                'cert_path': self.cert_path,
                'cleanup_policy': self.cleanup_policy,
                'common_name': self.common_name,
                'dns_names': self.dns_names,
                'ip_addresses': self.ip_addresses,
                'key_path': self.key_path,
                'managed_paths': self.managed_paths,
                'tempdir': self.tempdir,
            },
            sort_keys=True,
        )


class SslMaterialProvider:
    def supports_mode(self, mode: str) -> bool:
        return mode in {'live', 'ephemeral'}

    def acquire(self, requirement, *, mode: str) -> SslMaterialHandle:
        del mode
        config = SslMaterialConfig.from_requirement(requirement)
        tempdir, base_dir = _resolve_output_directory(config)
        cert_path = Path(config.cert_path or base_dir / 'server.crt')
        key_path = Path(config.key_path or base_dir / 'server.key')
        ca_cert_path = Path(config.ca_cert_path or base_dir / 'ca.crt')

        for path in (cert_path, key_path, ca_cert_path):
            path.parent.mkdir(parents=True, exist_ok=True)

        _generate_tls_materials(
            cert_path=cert_path,
            key_path=key_path,
            ca_cert_path=ca_cert_path,
            common_name=config.common_name,
            dns_names=config.dns_names,
            ip_addresses=config.ip_addresses,
            validity_days=config.validity_days,
        )

        return SslMaterialHandle(
            cert_path=str(cert_path),
            key_path=str(key_path),
            ca_cert_path=str(ca_cert_path),
            common_name=config.common_name,
            dns_names=config.dns_names,
            ip_addresses=config.ip_addresses,
            cleanup_policy=config.cleanup_policy,
            tempdir=tempdir,
            managed_paths=(
                str(cert_path),
                str(key_path),
                str(ca_cert_path),
            ),
        )

    def release(self, resource, requirement, *, mode: str) -> None:
        del requirement, mode
        if not isinstance(resource, SslMaterialHandle):
            return
        if resource.cleanup_policy == 'preserve':
            return
        _cleanup_ssl_materials(
            managed_paths=resource.managed_paths,
            tempdir=resource.tempdir,
        )

    def health_check(self, resource, requirement, *, mode: str) -> bool:
        del requirement, mode
        if not isinstance(resource, SslMaterialHandle):
            return False
        return all(Path(path).exists() for path in resource.managed_paths)

    def verify_integrity(self, resource, requirement, *, mode: str) -> bool:
        del requirement, mode
        if not isinstance(resource, SslMaterialHandle):
            return False

        try:
            x509.load_pem_x509_certificate(
                Path(resource.cert_path).read_bytes(),
            )
            x509.load_pem_x509_certificate(
                Path(resource.ca_cert_path).read_bytes(),
            )
            serialization.load_pem_private_key(
                Path(resource.key_path).read_bytes(),
                password=None,
            )
        except (OSError, ValueError):
            return False

        return True

    def describe_external_handle(
        self,
        resource,
        requirement,
        *,
        mode: str,
    ) -> str | None:
        del requirement, mode
        if not isinstance(resource, SslMaterialHandle):
            return None
        return resource.build_external_handle()

    def describe_capabilities(
        self,
        resource,
        requirement,
        *,
        mode: str,
    ) -> dict[str, object]:
        del requirement, mode
        if not isinstance(resource, SslMaterialHandle):
            return {}
        return {
            'tls.materials': True,
            'tls.cert_path': resource.cert_path,
            'tls.key_path': resource.key_path,
            'tls.ca_cert_path': resource.ca_cert_path,
        }

    def reap_orphan(
        self,
        external_handle: str,
        requirement,
        *,
        mode: str,
    ) -> None:
        del requirement, mode
        payload = _decode_external_handle(external_handle)
        if payload.get('cleanup_policy') == 'preserve':
            return
        managed_paths = _read_tuple_of_str(
            payload.get('managed_paths', ()),
            field_name='managed_paths',
        )
        tempdir = _read_optional_str(payload.get('tempdir'))
        _cleanup_ssl_materials(
            managed_paths=managed_paths,
            tempdir=tempdir,
        )

    def revoke_orphan_access(
        self,
        external_handle: str,
        requirement,
        *,
        mode: str,
    ) -> None:
        del external_handle, requirement, mode


def _resolve_output_directory(
    config: SslMaterialConfig,
) -> tuple[str | None, Path]:
    if (
        config.cert_path is not None
        and config.key_path is not None
        and config.ca_cert_path is not None
    ):
        return (None, Path.cwd() / '.cosecha' / 'runtime' / 'ssl')

    tempdir = tempfile.mkdtemp(prefix='cosecha-ssl-')
    return (tempdir, Path(tempdir))


def _generate_tls_materials(  # noqa: PLR0913
    *,
    cert_path: Path,
    key_path: Path,
    ca_cert_path: Path,
    common_name: str,
    dns_names: tuple[str, ...],
    ip_addresses: tuple[str, ...],
    validity_days: int,
) -> None:
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(
        public_exponent=65_537,
        key_size=2_048,
    )
    ca_subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, f'{common_name} Test CA')],
    )
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_subject)
        .issuer_name(ca_subject)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=validity_days))
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )

    server_key = rsa.generate_private_key(
        public_exponent=65_537,
        key_size=2_048,
    )
    server_subject = x509.Name(
        [x509.NameAttribute(NameOID.COMMON_NAME, common_name)],
    )
    san_entries = [x509.DNSName(name) for name in dns_names]
    san_entries.extend(
        x509.IPAddress(ipaddress.ip_address(address))
        for address in ip_addresses
    )
    server_cert = (
        x509.CertificateBuilder()
        .subject_name(server_subject)
        .issuer_name(ca_cert.subject)
        .public_key(server_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=validity_days))
        .add_extension(
            x509.BasicConstraints(ca=False, path_length=None),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectAlternativeName(san_entries),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(
                ca_key.public_key(),
            ),
            critical=False,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(
                server_key.public_key(),
            ),
            critical=False,
        )
        .sign(private_key=ca_key, algorithm=hashes.SHA256())
    )

    cert_path.write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        server_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        ),
    )
    ca_cert_path.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))


def _cleanup_ssl_materials(
    *,
    managed_paths: tuple[str, ...],
    tempdir: str | None,
) -> None:
    for path in managed_paths:
        with suppress(OSError):
            Path(path).unlink()

    if tempdir is None:
        return

    tempdir_path = Path(tempdir)
    with suppress(OSError):
        tempdir_path.rmdir()


def _normalize_cleanup_policy(value: object) -> SslCleanupPolicy:
    if value in {'auto', 'preserve'}:
        return value
    msg = "SSL cleanup_policy must be one of 'auto' or 'preserve'"
    raise ValueError(msg)


def _read_config_value(
    config: Mapping[str, object],
    key: str,
    *,
    env_names: tuple[str, ...] = (),
    default: object | None = None,
) -> object | None:
    for env_name in env_names:
        if env_name in os.environ:
            return os.environ[env_name]
    return config.get(key, default)


def _read_optional_str(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str) and value:
        return value
    msg = 'Expected optional non-empty string'
    raise ValueError(msg)


def _read_non_empty_str(value: object, *, field_name: str) -> str:
    if isinstance(value, str) and value:
        return value
    msg = f'SSL config field {field_name!r} must be a non-empty string'
    raise ValueError(msg)


def _read_positive_int(value: object, *, field_name: str) -> int:
    if isinstance(value, int) and not isinstance(value, bool) and value > 0:
        return value
    if isinstance(value, str) and value:
        return _read_positive_int(int(value), field_name=field_name)
    msg = f'SSL config field {field_name!r} must be a positive integer'
    raise ValueError(msg)


def _read_tuple_of_str(
    value: object,
    *,
    field_name: str,
) -> tuple[str, ...]:
    if isinstance(value, str):
        items = tuple(
            item.strip()
            for item in value.split(',')
            if item.strip()
        )
        if items:
            return items
    if isinstance(value, Iterable):
        items = tuple(
            item
            for item in value
            if isinstance(item, str) and item
        )
        if items:
            return items
    msg = f'SSL config field {field_name!r} must contain non-empty strings'
    raise ValueError(msg)


def _decode_external_handle(external_handle: str) -> dict[str, object]:
    try:
        decoded = json.loads(external_handle)
    except json.JSONDecodeError as error:
        msg = 'Invalid SSL external handle'
        raise ValueError(msg) from error
    if isinstance(decoded, dict):
        return {str(key): value for key, value in decoded.items()}
    msg = 'Invalid SSL external handle payload'
    raise ValueError(msg)
