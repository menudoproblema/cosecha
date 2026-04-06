from __future__ import annotations

from pathlib import Path

from cryptography import x509
from cryptography.x509.oid import NameOID

from cosecha.core.resources import ResourceRequirement
from cosecha.provider.ssl import SslMaterialHandle, SslMaterialProvider


def _build_requirement(
    *,
    config: dict[str, object],
    mode: str = 'ephemeral',
    scope: str = 'run',
) -> ResourceRequirement:
    return ResourceRequirement(
        name='tls',
        provider=SslMaterialProvider(),
        scope=scope,  # type: ignore[arg-type]
        mode=mode,  # type: ignore[arg-type]
        config=config,
    )


def test_ssl_provider_creates_default_materials() -> None:
    provider = SslMaterialProvider()
    requirement = _build_requirement(config={})

    resource = provider.acquire(requirement, mode='ephemeral')

    assert isinstance(resource, SslMaterialHandle)
    assert Path(resource.cert_path).exists()
    assert Path(resource.key_path).exists()
    assert Path(resource.ca_cert_path).exists()
    assert provider.health_check(resource, requirement, mode='ephemeral')
    assert provider.verify_integrity(resource, requirement, mode='ephemeral')

    provider.release(resource, requirement, mode='ephemeral')

    assert not Path(resource.cert_path).exists()


def test_ssl_provider_supports_explicit_paths_and_preserve(
    tmp_path: Path,
) -> None:
    provider = SslMaterialProvider()
    cert_path = tmp_path / 'tls' / 'server.crt'
    key_path = tmp_path / 'tls' / 'server.key'
    ca_cert_path = tmp_path / 'tls' / 'ca.crt'
    requirement = _build_requirement(
        config={
            'cert_path': str(cert_path),
            'key_path': str(key_path),
            'ca_cert_path': str(ca_cert_path),
            'cleanup_policy': 'preserve',
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')

    assert cert_path.exists()
    assert key_path.exists()
    assert ca_cert_path.exists()

    provider.release(resource, requirement, mode='ephemeral')

    assert cert_path.exists()
    assert key_path.exists()
    assert ca_cert_path.exists()


def test_ssl_provider_populates_subject_and_san_entries() -> None:
    provider = SslMaterialProvider()
    requirement = _build_requirement(
        config={
            'common_name': 'api.local',
            'dns_names': ['api.local', 'localhost'],
            'ip_addresses': ['127.0.0.1'],
        },
    )

    resource = provider.acquire(requirement, mode='ephemeral')
    cert = x509.load_pem_x509_certificate(
        Path(resource.cert_path).read_bytes(),
    )
    common_name = cert.subject.get_attributes_for_oid(
        NameOID.COMMON_NAME,
    )[0]
    san = cert.extensions.get_extension_for_class(
        x509.SubjectAlternativeName,
    ).value

    assert common_name.value == 'api.local'
    assert 'api.local' in san.get_values_for_type(x509.DNSName)
    assert 'localhost' in san.get_values_for_type(x509.DNSName)

    provider.release(resource, requirement, mode='ephemeral')


def test_ssl_environment_variables_override_manifest_config(
    monkeypatch,
) -> None:
    provider = SslMaterialProvider()
    requirement = _build_requirement(
        config={
            'common_name': 'manifest.local',
        },
    )
    monkeypatch.setenv('COSECHA_SSL_COMMON_NAME', 'env.local')

    resource = provider.acquire(requirement, mode='ephemeral')

    assert resource.common_name == 'env.local'

    provider.release(resource, requirement, mode='ephemeral')


def test_ssl_reap_orphan_removes_managed_files() -> None:
    provider = SslMaterialProvider()
    requirement = _build_requirement(config={})

    resource = provider.acquire(requirement, mode='ephemeral')
    external_handle = provider.describe_external_handle(
        resource,
        requirement,
        mode='ephemeral',
    )

    assert external_handle is not None
    provider.reap_orphan(external_handle, requirement, mode='ephemeral')

    assert not Path(resource.cert_path).exists()
