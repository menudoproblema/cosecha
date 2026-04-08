from __future__ import annotations

import json

from pathlib import Path

import pytest

from cryptography import x509
from cryptography.x509.oid import NameOID

from cosecha.core.resources import ResourceRequirement
from cosecha.provider.ssl import (
    SslMaterialHandle,
    SslMaterialProvider,
    provider as ssl_provider_module,
)


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
    assert provider.describe_capabilities(
        resource, requirement, mode='ephemeral',
    ) == {
        'tls.materials': True,
        'tls.cert_path': resource.cert_path,
        'tls.key_path': resource.key_path,
        'tls.ca_cert_path': resource.ca_cert_path,
    }

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


def test_ssl_provider_supports_modes_and_non_handle_guards() -> None:
    provider = SslMaterialProvider()
    requirement = _build_requirement(config={})

    assert provider.supports_mode('live') is True
    assert provider.supports_mode('ephemeral') is True
    assert provider.supports_mode('dry_run') is False

    provider.release(object(), requirement, mode='ephemeral')
    assert (
        provider.health_check(object(), requirement, mode='ephemeral') is False
    )
    assert (
        provider.verify_integrity(
            object(),
            requirement,
            mode='ephemeral',
        )
        is False
    )
    assert (
        provider.describe_external_handle(
            object(),
            requirement,
            mode='ephemeral',
        )
        is None
    )
    assert (
        provider.describe_capabilities(
            object(),
            requirement,
            mode='ephemeral',
        )
        == {}
    )
    provider.revoke_orphan_access(
        'external-handle',
        requirement,
        mode='ephemeral',
    )


def test_ssl_verify_integrity_returns_false_for_invalid_pem_files(
    tmp_path: Path,
) -> None:
    provider = SslMaterialProvider()
    requirement = _build_requirement(config={})
    cert_path = tmp_path / 'server.crt'
    key_path = tmp_path / 'server.key'
    ca_cert_path = tmp_path / 'ca.crt'
    cert_path.write_text('not-a-pem', encoding='utf-8')
    key_path.write_text('not-a-pem', encoding='utf-8')
    ca_cert_path.write_text('not-a-pem', encoding='utf-8')
    resource = SslMaterialHandle(
        cert_path=str(cert_path),
        key_path=str(key_path),
        ca_cert_path=str(ca_cert_path),
        common_name='localhost',
        dns_names=('localhost',),
        ip_addresses=('127.0.0.1',),
        cleanup_policy='preserve',
    )

    assert (
        provider.verify_integrity(resource, requirement, mode='ephemeral')
        is False
    )


def test_ssl_reap_orphan_preserve_does_not_cleanup(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    provider = SslMaterialProvider()
    requirement = _build_requirement(config={})
    calls: list[tuple[tuple[str, ...], str | None]] = []
    monkeypatch.setattr(
        ssl_provider_module,
        '_cleanup_ssl_materials',
        lambda *, managed_paths, tempdir: calls.append(
            (managed_paths, tempdir),
        ),
    )
    fake_managed_path = tmp_path / 'managed.cert'
    fake_tempdir = tmp_path / 'runtime'
    external_handle = json.dumps(
        {
            'cleanup_policy': 'preserve',
            'managed_paths': (str(fake_managed_path),),
            'tempdir': str(fake_tempdir),
        },
    )

    provider.reap_orphan(external_handle, requirement, mode='ephemeral')

    assert calls == []


def test_ssl_internal_helpers_cover_error_paths(tmp_path: Path) -> None:
    expected_days = 7
    assert (
        ssl_provider_module._read_positive_int(
            str(expected_days),
            field_name='days',
        )
        == expected_days
    )
    assert ssl_provider_module._read_tuple_of_str(
        'api.local, localhost',
        field_name='dns_names',
    ) == ('api.local', 'localhost')

    with pytest.raises(ValueError, match='cleanup_policy'):
        ssl_provider_module._normalize_cleanup_policy('bad')
    with pytest.raises(ValueError, match='optional non-empty string'):
        ssl_provider_module._read_optional_str('')
    with pytest.raises(ValueError, match='must be a non-empty string'):
        ssl_provider_module._read_non_empty_str('', field_name='common_name')
    with pytest.raises(ValueError, match='positive integer'):
        ssl_provider_module._read_positive_int(0, field_name='validity_days')
    with pytest.raises(ValueError, match='must contain non-empty strings'):
        ssl_provider_module._read_tuple_of_str([1], field_name='dns_names')
    with pytest.raises(ValueError, match='Invalid SSL external handle'):
        ssl_provider_module._decode_external_handle('{invalid json')
    with pytest.raises(
        ValueError, match='Invalid SSL external handle payload',
    ):
        ssl_provider_module._decode_external_handle('[]')

    managed_file = tmp_path / 'managed.txt'
    managed_file.write_text('temp', encoding='utf-8')
    ssl_provider_module._cleanup_ssl_materials(
        managed_paths=(str(managed_file),),
        tempdir=None,
    )
    assert not managed_file.exists()
