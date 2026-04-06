from __future__ import annotations

import pytest

from cosecha.core.manifest_loader import parse_cosecha_manifest_text
from cosecha.core.resources import (
    ResourceRequirement,
    validate_resource_requirements,
)


INITIALIZATION_TIMEOUT_SECONDS = 12.5
READINESS_MAX_WAIT_SECONDS = 3


def test_parse_resource_spec_keeps_config_and_initialization_fields(
    tmp_path,
) -> None:
    manifest = parse_cosecha_manifest_text(
        """
[manifest]
schema_version = 1

[[engines]]
id = "pytest"
type = "pytest"
name = "Pytest"
path = "tests"

[[resources]]
name = "seed"
provider = "cosecha.provider.mongodb:MongoResourceProvider"
scope = "run"
mode = "ephemeral"
backend = "mock"
database_name = "seed_db"

[[resources]]
name = "mongo"
provider = "cosecha.provider.mongodb:MongoResourceProvider"
scope = "run"
mode = "ephemeral"
depends_on = ["seed"]
initializes_from = ["seed"]
initialization_mode = "state_snapshot"
initialization_timeout_seconds = 12.5

[resources.config]
backend = "standalone"
database_prefix = "suite"
cleanup_policy = "drop"

[resources.readiness_policy]
retry_interval_seconds = 0.25
max_wait_seconds = 3

[[resource_bindings]]
engine_type = "pytest"
resource_name = "mongo"
fixture_name = "mongo"
""",
        manifest_path=tmp_path / 'cosecha.toml',
        schema_version=1,
        iter_hook_descriptors=lambda: (),
        resolve_engine_descriptor=lambda _engine_type: type(
            '_PytestDescriptor',
            (),
            {
                'validate_resource_binding': staticmethod(
                    lambda binding, *, manifest: None,
                ),
            },
        ),
    )

    mongo_spec = manifest.find_resource('mongo')
    requirement = mongo_spec.build_requirement(root_path=tmp_path)

    assert mongo_spec.config == {
        'backend': 'standalone',
        'database_prefix': 'suite',
        'cleanup_policy': 'drop',
    }
    assert mongo_spec.initializes_from == ('seed',)
    assert mongo_spec.initialization_mode == 'state_snapshot'
    assert (
        mongo_spec.initialization_timeout_seconds
        == INITIALIZATION_TIMEOUT_SECONDS
    )
    assert (
        mongo_spec.readiness_policy.max_wait_seconds
        == READINESS_MAX_WAIT_SECONDS
    )
    assert requirement.config == mongo_spec.config
    assert requirement.initializes_from == ('seed',)
    assert requirement.initialization_mode == 'state_snapshot'
    assert (
        requirement.initialization_timeout_seconds
        == INITIALIZATION_TIMEOUT_SECONDS
    )


def test_validate_resource_requirements_rejects_narrower_scope_dependency(
) -> None:
    with pytest.raises(
        ValueError,
        match=(
            r"Resource 'application/http' has invalid scope dependency: "
            r"'application/http' \(run\) depends_on 'database/main' \(test\)"
        ),
    ):
        validate_resource_requirements(
            (
                ResourceRequirement(
                    name='database/main',
                    setup=object,
                    scope='test',
                ),
                ResourceRequirement(
                    name='application/http',
                    setup=object,
                    scope='run',
                    depends_on=('database/main',),
                ),
            ),
        )


def test_validate_resource_requirements_rejects_narrower_scope_initializer(
) -> None:
    with pytest.raises(
        ValueError,
        match=(
            r"Resource 'application/http' has invalid initialization "
            r"source scope: 'application/http' \(worker\) initializes_from "
            r"'database/seed' \(test\)"
        ),
    ):
        validate_resource_requirements(
            (
                ResourceRequirement(
                    name='database/seed',
                    setup=object,
                    scope='test',
                ),
                ResourceRequirement(
                    name='application/http',
                    setup=object,
                    scope='worker',
                    initializes_from=('database/seed',),
                ),
            ),
        )
