from __future__ import annotations

import json

from pathlib import Path

from cosecha.core.config import ConfigSnapshot
from cosecha.core.instrumentation import (
    COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
)
from cosecha.core.runner import Runner
from cosecha.core.session_artifacts import SessionArtifact


def _build_artifact() -> SessionArtifact:
    return SessionArtifact(
        session_id='session-1',
        root_path='/workspace/demo',
        config_snapshot=ConfigSnapshot(
            root_path='/workspace/demo',
            output_mode='summary',
            output_detail='standard',
            capture_log=True,
            stop_on_error=False,
            concurrency=1,
            strict_step_ambiguity=False,
        ),
        capability_snapshots=(),
        recorded_at=1.0,
    )


def test_write_instrumentation_metadata_file_is_atomic(
    tmp_path,
    monkeypatch,
) -> None:
    metadata_path = tmp_path / 'instrumentation-metadata.json'
    monkeypatch.setenv(
        COSECHA_INSTRUMENTATION_METADATA_FILE_ENV,
        str(metadata_path),
    )

    runner = object.__new__(Runner)
    artifact = _build_artifact()
    db_path = tmp_path / 'kb.db'

    Runner._write_instrumentation_metadata_file(
        runner,
        artifact,
        db_path=db_path,
    )

    assert metadata_path.exists()
    assert not Path(f'{metadata_path}.tmp').exists()
    assert json.loads(metadata_path.read_text(encoding='utf-8')) == {
        'knowledge_base_path': str(db_path),
        'root_path': artifact.root_path,
        'session_id': artifact.session_id,
    }
