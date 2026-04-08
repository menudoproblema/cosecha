from __future__ import annotations

import builtins
import json
import sqlite3
import sys

from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.config import ConfigSnapshot
from cosecha.shell import launcher


def _snapshot() -> ConfigSnapshot:
    return ConfigSnapshot(
        root_path='/workspace/demo',
        output_mode='summary',
        output_detail='standard',
        capture_log=True,
        stop_on_error=False,
        concurrency=1,
        strict_step_ambiguity=False,
    )


def test_run_runner_cli_and_metadata_helpers(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    captured: dict[str, object] = {}
    monkeypatch.setattr(
        'cosecha.shell.runner_cli.main',
        lambda argv: captured.setdefault('argv', tuple(argv)),
    )
    assert launcher._run_runner_cli(['run']) == 0
    assert captured['argv'] == ('run',)

    assert launcher._load_metadata(tmp_path / 'missing.json') is None

    metadata_path = tmp_path / 'metadata.json'
    metadata_path.write_text('{"session_id":"session-1"}', encoding='utf-8')
    assert launcher._load_metadata(metadata_path) == {'session_id': 'session-1'}

    monkeypatch.setattr(
        launcher.ConfigSnapshot,
        'from_dict',
        classmethod(lambda cls, data: (_ for _ in ()).throw(ValueError('bad'))),
    )
    assert launcher._config_snapshot_from_metadata({'config_snapshot': {'x': 1}}) is None


def test_update_session_artifact_remaining_error_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    assert launcher._update_session_artifact(
        {'session_id': None, 'knowledge_base_path': str(tmp_path / 'kb.db')},
        summary=SimpleNamespace(instrumentation_name='coverage'),
    ) == (None, 'session metadata is incomplete')

    class _NoSummaryKB:
        def query_session_artifacts(self, query):
            del query
            return [SimpleNamespace(report_summary=None)]

        def close(self):
            return None

    monkeypatch.setattr(launcher, 'PersistentKnowledgeBase', lambda db_path: _NoSummaryKB())
    updated, warning = launcher._update_session_artifact(
        {'session_id': 'session-1', 'knowledge_base_path': str(tmp_path / 'kb.db')},
        summary=SimpleNamespace(instrumentation_name='coverage'),
    )
    assert updated is None
    assert warning == 'session artifact session-1 has no report summary'

    monkeypatch.setattr(
        launcher,
        'PersistentKnowledgeBase',
        lambda db_path: (_ for _ in ()).throw(sqlite3.OperationalError('locked')),
    )
    monkeypatch.setattr(launcher.time, 'sleep', lambda seconds: None)
    updated, warning = launcher._update_session_artifact(
        {'session_id': 'session-1', 'knowledge_base_path': str(tmp_path / 'kb.db')},
        summary=SimpleNamespace(instrumentation_name='coverage'),
    )
    assert updated is None
    assert 'failed to reopen knowledge base' in str(warning)


def test_render_and_warning_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rendered: dict[str, object] = {}

    class _Console:
        def print_summary(self, title: str, text: str) -> None:
            rendered['title'] = title
            rendered['text'] = text

    monkeypatch.setattr(launcher.Config, 'console_from_snapshot', lambda snapshot: _Console())
    launcher._render_coverage_summary(
        SimpleNamespace(payload={'total_coverage': 'n/a'}),
        config_snapshot=_snapshot(),
    )
    assert rendered == {}

    launcher._render_coverage_summary(
        SimpleNamespace(
            payload={
                'total_coverage': 91.0,
                'measurement_scope': 7,
                'engine_names': ['pytest'],
                'source_targets': ['src/app.py'],
                'includes_worker_processes': True,
            },
        ),
        config_snapshot=_snapshot(),
    )
    assert rendered['title'] == 'Coverage'
    assert '[controller_process]' in str(rendered['text'])
    assert 'engines: pytest' in str(rendered['text'])
    assert 'sources: src/app.py' in str(rendered['text'])
    assert 'worker processes are included' in str(rendered['text'])

    launcher._emit_coverage_warning('warning', config_snapshot=_snapshot())
    assert rendered['title'] == 'Coverage Warning'


def test_build_launcher_shadow_context(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    workspace = SimpleNamespace()
    execution_context = SimpleNamespace(
        invocation_id='session-1',
        knowledge_storage_root=tmp_path / '.cosecha',
    )
    monkeypatch.setattr(launcher, 'resolve_workspace', lambda start_path: workspace)
    monkeypatch.setattr(launcher, 'build_execution_context', lambda ws, invocation_id: execution_context)
    shadow = launcher._build_launcher_shadow_context()
    assert shadow.root_path == (tmp_path / '.cosecha' / 'shadow' / 'session-1').resolve()


def test_bootstrap_coverage_import_error_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_import = builtins.__import__

    def _broken_import(name, *args, **kwargs):
        if name == 'cosecha.instrumentation.coverage':
            raise ImportError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr('builtins.__import__', _broken_import)
    with pytest.raises(SystemExit, match='2'):
        launcher._bootstrap_coverage(['run', '--cov', 'src'])


def test_bootstrap_coverage_fallback_when_instrumenter_is_not_requested(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _CoverageInstrumenter:
        @classmethod
        def from_argv(cls, argv):
            del argv
            return None

    monkeypatch.setitem(
        sys.modules,
        'cosecha.instrumentation.coverage',
        SimpleNamespace(CoverageInstrumenter=_CoverageInstrumenter),
    )
    monkeypatch.setattr(launcher, '_run_runner_cli', lambda argv: 9)
    assert launcher._bootstrap_coverage(['run']) == 9


def test_bootstrap_coverage_requires_declared_ephemeral_capability(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class _CoverageInstrumenter:
        COSECHA_COMPONENT_ID = 'cosecha.instrumentation.coverage'

        @classmethod
        def from_argv(cls, argv):
            del argv
            return cls()

        @classmethod
        def describe_capabilities(cls):
            return ()

        def strip_bootstrap_options(self, argv):
            return list(argv)

    monkeypatch.setitem(
        sys.modules,
        'cosecha.instrumentation.coverage',
        SimpleNamespace(CoverageInstrumenter=_CoverageInstrumenter),
    )
    monkeypatch.setattr(
        launcher,
        '_build_launcher_shadow_context',
        lambda: launcher.ShadowExecutionContext(root_path=tmp_path / 'shadow').materialize(),
    )
    with pytest.raises(RuntimeError, match='must declare'):
        launcher._bootstrap_coverage(['run', '--cov', 'src'])


def test_bootstrap_coverage_missing_metadata_and_collect_error_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    class _CoverageInstrumenter:
        COSECHA_COMPONENT_ID = 'cosecha.instrumentation.coverage'

        @classmethod
        def from_argv(cls, argv):
            del argv
            return cls()

        @classmethod
        def describe_capabilities(cls):
            return ('x',)

        def strip_bootstrap_options(self, argv):
            return list(argv)

        def prepare(self, *, workdir):
            del workdir
            return SimpleNamespace(argv_prefix=['python'], env={}, workdir_files={}, warnings=())

        def collect(self, *, workdir):
            del workdir
            raise RuntimeError('collect boom')

    monkeypatch.setitem(
        sys.modules,
        'cosecha.instrumentation.coverage',
        SimpleNamespace(CoverageInstrumenter=_CoverageInstrumenter),
    )

    class _Shadow:
        def __init__(self, root_path: Path) -> None:
            self.root_path = root_path
            self.metadata_file = root_path / 'run-metadata.json'

        def env(self):
            return {}

        def cleanup(self, *, preserve: bool):
            return None

    shadow = _Shadow(tmp_path / 'shadow')
    monkeypatch.setattr(launcher, '_build_launcher_shadow_context', lambda: shadow)

    class _Binding:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(
        launcher,
        'binding_shadow',
        lambda *_args, **_kwargs: _Binding(),
    )
    monkeypatch.setattr(
        launcher,
        'acquire_shadow_handle',
        lambda component_id: SimpleNamespace(ephemeral_dir=lambda: tmp_path / 'ephemeral'),
    )
    monkeypatch.setattr(launcher, 'component_id_from_component_type', lambda component_type: 'component-x')
    monkeypatch.setattr(
        launcher,
        'build_ephemeral_artifact_capability',
        lambda descriptors, *, declared_component_id: SimpleNamespace(component_id='component-x'),
    )
    monkeypatch.setattr(
        launcher.subprocess,
        'run',
        lambda command, *, check, env: SimpleNamespace(returncode=4),
    )
    warnings: list[str] = []
    monkeypatch.setattr(
        launcher,
        '_emit_coverage_warning',
        lambda message, *, config_snapshot=None: warnings.append(message),
    )

    assert launcher._bootstrap_coverage(['run', '--cov', 'src']) == 4
    assert any('no session metadata was written' in message for message in warnings)

    shadow.metadata_file.parent.mkdir(parents=True, exist_ok=True)
    shadow.metadata_file.write_text(
        json.dumps({'session_id': 'session-1', 'knowledge_base_path': None}),
        encoding='utf-8',
    )
    monkeypatch.setattr(
        launcher,
        '_load_metadata',
        lambda path: {'session_id': 'session-1', 'knowledge_base_path': None},
    )
    warnings.clear()
    assert launcher._bootstrap_coverage(['run', '--cov', 'src']) == 4
    assert any('failed to collect coverage' in message for message in warnings)
