from __future__ import annotations

import asyncio

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from cosecha.core.engines.base import ExecutionContextMetadata
from cosecha.core.exceptions import CosechaParserError
from cosecha.core.items import TestPreflightDecision, TestResultStatus
from cosecha.core.reporter import QueuedReporter
from cosecha.engine.gherkin import engine as engine_module
from cosecha.engine.gherkin.collector import GherkinCollector, _parse_feature_content
from cosecha.engine.gherkin.engine import (
    GherkinEngine,
    _build_definition_descriptor_knowledge,
    _cast_optional_int,
    _cast_optional_str,
)
from cosecha.engine.gherkin.step_ast_discovery import (
    StaticDiscoveredStepFile,
    StaticStepDescriptor,
)
from cosecha.engine.gherkin.step_catalog import StepCatalog
from cosecha_internal.testkit import DummyReporter, build_config


class _FakeDomainEventStream:
    def __init__(self) -> None:
        self.events: list[object] = []

    async def emit(self, event: object) -> None:
        self.events.append(event)


def _build_engine(tmp_path: Path, **kwargs) -> GherkinEngine:
    engine = GherkinEngine('gherkin', **kwargs)
    config = build_config(tmp_path)
    engine.initialize(config, '')
    return engine


def _build_discovered_file(
    file_path: Path,
    *,
    digest: str = 'hash',
    discovery_mode: str = 'ast',
) -> StaticDiscoveredStepFile:
    descriptor = StaticStepDescriptor(
        step_type='given',
        patterns=('a step',),
        source_file=file_path,
        source_line=1,
        function_name='step_fn',
        file_path=file_path,
        module_import_path='steps.demo',
        literal_prefixes=('a step',),
        literal_suffixes=('a step',),
        literal_fragments=('a step',),
        anchor_tokens=('step',),
        parser_cls_name='ParseStepMatcher',
        discovery_mode='ast',
    )
    return StaticDiscoveredStepFile(
        file_path=file_path,
        module_import_path='steps.demo',
        descriptors=(descriptor,),
        discovery_mode=discovery_mode,  # type: ignore[arg-type]
        requires_fallback_import=discovery_mode == 'fallback_import',
        content_digest=digest,
        mtime_ns=1,
        file_size=1,
    )


def _build_test_item(root_path: Path, feature_path: Path):
    feature = _parse_feature_content(
        '\n'.join(
            (
                'Feature: Demo',
                '  Scenario: Works',
                '    Given a step',
            ),
        ),
        feature_path,
        root_path.parent,
    )
    return SimpleNamespace(
        feature=feature,
        scenario=feature.scenarios[0],
        path=feature_path,
        test_name='Scenario: Works',
    )


def test_constructor_and_initialize_paths(tmp_path: Path) -> None:
    with pytest.raises(TypeError, match='Gherkin collector'):
        GherkinEngine('gherkin', collector=SimpleNamespace())  # type: ignore[arg-type]

    override_dir = (tmp_path / 'defs').resolve()
    engine = GherkinEngine(
        'gherkin',
        reporter=DummyReporter(),
        definition_paths=(override_dir,),
    )
    config = build_config(tmp_path)
    config.concurrency = 2
    config.definition_paths = (tmp_path / 'configured',)
    engine.initialize(config, '')
    assert isinstance(engine.reporter, QueuedReporter)
    assert override_dir in config.definition_paths
    assert engine.step_registry is not None


def test_build_live_snapshot_payload_returns_phase_snapshot(
    tmp_path: Path,
) -> None:
    engine = _build_engine(tmp_path)
    test = _build_test_item(
        tmp_path,
        (tmp_path / 'suite' / 'demo.feature').resolve(),
    )
    node = SimpleNamespace(test=test)

    payload = engine.build_live_snapshot_payload(node, 'setup')

    assert payload == {
        'current_phase': 'setup',
        'feature_name': 'Demo',
        'scenario_name': 'Works',
        'step_count': 1,
        'test_path': str((tmp_path / 'suite' / 'demo.feature').resolve()),
    }


@pytest.mark.parametrize(
    ('feature', 'scenario'),
    (
        (None, SimpleNamespace(name='Works', all_steps=())),
        (SimpleNamespace(name='Demo'), None),
    ),
)
def test_build_live_snapshot_payload_returns_none_without_feature_or_scenario(
    tmp_path: Path,
    feature,
    scenario,
) -> None:
    engine = _build_engine(tmp_path)
    test = SimpleNamespace(
        feature=feature,
        scenario=scenario,
        path=(tmp_path / 'suite' / 'demo.feature').resolve(),
    )
    node = SimpleNamespace(test=test)

    assert engine.build_live_snapshot_payload(node, 'setup') is None


@pytest.mark.asyncio
async def test_collect_refreshes_lazy_resolver_and_binds_resources(
    monkeypatch,
    tmp_path: Path,
) -> None:
    engine = _build_engine(tmp_path)
    bound_requirements: list[tuple[object, ...]] = []
    test = SimpleNamespace(
        bind_manifest_resources=lambda requirements: bound_requirements.append(
            tuple(requirements),
        ),
    )
    engine.shared_resource_requirements = (SimpleNamespace(name='db'),)

    async def _fake_collect(self, _path=None, _excluded_paths=()):
        self.collector.collected_tests = (test,)
        self.collector.steps_directories = {
            (tmp_path / 'steps').resolve(),
        }

    monkeypatch.setattr(
        'cosecha.core.engines.base.Engine.collect',
        _fake_collect,
    )
    await engine.collect()
    assert bound_requirements
    assert engine.definition_catalog_directories
    assert engine.library_definition_knowledge_loaded is False
    assert engine.lazy_step_resolver is not None


def test_resolver_and_import_helpers(monkeypatch, tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    file_path = (tmp_path / 'steps.py').resolve()
    hook = SimpleNamespace(
        library_import_targets_by_file={file_path: 'package.steps'},
        library_discovered_step_files=(_build_discovered_file(file_path),),
    )
    engine.hooks = (hook,)
    engine.collector.knowledge_store = SimpleNamespace(
        get_discovered_step_files=lambda: (_build_discovered_file(file_path),),
    )

    combined = engine._build_combined_step_index()
    assert combined.find_candidate_files('given', 'a step') == (file_path,)
    assert engine._resolve_library_import_target(file_path) == 'package.steps'
    assert engine._resolve_library_import_target(tmp_path / 'other.py') == (
        tmp_path / 'other.py'
    ).resolve()
    engine.hooks = (SimpleNamespace(library_import_targets_by_file='invalid'),)
    assert engine._resolve_library_import_target(file_path) == file_path
    engine.hooks = (hook,)

    imported: list[tuple[str | Path, tuple[str, ...] | None]] = []
    monkeypatch.setattr(
        engine_module,
        'import_and_load_steps_from_module',
        lambda target, _registry, function_names=None: imported.append(
            (target, function_names),
        ),
    )
    engine._import_step_definition_file(file_path, SimpleNamespace(), ('step_fn',))
    assert imported == [('package.steps', ('step_fn',))]


@pytest.mark.asyncio
async def test_start_and_finish_session_wait_pending_tasks(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    engine.lazy_step_resolver = None

    await engine.start_session()
    assert engine.lazy_step_resolver is not None

    async def _ok():
        return None

    async def _boom():
        raise RuntimeError('ignore')

    engine.pending_domain_event_tasks = {
        asyncio.create_task(_ok()),
        asyncio.create_task(_boom()),
    }
    await engine.finish_session()
    assert engine.pending_domain_event_tasks == set()


@pytest.mark.asyncio
async def test_generate_context_emits_step_events_with_and_without_metadata(
    tmp_path: Path,
) -> None:
    engine = _build_engine(tmp_path)
    stream = _FakeDomainEventStream()
    engine._domain_event_stream = stream
    engine.lazy_step_resolver = SimpleNamespace(
        loaded_files={(tmp_path / 'steps.py').resolve()},
        find_candidate_files=lambda _type, _text: (
            (tmp_path / 'steps.py').resolve(),
        ),
    )
    test = _build_test_item(tmp_path, (tmp_path / 'suite' / 'demo.feature').resolve())
    context = await engine.generate_new_context(test)

    step = test.scenario.all_steps[0]
    await context.notify_step_started(step)
    assert stream.events == []

    context.set_execution_metadata(
        ExecutionContextMetadata(
            node_id='node-1',
            node_stable_id='stable-1',
            session_id='session',
            plan_id='plan',
            trace_id='trace',
            worker_id=3,
        ),
    )
    await context.notify_step_started(step)
    await context.notify_step_finished(step, status='passed', message='ok')
    assert len(stream.events) == 4

    engine.lazy_step_resolver = None
    context_without_snapshot = await engine.generate_new_context(test)
    context_without_snapshot.set_execution_metadata(
        ExecutionContextMetadata(
            node_id='node-2',
            node_stable_id='stable-2',
        ),
    )
    await context_without_snapshot.notify_step_finished(step, status='failed')
    assert len(stream.events) == 5

    engine._domain_event_stream = None
    context_without_stream = await engine.generate_new_context(test)
    context_without_stream.set_execution_metadata(
        ExecutionContextMetadata(
            node_id='node-3',
            node_stable_id='stable-3',
        ),
    )
    await context_without_stream.notify_step_started(step)


def test_preflight_and_index_accessors(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    skipped = engine.preflight_test(SimpleNamespace(scenario=SimpleNamespace(all_steps=())))
    assert isinstance(skipped, TestPreflightDecision)
    assert skipped.status == TestResultStatus.SKIPPED
    assert engine.preflight_test(SimpleNamespace(scenario=SimpleNamespace(all_steps=(1,)))) is None

    project_index = StepCatalog()
    engine.collector.knowledge_store = SimpleNamespace(
        get_project_step_index=lambda: project_index,
    )
    assert engine.get_project_definition_index() is project_index

    hook_without_files = SimpleNamespace()
    hook_with_files = SimpleNamespace(
        library_discovered_step_files=(
            _build_discovered_file((tmp_path / 'lib.py').resolve()),
        ),
    )
    engine.hooks = (hook_without_files, hook_with_files)
    library_index = engine.get_library_definition_index()
    assert library_index.find_candidate_files('given', 'a step')


@pytest.mark.asyncio
async def test_load_tests_from_content_delegates_to_collector(
    monkeypatch,
    tmp_path: Path,
) -> None:
    engine = _build_engine(tmp_path)
    expected = [SimpleNamespace(name='test')]

    async def _load_from_content(_self, _content: str, _path: Path):
        return expected

    monkeypatch.setattr(
        GherkinCollector,
        'load_tests_from_content',
        _load_from_content,
    )
    loaded = await engine.load_tests_from_content('Feature: x', tmp_path / 'x.feature')
    assert loaded == expected


@pytest.mark.asyncio
async def test_validate_draft_and_resolve_definition_paths(
    monkeypatch,
    tmp_path: Path,
) -> None:
    engine = _build_engine(tmp_path)
    feature_path = (tmp_path / 'suite' / 'demo.feature').resolve()
    feature_path.parent.mkdir(parents=True, exist_ok=True)

    async def _noop_loaded(self, _test_path: Path) -> None:
        del self
        return None

    monkeypatch.setattr(
        GherkinEngine,
        '_ensure_definition_catalog_loaded',
        _noop_loaded,
    )

    parser_error = CosechaParserError('bad', feature_path, 2, 3)
    async def _raise_parser_error(*_args):
        raise parser_error

    monkeypatch.setattr(
        GherkinCollector,
        'load_tests_from_content',
        _raise_parser_error,
    )
    parser_result = await engine.validate_draft('Feature: broken', feature_path)
    assert parser_result.issues[0].code == 'gherkin_parser_error'

    async def _return_empty_tests(*_args):
        return []

    monkeypatch.setattr(
        GherkinCollector,
        'load_tests_from_content',
        _return_empty_tests,
    )
    empty_result = await engine.validate_draft('Feature: empty', feature_path)
    assert empty_result.issues[0].code == 'no_executable_tests'

    test = SimpleNamespace(get_required_step_texts=lambda: (('given', 'a step'),))
    async def _return_single_test(*_args):
        return [test]

    monkeypatch.setattr(
        GherkinCollector,
        'load_tests_from_content',
        _return_single_test,
    )
    step_index = SimpleNamespace(
        find_candidate_files_for_steps=lambda _queries: {(tmp_path / 'a.py').resolve()},
        find_candidate_files=lambda *_args: set(),
    )
    library_index = SimpleNamespace(
        find_candidate_files_for_steps=lambda _queries: set(),
        find_candidate_files=lambda *_args: set(),
    )
    monkeypatch.setattr(
        GherkinCollector,
        'get_project_step_index',
        lambda _self: step_index,
    )
    monkeypatch.setattr(
        GherkinEngine,
        'get_library_definition_index',
        lambda _self: library_index,
    )
    monkeypatch.setattr(
        GherkinEngine,
        '_build_draft_validation_issues_for_tests',
        lambda _self, _tests: (),
    )
    validated = await engine.validate_draft('Feature: demo', feature_path)
    assert validated.test_count == 1
    assert validated.issues and validated.issues[0].code == 'missing_step_candidates'

    static_definition = SimpleNamespace(file_path='x.py')
    monkeypatch.setattr(
        GherkinEngine,
        '_resolve_static_definitions',
        lambda *_args, **_kwargs: (static_definition,),
    )
    resolved_static = await engine.resolve_definition(
        test_path=feature_path,
        step_type='given',
        step_text='a step',
    )
    assert resolved_static == (static_definition,)

    monkeypatch.setattr(
        GherkinEngine,
        '_resolve_static_definitions',
        lambda *_args, **_kwargs: (),
    )
    engine.step_registry = SimpleNamespace(find_match=lambda *_args: None)
    resolved_none = await engine.resolve_definition(
        test_path=feature_path,
        step_type='given',
        step_text='a step',
    )
    assert resolved_none == ()

    runtime_match = SimpleNamespace(
        step_definition=SimpleNamespace(
            location=SimpleNamespace(filename='runtime.py', line=10, column=2),
            func=SimpleNamespace(__name__='step_runtime', __doc__='doc'),
            category='runtime',
            step_type='given',
            step_text_list=(SimpleNamespace(text='a step'),),
        ),
    )
    engine.step_registry = SimpleNamespace(find_match=lambda *_args: runtime_match)
    resolved_runtime = await engine.resolve_definition(
        test_path=feature_path,
        step_type='given',
        step_text='a step',
    )
    assert resolved_runtime[0].resolution_source == 'runtime_registry'


def test_draft_issue_helpers_and_static_resolution(tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    engine.context_registry.add('layout', 'known', object())

    step = SimpleNamespace(
        text='a step',
        step_type='given',
        keyword='Given ',
        location=SimpleNamespace(line=4, column=3),
        table=None,
    )
    missing_issue = engine._build_draft_validation_step_issues(step, None)
    assert missing_issue[0].code == 'missing_step_definition'

    step_text = SimpleNamespace(
        min_table_rows=1,
        required_table_rows=0,
        can_use_table=True,
        layouts=(),
    )
    match = SimpleNamespace(step_text=step_text, arguments=())
    assert engine._build_draft_validation_table_issue(step, step_text, 0, 0) is not None
    step.table = SimpleNamespace(rows=())
    assert engine._build_draft_validation_table_issue(step, step_text, 0, 0) is not None

    step_text.min_table_rows = 0
    step_text.required_table_rows = 2
    step.table = None
    assert engine._build_draft_validation_table_issue(step, step_text, 0, 0) is not None
    step.table = SimpleNamespace(rows=(1,))
    assert engine._build_draft_validation_table_issue(step, step_text, 0, 0) is not None

    step_text.required_table_rows = 0
    step_text.can_use_table = False
    step.table = SimpleNamespace(rows=(1,))
    assert engine._build_draft_validation_table_issue(step, step_text, 0, 0) is not None

    step_text.can_use_table = True
    step.table = None
    layout_match = SimpleNamespace(
        step_text=SimpleNamespace(
            min_table_rows=0,
            required_table_rows=0,
            can_use_table=True,
            layouts=(SimpleNamespace(place_holder='name', layout='layout'),),
        ),
        arguments=(
            SimpleNamespace(name='other', value='ignored', start_column=1),
            SimpleNamespace(name='name', value='unknown', start_column=2),
            SimpleNamespace(name='name', value='<placeholder>', start_column=5),
            SimpleNamespace(name='name', value='known', start_column=7),
        ),
    )
    layout_issues = engine._build_draft_validation_layout_issues(
        layout_match,
        line_num=2,
        start_column=3,
    )
    assert len(layout_issues) == 1
    assert layout_issues[0].code == 'unknown_layout_reference'

    engine.step_registry = SimpleNamespace(find_match=lambda *_args: None)
    scenario = SimpleNamespace(all_steps=(step,))
    issues = engine._build_draft_validation_issues_for_tests(
        [SimpleNamespace(scenario=None), SimpleNamespace(scenario=scenario)],
    )
    assert issues
    required_table_match = SimpleNamespace(
        step_text=SimpleNamespace(
            min_table_rows=1,
            required_table_rows=0,
            can_use_table=True,
            layouts=(),
        ),
        arguments=(),
    )
    assert (
        engine._build_draft_validation_step_issues(step, required_table_match)[
            0
        ].code
        == 'missing_data_table'
    )

    step_text_without_requirements = SimpleNamespace(
        min_table_rows=0,
        required_table_rows=0,
        can_use_table=True,
        layouts=(),
    )
    match_without_issues = SimpleNamespace(
        step_text=step_text_without_requirements,
        arguments=(),
    )
    assert (
        engine._build_draft_validation_table_issue(
            step,
            step_text_without_requirements,
            0,
            0,
        )
        is None
    )
    assert engine._build_draft_validation_step_issues(step, match_without_issues) == ()

    file_path = (tmp_path / 'steps.py').resolve()
    empty_index = SimpleNamespace(
        find_candidate_files=lambda *_args: (),
        descriptors_for_file=lambda _file_path: (),
    )
    assert (
        engine._resolve_static_definitions(
            empty_index,
            step_type='given',
            step_text='a alice',
        )
        == ()
    )

    descriptor = StaticStepDescriptor(
        step_type='given',
        patterns=('a {name}',),
        source_file=file_path,
        source_line=1,
        function_name='fn',
        file_path=file_path,
        module_import_path='steps.demo',
        literal_prefixes=('a ',),
        literal_suffixes=(),
        literal_fragments=('a',),
        anchor_tokens=('name',),
        parser_cls_name='ParseStepMatcher',
    )
    step_index = SimpleNamespace(
        find_candidate_files=lambda *_args: (file_path,),
        descriptors_for_file=lambda _file_path: (descriptor, descriptor),
    )
    resolved = engine._resolve_static_definitions(
        step_index,
        step_type='given',
        step_text='a alice',
    )
    assert len(resolved) == 1
    assert engine._descriptor_matches_step_text(descriptor, 'a alice')
    assert engine._descriptor_supports_static_resolution(descriptor)

    non_conflicting_descriptor = replace(descriptor, step_type='when')
    non_conflicting_index = SimpleNamespace(
        find_candidate_files=lambda *_args: (file_path,),
        descriptors_for_file=lambda _file_path: (non_conflicting_descriptor,),
    )
    assert (
        engine._resolve_static_definitions(
            non_conflicting_index,
            step_type='given',
            step_text='a alice',
        )
        == ()
    )

    non_matching_descriptor = replace(descriptor, patterns=('different text',))
    non_matching_index = SimpleNamespace(
        find_candidate_files=lambda *_args: (file_path,),
        descriptors_for_file=lambda _file_path: (non_matching_descriptor,),
    )
    assert (
        engine._resolve_static_definitions(
            non_matching_index,
            step_type='given',
            step_text='a alice',
        )
        == ()
    )

    unsupported_descriptor = replace(descriptor, parser_cls_name='Custom')
    unsupported_index = SimpleNamespace(
        find_candidate_files=lambda *_args: (file_path,),
        descriptors_for_file=lambda _file_path: (unsupported_descriptor,),
    )
    assert (
        engine._resolve_static_definitions(
            unsupported_index,
            step_type='given',
            step_text='a alice',
        )
        == ()
    )


@pytest.mark.asyncio
async def test_prime_completions_snapshot_and_materialization_handlers(
    monkeypatch,
    tmp_path: Path,
) -> None:
    engine = _build_engine(tmp_path)
    candidate_calls: list[tuple[tuple[tuple[str, str], ...], tuple[Path, ...]]] = []
    engine.lazy_step_resolver = SimpleNamespace(
        prime_candidate_files=lambda required, files: candidate_calls.append(
            (required, files),
        ),
    )
    engine.prime_execution_node(
        SimpleNamespace(
            required_step_texts=(('given', 'x'),),
            step_candidate_files=('steps/demo.py',),
        ),
    )
    assert candidate_calls
    engine.prime_execution_node(SimpleNamespace(required_step_texts=(), step_candidate_files=()))

    monkeypatch.setattr(
        engine_module,
        'build_step_completion_suggestions',
        lambda *_args, **_kwargs: ('completion',),
    )
    assert engine.suggest_step_completions(
        step_type='given',
        initial_text='a',
        cursor_column=2,
        start_step_text_column=1,
    ) == ('completion',)

    assert engine._resolve_snapshot_path(str(tmp_path)).is_absolute()
    assert engine._resolve_snapshot_path('relative.feature') == (
        tmp_path / 'relative.feature'
    ).resolve()

    failures: list[str] = []
    engine.config.diagnostics.error = lambda message, **_kwargs: failures.append(message)  # type: ignore[method-assign]
    engine.collector.failed_files = set()
    engine._handle_step_materialization_failure(tmp_path / 'f.py', 'tb')
    assert failures and (tmp_path / 'f.py') in engine.collector.failed_files

    engine.collector.step_catalog = SimpleNamespace(
        fallback_files={(tmp_path / 'fallback.py')},
        descriptors_for_file=lambda _file_path: (1, 2),
    )
    stream = _FakeDomainEventStream()
    engine._domain_event_stream = stream
    engine.pending_domain_event_tasks = set()
    engine._handle_definition_materialized(tmp_path / 'fallback.py')
    await asyncio.sleep(0)
    assert stream.events
    engine._domain_event_stream = None
    engine._handle_definition_materialized(tmp_path / 'fallback.py')


@pytest.mark.asyncio
async def test_library_knowledge_loading_and_catalog_ensure(monkeypatch, tmp_path: Path) -> None:
    engine = _build_engine(tmp_path)
    load_calls: list[Path] = []
    file_a = (tmp_path / 'lib_a.py').resolve()
    file_b = (tmp_path / 'lib_b.py').resolve()
    async def _load_no_stream(root: Path) -> None:
        load_calls.append(root)

    hook = SimpleNamespace(
        library_discovered_step_files=[_build_discovered_file(file_a, digest='old')],
        load_library_definition_knowledge=_load_no_stream,
    )
    hook_without_loader = SimpleNamespace(library_discovered_step_files=[])
    engine.hooks = (hook_without_loader, hook)

    engine._domain_event_stream = None
    await engine._load_library_definition_knowledge()
    assert load_calls == [tmp_path]
    assert engine.library_definition_knowledge_loaded is True

    stream = _FakeDomainEventStream()
    engine._domain_event_stream = stream

    async def _load_and_mutate(_root_path: Path) -> None:
        hook.library_discovered_step_files = [_build_discovered_file(file_b, digest='new')]

    hook.load_library_definition_knowledge = _load_and_mutate
    engine.library_definition_knowledge_loaded = False
    hook_without_loader.library_discovered_step_files = []
    await engine._load_library_definition_knowledge()
    assert engine.library_definition_knowledge_loaded is True
    assert stream.events

    ensure_calls: list[str] = []

    async def _find_steps(self, _test_path: Path) -> None:
        del self
        ensure_calls.append('find')
        engine.collector.steps_directories = {tmp_path / 'steps'}

    async def _build_catalog(self) -> None:
        del self
        ensure_calls.append('build')

    async def _load_library(self) -> None:
        del self
        ensure_calls.append('library')

    refreshed: list[str] = []
    monkeypatch.setattr(
        GherkinCollector,
        'find_step_impl_directories',
        _find_steps,
    )
    monkeypatch.setattr(
        GherkinCollector,
        'build_step_catalog',
        _build_catalog,
    )
    monkeypatch.setattr(
        GherkinEngine,
        '_load_library_definition_knowledge',
        _load_library,
    )
    monkeypatch.setattr(
        GherkinEngine,
        'refresh_lazy_step_resolver',
        lambda _self: refreshed.append('refresh'),
    )
    engine.definition_catalog_directories = ()
    engine.library_definition_knowledge_loaded = False
    await engine._ensure_definition_catalog_loaded(tmp_path / 'suite' / 'x.feature')
    assert ensure_calls == ['find', 'build', 'library']
    assert refreshed == ['refresh', 'refresh']

    context_hook_calls: list[bool] = []
    async def _ensure_registry(_engine) -> None:
        context_hook_calls.append(True)

    engine.hooks = (
        SimpleNamespace(),
        SimpleNamespace(ensure_context_registry_loaded=_ensure_registry),
    )
    await engine._ensure_context_registry_loaded()
    assert context_hook_calls == [True]


def test_cast_helpers_and_definition_knowledge_helper(tmp_path: Path) -> None:
    file_path = (tmp_path / 'steps.py').resolve()
    descriptor = _build_discovered_file(file_path).descriptors[0]
    knowledge = _build_definition_descriptor_knowledge(descriptor)
    assert knowledge.payload['step_type'] == 'given'
    assert _cast_optional_str(None) is None
    assert _cast_optional_str(5) == '5'
    assert _cast_optional_int(None) is None
    assert _cast_optional_int('7') == 7
