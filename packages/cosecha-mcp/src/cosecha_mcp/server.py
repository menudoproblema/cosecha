from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from cosecha_mcp.service import CosechaMcpService


SERVICE = CosechaMcpService()
MCP_SERVER = FastMCP('Cosecha MCP', json_response=True)


@MCP_SERVER.tool()
def describe_workspace(start_path: str | None = None) -> dict[str, object]:
    """Describe the active Cosecha workspace and resolved test root."""
    return SERVICE.describe_workspace(start_path=start_path)


@MCP_SERVER.tool()
def describe_knowledge_base(
    start_path: str | None = None,
) -> dict[str, object]:
    """Inspect kb.db path, freshness, counts and latest persisted metadata."""
    return SERVICE.describe_knowledge_base(start_path=start_path)


@MCP_SERVER.tool()
def describe_path_freshness(
    path: str,
    engine_name: str | None = None,
    include_children: bool = True,
    limit: int = 200,
    start_path: str | None = None,
) -> dict[str, object]:
    """Compare indexed knowledge for one path against files on disk."""
    return SERVICE.describe_path_freshness(
        path=path,
        engine_name=engine_name,
        include_children=include_children,
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
def search_catalog(
    query: str,
    kinds: list[str] | None = None,
    engine_name: str | None = None,
    limit: int = 20,
    start_path: str | None = None,
) -> dict[str, object]:
    """Search persisted Cosecha knowledge by substring."""
    return SERVICE.search_catalog(
        query,
        kinds=kinds,
        engine_name=engine_name,
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def query_tests(
    engine_name: str | None = None,
    test_path: str | None = None,
    status: str | None = None,
    failure_kind: str | None = None,
    node_stable_id: str | None = None,
    plan_id: str | None = None,
    limit: int | None = None,
    start_path: str | None = None,
) -> dict[str, object]:
    """Query indexed tests from the persistent Cosecha knowledge base."""
    return await SERVICE.query_tests(
        engine_name=engine_name,
        test_path=test_path,
        status=status,
        failure_kind=failure_kind,
        node_stable_id=node_stable_id,
        plan_id=plan_id,
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def query_definitions(
    engine_name: str | None = None,
    file_path: str | None = None,
    step_type: str | None = None,
    step_text: str | None = None,
    discovery_mode: str | None = None,
    include_invalidated: bool = True,
    limit: int | None = None,
    start_path: str | None = None,
) -> dict[str, object]:
    """Query indexed definition knowledge with engine-aware step matching."""
    return await SERVICE.query_definitions(
        engine_name=engine_name,
        file_path=file_path,
        step_type=step_type,
        step_text=step_text,
        discovery_mode=discovery_mode,
        include_invalidated=include_invalidated,
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def query_registry_items(
    engine_name: str | None = None,
    module_spec: str | None = None,
    package_hash: str | None = None,
    layout_key: str | None = None,
    loader_schema_version: str | None = None,
    limit: int | None = None,
    start_path: str | None = None,
) -> dict[str, object]:
    """Query indexed registry layouts and loader snapshots."""
    return await SERVICE.query_registry_items(
        engine_name=engine_name,
        module_spec=module_spec,
        package_hash=package_hash,
        layout_key=layout_key,
        loader_schema_version=loader_schema_version,
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def query_resources(
    name: str | None = None,
    scope: str | None = None,
    last_test_id: str | None = None,
    limit: int | None = None,
    start_path: str | None = None,
) -> dict[str, object]:
    """Query resource lifecycle knowledge indexed by Cosecha."""
    return await SERVICE.query_resources(
        name=name,
        scope=scope,
        last_test_id=last_test_id,
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def read_session_artifacts(
    session_id: str | None = 'last',
    trace_id: str | None = None,
    limit: int | None = None,
    start_path: str | None = None,
) -> dict[str, object]:
    """Read persisted session artifacts for the requested or latest session."""
    return await SERVICE.read_session_artifacts(
        session_id=session_id,
        trace_id=trace_id,
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def list_recent_sessions(
    limit: int = 20,
    start_path: str | None = None,
) -> dict[str, object]:
    """List recent persisted Cosecha sessions from session artifacts."""
    return await SERVICE.list_recent_sessions(
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def describe_session_coverage(
    session_id: str | None = 'last',
    start_path: str | None = None,
) -> dict[str, object]:
    """Return the coverage instrumentation summary for a Cosecha session.

    Use ``session_id='last'`` (default) to describe the most recently
    persisted session, or pass an explicit session id to target a
    specific run. Passing ``None`` is rejected with an explicit error.

    The response always includes ``workspace``, ``session_id``,
    ``recorded_at``, ``has_coverage``, ``coverage_summary`` and
    ``total_coverage``. ``has_coverage`` is ``False`` when the session
    was not instrumented with ``--cov``, when the artifact is not yet
    visible, or when no session has been recorded at all; in those
    cases ``reason`` explains which case applied.

    When ``has_coverage`` is ``True``, ``coverage_summary`` contains an
    ``InstrumentationSummary`` with ``instrumentation_name='coverage'``
    and a ``payload`` with the following fields:

    - ``total_coverage``: float, percentage 0-100.
    - ``report_type``: ``'term'`` or ``'term-missing'``.
    - ``measurement_scope``: currently always ``'controller_process'``.
    - ``branch``: whether branch coverage was enabled.
    - ``source_targets``: the source roots coverage was measured over.
    - ``engine_names``: engines that participated in the run.
    - ``includes_python_subprocesses``: ``True`` when coverage was
      propagated to Python subprocesses spawned by the runner via
      ``COVERAGE_PROCESS_START`` and ``patch = subprocess``.
    - ``includes_worker_processes``: ``True`` only when Cosecha
      persistent workers are explicitly covered. In v1 this is always
      ``False`` and multi-worker runs should be interpreted as
      controller-only coverage.

    ``total_coverage`` at the top level is a convenience duplicate of
    ``coverage_summary.payload.total_coverage``.
    """
    return await SERVICE.describe_session_coverage(
        session_id=session_id,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def list_coverage_history(
    limit: int = 20,
    start_path: str | None = None,
) -> dict[str, object]:
    """List recent Cosecha sessions that recorded coverage instrumentation.

    Returns ``entries`` ordered by ``recorded_at`` descending (most
    recent first), skipping sessions that did not run with ``--cov``.
    ``limit`` bounds the number of persisted sessions inspected, not
    the number of entries returned: if the newest N sessions have no
    coverage, ``entries`` will be empty even with a large limit.

    Each entry contains:

    - ``session_id``, ``recorded_at``, ``has_failures``.
    - ``total_coverage``: float, percentage 0-100.
    - ``branch``: whether branch coverage was enabled for that run.
    - ``source_targets``: the source roots coverage was measured over.
    - ``measurement_scope``: see ``describe_session_coverage``.

    Consumers can compute trends or regressions by comparing
    ``entries[0]`` against ``entries[1:]``. Entries with failing tests
    are included so callers can decide whether to ignore them.
    """
    return await SERVICE.list_coverage_history(
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def inspect_test_plan(
    test_path: str | None = None,
    paths: list[str] | None = None,
    selection_labels: list[str] | None = None,
    test_limit: int | None = None,
    mode: str = 'relaxed',
    start_path: str | None = None,
) -> dict[str, object]:
    """Analyze the plan Cosecha would build for selected paths or labels."""
    return await SERVICE.inspect_test_plan(
        test_path=test_path,
        paths=paths,
        selection_labels=selection_labels,
        test_limit=test_limit,
        mode=mode,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def get_execution_timeline(
    session_id: str | None = 'last',
    plan_id: str | None = None,
    node_stable_id: str | None = None,
    event_type: str | None = None,
    limit: int | None = 200,
    start_path: str | None = None,
) -> dict[str, object]:
    """Return domain events for the latest or requested execution timeline."""
    return await SERVICE.get_execution_timeline(
        session_id=session_id,
        plan_id=plan_id,
        node_stable_id=node_stable_id,
        event_type=event_type,
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
def list_test_execution_history(
    test_path: str | None = None,
    engine_name: str | None = None,
    status: str | None = None,
    session_id: str | None = None,
    limit: int = 100,
    start_path: str | None = None,
) -> dict[str, object]:
    """List historical test.finished events from the domain event log."""
    return SERVICE.list_test_execution_history(
        test_path=test_path,
        engine_name=engine_name,
        status=status,
        session_id=session_id,
        limit=limit,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def list_engines_and_capabilities(
    selected_engines: list[str] | None = None,
    start_path: str | None = None,
) -> dict[str, object]:
    """List active engines and capability snapshots materialized from the manifest."""
    return await SERVICE.list_engines_and_capabilities(
        selected_engines=selected_engines,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def refresh_knowledge_base(
    paths: list[str] | None = None,
    selection_labels: list[str] | None = None,
    test_limit: int | None = None,
    mode: str = 'strict',
    rebuild: bool = False,
    start_path: str | None = None,
) -> dict[str, object]:
    """Refresh or rebuild kb.db by running plan analysis without executing tests."""
    return await SERVICE.refresh_knowledge_base(
        paths=paths,
        selection_labels=selection_labels,
        test_limit=test_limit,
        mode=mode,
        rebuild=rebuild,
        start_path=start_path,
    )


@MCP_SERVER.tool()
async def run_tests(
    paths: list[str] | None = None,
    selection_labels: list[str] | None = None,
    test_limit: int | None = None,
    selected_engines: list[str] | None = None,
    start_path: str | None = None,
) -> dict[str, object]:
    """Execute tests for the selected paths or labels in the active workspace.

    **Opt-in required**: this tool is disabled by default and raises
    ``PermissionError`` unless ``COSECHA_MCP_ENABLE_RUN_TESTS=1`` is
    set in the environment where cosecha-mcp runs.

    **Selection semantics**:

    - ``paths`` accepts file or directory paths relative to the tests
      root (or absolute paths inside it). File-level granularity only:
      Cosecha v1 does NOT support pytest-style test selectors like
      ``tests/foo.py::test_bar`` or ``tests/foo.py::TestClass::test_m``.
      Passing a ``::``-qualified path matches zero tests and the
      underlying ``cosecha run`` exits with code 5
      (``no tests collected``), so the operation result will reflect
      an empty selection rather than a spurious success. If you need
      to run a single test today, run its full file instead.
    - ``selection_labels`` filters by tags/labels, if the engines use
      them. Prefix with ``~`` to exclude (e.g. ``~slow``).
    - ``test_limit`` bounds the number of tests executed AFTER
      selection. It does NOT select a specific test: ``test_limit=1``
      runs an arbitrary single test from the selection.
    - ``selected_engines`` restricts execution to a subset of engines
      (e.g. ``['pytest']`` or ``['gherkin']``).

    **Rerun-failed pattern** (manual composition, no dedicated tool):

    1. Call ``read_session_artifacts(session_id='last')`` and read
       ``report_summary.failed_files`` to get failed file paths, or
       call ``query_tests(status='failed')`` for per-test records.
    2. Pass those paths to ``run_tests(paths=...)``. Because Cosecha
       only supports file granularity, this reruns every test in each
       failed file, not only the individual failing tests.
    3. After the rerun, call ``read_session_artifacts`` again to check
       the new ``has_failures`` and the updated failed set.

    **Return shape**: ``has_failures`` (bool), ``knowledge_base``
    (freshness snapshot), ``selected_engines``, plus the serialized
    operation result.
    """
    return await SERVICE.run_tests(
        paths=paths,
        selection_labels=selection_labels,
        test_limit=test_limit,
        selected_engines=selected_engines,
        start_path=start_path,
    )


def main() -> None:
    MCP_SERVER.run(transport='stdio')
