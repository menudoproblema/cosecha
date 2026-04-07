from __future__ import annotations

import tomllib

from pathlib import Path

from cosecha.core.cosecha_manifest import load_cosecha_manifest
from cosecha.core.manifest_selection import engine_path_matches_request


def _load_pyproject() -> dict[str, object]:
    pyproject_path = Path(__file__).resolve().parents[1] / "pyproject.toml"
    return tomllib.loads(pyproject_path.read_text(encoding="utf-8"))


def test_cosecha_default_dependencies_keep_cli_bundle_minimal() -> None:
    project = _load_pyproject()["project"]
    dependencies = set(project["dependencies"])

    assert "cosecha-core" in dependencies
    assert "cosecha-engine-pytest" in dependencies
    assert "cosecha-reporter-console" in dependencies
    assert "cosecha-engine-gherkin" not in dependencies
    assert "cosecha-plugin-timing" not in dependencies


def test_cosecha_scripts_and_hook_entry_points_live_in_cli_package() -> None:
    project = _load_pyproject()["project"]
    scripts = project["scripts"]
    entry_points = project["entry-points"]

    assert scripts["cosecha"] == "cosecha.shell.launcher:main"
    assert entry_points["cosecha.hooks"]["mochuelo_runtime_service"] == (
        "cosecha.shell.mochuelo_runtime:MochueloRuntimeServiceHookDescriptor"
    )


def test_cosecha_optional_dependencies_stay_outside_default_bundle() -> None:
    project = _load_pyproject()["project"]
    optional_dependencies = project["optional-dependencies"]

    assert optional_dependencies["gherkin"] == ["cosecha-engine-gherkin"]
    assert optional_dependencies["json"] == ["cosecha-reporter-json"]
    assert optional_dependencies["junit"] == ["cosecha-reporter-junit"]
    assert optional_dependencies["http"] == ["cosecha-provider-http"]
    assert optional_dependencies["http-uvicorn"] == [
        "cosecha-provider-http[uvicorn]",
    ]
    assert optional_dependencies["http-hypercorn"] == [
        "cosecha-provider-http[hypercorn]",
    ]
    assert optional_dependencies["mongo"] == ["cosecha-provider-mongodb"]
    assert optional_dependencies["ssl"] == ["cosecha-provider-ssl"]
    assert optional_dependencies["coverage"] == ["cosecha-instrumentation-coverage"]
    assert optional_dependencies["timing"] == ["cosecha-plugin-timing"]
    assert optional_dependencies["lsp"] == ["cosecha-lsp"]
    assert optional_dependencies["mcp"] == ["cosecha-mcp"]
    assert optional_dependencies["devtools"] == [
        "cosecha-lsp",
        "cosecha-mcp",
    ]
    assert optional_dependencies["all"] == [
        "cosecha-engine-gherkin",
        "cosecha-reporter-json",
        "cosecha-reporter-junit",
        "cosecha-provider-http[uvicorn]",
        "cosecha-provider-mongodb",
        "cosecha-provider-ssl",
        "cosecha-instrumentation-coverage",
        "cosecha-plugin-timing",
    ]
    assert optional_dependencies["full"] == [
        "cosecha-engine-gherkin",
        "cosecha-reporter-json",
        "cosecha-reporter-junit",
        "cosecha-provider-http[uvicorn]",
        "cosecha-provider-mongodb",
        "cosecha-provider-ssl",
        "cosecha-instrumentation-coverage",
        "cosecha-plugin-timing",
        "cosecha-lsp",
        "cosecha-mcp",
    ]


def test_cosecha_cli_package_keeps_only_shell_surface() -> None:
    package_root = Path(__file__).resolve().parents[1] / "src" / "cosecha"
    forbidden_modules = (
        package_root / "config.py",
        package_root / "cosecha_manifest.py",
        package_root / "exceptions.py",
        package_root / "hooks.py",
        package_root / "items.py",
        package_root / "runtime_profiles.py",
        package_root / "utils.py",
        package_root / "engines",
    )

    for forbidden_path in forbidden_modules:
        assert not forbidden_path.exists(), str(forbidden_path)


def test_cosecha_lsp_exposes_canonical_script_outside_cli_bundle() -> None:
    pyproject_path = (
        Path(__file__).resolve().parents[3]
        / "packages"
        / "cosecha-lsp"
        / "pyproject.toml"
    )
    project = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))["project"]

    assert project["scripts"]["cosecha-lsp"] == "cosecha_lsp.lsp_server:main"
    assert project["scripts"]["granjero"] == "cosecha_lsp.lsp_server:main"
    assert "cosecha" not in project["dependencies"]
    assert "cosecha-core" in project["dependencies"]
    assert "cosecha-engine-gherkin" in project["dependencies"]
    assert "pygls==1.3.1" in project["dependencies"]
    assert "lsprotocol==2023.0.1" in project["dependencies"]


def test_cosecha_core_no_longer_depends_on_parse() -> None:
    pyproject_path = (
        Path(__file__).resolve().parents[3]
        / "packages"
        / "cosecha-core"
        / "pyproject.toml"
    )
    project = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))["project"]

    assert "parse==1.21.0" not in project["dependencies"]


def test_cosecha_core_and_console_stack_no_longer_depend_on_rich() -> None:
    package_names = (
        "cosecha-core",
        "cosecha-reporter-console",
        "cosecha-instrumentation-coverage",
    )

    for package_name in package_names:
        pyproject_path = (
            Path(__file__).resolve().parents[3]
            / "packages"
            / package_name
            / "pyproject.toml"
        )
        project = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))["project"]
        assert not any(
            dependency.startswith("rich") for dependency in project["dependencies"]
        )


def test_gherkin_engine_depends_on_parse_but_not_rich() -> None:
    pyproject_path = (
        Path(__file__).resolve().parents[3]
        / "packages"
        / "cosecha-engine-gherkin"
        / "pyproject.toml"
    )
    project = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))["project"]

    assert "parse==1.21.0" in project["dependencies"]
    assert not any(
        dependency.startswith("rich") for dependency in project["dependencies"]
    )
    assert (
        project["entry-points"]["cosecha.console.presenters"]["gherkin"]
        == "cosecha.engine.gherkin.discovery:GherkinConsolePresenter"
    )


def test_timing_plugin_uses_namespace_package_convention() -> None:
    pyproject_path = (
        Path(__file__).resolve().parents[3]
        / "packages"
        / "cosecha-plugin-timing"
        / "pyproject.toml"
    )
    project = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))["project"]

    assert (
        project["entry-points"]["cosecha.plugins"]["timing"]
        == "cosecha.plugin.timing:TimingPlugin"
    )


def test_runtime_dependency_policy_uses_exact_versions() -> None:
    package_expectations = {
        "cosecha-core": {"cxp==2.0.0", "msgspec==0.20.0"},
        "cosecha-engine-gherkin": {
            "gherkin-official==38.0.0",
            "parse==1.21.0",
        },
        "cosecha-engine-pytest": {"pytest==9.0.2"},
        "cosecha-lsp": {"lsprotocol==2023.0.1", "pygls==1.3.1"},
        "cosecha-mcp": {"mcp==1.27.0"},
        "cosecha-instrumentation-coverage": {"coverage==7.13.3"},
    }

    base_path = Path(__file__).resolve().parents[3] / "packages"
    for package_name, expected_dependencies in package_expectations.items():
        project = tomllib.loads(
            (base_path / package_name / "pyproject.toml").read_text(
                encoding="utf-8",
            ),
        )["project"]
        dependencies = set(project["dependencies"])
        assert expected_dependencies.issubset(dependencies)


def test_only_cosecha_core_depends_directly_on_cxp() -> None:
    base_path = Path(__file__).resolve().parents[3] / "packages"
    package_names = sorted(path.name for path in base_path.iterdir() if path.is_dir())

    for package_name in package_names:
        project = tomllib.loads(
            (base_path / package_name / "pyproject.toml").read_text(
                encoding="utf-8",
            ),
        )["project"]
        dependencies = set(project.get("dependencies", ()))
        if package_name == "cosecha-core":
            assert "cxp==2.0.0" in dependencies
            continue
        assert not any(
            dependency == "cxp"
            or dependency.startswith("cxp==")
            or dependency.startswith("cxp>=")
            for dependency in dependencies
        )


def test_workspace_manifest_enables_pytest_for_all_package_test_roots() -> None:
    workspace_root = Path(__file__).resolve().parents[3]
    manifest = load_cosecha_manifest(workspace_root / "cosecha.toml")

    assert manifest is not None

    pytest_engine_paths = tuple(
        engine.path for engine in manifest.engines if engine.type == "pytest"
    )
    package_test_roots = tuple(
        sorted(
            path.relative_to(workspace_root).as_posix()
            for path in (workspace_root / "packages").glob("*/tests")
            if path.is_dir()
        )
    )

    assert pytest_engine_paths
    assert package_test_roots
    assert all(
        any(
            engine_path_matches_request(engine_path, package_test_root)
            for engine_path in pytest_engine_paths
        )
        for package_test_root in package_test_roots
    )
