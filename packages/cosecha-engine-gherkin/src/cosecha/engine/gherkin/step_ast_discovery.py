from __future__ import annotations

import ast
import re

from dataclasses import dataclass
from hashlib import sha256
from typing import TYPE_CHECKING, Literal


if TYPE_CHECKING:  # pragma: no cover
    from pathlib import Path

    from cosecha.engine.gherkin.types import StepType


type DiscoveryMode = Literal['ast', 'fallback_import']

_STEP_DECORATORS: frozenset[str] = frozenset(
    {'but', 'given', 'step', 'then', 'when'},
)
_SUPPORTED_MODULES: frozenset[str] = frozenset(
    {
        'cosecha.engine.gherkin',
        'cosecha.engine.gherkin.steps',
    },
)
_MIN_ANCHOR_TOKEN_LENGTH = 3


@dataclass(slots=True, frozen=True)
class StaticStepDescriptor:
    step_type: StepType
    patterns: tuple[str, ...]
    source_file: Path
    source_line: int
    function_name: str
    file_path: Path
    module_import_path: str | None
    literal_prefixes: tuple[str, ...]
    literal_suffixes: tuple[str, ...]
    literal_fragments: tuple[str, ...]
    anchor_tokens: tuple[str, ...]
    dynamic_fragment_count: int = 0
    documentation: str | None = None
    parser_cls_name: str | None = None
    category: str | None = None
    discovery_mode: DiscoveryMode = 'ast'
    mtime_ns: int = 0
    file_size: int = 0


@dataclass(slots=True, frozen=True)
class StaticDiscoveredStepFile:
    file_path: Path
    module_import_path: str | None
    descriptors: tuple[StaticStepDescriptor, ...]
    discovery_mode: DiscoveryMode
    requires_fallback_import: bool
    content_digest: str
    mtime_ns: int
    file_size: int


@dataclass(slots=True, frozen=True)
class _FileDiscoveryContext:
    file_path: Path
    module_import_path: str | None
    mtime_ns: int
    file_size: int


@dataclass(slots=True)
class _ImportTable:
    module_aliases: dict[str, str]
    symbol_aliases: dict[str, str]
    unsupported_module_aliases: set[str]
    unsupported_symbol_aliases: set[str]


def _literal_prefix(pattern: str) -> str:
    bracket_index = pattern.find('{')
    if bracket_index == -1:
        return pattern

    return pattern[:bracket_index]


def _literal_suffix(pattern: str) -> str:
    closing_bracket_index = pattern.rfind('}')
    if closing_bracket_index == -1:
        return pattern

    return pattern[closing_bracket_index + 1 :]


def _extract_literal_suffixes(
    patterns: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(_literal_suffix(pattern) for pattern in patterns)


def _extract_literal_fragments(
    patterns: tuple[str, ...],
) -> tuple[str, ...]:
    literal_fragments: list[str] = []
    seen_fragments: set[str] = set()
    for pattern in patterns:
        for fragment in re.split(r'\{[^}]*\}', pattern):
            normalized_fragment = fragment.strip().lower()
            if (
                not normalized_fragment
                or normalized_fragment in seen_fragments
            ):
                continue
            seen_fragments.add(normalized_fragment)
            literal_fragments.append(normalized_fragment)

    return tuple(literal_fragments)


def _extract_anchor_tokens(patterns: tuple[str, ...]) -> tuple[str, ...]:
    anchor_tokens: list[str] = []
    seen_tokens: set[str] = set()
    for pattern in patterns:
        literal_fragments = tuple(
            fragment.strip().lower()
            for fragment in re.split(r'\{[^}]*\}', pattern)
            if fragment.strip()
        )
        for fragment in literal_fragments:
            for token in re.findall(r'[a-z0-9_]+', fragment):
                if (
                    len(token) < _MIN_ANCHOR_TOKEN_LENGTH
                    or token in seen_tokens
                ):
                    continue
                seen_tokens.add(token)
                anchor_tokens.append(token)

    return tuple(anchor_tokens)


def _count_dynamic_fragments(patterns: tuple[str, ...]) -> int:
    return max(
        (len(re.findall(r'\{[^}]*\}', pattern)) for pattern in patterns),
        default=0,
    )


def _resolve_module_import_path(
    root_path: Path,
    file_path: Path,
) -> str | None:
    try:
        relative_parts = file_path.relative_to(root_path).with_suffix('').parts
    except Exception:
        return None

    if not relative_parts:
        return None

    return '.'.join(relative_parts)


def _build_content_digest(source: str) -> str:
    return sha256(source.encode('utf-8')).hexdigest()


def _build_import_table(module: ast.Module) -> _ImportTable:
    module_aliases: dict[str, str] = {}
    symbol_aliases: dict[str, str] = {}
    unsupported_module_aliases: set[str] = set()
    unsupported_symbol_aliases: set[str] = set()

    for statement in module.body:
        if isinstance(statement, ast.ImportFrom):
            module_name = statement.module
            if module_name not in _SUPPORTED_MODULES:
                continue

            for alias in statement.names:
                if alias.name not in _STEP_DECORATORS:
                    continue
                target_name = alias.asname or alias.name
                symbol_aliases[target_name] = alias.name
                if alias.asname is not None:
                    unsupported_symbol_aliases.add(target_name)

        elif isinstance(statement, ast.Import):
            for alias in statement.names:
                if alias.name not in _SUPPORTED_MODULES:
                    continue

                alias_name = (
                    alias.asname or alias.name.rsplit('.', maxsplit=1)[-1]
                )
                module_aliases[alias_name] = alias.name
                if alias.asname is not None:
                    unsupported_module_aliases.add(alias_name)

    return _ImportTable(
        module_aliases=module_aliases,
        symbol_aliases=symbol_aliases,
        unsupported_module_aliases=unsupported_module_aliases,
        unsupported_symbol_aliases=unsupported_symbol_aliases,
    )


def _references_unsupported_step_decorator(
    decorator: ast.expr,
    import_table: _ImportTable,
) -> bool:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator

    if (
        isinstance(target, ast.Name)
        and target.id in import_table.unsupported_symbol_aliases
    ):
        return True

    return (
        isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.value.id in import_table.unsupported_module_aliases
        and target.attr in _STEP_DECORATORS
    )


def _resolve_decorator_name(
    decorator: ast.expr,
    import_table: _ImportTable,
) -> StepType | None:
    target = decorator.func if isinstance(decorator, ast.Call) else decorator

    if isinstance(target, ast.Name):
        if target.id in _STEP_DECORATORS:
            return target.id  # type: ignore[return-value]

        alias_target = import_table.symbol_aliases.get(target.id)
        if alias_target in _STEP_DECORATORS:
            return alias_target  # type: ignore[return-value]

        return None

    if (
        isinstance(target, ast.Attribute)
        and isinstance(target.value, ast.Name)
        and target.attr in _STEP_DECORATORS
    ):
        module_name = import_table.module_aliases.get(target.value.id)
        if module_name in _SUPPORTED_MODULES:
            return target.attr  # type: ignore[return-value]

    return None


def _extract_parser_cls_name(value: ast.expr) -> str | None:
    if isinstance(value, ast.Constant) and value.value is None:
        return None

    if isinstance(value, ast.Name):
        return value.id

    if isinstance(value, ast.Attribute):
        parts: list[str] = [value.attr]
        current = value.value
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if isinstance(current, ast.Name):
            parts.append(current.id)
            return '.'.join(reversed(parts))

    msg = 'Unsupported parser_cls expression'
    raise ValueError(msg)


def _extract_category(value: ast.expr) -> str | None:
    if isinstance(value, ast.Constant):
        if value.value is None:
            return None
        if isinstance(value.value, str):
            return value.value

    msg = 'Unsupported category expression'
    raise ValueError(msg)


def _extract_patterns(call: ast.Call) -> tuple[str, ...]:
    patterns: list[str] = []
    for argument in call.args:
        if isinstance(argument, ast.Constant) and isinstance(
            argument.value,
            str,
        ):
            patterns.append(argument.value)
            continue

        msg = 'Unsupported step pattern expression'
        raise ValueError(msg)

    if not patterns:
        msg = 'Step decorator requires at least one literal pattern'
        raise ValueError(msg)

    return tuple(patterns)


def _extract_descriptor(
    node: ast.AsyncFunctionDef | ast.FunctionDef,
    decorator: ast.expr,
    import_table: _ImportTable,
    file_context: _FileDiscoveryContext,
) -> StaticStepDescriptor | None:
    if _references_unsupported_step_decorator(decorator, import_table):
        msg = 'Unsupported aliased step decorator'
        raise ValueError(msg)

    step_type = _resolve_decorator_name(decorator, import_table)
    if step_type is None:
        return None

    if not isinstance(decorator, ast.Call):
        msg = 'Unsupported bare step decorator'
        raise ValueError(msg)

    patterns = _extract_patterns(decorator)
    parser_cls_name: str | None = None
    category: str | None = None

    for keyword in decorator.keywords:
        if keyword.arg == 'parser_cls':
            parser_cls_name = _extract_parser_cls_name(keyword.value)
            continue

        if keyword.arg == 'category':
            category = _extract_category(keyword.value)
            continue

        msg = f'Unsupported keyword argument: {keyword.arg!r}'
        raise ValueError(msg)

    return StaticStepDescriptor(
        step_type=step_type,
        patterns=patterns,
        source_file=file_context.file_path,
        source_line=node.lineno,
        function_name=node.name,
        file_path=file_context.file_path,
        module_import_path=file_context.module_import_path,
        literal_prefixes=tuple(
            _literal_prefix(pattern) for pattern in patterns
        ),
        literal_suffixes=_extract_literal_suffixes(patterns),
        literal_fragments=_extract_literal_fragments(patterns),
        anchor_tokens=_extract_anchor_tokens(patterns),
        dynamic_fragment_count=_count_dynamic_fragments(patterns),
        documentation=ast.get_docstring(node),
        parser_cls_name=parser_cls_name,
        category=category,
        mtime_ns=file_context.mtime_ns,
        file_size=file_context.file_size,
    )


class StepDiscoveryService:
    def __init__(self, root_path: Path) -> None:
        self.root_path = root_path.resolve()

    def discover_step_file(
        self,
        file_path: Path,
    ) -> StaticDiscoveredStepFile:
        resolved_file_path = file_path.resolve()
        stat = resolved_file_path.stat()
        file_context = _FileDiscoveryContext(
            file_path=resolved_file_path,
            module_import_path=_resolve_module_import_path(
                self.root_path,
                resolved_file_path,
            ),
            mtime_ns=stat.st_mtime_ns,
            file_size=stat.st_size,
        )

        try:
            source = resolved_file_path.read_text(encoding='utf-8')
            module = ast.parse(source, filename=str(resolved_file_path))
        except Exception:
            return StaticDiscoveredStepFile(
                file_path=resolved_file_path,
                module_import_path=file_context.module_import_path,
                descriptors=(),
                discovery_mode='fallback_import',
                requires_fallback_import=True,
                content_digest=(
                    _build_content_digest(source)
                    if 'source' in locals()
                    else ''
                ),
                mtime_ns=file_context.mtime_ns,
                file_size=file_context.file_size,
            )

        import_table = _build_import_table(module)
        descriptors: list[StaticStepDescriptor] = []
        requires_fallback_import = False

        for node in module.body:
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue

            for decorator in node.decorator_list:
                try:
                    descriptor = _extract_descriptor(
                        node,
                        decorator,
                        import_table,
                        file_context,
                    )
                except ValueError:
                    if (
                        _resolve_decorator_name(decorator, import_table)
                        is None
                    ):
                        if _references_unsupported_step_decorator(
                            decorator,
                            import_table,
                        ):
                            requires_fallback_import = True
                        continue
                    requires_fallback_import = True
                    continue

                if descriptor is not None:
                    descriptors.append(descriptor)

        discovery_mode: DiscoveryMode = (
            'fallback_import' if requires_fallback_import else 'ast'
        )
        return StaticDiscoveredStepFile(
            file_path=resolved_file_path,
            module_import_path=file_context.module_import_path,
            descriptors=tuple(descriptors),
            discovery_mode=discovery_mode,
            requires_fallback_import=requires_fallback_import,
            content_digest=_build_content_digest(source),
            mtime_ns=file_context.mtime_ns,
            file_size=file_context.file_size,
        )

    def discover_step_files(
        self,
        file_paths: tuple[Path, ...],
    ) -> tuple[StaticDiscoveredStepFile, ...]:
        return tuple(
            self.discover_step_file(file_path) for file_path in file_paths
        )
