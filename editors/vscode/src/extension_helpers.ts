import * as path from 'node:path';

export type SessionArtifactLike = {
    has_failures?: boolean | null;
    recorded_at: number;
    report_summary?: Record<string, unknown> | null;
};

export type LanguageServerAction = 'skip' | 'start' | 'restart';
export type LanguageServerAutostartPolicy = 'allowed' | 'blocked';
export type RealpathLookup = (inputPath: string) => string | undefined;
export type SelectionLabelCarrier = {
    selection_labels?: string[] | readonly string[] | null;
};
export type AnchoredTestLike = SelectionLabelCarrier & {
    source_line?: number | null;
    test_name?: string | null;
};
export type AnchoredTestGroup<T extends AnchoredTestLike> = {
    anchorLine: number;
    tests: T[];
};

export function resolveCodeLensAnchorLine(options: {
    documentLines: string[];
    hintedSourceLine?: number | null;
    testName?: string | null;
}): number | undefined {
    const { documentLines, hintedSourceLine, testName } = options;
    const normalizedTestName = normalizeScenarioAnchorText(testName);

    if (normalizedTestName) {
        const hintedIndex = Math.max(0, (hintedSourceLine ?? 1) - 1);
        const exactMatchNearHint = findMatchingLineIndex(
            documentLines,
            normalizedTestName,
            Math.max(0, hintedIndex - 8),
            Math.min(documentLines.length - 1, hintedIndex),
        );
        if (exactMatchNearHint != null) {
            return exactMatchNearHint;
        }

        const exactMatchAnywhere = findMatchingLineIndex(
            documentLines,
            normalizedTestName,
            0,
            documentLines.length - 1,
        );
        if (exactMatchAnywhere != null) {
            return exactMatchAnywhere;
        }
    }

    if (hintedSourceLine != null && hintedSourceLine > 0) {
        return hintedSourceLine - 1;
    }

    return undefined;
}

export function collectSelectionLabelCounts(
    tests: SelectionLabelCarrier[],
): Map<string, number> {
    const counts = new Map<string, number>();
    for (const test of tests) {
        const labels = test.selection_labels;
        if (!Array.isArray(labels)) {
            continue;
        }
        for (const rawLabel of labels) {
            if (typeof rawLabel !== 'string') {
                continue;
            }
            const label = rawLabel.trim();
            if (!label) {
                continue;
            }
            counts.set(label, (counts.get(label) ?? 0) + 1);
        }
    }

    return new Map(
        Array.from(counts.entries()).sort(([left], [right]) =>
            left.localeCompare(right),
        ),
    );
}

export function groupTestsByAnchor<T extends AnchoredTestLike>(options: {
    documentLines: string[];
    tests: T[];
}): Map<number, AnchoredTestGroup<T>> {
    const groups = new Map<number, AnchoredTestGroup<T>>();
    for (const test of options.tests) {
        const anchorLine = resolveCodeLensAnchorLine({
            documentLines: options.documentLines,
            hintedSourceLine: test.source_line,
            testName: test.test_name,
        });
        if (anchorLine == null || anchorLine < 0) {
            continue;
        }

        const group = groups.get(anchorLine);
        if (group) {
            group.tests.push(test);
            continue;
        }

        groups.set(anchorLine, {
            anchorLine,
            tests: [test],
        });
    }

    return groups;
}

export function resolveConfiguredInterpreterPath(
    configuredPath: string | undefined,
    workspacePath: string,
): string | undefined {
    if (!configuredPath || configuredPath.trim().length === 0) {
        return undefined;
    }

    const expandedPath = configuredPath
        .replaceAll('${workspaceFolder}', workspacePath)
        .replaceAll('${workspaceRoot}', workspacePath)
        .trim();
    const resolvedPath = path.isAbsolute(expandedPath)
        ? expandedPath
        : path.resolve(workspacePath, expandedPath);

    return path.normalize(resolvedPath);
}

export function selectPreferredWorkspacePath(options: {
    activeDocumentPath?: string;
    targetPath?: string;
    workspacePaths: string[];
}): string | undefined {
    const { activeDocumentPath, targetPath, workspacePaths } = options;
    const targetWorkspace = findContainingWorkspacePath(
        targetPath,
        workspacePaths,
    );
    if (targetWorkspace) {
        return targetWorkspace;
    }

    const activeWorkspace = findContainingWorkspacePath(
        activeDocumentPath,
        workspacePaths,
    );
    if (activeWorkspace) {
        return activeWorkspace;
    }

    return workspacePaths[0];
}

export function determineLanguageServerAction(options: {
    enabled: boolean;
    nextWorkspacePath?: string;
    runningWorkspacePath?: string;
}): LanguageServerAction {
    const { enabled, nextWorkspacePath, runningWorkspacePath } = options;
    if (!enabled || !nextWorkspacePath) {
        return 'skip';
    }
    if (!runningWorkspacePath) {
        return 'start';
    }
    if (runningWorkspacePath === nextWorkspacePath) {
        return 'skip';
    }

    return 'restart';
}

export function determineLanguageServerAutostartPolicy(options: {
    blockedWorkspacePath?: string;
    force?: boolean;
    workspacePath?: string;
}): LanguageServerAutostartPolicy {
    const { blockedWorkspacePath, force, workspacePath } = options;
    if (force || !workspacePath || !blockedWorkspacePath) {
        return 'allowed';
    }
    if (blockedWorkspacePath !== workspacePath) {
        return 'allowed';
    }

    return 'blocked';
}

export function preferWorkspaceInterpreterShim(options: {
    interpreterPath: string;
    workspaceInterpreterPaths: string[];
    realpathLookup: RealpathLookup;
}): string {
    const {
        interpreterPath,
        realpathLookup,
        workspaceInterpreterPaths,
    } = options;
    const normalizedInterpreterPath = path.normalize(interpreterPath);
    if (workspaceInterpreterPaths.includes(normalizedInterpreterPath)) {
        return normalizedInterpreterPath;
    }

    const interpreterRealpath = realpathLookup(normalizedInterpreterPath);
    if (!interpreterRealpath) {
        return normalizedInterpreterPath;
    }

    for (const workspaceInterpreterPath of workspaceInterpreterPaths) {
        const normalizedWorkspaceInterpreterPath = path.normalize(
            workspaceInterpreterPath,
        );
        const workspaceInterpreterRealpath = realpathLookup(
            normalizedWorkspaceInterpreterPath,
        );
        if (
            workspaceInterpreterRealpath
            && workspaceInterpreterRealpath === interpreterRealpath
        ) {
            return normalizedWorkspaceInterpreterPath;
        }
    }

    return normalizedInterpreterPath;
}

export function shouldPreferWorkspaceInterpreterShim(
    source: string,
): boolean {
    return source === 'python.interpreterPath';
}

export function normalizeLanguageServerFailureMessage(
    rawMessage: string | undefined,
): string | undefined {
    if (!rawMessage) {
        return undefined;
    }

    const lines = rawMessage
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line.length > 0);
    if (lines.length === 0) {
        return undefined;
    }

    return lines[lines.length - 1];
}

export function isNonRecoverableLanguageServerFailure(
    rawMessage: string | undefined,
): boolean {
    const normalized = normalizeLanguageServerFailureMessage(rawMessage)
        ?.toLowerCase();
    if (!normalized) {
        return false;
    }

    return (
        normalized.includes('no solution found when resolving dependencies')
        || normalized.includes('requirements are unsatisfiable')
        || normalized.includes('module not found')
        || normalized.includes('modulenotfounderror')
        || normalized.includes('unknown runtime interface')
        || normalized.includes('no se ha encontrado cosecha.toml')
    );
}

export function readSessionStatusCounts(
    session: SessionArtifactLike,
): Map<string, number> {
    const reportSummary = asRecord(session.report_summary);
    const statusCounts = reportSummary?.status_counts;
    const counts = new Map<string, number>();

    if (Array.isArray(statusCounts)) {
        for (const entry of statusCounts) {
            if (!Array.isArray(entry) || entry.length < 2) {
                continue;
            }
            const [name, rawCount] = entry;
            if (typeof name !== 'string' || typeof rawCount !== 'number') {
                continue;
            }
            counts.set(name, rawCount);
        }
        return counts;
    }

    const statusCountsRecord = asRecord(statusCounts);
    if (!statusCountsRecord) {
        return counts;
    }

    for (const [name, rawCount] of Object.entries(statusCountsRecord)) {
        if (typeof rawCount === 'number') {
            counts.set(name, rawCount);
        }
    }

    return counts;
}

export function formatSessionStatusCounts(
    statusCounts: Map<string, number>,
): string | undefined {
    const segments = [
        formatStatusCountSegment(statusCounts, 'passed'),
        formatStatusCountSegment(statusCounts, 'failed'),
        formatStatusCountSegment(statusCounts, 'error'),
        formatStatusCountSegment(statusCounts, 'skipped'),
        formatStatusCountSegment(statusCounts, 'running'),
        formatStatusCountSegment(statusCounts, 'pending'),
    ].filter((value): value is string => Boolean(value));

    return segments.length > 0 ? segments.join(' · ') : undefined;
}

function findContainingWorkspacePath(
    candidatePath: string | undefined,
    workspacePaths: string[],
): string | undefined {
    if (!candidatePath) {
        return undefined;
    }

    const normalizedCandidatePath = path.resolve(candidatePath);
    let bestMatch: string | undefined;
    for (const workspacePath of workspacePaths) {
        const normalizedWorkspacePath = path.resolve(workspacePath);
        if (
            normalizedCandidatePath !== normalizedWorkspacePath
            && !normalizedCandidatePath.startsWith(
                `${normalizedWorkspacePath}${path.sep}`,
            )
        ) {
            continue;
        }
        if (!bestMatch || normalizedWorkspacePath.length > bestMatch.length) {
            bestMatch = normalizedWorkspacePath;
        }
    }

    return bestMatch;
}

function formatStatusCountSegment(
    statusCounts: Map<string, number>,
    status: string,
): string | undefined {
    const count = statusCounts.get(status);
    if (typeof count !== 'number' || count <= 0) {
        return undefined;
    }

    return `${status} ${count}`;
}

function findMatchingLineIndex(
    documentLines: string[],
    expectedText: string,
    startIndex: number,
    endIndex: number,
): number | undefined {
    for (let index = startIndex; index <= endIndex; index += 1) {
        if (documentLines[index]?.trim() === expectedText) {
            return index;
        }
    }

    return undefined;
}

export function normalizeScenarioAnchorText(
    rawTestName: string | null | undefined,
): string | undefined {
    const trimmed = rawTestName?.trim();
    if (!trimmed) {
        return undefined;
    }

    return trimmed.replace(/\s+\[Example #[^\]]+\]$/, '');
}

function asRecord(
    value: unknown,
): Record<string, unknown> | undefined {
    return (
        value !== null
        && typeof value === 'object'
        && !Array.isArray(value)
    )
        ? value as Record<string, unknown>
        : undefined;
}
