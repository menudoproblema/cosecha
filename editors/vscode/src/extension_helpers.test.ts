import test from 'node:test';
import assert from 'node:assert/strict';
import * as path from 'node:path';
import * as fs from 'node:fs';
import * as os from 'node:os';

import {
    groupTestsByAnchor,
    collectSelectionLabelCounts,
    determineLanguageServerAction,
    determineLanguageServerAutostartPolicy,
    formatSessionStatusCounts,
    isNonRecoverableLanguageServerFailure,
    normalizeLanguageServerFailureMessage,
    preferWorkspaceInterpreterShim,
    readSessionStatusCounts,
    resolveCodeLensAnchorLine,
    resolveConfiguredInterpreterPath,
    shouldPreferWorkspaceInterpreterShim,
    selectPreferredWorkspacePath,
} from './extension_helpers.js';

test('resolveConfiguredInterpreterPath resolves relative paths from workspace', () => {
    const workspacePath = '/tmp/cosecha-workspace';

    assert.equal(
        resolveConfiguredInterpreterPath('.venv/bin/python', workspacePath),
        path.join(workspacePath, '.venv', 'bin', 'python'),
    );
});

test('resolveConfiguredInterpreterPath expands workspace variables', () => {
    const workspacePath = '/tmp/cosecha-workspace';

    assert.equal(
        resolveConfiguredInterpreterPath(
            '${workspaceFolder}/venv/bin/python',
            workspacePath,
        ),
        path.join(workspacePath, 'venv', 'bin', 'python'),
    );
});

test('selectPreferredWorkspacePath prefers the target path workspace', () => {
    const workspacePaths = [
        '/tmp/first-workspace',
        '/tmp/second-workspace',
    ];

    const preferredPath = selectPreferredWorkspacePath({
        targetPath: '/tmp/second-workspace/tests/example.feature',
        activeDocumentPath: '/tmp/first-workspace/tests/active.feature',
        workspacePaths,
    });

    assert.equal(preferredPath, '/tmp/second-workspace');
});

test('selectPreferredWorkspacePath falls back to active document workspace', () => {
    const workspacePaths = [
        '/tmp/first-workspace',
        '/tmp/second-workspace',
    ];

    const preferredPath = selectPreferredWorkspacePath({
        activeDocumentPath: '/tmp/second-workspace/tests/active.feature',
        workspacePaths,
    });

    assert.equal(preferredPath, '/tmp/second-workspace');
});

test('determineLanguageServerAction restarts when workspace changes', () => {
    assert.equal(
        determineLanguageServerAction({
            enabled: true,
            nextWorkspacePath: '/tmp/second-workspace',
            runningWorkspacePath: '/tmp/first-workspace',
        }),
        'restart',
    );
});

test('determineLanguageServerAction starts when workspace appears after activation', () => {
    assert.equal(
        determineLanguageServerAction({
            enabled: true,
            nextWorkspacePath: '/tmp/workspace',
            runningWorkspacePath: undefined,
        }),
        'start',
    );
});

test('determineLanguageServerAutostartPolicy blocks repeated autostart for the same workspace', () => {
    assert.equal(
        determineLanguageServerAutostartPolicy({
            blockedWorkspacePath: '/tmp/workspace',
            workspacePath: '/tmp/workspace',
        }),
        'blocked',
    );
    assert.equal(
        determineLanguageServerAutostartPolicy({
            blockedWorkspacePath: '/tmp/workspace',
            workspacePath: '/tmp/workspace',
            force: true,
        }),
        'allowed',
    );
});

test('preferWorkspaceInterpreterShim keeps the workspace venv executable when a system realpath matches it', () => {
    const workspaceInterpreterPath = '/tmp/project/.venv/bin/python';
    const systemInterpreterPath = '/opt/homebrew/bin/python3.14';
    const realpaths = new Map([
        [workspaceInterpreterPath, '/Cellar/python@3.14/bin/python3.14'],
        [systemInterpreterPath, '/Cellar/python@3.14/bin/python3.14'],
    ]);

    assert.equal(
        preferWorkspaceInterpreterShim({
            interpreterPath: systemInterpreterPath,
            workspaceInterpreterPaths: [workspaceInterpreterPath],
            realpathLookup: (inputPath) => realpaths.get(inputPath),
        }),
        workspaceInterpreterPath,
    );
});

test('shouldPreferWorkspaceInterpreterShim only applies to python extension selections', () => {
    assert.equal(
        shouldPreferWorkspaceInterpreterShim('python.interpreterPath'),
        true,
    );
    assert.equal(
        shouldPreferWorkspaceInterpreterShim('cosecha.pythonInterpreterPath'),
        false,
    );
});

test('workspace env resolution can use the venv folder path directly', async () => {
    const root = await fs.promises.mkdtemp(path.join(os.tmpdir(), 'cosecha-vscode-env-'));
    try {
        const environmentPath = path.join(root, 'venv3.14');
        const binPath = path.join(environmentPath, 'bin');
        await fs.promises.mkdir(binPath, { recursive: true });
        await fs.promises.writeFile(path.join(environmentPath, 'pyvenv.cfg'), '', 'utf8');
        await fs.promises.writeFile(path.join(binPath, 'python'), '', 'utf8');

        assert.equal(
            path.join(environmentPath, 'bin', 'python'),
            path.join(environmentPath, 'bin', 'python'),
        );
    } finally {
        await fs.promises.rm(root, { recursive: true, force: true });
    }
});

test('normalizeLanguageServerFailureMessage keeps the last meaningful line', () => {
    assert.equal(
        normalizeLanguageServerFailureMessage(
            '\n  × No solution found\n  ╰─▶ requirements are unsatisfiable.\n',
        ),
        '╰─▶ requirements are unsatisfiable.',
    );
});

test('isNonRecoverableLanguageServerFailure detects dependency resolution failures', () => {
    assert.equal(
        isNonRecoverableLanguageServerFailure(
            'No solution found when resolving dependencies',
        ),
        true,
    );
    assert.equal(
        isNonRecoverableLanguageServerFailure(
            'Pending response rejected since connection got disposed',
        ),
        false,
    );
});

test('readSessionStatusCounts accepts object payloads', () => {
    const counts = readSessionStatusCounts({
        recorded_at: Date.now(),
        report_summary: {
            status_counts: {
                passed: 3,
                failed: 1,
                skipped: 0,
            },
        },
    });

    assert.deepEqual(
        Array.from(counts.entries()),
        [
            ['passed', 3],
            ['failed', 1],
            ['skipped', 0],
        ],
    );
});

test('readSessionStatusCounts accepts array payloads and summary formatting is ordered', () => {
    const counts = readSessionStatusCounts({
        recorded_at: Date.now(),
        report_summary: {
            status_counts: [
                ['failed', 2],
                ['passed', 5],
                ['pending', 1],
            ],
        },
    });

    assert.deepEqual(
        Array.from(counts.entries()),
        [
            ['failed', 2],
            ['passed', 5],
            ['pending', 1],
        ],
    );
    assert.equal(
        formatSessionStatusCounts(counts),
        'passed 5 · failed 2 · pending 1',
    );
});

test('resolveCodeLensAnchorLine prefers the matching scenario title above the hinted line', () => {
    const lines = [
        '@tag',
        'Feature: demo',
        '',
        '  Scenario: Executes successfully with name',
        '    Given something',
        '      | row |',
    ];

    assert.equal(
        resolveCodeLensAnchorLine({
            documentLines: lines,
            hintedSourceLine: 6,
            testName: 'Scenario: Executes successfully with name',
        }),
        3,
    );
});

test('resolveCodeLensAnchorLine ignores example suffixes when anchoring scenario outlines', () => {
    const lines = [
        'Feature: demo',
        '',
        '  Scenario: Executes successfully',
        '    Given one',
        '      | <value> |',
    ];

    assert.equal(
        resolveCodeLensAnchorLine({
            documentLines: lines,
            hintedSourceLine: 5,
            testName: 'Scenario: Executes successfully [Example #2]',
        }),
        2,
    );
});

test('resolveCodeLensAnchorLine falls back to the hinted source line when no title match exists', () => {
    assert.equal(
        resolveCodeLensAnchorLine({
            documentLines: ['Feature: demo', '  Given something'],
            hintedSourceLine: 2,
            testName: 'Scenario: missing',
        }),
        1,
    );
});

test('collectSelectionLabelCounts aggregates and sorts effective labels', () => {
    const counts = collectSelectionLabelCounts([
        { selection_labels: ['@requires:core/system', '@team:billing'] },
        { selection_labels: ['@team:billing', '@team:critical'] },
        { selection_labels: ['  @team:billing  ', ''] },
    ]);

    assert.deepEqual(
        Array.from(counts.entries()),
        [
            ['@requires:core/system', 1],
            ['@team:billing', 3],
            ['@team:critical', 1],
        ],
    );
});

test('groupTestsByAnchor groups duplicated scenario records under one anchor', () => {
    const groups = groupTestsByAnchor({
        documentLines: [
            '@tag',
            'Feature: demo',
            '',
            '  Scenario: Executes successfully with name',
            '    Given one',
            '    Then two',
        ],
        tests: [
            {
                test_name: 'Scenario: Executes successfully with name',
                source_line: 5,
                selection_labels: ['@a'],
            },
            {
                test_name: 'Scenario: Executes successfully with name',
                source_line: 6,
                selection_labels: ['@b'],
            },
        ],
    });

    assert.equal(groups.size, 1);
    const firstGroup = Array.from(groups.values())[0];
    assert.equal(firstGroup.anchorLine, 3);
    assert.equal(firstGroup.tests.length, 2);
});

test('groupTestsByAnchor merges example variants on the same scenario anchor', () => {
    const groups = groupTestsByAnchor({
        documentLines: [
            'Feature: demo',
            '',
            '  Scenario: Executes successfully',
            '    Given one',
            '      | <value> |',
        ],
        tests: [
            {
                test_name: 'Scenario: Executes successfully [Example #1]',
                source_line: 5,
                selection_labels: ['@a'],
            },
            {
                test_name: 'Scenario: Executes successfully [Example #2]',
                source_line: 5,
                selection_labels: ['@b'],
            },
        ],
    });

    assert.equal(groups.size, 1);
    const firstGroup = Array.from(groups.values())[0];
    assert.equal(firstGroup.anchorLine, 2);
    assert.equal(firstGroup.tests.length, 2);
});
