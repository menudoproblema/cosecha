import * as fs from 'node:fs';
import * as path from 'node:path';
import { spawn, spawnSync } from 'node:child_process';

import * as vscode from 'vscode';
import {
    CloseAction,
    ErrorAction,
    LanguageClient,
    LanguageClientOptions,
    ServerOptions,
} from 'vscode-languageclient/node';
import {
    determineLanguageServerAction,
    determineLanguageServerAutostartPolicy,
    formatSessionStatusCounts,
    isNonRecoverableLanguageServerFailure,
    normalizeLanguageServerFailureMessage,
    preferWorkspaceInterpreterShim,
    readSessionStatusCounts,
    resolveConfiguredInterpreterPath,
    shouldPreferWorkspaceInterpreterShim,
    selectPreferredWorkspacePath,
} from './extension_helpers.js';


const TERMINAL_NAME = 'Cosecha';
const STATUS_BAR_COMMAND = 'cosecha.run';
const DEFAULT_CLI_COMMAND = 'uv run cosecha';
const DEFAULT_LSP_COMMAND = ['uv', 'run', 'cosecha-lsp'];
const SESSION_LIMIT = 10;
const BRIDGE_CACHE_TTL_MS = 750;
const REFRESH_DEBOUNCE_MS = 200;
const BRIDGE_SCRIPT_PATH = path.resolve(
    __dirname,
    '..',
    'python',
    'cosecha_bridge.py',
);

let languageClient: LanguageClient | undefined;
let languageClientWorkspacePath: string | undefined;
let languageClientTransition: Promise<void> = Promise.resolve();
let languageClientStopRequested = false;
let outputChannel: vscode.OutputChannel | undefined;
const bridgeCache = new Map<
    string,
    {
        expiresAt: number;
        promise: Promise<unknown>;
        workspaceFingerprint?: string;
        workspacePath: string;
    }
>();
const workspaceFingerprints = new Map<string, string>();
const blockedLspWorkspaces = new Map<
    string,
    { detail: string; notified: boolean; blockedAt: number }
>();

type ResolvedPythonEnvironment = {
    source: string;
    interpreterPath: string;
};

type PythonEnvironmentQuickPickItem = vscode.QuickPickItem & {
    interpreterPath?: string;
    actionKind: 'candidate' | 'browse' | 'clear';
};

type PythonRunner = {
    executable: string;
    args: string[];
    label: string;
};

type CliRunOptions = {
    refreshAfterSuccess?: boolean;
    refreshScope?: RefreshScope;
    reveal?: vscode.TaskRevealKind;
    title?: string;
    workspaceFolder?: vscode.WorkspaceFolder;
};

type RefreshScope = 'all' | 'editor' | 'knowledge' | 'sessions';

type KnowledgeBaseInfo = {
    exists: boolean;
    knowledge_base_path: string;
    manifest_path?: string | null;
    project_path: string;
    root_path: string;
    size_bytes?: number | null;
    updated_at?: number | null;
    domain_event_count?: number;
    latest_event_sequence_number?: number | null;
    latest_event_timestamp?: number | null;
    schema_version?: string | null;
    current_snapshot_counts?: {
        definitions: number;
        registry_snapshots: number;
        resources: number;
        tests: number;
    };
    latest_session_artifact?: SessionArtifactRecord | null;
};

type TestRecord = {
    node_stable_id: string;
    engine_name: string;
    test_name: string;
    test_path: string;
    source_line?: number | null;
    status?: string | null;
    duration?: number | null;
    session_id?: string | null;
    trace_id?: string | null;
};

type DefinitionDescriptor = {
    source_line: number;
    function_name: string;
    category?: string | null;
    documentation?: string | null;
    provider_kind?: string | null;
    provider_name?: string | null;
};

type DefinitionRecord = {
    engine_name: string;
    file_path: string;
    definition_count: number;
    discovery_mode: string;
    descriptors: DefinitionDescriptor[];
};

type SessionArtifactRecord = {
    session_id: string;
    trace_id?: string | null;
    recorded_at: number;
    root_path: string;
    workspace_fingerprint?: string | null;
    has_failures?: boolean | null;
    report_summary?: Record<string, unknown> | null;
    plan_explanation?: Record<string, unknown> | null;
    timing?: Record<string, unknown> | null;
};

type SessionSummaryRecord = {
    coverage_total?: number | null;
    engine_count?: number;
    engine_summaries?: Array<Record<string, unknown>>;
    failed_example_count?: number;
    failed_examples?: string[];
    failed_file_count?: number;
    failed_files?: string[];
    failure_kind_counts?: Record<string, number>;
    has_failures?: boolean | null;
    instrumentation_summaries?: Record<string, Record<string, unknown>>;
    live_engine_snapshot_summaries?: Array<Record<string, unknown>>;
    live_snapshot_breakdown?: Record<string, number>;
    live_snapshot_count?: number;
    plan_id?: string | null;
    recorded_at?: number | null;
    root_path?: string | null;
    session_id?: string | null;
    status_counts?: Record<string, number> | Array<[string, number]>;
    total_tests?: number;
    trace_id?: string | null;
    workspace_fingerprint?: string | null;
};

type WorkspaceInfo = {
    knowledge_base_path: string;
    manifest_path?: string | null;
    project_path: string;
    root_path: string;
    workspace_root?: string;
    knowledge_anchor?: string;
    execution_root?: string;
    workspace_fingerprint?: string | null;
};

type SessionContextTarget = {
    root_path: string;
    session_id: string;
};

type LastSessionStatus = 'failed' | 'mixed' | 'passed' | 'running' | 'skipped';

type LatestSessionResult = {
    sessionId?: string;
};

type LineStatusSummary = {
    hoverMessage: vscode.MarkdownString;
    line: number;
    status: LastSessionStatus;
};

type BridgeOperationMap = {
    describe_workspace: WorkspaceInfo;
    describe_knowledge_base: KnowledgeBaseInfo;
    query_tests: { tests: TestRecord[]; workspace: WorkspaceInfo };
    query_definitions: {
        definitions: DefinitionRecord[];
        workspace: WorkspaceInfo;
    };
    list_recent_sessions: {
        artifacts: SessionArtifactRecord[];
        workspace: WorkspaceInfo;
    };
    read_session_artifact: {
        artifacts: SessionArtifactRecord[];
        workspace: WorkspaceInfo;
    };
};

type BridgeOperation = keyof BridgeOperationMap;

class CosechaTreeNode extends vscode.TreeItem {
    children?: CosechaTreeNode[];
    sessionTarget?: SessionContextTarget;

    constructor(
        label: string,
        collapsibleState: vscode.TreeItemCollapsibleState = (
            vscode.TreeItemCollapsibleState.None
        ),
        children?: CosechaTreeNode[],
    ) {
        super(label, collapsibleState);
        this.children = children;
    }
}

class LastSessionDecorations implements vscode.Disposable {
    private readonly passedDecoration: vscode.TextEditorDecorationType;

    private readonly failedDecoration: vscode.TextEditorDecorationType;

    private readonly skippedDecoration: vscode.TextEditorDecorationType;

    private readonly runningDecoration: vscode.TextEditorDecorationType;

    private readonly mixedDecoration: vscode.TextEditorDecorationType;

    constructor(
        private readonly context: vscode.ExtensionContext,
    ) {
        this.passedDecoration = vscode.window.createTextEditorDecorationType({
            gutterIconPath: vscode.Uri.file(
                context.asAbsolutePath('media/status-pass.svg'),
            ),
            gutterIconSize: 'contain',
        });
        this.failedDecoration = vscode.window.createTextEditorDecorationType({
            gutterIconPath: vscode.Uri.file(
                context.asAbsolutePath('media/status-fail.svg'),
            ),
            gutterIconSize: 'contain',
        });
        this.skippedDecoration = vscode.window.createTextEditorDecorationType({
            gutterIconPath: vscode.Uri.file(
                context.asAbsolutePath('media/status-skip.svg'),
            ),
            gutterIconSize: 'contain',
        });
        this.runningDecoration = vscode.window.createTextEditorDecorationType({
            gutterIconPath: vscode.Uri.file(
                context.asAbsolutePath('media/status-running.svg'),
            ),
            gutterIconSize: 'contain',
        });
        this.mixedDecoration = vscode.window.createTextEditorDecorationType({
            gutterIconPath: vscode.Uri.file(
                context.asAbsolutePath('media/status-mixed.svg'),
            ),
            gutterIconSize: 'contain',
        });
    }

    dispose(): void {
        this.passedDecoration.dispose();
        this.failedDecoration.dispose();
        this.skippedDecoration.dispose();
        this.runningDecoration.dispose();
        this.mixedDecoration.dispose();
    }

    async refresh(): Promise<void> {
        await Promise.all(
            vscode.window.visibleTextEditors.map(async (editor) => {
                await this.refreshEditor(editor);
            }),
        );
    }

    async refreshEditor(editor: vscode.TextEditor): Promise<void> {
        const document = editor.document;
        if (
            !isCosechaCodeLensLanguage(document)
            || !isWorkspaceDocument(document)
        ) {
            this.clearEditor(editor);
            return;
        }

        const workspaceFolder = vscode.workspace.getWorkspaceFolder(
            document.uri,
        );
        if (!workspaceFolder) {
            this.clearEditor(editor);
            return;
        }

        try {
            const bridge = new CosechaBridge(workspaceFolder.uri.fsPath);
            const [latestSession, tests] = await Promise.all([
                getLatestSessionResult(bridge),
                bridge.queryTestsForFile(document.uri.fsPath),
            ]);
            const lineStatuses = buildLineStatusSummaries(
                tests,
                latestSession.sessionId,
            );

            this.applyEditorStatuses(editor, lineStatuses);
        } catch {
            this.clearEditor(editor);
        }
    }

    private applyEditorStatuses(
        editor: vscode.TextEditor,
        lineStatuses: LineStatusSummary[],
    ): void {
        const passed: vscode.DecorationOptions[] = [];
        const failed: vscode.DecorationOptions[] = [];
        const skipped: vscode.DecorationOptions[] = [];
        const running: vscode.DecorationOptions[] = [];
        const mixed: vscode.DecorationOptions[] = [];

        for (const lineStatus of lineStatuses) {
            const decoration = {
                hoverMessage: lineStatus.hoverMessage,
                range: new vscode.Range(lineStatus.line, 0, lineStatus.line, 0),
            };
            if (lineStatus.status === 'passed') {
                passed.push(decoration);
                continue;
            }
            if (lineStatus.status === 'failed') {
                failed.push(decoration);
                continue;
            }
            if (lineStatus.status === 'skipped') {
                skipped.push(decoration);
                continue;
            }
            if (lineStatus.status === 'running') {
                running.push(decoration);
                continue;
            }
            mixed.push(decoration);
        }

        editor.setDecorations(this.passedDecoration, passed);
        editor.setDecorations(this.failedDecoration, failed);
        editor.setDecorations(this.skippedDecoration, skipped);
        editor.setDecorations(this.runningDecoration, running);
        editor.setDecorations(this.mixedDecoration, mixed);
    }

    private clearEditor(editor: vscode.TextEditor): void {
        editor.setDecorations(this.passedDecoration, []);
        editor.setDecorations(this.failedDecoration, []);
        editor.setDecorations(this.skippedDecoration, []);
        editor.setDecorations(this.runningDecoration, []);
        editor.setDecorations(this.mixedDecoration, []);
    }
}

class KnowledgeTreeProvider
implements vscode.TreeDataProvider<CosechaTreeNode> {
    private readonly emitter = new vscode.EventEmitter<
        CosechaTreeNode | undefined | null | void
    >();

    readonly onDidChangeTreeData = this.emitter.event;

    refresh(): void {
        this.emitter.fire();
    }

    async getChildren(
        element?: CosechaTreeNode,
    ): Promise<CosechaTreeNode[]> {
        if (element?.children) {
            return element.children;
        }

        const workspaceFolder = getPreferredWorkspaceFolder();
        if (!workspaceFolder) {
            return [buildInfoNode('Abre un workspace de Cosecha.')];
        }

        const bridge = new CosechaBridge(workspaceFolder.uri.fsPath);
        try {
            const [workspaceInfo, knowledgeBase] = await Promise.all([
                bridge.describeWorkspace(),
                bridge.describeKnowledgeBase(),
            ]);
            const rootItems: CosechaTreeNode[] = [
                buildKnowledgeBaseNode(
                    knowledgeBase,
                    workspaceInfo,
                    workspaceFolder.uri.fsPath,
                ),
                buildWorkspaceContextNode(
                    workspaceInfo,
                    workspaceFolder.uri.fsPath,
                ),
            ];

            if (knowledgeBase.current_snapshot_counts) {
                rootItems.push(
                    buildSnapshotCountsNode(
                        knowledgeBase,
                        workspaceFolder.uri.fsPath,
                    ),
                );
            }

            const activeDocument = vscode.window.activeTextEditor?.document;
            if (
                activeDocument
                && isWorkspaceDocument(activeDocument)
                && isCosechaCodeLensLanguage(activeDocument)
            ) {
                const [latestSession, tests] = await Promise.all([
                    getLatestSessionResult(bridge),
                    bridge.queryTestsForFile(activeDocument.uri.fsPath),
                ]);
                rootItems.push(
                    buildCurrentFileTestsNode(
                        tests,
                        latestSession.sessionId,
                        workspaceFolder.uri.fsPath,
                    ),
                );

                const definitions = await bridge.queryDefinitionsForFile(
                    activeDocument.uri.fsPath,
                );
                rootItems.push(
                    buildCurrentFileDefinitionsNode(
                        definitions,
                        workspaceFolder.uri.fsPath,
                    ),
                );
            } else {
                rootItems.push(
                    buildInfoNode(
                        'Abre un .feature o .py del workspace para ver tests y definiciones.',
                    ),
                );
            }

            return rootItems;
        } catch (error) {
            return [buildErrorNode(error)];
        }
    }

    getTreeItem(element: CosechaTreeNode): vscode.TreeItem {
        return element;
    }
}

class SessionsTreeProvider
implements vscode.TreeDataProvider<CosechaTreeNode> {
    private readonly emitter = new vscode.EventEmitter<
        CosechaTreeNode | undefined | null | void
    >();

    readonly onDidChangeTreeData = this.emitter.event;

    refresh(): void {
        this.emitter.fire();
    }

    async getChildren(
        element?: CosechaTreeNode,
    ): Promise<CosechaTreeNode[]> {
        if (element?.children) {
            return element.children;
        }

        const workspaceFolder = getPreferredWorkspaceFolder();
        if (!workspaceFolder) {
            return [buildInfoNode('Abre un workspace de Cosecha.')];
        }

        const bridge = new CosechaBridge(workspaceFolder.uri.fsPath);
        try {
            const sessions = await bridge.listRecentSessions(SESSION_LIMIT);
            if (sessions.length === 0) {
                return [buildInfoNode('No hay sesiones persistidas en la KB.')];
            }

            return sessions.map((session) =>
                buildSessionNode(session, workspaceFolder),
            );
        } catch (error) {
            return [buildErrorNode(error)];
        }
    }

    getTreeItem(element: CosechaTreeNode): vscode.TreeItem {
        return element;
    }
}

class QuickActionsTreeProvider
implements vscode.TreeDataProvider<CosechaTreeNode> {
    private readonly emitter = new vscode.EventEmitter<
        CosechaTreeNode | undefined | null | void
    >();

    readonly onDidChangeTreeData = this.emitter.event;

    refresh(): void {
        this.emitter.fire();
    }

    async getChildren(
        element?: CosechaTreeNode,
    ): Promise<CosechaTreeNode[]> {
        if (element?.children) {
            return element.children;
        }

        const activePath = getActiveWorkspaceRelativePath();
        const isGherkinActive = (
            vscode.window.activeTextEditor?.document.languageId === 'gherkin'
        );

        return [
            buildActionNode(
                'Run current scope',
                'Ejecuta Cosecha sobre el fichero activo o el workspace.',
                'play',
                'cosecha.run',
            ),
            buildActionNode(
                'Manifest validate',
                'Valida el manifiesto activo del workspace.',
                'check-all',
                'cosecha.manifestValidate',
            ),
            buildActionNode(
                'Restart LSP',
                'Reinicia el servidor de lenguaje de Cosecha.',
                'debug-restart',
                'cosecha.restartLanguageServer',
            ),
            buildActionNode(
                'Show backend',
                'Muestra el interprete y los comandos resueltos.',
                'terminal',
                'cosecha.showBackend',
            ),
            buildActionNode(
                'Select Python environment',
                'Selecciona manualmente el interprete Python para Cosecha.',
                'python',
                'cosecha.selectPythonEnvironment',
            ),
            buildActionNode(
                'Show Knowledge Base',
                activePath
                    ? `Abre el estado de la KB para ${activePath}.`
                    : 'Abre el estado de la KB del workspace.',
                'database',
                'cosecha.showKnowledgeBaseInfo',
            ),
            buildActionNode(
                'Refresh views',
                'Recarga Knowledge Base, sesiones y CodeLens.',
                'refresh',
                'cosecha.refreshData',
            ),
            buildActionNode(
                'Rebuild Knowledge Base',
                'Reconstruye la KB persistente del workspace.',
                'sync',
                'cosecha.knowledgeRebuild',
            ),
            buildActionNode(
                'Insert Gherkin data table',
                isGherkinActive
                    ? 'Inserta una tabla Gherkin en el cursor.'
                    : 'Abre un .feature para insertar una tabla Gherkin.',
                'table',
                'cosecha.insertGherkinDataTable',
            ),
        ];
    }

    getTreeItem(element: CosechaTreeNode): vscode.TreeItem {
        return element;
    }
}

class CosechaCodeLensProvider implements vscode.CodeLensProvider {
    private readonly emitter = new vscode.EventEmitter<void>();

    readonly onDidChangeCodeLenses = this.emitter.event;

    refresh(): void {
        this.emitter.fire();
    }

    async provideCodeLenses(
        document: vscode.TextDocument,
    ): Promise<vscode.CodeLens[]> {
        if (
            !isCosechaCodeLensLanguage(document)
            || !isWorkspaceDocument(document)
        ) {
            return [];
        }

        const workspaceFolder = vscode.workspace.getWorkspaceFolder(
            document.uri,
        );
        if (!workspaceFolder) {
            return [];
        }

        const lenses: vscode.CodeLens[] = [];
        const topRange = new vscode.Range(0, 0, 0, 0);
        const relativePath = getWorkspaceRelativePath(document.uri);
        if (!relativePath) {
            return [];
        }

        lenses.push(
            new vscode.CodeLens(topRange, {
                command: 'cosecha.runTestPath',
                title: 'Run Cosecha',
                arguments: [
                    toCosechaSelector(relativePath),
                    path.basename(relativePath),
                    workspaceFolder.uri.fsPath,
                ],
            }),
            new vscode.CodeLens(topRange, {
                command: 'cosecha.planExplain',
                title: 'Plan Explain',
            }),
            new vscode.CodeLens(topRange, {
                command: 'cosecha.showKnowledgeBaseInfo',
                title: 'Knowledge Base',
                arguments: [workspaceFolder.uri.fsPath],
            }),
        );

        if (document.languageId === 'gherkin') {
            lenses.push(
                new vscode.CodeLens(topRange, {
                    command: 'cosecha.insertGherkinDataTable',
                    title: 'Insert Data Table',
                }),
            );
        }

        try {
            const bridge = new CosechaBridge(workspaceFolder.uri.fsPath);
            const [latestSession, tests] = await Promise.all([
                getLatestSessionResult(bridge),
                bridge.queryTestsForFile(document.uri.fsPath),
            ]);
            for (const test of tests) {
                if (test.source_line == null || test.source_line <= 0) {
                    continue;
                }
                const range = new vscode.Range(
                    test.source_line - 1,
                    0,
                    test.source_line - 1,
                    0,
                );
                const lastSessionStatus = resolveLastSessionStatus(
                    test,
                    latestSession.sessionId,
                );
                lenses.push(
                    new vscode.CodeLens(range, {
                        command: 'cosecha.runTestPath',
                        title: 'Run with Cosecha',
                        arguments: [
                            test.test_path,
                            test.test_name,
                            workspaceFolder.uri.fsPath,
                        ],
                    }),
                    ...(lastSessionStatus
                        ? [
                            new vscode.CodeLens(range, {
                                command: 'cosecha.showSessionArtifact',
                                title: formatLastSessionTitle(lastSessionStatus),
                                arguments: [
                                    test.session_id,
                                    workspaceFolder.uri.fsPath,
                                ],
                            }),
                        ]
                        : []),
                    new vscode.CodeLens(range, {
                        command: 'cosecha.showTestRecord',
                        title: 'Show KB Record',
                        arguments: [test],
                    }),
                );
            }
        } catch {
            return lenses;
        }

        return lenses;
    }
}

class CosechaBridge {
    constructor(private readonly workspacePath: string) {}

    async describeWorkspace(): Promise<WorkspaceInfo> {
        return executeBridgeOperation(
            this.workspacePath,
            'describe_workspace',
            {},
        );
    }

    async describeKnowledgeBase(): Promise<KnowledgeBaseInfo> {
        return executeBridgeOperation(
            this.workspacePath,
            'describe_knowledge_base',
            {},
        );
    }

    async queryTestsForFile(filePath: string): Promise<TestRecord[]> {
        const response = await executeBridgeOperation(
            this.workspacePath,
            'query_tests',
            {
                test_path: filePath,
                limit: 256,
            },
        );
        return response.tests;
    }

    async queryDefinitionsForFile(
        filePath: string,
    ): Promise<DefinitionRecord[]> {
        const response = await executeBridgeOperation(
            this.workspacePath,
            'query_definitions',
            {
                file_path: filePath,
                limit: 64,
            },
        );
        return response.definitions;
    }

    async listRecentSessions(limit: number): Promise<SessionArtifactRecord[]> {
        const response = await executeBridgeOperation(
            this.workspacePath,
            'list_recent_sessions',
            { limit },
        );
        return response.artifacts;
    }

    async readSessionArtifact(
        sessionId: string,
    ): Promise<SessionArtifactRecord | undefined> {
        const response = await executeBridgeOperation(
            this.workspacePath,
            'read_session_artifact',
            {
                session_id: sessionId,
                limit: 1,
            },
        );
        return response.artifacts[0];
    }
}

export async function activate(
    context: vscode.ExtensionContext,
): Promise<void> {
    outputChannel = vscode.window.createOutputChannel('Cosecha');
    context.subscriptions.push(outputChannel);

    const knowledgeTreeProvider = new KnowledgeTreeProvider();
    const sessionsTreeProvider = new SessionsTreeProvider();
    const quickActionsTreeProvider = new QuickActionsTreeProvider();
    const codeLensProvider = new CosechaCodeLensProvider();
    const lastSessionDecorations = new LastSessionDecorations(context);

    context.subscriptions.push(
        lastSessionDecorations,
        vscode.window.registerTreeDataProvider(
            'cosechaQuickActions',
            quickActionsTreeProvider,
        ),
        vscode.window.registerTreeDataProvider(
            'cosechaKnowledge',
            knowledgeTreeProvider,
        ),
        vscode.window.registerTreeDataProvider(
            'cosechaSessions',
            sessionsTreeProvider,
        ),
        vscode.languages.registerCodeLensProvider(
            [{ language: 'gherkin' }, { language: 'python' }],
            codeLensProvider,
        ),
    );

    const refreshViewsNow = (
        scope: RefreshScope = 'all',
    ): void => {
        if (scope === 'all') {
            quickActionsTreeProvider.refresh();
        }
        if (scope === 'all' || scope === 'knowledge') {
            knowledgeTreeProvider.refresh();
        }
        if (scope === 'all' || scope === 'sessions') {
            sessionsTreeProvider.refresh();
        }
        if (
            scope === 'all'
            || scope === 'knowledge'
            || scope === 'sessions'
            || scope === 'editor'
        ) {
            codeLensProvider.refresh();
            void lastSessionDecorations.refresh();
        }
    };
    let refreshTimer: NodeJS.Timeout | undefined;
    const refreshViews = (
        scope: RefreshScope = 'all',
    ): void => {
        if (refreshTimer) {
            clearTimeout(refreshTimer);
        }
        refreshTimer = setTimeout(() => {
            refreshTimer = undefined;
            refreshViewsNow(scope);
        }, REFRESH_DEBOUNCE_MS);
    };
    context.subscriptions.push({
        dispose: () => {
            if (refreshTimer) {
                clearTimeout(refreshTimer);
            }
        },
    });

    registerCommands(
        context,
        refreshViews,
        refreshViewsNow,
    );
    registerStatusBar(context);
    registerRefreshTriggers(context, refreshViews);
    await startLanguageServerIfEnabled(context);
}

export async function deactivate(): Promise<void> {
    await enqueueLanguageClientTransition(async () => {
        await stopLanguageClient('Error stopping Cosecha LSP during deactivate');
    });
}

function registerCommands(
    context: vscode.ExtensionContext,
    refreshViews: (scope?: RefreshScope) => void,
    refreshViewsNow: (scope?: RefreshScope) => void,
): void {
    context.subscriptions.push(
        vscode.commands.registerCommand('cosecha.showBackend', async () =>
            showBackend(),
        ),
        vscode.commands.registerCommand(
            'cosecha.selectPythonEnvironment',
            async () => selectPythonEnvironment(context, refreshViewsNow),
        ),
        vscode.commands.registerCommand(
            'cosecha.clearPythonEnvironment',
            async () => clearPythonEnvironment(context, refreshViewsNow),
        ),
        vscode.commands.registerCommand(
            'cosecha.showKnowledgeBaseInfo',
            async (workspacePath?: string) =>
                showKnowledgeBaseInfo(workspacePath),
        ),
        vscode.commands.registerCommand(
            'cosecha.restartLanguageServer',
            async () => restartLanguageServer(context),
        ),
        vscode.commands.registerCommand(
            'cosecha.openJsonPayload',
            async (title: string, payload: object, workspacePath?: string) =>
                openJsonPayload(title, payload, workspacePath),
        ),
        vscode.commands.registerCommand(
            'cosecha.showSessionArtifact',
            async (sessionId: string, workspacePath?: string) =>
                showSessionArtifact(sessionId, workspacePath),
        ),
        vscode.commands.registerCommand(
            'cosecha.showTestRecord',
            async (testRecord: TestRecord) =>
                openJsonPayload(
                    `Cosecha Test: ${testRecord.test_name}`,
                    testRecord,
                    getPreferredWorkspaceFolder()?.uri.fsPath,
                ),
        ),
        vscode.commands.registerCommand(
            'cosecha.insertGherkinDataTable',
            async () => insertGherkinDataTable(),
        ),
        vscode.commands.registerCommand(
            'cosecha.runTestPath',
            async (testPath: string, label?: string, workspacePath?: string) => {
                appendOutput(
                    `Run scope ${testPath}`
                    + (label ? ` from ${label}` : ''),
                );
                await runCliCommand(`run --path ${quote(testPath)}`, {
                    refreshAfterSuccess: true,
                    refreshScope: 'all',
                    title: `Cosecha Run: ${label ?? testPath}`,
                    workspaceFolder: getPreferredWorkspaceFolder(workspacePath),
                });
            },
        ),
        vscode.commands.registerCommand(
            'cosecha.manifestValidate',
            async () => runCliCommand('manifest validate'),
        ),
        vscode.commands.registerCommand('cosecha.run', async () =>
            runCliCommand(buildContextualRunCommand(), {
                refreshAfterSuccess: true,
                refreshScope: 'all',
                title: 'Cosecha Run',
            }),
        ),
        vscode.commands.registerCommand(
            'cosecha.planExplain',
            async () => runCliCommand(buildContextualPlanExplainCommand()),
        ),
        vscode.commands.registerCommand('cosecha.doctor', async () =>
            runCliCommand('doctor'),
        ),
        vscode.commands.registerCommand(
            'cosecha.knowledgeRebuild',
            async () => {
                const workspaceFolder = getPreferredWorkspaceFolder();
                const exitCode = await runCliCommand('knowledge rebuild', {
                    refreshAfterSuccess: true,
                    refreshScope: 'all',
                    title: 'Cosecha Knowledge Rebuild',
                    workspaceFolder,
                });
                if (exitCode === 0) {
                    invalidateBridgeCache(workspaceFolder?.uri.fsPath);
                    refreshViewsNow('all');
                }
            },
        ),
        vscode.commands.registerCommand(
            'cosecha.copySessionId',
            async (value: SessionContextTarget | CosechaTreeNode) => {
                const target = resolveSessionContextTarget(value);
                if (!target) {
                    return;
                }
                await vscode.env.clipboard.writeText(target.session_id);
                void vscode.window.showInformationMessage(
                    `Session id copiado: ${target.session_id}`,
                );
            },
        ),
        vscode.commands.registerCommand(
            'cosecha.showSessionSummary',
            async (value: SessionContextTarget | CosechaTreeNode) => {
                const target = resolveSessionContextTarget(value);
                if (!target) {
                    return;
                }
                await showSessionSummary(target.session_id, target.root_path);
            },
        ),
        vscode.commands.registerCommand(
            'cosecha.exportSessionArtifact',
            async (value: SessionContextTarget | CosechaTreeNode) => {
                const target = resolveSessionContextTarget(value);
                if (!target) {
                    return;
                }
                await exportSessionArtifact(
                    target.session_id,
                    target.root_path,
                );
            },
        ),
        vscode.commands.registerCommand(
            'cosecha.openSessionFile',
            async (value: SessionContextTarget | CosechaTreeNode) => {
                const target = resolveSessionContextTarget(value);
                if (!target) {
                    return;
                }
                await openSessionFile(target.session_id, target.root_path);
            },
        ),
        vscode.commands.registerCommand(
            'cosecha.rerunSessionScope',
            async (value: SessionContextTarget | CosechaTreeNode) => {
                const target = resolveSessionContextTarget(value);
                if (!target) {
                    return;
                }
                await rerunSessionScope(target.session_id, target.root_path);
            },
        ),
        vscode.commands.registerCommand(
            'cosecha.gherkinValidateCurrentFile',
            async () => runCurrentFileValidation('.feature'),
        ),
        vscode.commands.registerCommand(
            'cosecha.pytestValidateCurrentFile',
            async () => runCurrentFileValidation('.py'),
        ),
        vscode.commands.registerCommand(
            'cosecha.refreshData',
            async (
                scope: RefreshScope = 'all',
                workspacePath?: string,
            ) => {
                invalidateBridgeCache(workspacePath);
                refreshViewsNow(scope);
            },
        ),
    );
}

function registerRefreshTriggers(
    context: vscode.ExtensionContext,
    refreshViews: (scope?: RefreshScope) => void,
): void {
    const refreshAndMaybeStartLsp = (
        document?: vscode.TextDocument,
    ): void => {
        refreshViews();
        if (
            !document
            || document.languageId === 'gherkin'
        ) {
            void startLanguageServerIfEnabled(context);
        }
    };

    context.subscriptions.push(
        vscode.window.onDidChangeActiveTextEditor((editor) => {
            refreshAndMaybeStartLsp(editor?.document);
        }),
        vscode.window.onDidChangeVisibleTextEditors(() => {
            refreshAndMaybeStartLsp();
        }),
        vscode.workspace.onDidSaveTextDocument((document) => {
            refreshAndMaybeStartLsp(document);
        }),
        vscode.workspace.onDidOpenTextDocument((document) => {
            refreshAndMaybeStartLsp(document);
        }),
        vscode.workspace.onDidChangeWorkspaceFolders(() => {
            refreshAndMaybeStartLsp();
        }),
        vscode.workspace.onDidChangeConfiguration((event) => {
            if (
                event.affectsConfiguration('cosecha')
                || event.affectsConfiguration('python')
            ) {
                clearLanguageServerAutostartBlock();
                refreshAndMaybeStartLsp();
            }
        }),
    );
}

function registerStatusBar(
    context: vscode.ExtensionContext,
): void {
    const item = vscode.window.createStatusBarItem(
        vscode.StatusBarAlignment.Left,
        100,
    );
    item.name = 'Cosecha Run';
    item.command = STATUS_BAR_COMMAND;
    item.text = '$(beaker) Cosecha';
    item.tooltip = 'Ejecuta Cosecha sobre el fichero activo o el workspace.';
    item.show();

    const updateText = (): void => {
        const activePath = getActiveWorkspaceRelativePath();
        if (!activePath) {
            item.text = '$(beaker) Cosecha';
            item.tooltip = 'Ejecuta Cosecha sobre el workspace.';
            return;
        }

        item.text = `$(beaker) Cosecha: ${path.basename(activePath)}`;
        item.tooltip = `Ejecuta Cosecha sobre ${activePath}.`;
    };

    context.subscriptions.push(
        item,
        vscode.window.onDidChangeActiveTextEditor(updateText),
        vscode.workspace.onDidChangeWorkspaceFolders(updateText),
    );
    updateText();
}

async function startLanguageServerIfEnabled(
    context: vscode.ExtensionContext,
): Promise<void> {
    return enqueueLanguageClientTransition(async () => {
        await startLanguageServerIfEnabledInner(context);
    });
}

async function startLanguageServerIfEnabledInner(
    context: vscode.ExtensionContext,
    options: { force?: boolean } = {},
): Promise<boolean> {
    const configuration = vscode.workspace.getConfiguration('cosecha');
    const enabled = configuration.get<boolean>(
        'enableLanguageServer',
        true,
    );

    const workspaceFolder = getPreferredWorkspaceFolder();
    const action = determineLanguageServerAction({
        enabled,
        nextWorkspacePath: workspaceFolder?.uri.fsPath,
        runningWorkspacePath: languageClientWorkspacePath,
    });
    if (action === 'skip') {
        return false;
    }

    if (!workspaceFolder) {
        return false;
    }

    const autostartPolicy = determineLanguageServerAutostartPolicy({
        blockedWorkspacePath: blockedLspWorkspaces.has(workspaceFolder.uri.fsPath)
            ? workspaceFolder.uri.fsPath
            : undefined,
        force: options.force,
        workspacePath: workspaceFolder.uri.fsPath,
    });
    if (autostartPolicy === 'blocked') {
        return false;
    }

    if (action === 'restart' && languageClient) {
        await stopLanguageClient('Error stopping stale Cosecha LSP');
    }

    const command = await resolveLspCommand(
        workspaceFolder.uri.fsPath,
        configuration.get<unknown>('lspCommand'),
    );
    if (!command) {
        showMissingBackendError('servidor LSP', workspaceFolder.uri.fsPath);
        blockLanguageServerAutostart(
            workspaceFolder.uri.fsPath,
            'No se pudo resolver un comando ejecutable para el LSP.',
            { notify: false },
        );
        return false;
    }

    const [executable, ...args] = command;
    const probeResult = await probeLanguageServerCommand(
        executable,
        args,
        workspaceFolder.uri.fsPath,
    );
    if (!probeResult.ok) {
        handleLanguageServerStartupFailure(
            workspaceFolder.uri.fsPath,
            probeResult.detail,
            { notify: !options.force },
        );
        return false;
    }
    clearLanguageServerAutostartBlock(workspaceFolder.uri.fsPath);

    const serverOptions: ServerOptions = {
        command: executable,
        args,
        options: { cwd: workspaceFolder.uri.fsPath },
    };

    const clientOptions: LanguageClientOptions = {
        documentSelector: [{ scheme: 'file', language: 'gherkin' }],
        errorHandler: {
            error: (error) => {
                appendOutput(
                    'Cosecha LSP transport error: '
                    + (error instanceof Error ? error.message : String(error)),
                );
                return {
                    action: ErrorAction.Shutdown,
                    handled: true,
                };
            },
            closed: () => {
                const currentWorkspacePath = languageClientWorkspacePath
                    ?? workspaceFolder.uri.fsPath;
                if (!languageClientStopRequested) {
                    handleLanguageServerStartupFailure(
                        currentWorkspacePath,
                        'El proceso del LSP se cerro de forma inesperada.',
                        { notify: true },
                    );
                }
                languageClient = undefined;
                languageClientWorkspacePath = undefined;
                return {
                    action: CloseAction.DoNotRestart,
                    handled: true,
                };
            },
        },
    };

    languageClient = new LanguageClient(
        'cosechaLsp',
        'Cosecha Language Server',
        serverOptions,
        clientOptions,
    );
    languageClientWorkspacePath = workspaceFolder.uri.fsPath;

    try {
        await languageClient.start();
        context.subscriptions.push(languageClient);
        clearLanguageServerAutostartBlock(workspaceFolder.uri.fsPath);
        appendOutput(`Cosecha LSP started for ${workspaceFolder.name}.`);
        return true;
    } catch (error) {
        languageClient = undefined;
        languageClientWorkspacePath = undefined;
        const detail = error instanceof Error ? error.message : String(error);
        handleLanguageServerStartupFailure(
            workspaceFolder.uri.fsPath,
            detail,
            { notify: !options.force },
        );
        return false;
    }
}

async function restartLanguageServer(
    context: vscode.ExtensionContext,
): Promise<void> {
    await enqueueLanguageClientTransition(async () => {
        const workspacePath = getPreferredWorkspaceFolder()?.uri.fsPath;
        if (workspacePath) {
            clearLanguageServerAutostartBlock(workspacePath);
        }
        await stopLanguageClient('Error stopping Cosecha LSP');
        const started = await startLanguageServerIfEnabledInner(context, {
            force: true,
        });
        if (started) {
            void vscode.window.showInformationMessage(
                'Cosecha LSP reiniciado.',
            );
            return;
        }
        if (workspacePath) {
            const blockedState = blockedLspWorkspaces.get(workspacePath);
            if (blockedState) {
                void vscode.window.showWarningMessage(
                    `No se pudo arrancar el LSP de Cosecha: ${blockedState.detail}`,
                    'Show Backend',
                ).then((selection) => {
                    if (selection === 'Show Backend') {
                        void vscode.commands.executeCommand('cosecha.showBackend');
                    }
                });
            }
        }
    });
}

function enqueueLanguageClientTransition(
    operation: () => Promise<void>,
): Promise<void> {
    const nextTransition = languageClientTransition
        .catch((error) => {
            appendOutput(
                'Previous Cosecha LSP transition failed: '
                + (error instanceof Error ? error.message : String(error)),
            );
        })
        .then(operation);

    languageClientTransition = nextTransition.catch((error) => {
        appendOutput(
            'Cosecha LSP transition failed: '
            + (error instanceof Error ? error.message : String(error)),
        );
    });

    return nextTransition;
}

async function stopLanguageClient(
    errorPrefix: string,
): Promise<void> {
    if (!languageClient) {
        languageClientWorkspacePath = undefined;
        return;
    }

    const client = languageClient;
    languageClient = undefined;
    languageClientWorkspacePath = undefined;
    try {
        languageClientStopRequested = true;
        await client.stop();
    } catch (error) {
        appendOutput(
            `${errorPrefix}: `
            + (error instanceof Error ? error.message : String(error)),
        );
    } finally {
        languageClientStopRequested = false;
    }
}

function clearLanguageServerAutostartBlock(workspacePath?: string): void {
    if (!workspacePath) {
        blockedLspWorkspaces.clear();
        return;
    }

    blockedLspWorkspaces.delete(workspacePath);
}

function blockLanguageServerAutostart(
    workspacePath: string,
    detail: string,
    options: { notify: boolean },
): void {
    const existingState = blockedLspWorkspaces.get(workspacePath);
    blockedLspWorkspaces.set(workspacePath, {
        blockedAt: Date.now(),
        detail,
        notified: existingState?.notified ?? false,
    });
    appendOutput(
        `Cosecha LSP blocked for ${workspacePath}: ${detail}`,
    );
    if (!options.notify) {
        return;
    }

    const currentState = blockedLspWorkspaces.get(workspacePath);
    if (!currentState || currentState.notified) {
        return;
    }
    currentState.notified = true;
    void vscode.window.showWarningMessage(
        'Cosecha desactivo el arranque automatico del LSP para este workspace '
        + `porque el backend fallo al arrancar: ${detail}`,
        'Restart LSP',
        'Show Backend',
    ).then((selection) => {
        if (selection === 'Restart LSP') {
            void vscode.commands.executeCommand('cosecha.restartLanguageServer');
        }
        if (selection === 'Show Backend') {
            void vscode.commands.executeCommand('cosecha.showBackend');
        }
    });
}

function handleLanguageServerStartupFailure(
    workspacePath: string,
    rawDetail: string,
    options: { notify: boolean },
): void {
    const detail = normalizeLanguageServerFailureMessage(rawDetail)
        ?? 'El backend del LSP se cerro antes de inicializarse.';
    const notify = options.notify || !isNonRecoverableLanguageServerFailure(detail);
    blockLanguageServerAutostart(workspacePath, detail, { notify });
}

async function probeLanguageServerCommand(
    executable: string,
    args: string[],
    cwd: string,
    timeoutMs = 1200,
): Promise<{ ok: true } | { detail: string; ok: false }> {
    return await new Promise((resolve) => {
        const child = spawn(executable, args, {
            cwd,
            stdio: ['pipe', 'pipe', 'pipe'],
        });
        let settled = false;
        let stdout = '';
        let stderr = '';

        const settle = (result: { ok: true } | { detail: string; ok: false }): void => {
            if (settled) {
                return;
            }
            settled = true;
            resolve(result);
        };

        const timer = setTimeout(() => {
            if (settled) {
                return;
            }
            child.kill();
            settle({ ok: true });
        }, timeoutMs);

        child.stdout.setEncoding('utf8');
        child.stdout.on('data', (chunk: string) => {
            stdout += chunk;
        });
        child.stderr.setEncoding('utf8');
        child.stderr.on('data', (chunk: string) => {
            stderr += chunk;
        });
        child.on('error', (error) => {
            clearTimeout(timer);
            settle({
                detail: error instanceof Error ? error.message : String(error),
                ok: false,
            });
        });
        child.on('close', (exitCode, signal) => {
            clearTimeout(timer);
            if (settled) {
                return;
            }
            if (signal === 'SIGTERM' || signal === 'SIGKILL') {
                settle({ ok: true });
                return;
            }

            settle({
                detail: (
                    normalizeLanguageServerFailureMessage(stderr)
                    ?? normalizeLanguageServerFailureMessage(stdout)
                    ?? `El proceso del LSP termino inmediatamente con exit=${exitCode ?? 'unknown'}.`
                ),
                ok: false,
            });
        });
    });
}

async function runCliCommand(
    subcommand: string,
    options: CliRunOptions = {},
): Promise<number | undefined> {
    const workspaceFolder = options.workspaceFolder ?? getPreferredWorkspaceFolder();
    if (!workspaceFolder) {
        void vscode.window.showWarningMessage(
            'Abre un workspace antes de lanzar comandos de Cosecha.',
        );
        return undefined;
    }

    const configuration = vscode.workspace.getConfiguration('cosecha');
    const cliCommand = await resolveCliCommand(
        workspaceFolder.uri.fsPath,
        configuration.get<string>('cliCommand', DEFAULT_CLI_COMMAND),
    );
    if (!cliCommand) {
        showMissingBackendError('CLI', workspaceFolder.uri.fsPath);
        return undefined;
    }

    const commandLine = `${cliCommand} ${subcommand}`;
    const task = new vscode.Task(
        {
            type: 'cosecha',
            subcommand,
            workspacePath: workspaceFolder.uri.fsPath,
        },
        workspaceFolder,
        options.title ?? `Cosecha: ${subcommand}`,
        'cosecha',
        new vscode.ShellExecution(commandLine, {
            cwd: workspaceFolder.uri.fsPath,
        }),
        [],
    );
    task.presentationOptions = {
        reveal: options.reveal ?? vscode.TaskRevealKind.Always,
        panel: vscode.TaskPanelKind.Dedicated,
        clear: false,
        focus: false,
    };

    appendOutput(
        `Starting task in ${workspaceFolder.name}: ${commandLine}`,
    );
    const execution = await vscode.tasks.executeTask(task);
    const exitCode = await waitForTaskExecution(execution);
    appendOutput(
        `Finished task in ${workspaceFolder.name}: ${commandLine} `
        + `(exit=${exitCode ?? 'unknown'})`,
    );

    if (exitCode !== undefined) {
        invalidateBridgeCache(workspaceFolder.uri.fsPath);
        if (options.refreshAfterSuccess) {
            void vscode.commands.executeCommand(
                'cosecha.refreshData',
                options.refreshScope ?? 'all',
                workspaceFolder.uri.fsPath,
            );
        }
    }

    return exitCode;
}

function buildContextualRunCommand(): string {
    const activePath = getActiveWorkspaceRelativePath();
    if (!activePath) {
        return 'run';
    }

    return `run --path ${quote(toCosechaSelector(activePath))}`;
}

function buildContextualPlanExplainCommand(): string {
    const activePath = getActiveWorkspaceRelativePath();
    if (!activePath) {
        return 'plan explain';
    }

    return `plan explain --path ${quote(toCosechaSelector(activePath))}`;
}

function buildCurrentFileValidationCommand(
    expectedExtension: '.feature' | '.py',
): string | undefined {
    const activeEditor = vscode.window.activeTextEditor;
    if (!activeEditor) {
        showValidationWarning('No hay un fichero activo para validar.');
        return undefined;
    }

    const filePath = activeEditor.document.uri.fsPath;
    if (!filePath.endsWith(expectedExtension)) {
        showValidationWarning(
            `El fichero activo no termina en ${expectedExtension}.`,
        );
        return undefined;
    }

    const relativePath = getWorkspaceRelativePath(activeEditor.document.uri);
    if (!relativePath) {
        showValidationWarning(
            'El fichero activo no pertenece al workspace actual.',
        );
        return undefined;
    }

    const selector = quote(toCosechaSelector(relativePath));
    if (expectedExtension === '.feature') {
        return `gherkin validate ${selector}`;
    }

    return `pytest validate ${selector}`;
}

async function runCurrentFileValidation(
    expectedExtension: '.feature' | '.py',
): Promise<void> {
    const command = buildCurrentFileValidationCommand(expectedExtension);
    if (!command) {
        return;
    }

    await runCliCommand(command);
}

function showValidationWarning(message: string): void {
    void vscode.window.showWarningMessage(message);
}

function getPreferredWorkspaceFolder(
    target?: string | vscode.Uri,
): vscode.WorkspaceFolder | undefined {
    const workspaceFolders = vscode.workspace.workspaceFolders ?? [];
    if (workspaceFolders.length === 0) {
        return undefined;
    }

    const workspacePaths = workspaceFolders.map(
        (workspaceFolder) => workspaceFolder.uri.fsPath,
    );
    const preferredWorkspacePath = selectPreferredWorkspacePath({
        targetPath: typeof target === 'string' ? target : target?.fsPath,
        activeDocumentPath: vscode.window.activeTextEditor?.document.uri.fsPath,
        workspacePaths,
    });
    if (!preferredWorkspacePath) {
        return undefined;
    }

    return workspaceFolders.find(
        (workspaceFolder) => workspaceFolder.uri.fsPath === preferredWorkspacePath,
    );
}

function getOrCreateTerminal(cwd: string): vscode.Terminal {
    const terminalName = buildTerminalName(cwd);
    const existingTerminal = vscode.window.terminals.find(
        (terminal) => terminal.name === terminalName,
    );

    if (existingTerminal) {
        return existingTerminal;
    }

    return vscode.window.createTerminal({
        name: terminalName,
        cwd,
    });
}

function buildTerminalName(cwd: string): string {
    const workspaceName = path.basename(cwd) || 'workspace';
    return `${TERMINAL_NAME}: ${workspaceName}`;
}

function getActiveWorkspaceRelativePath(): string | undefined {
    const activeEditor = vscode.window.activeTextEditor;
    if (!activeEditor) {
        return undefined;
    }

    return getWorkspaceRelativePath(activeEditor.document.uri);
}

function getWorkspaceRelativePath(uri: vscode.Uri): string | undefined {
    const workspaceFolder = vscode.workspace.getWorkspaceFolder(uri);
    if (!workspaceFolder) {
        return undefined;
    }

    return path.relative(workspaceFolder.uri.fsPath, uri.fsPath);
}

function isWorkspaceDocument(document: vscode.TextDocument): boolean {
    return vscode.workspace.getWorkspaceFolder(document.uri) !== undefined;
}

function isCosechaCodeLensLanguage(
    document: vscode.TextDocument,
): boolean {
    return document.languageId === 'gherkin' || document.languageId === 'python';
}

function toCosechaSelector(relativePath: string): string {
    return relativePath.startsWith(`tests${path.sep}`)
        ? relativePath.slice(`tests${path.sep}`.length)
        : relativePath.replaceAll(path.sep, '/');
}

function quote(value: string): string {
    if (!value.includes(' ')) {
        return value;
    }

    return `"${value.replace(/"/g, '\\"')}"`;
}

function normalizeCommand(
    value: unknown,
    fallback: string[],
): string[] {
    if (!Array.isArray(value)) {
        return fallback;
    }

    const tokens = value.filter(
        (item): item is string =>
            typeof item === 'string' && item.trim().length > 0,
    );
    if (tokens.length === 0) {
        return fallback;
    }

    return tokens;
}

function getCosechaConfiguration(
    workspacePath?: string,
): vscode.WorkspaceConfiguration {
    return vscode.workspace.getConfiguration(
        'cosecha',
        workspacePath ? vscode.Uri.file(workspacePath) : undefined,
    );
}

async function resolveCliCommand(
    workspacePath: string,
    configuredCommand: string,
): Promise<string | undefined> {
    const trimmedCommand = configuredCommand.trim();
    if (
        trimmedCommand
        && trimmedCommand !== DEFAULT_CLI_COMMAND
        && commandIsAvailable(extractExecutable(trimmedCommand), workspacePath)
    ) {
        return trimmedCommand;
    }

    const environment = await resolvePythonEnvironment(workspacePath);
    if (
        environment
        && pythonModuleIsAvailable(
            environment.interpreterPath,
            'cosecha.shell.runner_cli',
            workspacePath,
        )
    ) {
        return `${quote(environment.interpreterPath)} -m cosecha.shell.runner_cli`;
    }

    const workspaceScript = findWorkspaceExecutable(workspacePath, 'cosecha');
    if (workspaceScript) {
        return quote(workspaceScript);
    }

    if (commandIsAvailable('uv', workspacePath)) {
        return DEFAULT_CLI_COMMAND;
    }

    return undefined;
}

async function resolveLspCommand(
    workspacePath: string,
    configuredCommand: unknown,
): Promise<string[] | undefined> {
    const normalizedConfigured = normalizeCommand(
        configuredCommand,
        DEFAULT_LSP_COMMAND,
    );
    if (
        normalizedConfigured.length > 0
        && !commandsEqual(normalizedConfigured, DEFAULT_LSP_COMMAND)
        && commandIsAvailable(normalizedConfigured[0], workspacePath)
    ) {
        return normalizedConfigured;
    }

    const environment = await resolvePythonEnvironment(workspacePath);
    if (
        environment
        && pythonModuleIsAvailable(
            environment.interpreterPath,
            'cosecha_lsp.lsp_server',
            workspacePath,
        )
    ) {
        return [environment.interpreterPath, '-m', 'cosecha_lsp.lsp_server'];
    }

    const workspaceCommand =
        findWorkspaceExecutable(workspacePath, 'cosecha-lsp')
        ?? findWorkspaceExecutable(workspacePath, 'granjero');
    if (workspaceCommand) {
        return [workspaceCommand];
    }

    if (commandIsAvailable('uv', workspacePath)) {
        return DEFAULT_LSP_COMMAND;
    }

    return undefined;
}

async function resolvePythonEnvironment(
    workspacePath: string,
): Promise<ResolvedPythonEnvironment | undefined> {
    const seenPaths = new Set<string>();
    const candidates = await gatherInterpreterCandidates(workspacePath);
    for (const candidate of candidates) {
        const normalizedPath = normalizeCandidateInterpreterPath(
            candidate.interpreterPath,
            workspacePath,
            candidate.source,
        );
        if (seenPaths.has(normalizedPath) || !fs.existsSync(normalizedPath)) {
            continue;
        }

        seenPaths.add(normalizedPath);
        return {
            source: candidate.source,
            interpreterPath: normalizedPath,
        };
    }

    return undefined;
}

async function gatherInterpreterCandidates(
    workspacePath: string,
): Promise<ResolvedPythonEnvironment[]> {
    const candidates: ResolvedPythonEnvironment[] = [];
    const cosechaConfig = getCosechaConfiguration(workspacePath);
    const configuredCosechaInterpreterPath = resolveConfiguredInterpreterPath(
        cosechaConfig.get<string>('pythonInterpreterPath'),
        workspacePath,
    );
    if (configuredCosechaInterpreterPath) {
        candidates.push({
            source: 'cosecha.pythonInterpreterPath',
            interpreterPath: configuredCosechaInterpreterPath,
        });
    }

    const pythonExtensionInterpreter = await getPythonExtensionInterpreter();
    if (pythonExtensionInterpreter) {
        candidates.push({
            source: 'python.interpreterPath',
            interpreterPath: pythonExtensionInterpreter,
        });
    }

    const pythonConfig = vscode.workspace.getConfiguration(
        'python',
        vscode.Uri.file(workspacePath),
    );
    const defaultInterpreterPath = resolveConfiguredInterpreterPath(
        pythonConfig.get<string>('defaultInterpreterPath'),
        workspacePath,
    );
    if (defaultInterpreterPath) {
        candidates.push({
            source: 'python.defaultInterpreterPath',
            interpreterPath: defaultInterpreterPath,
        });
    }

    const legacyInterpreterPath = resolveConfiguredInterpreterPath(
        pythonConfig.get<string>('pythonPath'),
        workspacePath,
    );
    if (legacyInterpreterPath) {
        candidates.push({
            source: 'python.pythonPath',
            interpreterPath: legacyInterpreterPath,
        });
    }

    for (const environmentPath of findWorkspaceEnvironmentDirectories(workspacePath)) {
        const interpreterPath = findInterpreterInEnvironment(environmentPath);
        if (!interpreterPath) {
            continue;
        }

        candidates.push({
            source: `workspace:${path.basename(environmentPath)}`,
            interpreterPath,
        });
    }

    const activeVirtualEnv = process.env.VIRTUAL_ENV;
    if (activeVirtualEnv) {
        const activeInterpreter = findInterpreterInEnvironment(activeVirtualEnv);
        if (activeInterpreter) {
            candidates.push({
                source: 'VIRTUAL_ENV',
                interpreterPath: activeInterpreter,
            });
        }
    }

    return candidates;
}

async function resolvePythonRunner(
    workspacePath: string,
): Promise<PythonRunner | undefined> {
    const environment = await resolvePythonEnvironment(workspacePath);
    if (environment) {
        return {
            executable: environment.interpreterPath,
            args: [],
            label: environment.source,
        };
    }

    if (commandIsAvailable('uv', workspacePath)) {
        return {
            executable: 'uv',
            args: ['run', 'python'],
            label: 'uv',
        };
    }

    return undefined;
}

async function getPythonExtensionInterpreter(): Promise<string | undefined> {
    try {
        const interpreterPath = await vscode.commands.executeCommand<string>(
            'python.interpreterPath',
        );
        if (
            typeof interpreterPath === 'string'
            && interpreterPath.trim().length > 0
        ) {
            return interpreterPath;
        }
    } catch {
        return undefined;
    }

    return undefined;
}

function commandsEqual(left: string[], right: string[]): boolean {
    if (left.length !== right.length) {
        return false;
    }

    return left.every((value, index) => value === right[index]);
}

function commandIsAvailable(
    executable: string | undefined,
    workspacePath: string,
): boolean {
    if (!executable) {
        return false;
    }

    if (executable.includes(path.sep) || path.isAbsolute(executable)) {
        const resolvedPath = path.isAbsolute(executable)
            ? executable
            : path.resolve(workspacePath, executable);
        return fs.existsSync(resolvedPath);
    }

    const lookupCommand = process.platform === 'win32' ? 'where' : 'which';
    const result = spawnSync(lookupCommand, [executable], {
        cwd: workspacePath,
        stdio: 'ignore',
    });
    return result.status === 0;
}

function pythonModuleIsAvailable(
    interpreterPath: string,
    moduleName: string,
    workspacePath: string,
): boolean {
    const result = spawnSync(
        interpreterPath,
        [
            '-c',
            (
                'import importlib.util, sys; '
                + `sys.exit(0 if importlib.util.find_spec(${JSON.stringify(moduleName)}) else 1)`
            ),
        ],
        {
            cwd: workspacePath,
            stdio: 'ignore',
        },
    );

    return result.status === 0;
}

function findWorkspaceExecutable(
    workspacePath: string,
    commandName: string,
): string | undefined {
    for (const environmentPath of findWorkspaceEnvironmentDirectories(workspacePath)) {
        const scriptsDir = process.platform === 'win32' ? 'Scripts' : 'bin';
        const extensions = process.platform === 'win32'
            ? ['.exe', '.cmd', '.bat', '']
            : [''];
        for (const extension of extensions) {
            const candidate = path.join(
                environmentPath,
                scriptsDir,
                `${commandName}${extension}`,
            );
            if (fs.existsSync(candidate)) {
                return candidate;
            }
        }
    }

    return undefined;
}

function findWorkspaceEnvironmentDirectories(
    workspacePath: string,
): string[] {
    const candidateNames = ['.venv', 'venv', 'env', '.env'];
    const candidates = new Set<string>(
        candidateNames.map((name) => path.join(workspacePath, name)),
    );

    try {
        const entries = fs.readdirSync(workspacePath, { withFileTypes: true });
        for (const entry of entries) {
            if (!entry.isDirectory()) {
                continue;
            }

            const candidatePath = path.join(workspacePath, entry.name);
            if (fs.existsSync(path.join(candidatePath, 'pyvenv.cfg'))) {
                candidates.add(candidatePath);
            }
        }
    } catch {
        return Array.from(candidates);
    }

    return Array.from(candidates);
}

function findInterpreterInEnvironment(
    environmentPath: string,
): string | undefined {
    const scriptsDir = process.platform === 'win32' ? 'Scripts' : 'bin';
    const names = process.platform === 'win32'
        ? ['python.exe', 'python3.exe', 'python', 'python3']
        : ['python', 'python3'];

    for (const name of names) {
        const candidate = path.join(environmentPath, scriptsDir, name);
        if (fs.existsSync(candidate)) {
            return candidate;
        }
    }

    return undefined;
}

function listWorkspaceInterpreterPaths(workspacePath: string): string[] {
    const interpreterPaths: string[] = [];
    for (const environmentPath of findWorkspaceEnvironmentDirectories(workspacePath)) {
        const interpreterPath = findInterpreterInEnvironment(environmentPath);
        if (!interpreterPath) {
            continue;
        }
        interpreterPaths.push(path.normalize(interpreterPath));
    }

    return interpreterPaths;
}

function normalizeSelectedInterpreterPath(
    interpreterPath: string,
    workspacePath: string,
): string {
    return path.normalize(interpreterPath);
}

function resolveBrowsedInterpreterPath(
    selectedPath: string,
): string | undefined {
    const normalizedSelectionPath = path.normalize(selectedPath);
    try {
        const stat = fs.statSync(normalizedSelectionPath);
        if (stat.isDirectory()) {
            const environmentPath = findOwningEnvironmentPath(normalizedSelectionPath);
            return (
                findInterpreterInEnvironment(
                    environmentPath ?? normalizedSelectionPath,
                )
            );
        }
    } catch {
        return undefined;
    }

    const environmentPath = findOwningEnvironmentPath(normalizedSelectionPath);
    if (environmentPath) {
        return findInterpreterInEnvironment(environmentPath) ?? normalizedSelectionPath;
    }

    return normalizedSelectionPath;
}

function findOwningEnvironmentPath(
    selectedPath: string,
): string | undefined {
    let currentPath = selectedPath;
    try {
        if (!fs.statSync(currentPath).isDirectory()) {
            currentPath = path.dirname(currentPath);
        }
    } catch {
        return undefined;
    }

    for (let index = 0; index < 4; index += 1) {
        if (fs.existsSync(path.join(currentPath, 'pyvenv.cfg'))) {
            return currentPath;
        }

        const parentPath = path.dirname(currentPath);
        if (parentPath === currentPath) {
            break;
        }
        currentPath = parentPath;
    }

    return undefined;
}

function normalizeCandidateInterpreterPath(
    interpreterPath: string,
    workspacePath: string,
    source: string,
): string {
    if (!shouldPreferWorkspaceInterpreterShim(source)) {
        return normalizeSelectedInterpreterPath(interpreterPath, workspacePath);
    }

    return preferWorkspaceInterpreterShim({
        interpreterPath,
        workspaceInterpreterPaths: listWorkspaceInterpreterPaths(workspacePath),
        realpathLookup: safeRealpath,
    });
}

function safeRealpath(inputPath: string): string | undefined {
    try {
        return fs.realpathSync.native(inputPath);
    } catch {
        return undefined;
    }
}

function extractExecutable(command: string): string | undefined {
    const match = command.match(/^(?:"([^"]+)"|'([^']+)'|(\S+))/);
    if (!match) {
        return undefined;
    }

    return match[1] ?? match[2] ?? match[3];
}

function showMissingBackendError(
    target: string,
    workspacePath: string,
): void {
    void vscode.window.showErrorMessage(
        `No se encontro ${target} de Cosecha para ${workspacePath}. `
        + 'Instala `uv` o selecciona un entorno Python que tenga `cosecha` y `cosecha-lsp`.',
        'Seleccionar entorno',
        'Show Backend',
    ).then((selection) => {
        if (selection === 'Seleccionar entorno') {
            void vscode.commands.executeCommand(
                'cosecha.selectPythonEnvironment',
            );
        }
        if (selection === 'Show Backend') {
            void vscode.commands.executeCommand('cosecha.showBackend');
        }
    });
}

async function executeBridgeOperation<T extends BridgeOperation>(
    workspacePath: string,
    operation: T,
    payload: Record<string, unknown>,
): Promise<BridgeOperationMap[T]> {
    const cacheKey = buildBridgeCacheKey(workspacePath, operation, payload);
    const cachedEntry = bridgeCache.get(cacheKey);
    if (cachedEntry && cachedEntry.expiresAt > Date.now()) {
        return cachedEntry.promise as Promise<BridgeOperationMap[T]>;
    }

    const bridgePromise = executeBridgeOperationUncached(
        workspacePath,
        operation,
        payload,
    );
    const cacheEntry = {
        expiresAt: Date.now() + BRIDGE_CACHE_TTL_MS,
        promise: bridgePromise,
        workspaceFingerprint: workspaceFingerprints.get(workspacePath),
        workspacePath,
    };
    bridgeCache.set(cacheKey, cacheEntry);

    void bridgePromise
        .then((result) => {
            const workspaceFingerprint = extractWorkspaceFingerprint(result);
            if (!workspaceFingerprint) {
                return;
            }
            workspaceFingerprints.set(workspacePath, workspaceFingerprint);
            cacheEntry.workspaceFingerprint = workspaceFingerprint;
        })
        .catch(() => undefined);

    try {
        return await bridgePromise;
    } catch (error) {
        bridgeCache.delete(cacheKey);
        throw error;
    }
}

async function executeBridgeOperationUncached<T extends BridgeOperation>(
    workspacePath: string,
    operation: T,
    payload: Record<string, unknown>,
): Promise<BridgeOperationMap[T]> {
    const runner = await resolvePythonRunner(workspacePath);
    if (!runner) {
        throw new Error('No se pudo resolver un interprete Python para la bridge.');
    }
    if (!fs.existsSync(BRIDGE_SCRIPT_PATH)) {
        throw new Error(
            `No se encontro la bridge Python de Cosecha en ${BRIDGE_SCRIPT_PATH}.`,
        );
    }

    const requestPayload = {
        operation,
        start_path: workspacePath,
        ...payload,
    };
    const result = await runJsonProcess(
        runner.executable,
        [...runner.args, BRIDGE_SCRIPT_PATH],
        workspacePath,
        JSON.stringify(requestPayload),
    );

    if (result.exitCode !== 0) {
        const rawMessage = result.stderr.trim() || result.stdout.trim();
        const message = normalizeBridgeErrorMessage(rawMessage);
        throw new Error(
            message
            || `La bridge de Cosecha fallo usando ${runner.label}.`,
        );
    }

    const stdout = result.stdout.trim();
    if (!stdout) {
        throw new Error('La bridge de Cosecha devolvio una respuesta vacia.');
    }

    return JSON.parse(stdout) as BridgeOperationMap[T];
}

async function runJsonProcess(
    executable: string,
    args: string[],
    cwd: string,
    input: string,
): Promise<{ exitCode: number | null; stderr: string; stdout: string }> {
    return await new Promise((resolve, reject) => {
        const child = spawn(executable, args, {
            cwd,
            stdio: ['pipe', 'pipe', 'pipe'],
        });
        let stdout = '';
        let stderr = '';

        child.stdout.setEncoding('utf8');
        child.stdout.on('data', (chunk: string) => {
            stdout += chunk;
        });

        child.stderr.setEncoding('utf8');
        child.stderr.on('data', (chunk: string) => {
            stderr += chunk;
        });

        child.on('error', (error) => {
            reject(error);
        });
        child.on('close', (exitCode) => {
            resolve({ exitCode, stderr, stdout });
        });

        child.stdin.write(input);
        child.stdin.end();
    });
}

function buildBridgeCacheKey(
    workspacePath: string,
    operation: BridgeOperation,
    payload: Record<string, unknown>,
): string {
    const workspaceFingerprint = workspaceFingerprints.get(workspacePath) ?? 'unknown';
    return (
        `${workspacePath}::${workspaceFingerprint}::${operation}::`
        + JSON.stringify(payload)
    );
}

function invalidateBridgeCache(workspacePath?: string): void {
    if (!workspacePath) {
        bridgeCache.clear();
        workspaceFingerprints.clear();
        return;
    }

    const workspaceFingerprint = workspaceFingerprints.get(workspacePath);
    workspaceFingerprints.delete(workspacePath);

    for (const [cacheKey, cacheEntry] of bridgeCache.entries()) {
        if (
            cacheKey.startsWith(`${workspacePath}::`)
            || cacheEntry.workspacePath === workspacePath
            || (
                workspaceFingerprint !== undefined
                && cacheEntry.workspaceFingerprint === workspaceFingerprint
            )
        ) {
            bridgeCache.delete(cacheKey);
        }
    }
}

function extractWorkspaceFingerprint(value: unknown): string | undefined {
    const directRecord = asRecord(value);
    const directFingerprint = directRecord?.workspace_fingerprint;
    if (
        typeof directFingerprint === 'string'
        && directFingerprint.trim().length > 0
    ) {
        return directFingerprint;
    }

    const nestedWorkspace = asRecord(directRecord?.workspace);
    const nestedFingerprint = nestedWorkspace?.workspace_fingerprint;
    if (
        typeof nestedFingerprint === 'string'
        && nestedFingerprint.trim().length > 0
    ) {
        return nestedFingerprint;
    }

    return undefined;
}

async function waitForTaskExecution(
    execution: vscode.TaskExecution,
): Promise<number | undefined> {
    return new Promise((resolve) => {
        let resolved = false;
        const cleanup = (): void => {
            processDisposable.dispose();
            endDisposable.dispose();
        };
        const finish = (exitCode: number | undefined): void => {
            if (resolved) {
                return;
            }
            resolved = true;
            cleanup();
            resolve(exitCode);
        };
        const processDisposable = vscode.tasks.onDidEndTaskProcess((event) => {
            if (event.execution === execution) {
                finish(event.exitCode);
            }
        });
        const endDisposable = vscode.tasks.onDidEndTask((event) => {
            if (event.execution === execution) {
                finish(undefined);
            }
        });
    });
}

function normalizeBridgeErrorMessage(
    rawMessage: string | undefined,
): string | undefined {
    if (!rawMessage) {
        return undefined;
    }

    const schemaMismatchMatch = rawMessage.match(
        /schema mismatch: expected (\d+), got ['"]?(\d+)['"]?/i,
    );
    if (schemaMismatchMatch) {
        const [, expectedVersion, currentVersion] = schemaMismatchMatch;
        return (
            'La Knowledge Base del workspace esta desactualizada '
            + `(schema ${currentVersion}, esperado ${expectedVersion}). `
            + 'Ejecuta "Cosecha: Rebuild Knowledge Base".'
        );
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

async function showBackend(): Promise<void> {
    const workspaceFolder = getPreferredWorkspaceFolder();
    if (!workspaceFolder) {
        void vscode.window.showWarningMessage(
            'Abre un workspace antes de inspeccionar el backend de Cosecha.',
        );
        return;
    }

    const workspacePath = workspaceFolder.uri.fsPath;
    const configuration = getCosechaConfiguration(workspacePath);
    const environment = await resolvePythonEnvironment(workspacePath);
    const pythonRunner = await resolvePythonRunner(workspacePath);
    const configuredManualInterpreterPath = resolveConfiguredInterpreterPath(
        configuration.get<string>('pythonInterpreterPath'),
        workspacePath,
    );
    const manualInterpreterPath = configuredManualInterpreterPath
        ? normalizeSelectedInterpreterPath(
            configuredManualInterpreterPath,
            workspacePath,
        )
        : undefined;
    const cliCommand = await resolveCliCommand(
        workspacePath,
        configuration.get<string>('cliCommand', DEFAULT_CLI_COMMAND),
    );
    const lspCommand = await resolveLspCommand(
        workspacePath,
        configuration.get<unknown>('lspCommand'),
    );
    let workspaceInfo: WorkspaceInfo | undefined;
    if (pythonRunner) {
        try {
            workspaceInfo = await new CosechaBridge(workspacePath).describeWorkspace();
        } catch (error) {
            appendOutput(
                'Unable to resolve canonical workspace context: '
                + (error instanceof Error ? error.message : String(error)),
            );
        }
    }

    outputChannel?.clear();
    outputChannel?.appendLine('Cosecha backend');
    outputChannel?.appendLine(`workspace: ${workspacePath}`);
    outputChannel?.appendLine(
        `manual python: ${manualInterpreterPath ?? 'not set'}`,
    );

    if (!environment) {
        outputChannel?.appendLine('python: not resolved');
    } else {
        outputChannel?.appendLine(`python source: ${environment.source}`);
        outputChannel?.appendLine(
            `python interpreter: ${environment.interpreterPath}`,
        );

        const interpreterInfo = inspectPythonInterpreter(
            environment.interpreterPath,
            workspacePath,
        );
        if (interpreterInfo.version) {
            outputChannel?.appendLine(
                `python version: ${interpreterInfo.version}`,
            );
        }
        if (interpreterInfo.prefix) {
            outputChannel?.appendLine(`python prefix: ${interpreterInfo.prefix}`);
        }
    }

    outputChannel?.appendLine(
        `python runner: ${
            pythonRunner
                ? [pythonRunner.executable, ...pythonRunner.args].join(' ')
                : 'not resolved'
        }`,
    );
    outputChannel?.appendLine(`cli: ${cliCommand ?? 'not resolved'}`);
    outputChannel?.appendLine(
        `lsp: ${lspCommand ? lspCommand.map(quote).join(' ') : 'not resolved'}`,
    );
    if (workspaceInfo) {
        outputChannel?.appendLine(
            `workspace root: ${workspaceInfo.workspace_root ?? workspaceInfo.project_path}`,
        );
        outputChannel?.appendLine(
            `knowledge anchor: ${workspaceInfo.knowledge_anchor ?? workspaceInfo.root_path}`,
        );
        outputChannel?.appendLine(
            `execution root: ${workspaceInfo.execution_root ?? 'not resolved'}`,
        );
        outputChannel?.appendLine(
            `manifest: ${workspaceInfo.manifest_path ?? 'not resolved'}`,
        );
        outputChannel?.appendLine(
            `knowledge base: ${workspaceInfo.knowledge_base_path}`,
        );
        outputChannel?.appendLine(
            `workspace fingerprint: ${
                workspaceInfo.workspace_fingerprint ?? 'not resolved'
            }`,
        );
    }

    outputChannel?.show(true);
    void vscode.window.showInformationMessage(
        'Cosecha backend escrito en el panel Output.',
    );
}

function inspectPythonInterpreter(
    interpreterPath: string,
    workspacePath: string,
): { version?: string; prefix?: string } {
    const result = spawnSync(
        interpreterPath,
        [
            '-c',
            (
                'import sys; '
                + 'print(f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"); '
                + 'print(sys.prefix)'
            ),
        ],
        {
            cwd: workspacePath,
            encoding: 'utf8',
            stdio: ['ignore', 'pipe', 'ignore'],
        },
    );
    if (result.status !== 0) {
        return {};
    }

    const [version, prefix] = result.stdout
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter((line) => line.length > 0);
    return { version, prefix };
}

async function selectPythonEnvironment(
    context: vscode.ExtensionContext,
    refreshViewsNow: () => void,
): Promise<void> {
    const workspaceFolder = getPreferredWorkspaceFolder();
    if (!workspaceFolder) {
        void vscode.window.showWarningMessage(
            'Abre un workspace antes de seleccionar un entorno Python.',
        );
        return;
    }

    const workspacePath = workspaceFolder.uri.fsPath;
    const currentInterpreterPath = resolveConfiguredInterpreterPath(
        getCosechaConfiguration(workspacePath).get<string>('pythonInterpreterPath'),
        workspacePath,
    );
    const quickPickItems = buildPythonEnvironmentQuickPickItems(
        await gatherInterpreterCandidates(workspacePath),
        workspacePath,
        currentInterpreterPath,
    );
    const selection = await vscode.window.showQuickPick<PythonEnvironmentQuickPickItem>(quickPickItems, {
        placeHolder: 'Selecciona el entorno virtual que usara Cosecha',
    });
    if (!selection) {
        return;
    }

    if (selection.actionKind === 'browse') {
        const pickedUri = await vscode.window.showOpenDialog({
            canSelectFiles: false,
            canSelectFolders: true,
            canSelectMany: false,
            defaultUri: workspaceFolder.uri,
            openLabel: 'Seleccionar virtualenv',
        });
        if (!pickedUri?.[0]) {
            return;
        }
        const selectedPath = resolveBrowsedInterpreterPath(pickedUri[0].fsPath);
        if (!selectedPath) {
            void vscode.window.showErrorMessage(
                'No se pudo resolver un interprete Python desde esa seleccion. '
                + 'Selecciona la carpeta del entorno virtual.',
            );
            return;
        }

        await updateSelectedPythonEnvironment(
            context,
            refreshViewsNow,
            workspaceFolder,
            selectedPath,
        );
        return;
    }

    if (selection.actionKind === 'clear') {
        await clearPythonEnvironment(context, refreshViewsNow, workspaceFolder);
        return;
    }

    if (!selection.interpreterPath) {
        return;
    }

    await updateSelectedPythonEnvironment(
        context,
        refreshViewsNow,
        workspaceFolder,
        selection.interpreterPath,
    );
}

async function clearPythonEnvironment(
    context: vscode.ExtensionContext,
    refreshViewsNow: () => void,
    workspaceFolder = getPreferredWorkspaceFolder(),
): Promise<void> {
    if (!workspaceFolder) {
        void vscode.window.showWarningMessage(
            'Abre un workspace antes de limpiar el entorno Python de Cosecha.',
        );
        return;
    }

    await getCosechaConfiguration(workspaceFolder.uri.fsPath).update(
        'pythonInterpreterPath',
        undefined,
        vscode.ConfigurationTarget.WorkspaceFolder,
    );
    invalidateBridgeCache(workspaceFolder.uri.fsPath);
    refreshViewsNow();
    await restartLanguageServer(context);
    void vscode.window.showInformationMessage(
        'Seleccion manual de entorno Python limpiada para Cosecha.',
    );
}

async function updateSelectedPythonEnvironment(
    context: vscode.ExtensionContext,
    refreshViewsNow: () => void,
    workspaceFolder: vscode.WorkspaceFolder,
    interpreterPath: string,
): Promise<void> {
    const normalizedInterpreterPath = normalizeSelectedInterpreterPath(
        interpreterPath,
        workspaceFolder.uri.fsPath,
    );
    const availability = inspectCosechaBackendAvailability(
        normalizedInterpreterPath,
        workspaceFolder.uri.fsPath,
    );
    if (!availability.hasCli && !availability.hasLsp) {
        void vscode.window.showErrorMessage(
            `El interprete ${normalizedInterpreterPath} no tiene ni CLI ni LSP de Cosecha. `
            + 'Selecciona la carpeta del entorno virtual en lugar del ejecutable.',
        );
        return;
    }

    await getCosechaConfiguration(workspaceFolder.uri.fsPath).update(
        'pythonInterpreterPath',
        normalizedInterpreterPath,
        vscode.ConfigurationTarget.WorkspaceFolder,
    );
    invalidateBridgeCache(workspaceFolder.uri.fsPath);
    refreshViewsNow();
    await restartLanguageServer(context);
    if (!availability.hasCli || !availability.hasLsp) {
        const missingComponents = [
            availability.hasCli ? undefined : 'CLI',
            availability.hasLsp ? undefined : 'LSP',
        ].filter((value): value is string => Boolean(value));
        void vscode.window.showWarningMessage(
            `Cosecha usara ${normalizedInterpreterPath} en ${workspaceFolder.name}, `
            + `pero faltan componentes: ${missingComponents.join(', ')}.`,
        );
        return;
    }
    void vscode.window.showInformationMessage(
        `Cosecha usara ${normalizedInterpreterPath} en ${workspaceFolder.name}.`,
    );
}

function inspectCosechaBackendAvailability(
    interpreterPath: string,
    workspacePath: string,
): { hasCli: boolean; hasLsp: boolean } {
    return {
        hasCli: pythonModuleIsAvailable(
            interpreterPath,
            'cosecha.shell.runner_cli',
            workspacePath,
        ),
        hasLsp: pythonModuleIsAvailable(
            interpreterPath,
            'cosecha_lsp.lsp_server',
            workspacePath,
        ),
    };
}

function buildPythonEnvironmentQuickPickItems(
    candidates: ResolvedPythonEnvironment[],
    workspacePath: string,
    currentInterpreterPath?: string,
): PythonEnvironmentQuickPickItem[] {
    const items: PythonEnvironmentQuickPickItem[] = [];
    const seenPaths = new Set<string>();
    const normalizedCurrentInterpreterPath = currentInterpreterPath
        ? normalizeSelectedInterpreterPath(currentInterpreterPath, workspacePath)
        : undefined;

    for (const candidate of candidates) {
        const normalizedPath = normalizeCandidateInterpreterPath(
            candidate.interpreterPath,
            workspacePath,
            candidate.source,
        );
        if (seenPaths.has(normalizedPath) || !fs.existsSync(normalizedPath)) {
            continue;
        }
        seenPaths.add(normalizedPath);

        const interpreterInfo = inspectPythonInterpreter(
            normalizedPath,
            workspacePath,
        );
        const availability = inspectCosechaBackendAvailability(
            normalizedPath,
            workspacePath,
        );
        const availableModules = [
            availability.hasCli ? 'cli' : undefined,
            availability.hasLsp ? 'lsp' : undefined,
        ].filter((value): value is string => Boolean(value));

        items.push({
            label: normalizedPath === normalizedCurrentInterpreterPath
                ? `$(check) ${path.basename(normalizedPath)}`
                : path.basename(normalizedPath),
            description: candidate.source,
            detail: [
                normalizedPath,
                interpreterInfo.version
                    ? `Python ${interpreterInfo.version}`
                    : undefined,
                availableModules.length > 0
                    ? `Cosecha: ${availableModules.join(', ')}`
                    : 'Cosecha no disponible',
            ].filter((value): value is string => Boolean(value)).join(' · '),
            interpreterPath: normalizedPath,
            actionKind: 'candidate',
        });
    }

    items.push({
        label: '$(folder-opened) Select Virtualenv Folder...',
        detail: 'Seleccionar manualmente la carpeta del entorno virtual',
        actionKind: 'browse',
    });
    items.push({
        label: '$(close) Clear selection',
        detail: 'Volver a la deteccion automatica del entorno',
        actionKind: 'clear',
    });

    return items;
}

async function showKnowledgeBaseInfo(
    workspacePath?: string,
): Promise<void> {
    const workspaceFolder = getPreferredWorkspaceFolder(workspacePath);
    if (!workspaceFolder) {
        return;
    }

    const bridge = new CosechaBridge(workspaceFolder.uri.fsPath);
    let payload: KnowledgeBaseInfo;
    let workspaceInfo: WorkspaceInfo;
    try {
        [workspaceInfo, payload] = await Promise.all([
            bridge.describeWorkspace(),
            bridge.describeKnowledgeBase(),
        ]);
    } catch (error) {
        await showCosechaOperationError(error);
        return;
    }
    await openJsonPayload(
        'Cosecha Knowledge Base',
        {
            workspace: workspaceInfo,
            knowledge_base: payload,
        },
        workspaceFolder.uri.fsPath,
    );
}

async function showSessionArtifact(
    sessionId: string,
    workspacePath?: string,
): Promise<void> {
    const artifact = await readSessionArtifactForWorkspace(
        sessionId,
        workspacePath,
    );
    if (!artifact) {
        return;
    }

    await openJsonPayload(
        `Cosecha Session ${sessionId}`,
        artifact,
        workspacePath,
    );
}

async function showSessionSummary(
    sessionId: string,
    workspacePath?: string,
): Promise<void> {
    const artifact = await readSessionArtifactForWorkspace(
        sessionId,
        workspacePath,
    );
    if (!artifact) {
        return;
    }

    await openSessionSummaryPayload(
        `Cosecha Session Summary ${sessionId}`,
        artifact,
        workspacePath,
    );
}

async function exportSessionArtifact(
    sessionId: string,
    workspacePath?: string,
): Promise<void> {
    const artifact = await readSessionArtifactForWorkspace(
        sessionId,
        workspacePath,
    );
    if (!artifact) {
        return;
    }

    await exportJsonPayload(
        `Cosecha Session ${sessionId}`,
        JSON.stringify(artifact, null, 2),
        workspacePath,
    );
}

async function openSessionFile(
    sessionId: string,
    workspacePath?: string,
): Promise<void> {
    const artifact = await readSessionArtifactForWorkspace(
        sessionId,
        workspacePath,
    );
    if (!artifact) {
        return;
    }

    const selection = await pickSessionFile(artifact);
    if (!selection) {
        void vscode.window.showWarningMessage(
            `La sesion ${sessionId} no expone ficheros navegables.`,
        );
        return;
    }

    const targetPath = resolveSessionFilePath(artifact, selection);
    const document = await vscode.workspace.openTextDocument(
        vscode.Uri.file(targetPath),
    );
    await vscode.window.showTextDocument(document, {
        preview: false,
    });
}

async function rerunSessionScope(
    sessionId: string,
    workspacePath?: string,
): Promise<void> {
    const artifact = await readSessionArtifactForWorkspace(
        sessionId,
        workspacePath,
    );
    if (!artifact) {
        return;
    }

    const workspaceFolder = getPreferredWorkspaceFolder(workspacePath);
    if (!workspaceFolder) {
        return;
    }

    const selectedPath = await pickSessionFile(artifact, true);
    const subcommand = selectedPath
        ? `run --path ${quote(normalizeSessionSelector(selectedPath))}`
        : 'run';
    await runCliCommand(subcommand, {
        refreshAfterSuccess: true,
        refreshScope: 'all',
        title: `Cosecha Rerun: ${sessionId}`,
        workspaceFolder,
    });
}

async function readSessionArtifactForWorkspace(
    sessionId: string,
    workspacePath?: string,
): Promise<SessionArtifactRecord | undefined> {
    const workspaceFolder = getPreferredWorkspaceFolder(workspacePath);
    if (!workspaceFolder) {
        return undefined;
    }

    const bridge = new CosechaBridge(workspaceFolder.uri.fsPath);
    let artifact: SessionArtifactRecord | undefined;
    try {
        artifact = await bridge.readSessionArtifact(sessionId);
    } catch (error) {
        await showCosechaOperationError(error);
        return undefined;
    }
    if (!artifact) {
        void vscode.window.showWarningMessage(
            `No se encontro la sesion ${sessionId}.`,
        );
        return undefined;
    }

    return artifact;
}

async function openSessionSummaryPayload(
    title: string,
    artifact: SessionArtifactRecord,
    workspacePath?: string,
): Promise<void> {
    const panel = vscode.window.createWebviewPanel(
        'cosechaSessionSummary',
        title,
        vscode.ViewColumn.Active,
        {
            enableScripts: true,
            retainContextWhenHidden: true,
        },
    );
    const sessionSummary = readSessionSummary(artifact);
    const serializedSummary = JSON.stringify(sessionSummary, null, 2);
    panel.webview.html = renderSessionSummaryWebview(
        title,
        artifact,
        sessionSummary,
        serializedSummary,
    );

    panel.webview.onDidReceiveMessage(async (message) => {
        if (!message || typeof message !== 'object') {
            return;
        }

        if (message.command === 'copyJson') {
            await vscode.env.clipboard.writeText(serializedSummary);
            void vscode.window.showInformationMessage(
                'Resumen de sesion copiado al portapapeles.',
            );
            return;
        }
        if (message.command === 'exportJson') {
            await exportJsonPayload(title, serializedSummary, workspacePath);
            return;
        }
        if (message.command === 'openRawJson') {
            await openJsonPayload(
                `${title} Raw JSON`,
                sessionSummary,
                workspacePath,
            );
        }
    });

    appendOutput(`${title} opened`);
}

async function openJsonPayload(
    title: string,
    payload: object,
    workspacePath?: string,
): Promise<void> {
    const panel = vscode.window.createWebviewPanel(
        'cosechaJsonPayload',
        title,
        vscode.ViewColumn.Active,
        {
            enableScripts: true,
            retainContextWhenHidden: true,
        },
    );
    const serializedPayload = JSON.stringify(payload, null, 2);
    panel.webview.html = renderJsonPayloadWebview(
        panel.webview,
        title,
        payload,
        serializedPayload,
    );

    panel.webview.onDidReceiveMessage(async (message) => {
        if (!message || typeof message !== 'object') {
            return;
        }

        if (message.command === 'exportJson') {
            await exportJsonPayload(title, serializedPayload, workspacePath);
            return;
        }

        if (message.command === 'copyJson') {
            await vscode.env.clipboard.writeText(serializedPayload);
            void vscode.window.showInformationMessage(
                'JSON copiado al portapapeles.',
            );
        }
    });

    appendOutput(`${title} opened`);
}

async function exportJsonPayload(
    title: string,
    serializedPayload: string,
    workspacePath?: string,
): Promise<void> {
    const suggestedName = slugifyFileName(title) || 'cosecha-payload';
    const targetUri = await vscode.window.showSaveDialog({
        defaultUri: vscode.Uri.file(
            path.join(
                getPreferredWorkspaceFolder(workspacePath)?.uri.fsPath
                ?? process.cwd(),
                `${suggestedName}.json`,
            ),
        ),
        filters: {
            JSON: ['json'],
        },
        saveLabel: 'Export JSON',
    });
    if (!targetUri) {
        return;
    }

    await vscode.workspace.fs.writeFile(
        targetUri,
        Buffer.from(serializedPayload, 'utf8'),
    );
    void vscode.window.showInformationMessage(
        `JSON exportado a ${targetUri.fsPath}.`,
    );
}

async function pickSessionFile(
    artifact: SessionArtifactRecord,
    allowEmpty = false,
): Promise<string | undefined> {
    const failedFiles = readSessionFailedFiles(artifact);
    if (failedFiles.length === 0) {
        return allowEmpty ? undefined : undefined;
    }
    if (failedFiles.length === 1) {
        return failedFiles[0];
    }

    const options = failedFiles.map((filePath) => ({
        label: filePath,
        description: 'failed file',
    }));
    if (allowEmpty) {
        options.unshift({
            label: 'Workspace root',
            description: 'rerun the whole workspace scope',
        });
    }

    const selection = await vscode.window.showQuickPick(options, {
        placeHolder: 'Selecciona el fichero de la sesion',
    });
    if (!selection) {
        return undefined;
    }
    if (allowEmpty && selection.label === 'Workspace root') {
        return undefined;
    }

    return selection.label;
}

function resolveSessionFilePath(
    artifact: SessionArtifactRecord,
    sessionPath: string,
): string {
    if (path.isAbsolute(sessionPath)) {
        return sessionPath;
    }

    return path.join(
        artifact.root_path,
        sessionPath.replaceAll('/', path.sep),
    );
}

function normalizeSessionSelector(sessionPath: string): string {
    return sessionPath.replaceAll(path.sep, '/').replaceAll(/^\.?\//g, '');
}

async function insertGherkinDataTable(): Promise<void> {
    const editor = vscode.window.activeTextEditor;
    if (!editor || editor.document.languageId !== 'gherkin') {
        void vscode.window.showWarningMessage(
            'Abre un fichero .feature para insertar una tabla Gherkin.',
        );
        return;
    }

    const rows = await promptPositiveInteger('Filas de datos', '2');
    if (rows === undefined) {
        return;
    }

    const columns = await promptPositiveInteger('Columnas', '2');
    if (columns === undefined) {
        return;
    }

    const table = generateGherkinDataTable(rows, columns);
    await editor.insertSnippet(
        new vscode.SnippetString(table),
        editor.selection.active,
    );
}

async function promptPositiveInteger(
    prompt: string,
    defaultValue: string,
): Promise<number | undefined> {
    const rawValue = await vscode.window.showInputBox({
        prompt,
        value: defaultValue,
        validateInput: (value) => {
            const parsed = Number.parseInt(value, 10);
            return parsed > 0 ? undefined : 'Introduce un entero mayor que 0.';
        },
    });
    if (!rawValue) {
        return undefined;
    }

    return Number.parseInt(rawValue, 10);
}

function generateGherkinDataTable(rows: number, columns: number): string {
    const header = `| ${Array.from(
        { length: columns },
        (_, index) => `\${${index + 1}:column}`,
    ).join(' | ')} |`;
    const row = `| ${Array.from(
        { length: columns },
        () => '   ',
    ).join(' | ')} |`;

    return [header, ...Array.from({ length: rows }, () => row)].join('\n');
}

function buildKnowledgeBaseNode(
    knowledgeBase: KnowledgeBaseInfo,
    workspaceInfo: WorkspaceInfo,
    workspacePath?: string,
): CosechaTreeNode {
    const alignmentIssue = detectKnowledgeBaseAlignmentIssue(
        knowledgeBase,
        workspaceInfo,
    );
    const label = knowledgeBase.exists
        ? 'Knowledge Base disponible'
        : 'Knowledge Base ausente';
    const node = new CosechaTreeNode(label);
    node.description = [
        knowledgeBase.schema_version
            ? `schema ${knowledgeBase.schema_version}`
            : 'sin esquema',
        alignmentIssue ? 'desalineada' : undefined,
    ]
        .filter((value): value is string => Boolean(value))
        .join(' · ');
    node.iconPath = new vscode.ThemeIcon(
        alignmentIssue
            ? 'warning'
            : knowledgeBase.exists
                ? 'database'
                : 'warning',
    );
    node.command = {
        command: 'cosecha.showKnowledgeBaseInfo',
        title: 'Show Knowledge Base',
        arguments: [workspacePath],
    };
    node.tooltip = alignmentIssue
        ? `${knowledgeBase.knowledge_base_path}\n${alignmentIssue}`
        : knowledgeBase.knowledge_base_path;
    return node;
}

function buildWorkspaceContextNode(
    workspaceInfo: WorkspaceInfo,
    workspacePath?: string,
): CosechaTreeNode {
    const node = new CosechaTreeNode('Workspace context');
    const fingerprint = workspaceInfo.workspace_fingerprint;
    node.description = [
        workspaceInfo.knowledge_anchor
            ? path.basename(workspaceInfo.knowledge_anchor)
            : undefined,
        fingerprint ? shortFingerprint(fingerprint) : undefined,
    ]
        .filter((value): value is string => Boolean(value))
        .join(' · ');
    node.iconPath = new vscode.ThemeIcon('folder-library');
    node.command = {
        command: 'cosecha.showKnowledgeBaseInfo',
        title: 'Show Knowledge Base',
        arguments: [workspacePath],
    };
    node.tooltip = [
        `workspace_root: ${workspaceInfo.workspace_root ?? workspaceInfo.project_path}`,
        `knowledge_anchor: ${workspaceInfo.knowledge_anchor ?? workspaceInfo.root_path}`,
        workspaceInfo.execution_root
            ? `execution_root: ${workspaceInfo.execution_root}`
            : undefined,
        workspaceInfo.workspace_fingerprint
            ? `workspace_fingerprint: ${workspaceInfo.workspace_fingerprint}`
            : undefined,
    ]
        .filter((value): value is string => Boolean(value))
        .join('\n');
    return node;
}

function buildSnapshotCountsNode(
    knowledgeBase: KnowledgeBaseInfo,
    workspacePath?: string,
): CosechaTreeNode {
    const counts = knowledgeBase.current_snapshot_counts;
    const node = new CosechaTreeNode('Snapshot counts');
    node.description = counts
        ? `tests ${counts.tests} · defs ${counts.definitions}`
        : '';
    node.iconPath = new vscode.ThemeIcon('symbol-number');
    node.command = {
        command: 'cosecha.showKnowledgeBaseInfo',
        title: 'Show Knowledge Base',
        arguments: [workspacePath],
    };
    return node;
}

function buildCurrentFileTestsNode(
    tests: TestRecord[],
    latestSessionId?: string,
    workspacePath?: string,
): CosechaTreeNode {
    if (tests.length === 0) {
        return buildInfoNode('Sin tests indexados para el fichero activo.');
    }

    const children = tests.map((test) => {
        const node = new CosechaTreeNode(test.test_name);
        const lastSessionStatus = resolveLastSessionStatus(test, latestSessionId);
        node.description = buildTestDescription(test, latestSessionId);
        node.iconPath = new vscode.ThemeIcon(
            lastSessionStatus
                ? getThemeIconForStatus(lastSessionStatus)
                : 'beaker',
        );
        node.command = {
            command: 'cosecha.runTestPath',
            title: 'Run Test Path',
            arguments: [test.test_path, test.test_name, workspacePath],
        };
        return node;
    });

    const group = new CosechaTreeNode(
        `Tests del fichero activo (${tests.length})`,
        vscode.TreeItemCollapsibleState.Expanded,
        children,
    );
    group.iconPath = new vscode.ThemeIcon('testing-run-icon');
    return group;
}

function buildCurrentFileDefinitionsNode(
    definitions: DefinitionRecord[],
    workspacePath?: string,
): CosechaTreeNode {
    const flattenedDescriptors = definitions.flatMap((definition) =>
        definition.descriptors.map((descriptor) => ({
            ...descriptor,
            engine_name: definition.engine_name,
        })),
    );
    if (flattenedDescriptors.length === 0) {
        return buildInfoNode('Sin definiciones indexadas para el fichero activo.');
    }

    const children = flattenedDescriptors.map((descriptor) => {
        const node = new CosechaTreeNode(descriptor.function_name);
        node.description = [
            descriptor.engine_name,
            `L${descriptor.source_line}`,
            descriptor.category ?? undefined,
        ]
            .filter((value): value is string => Boolean(value))
            .join(' · ');
        node.iconPath = new vscode.ThemeIcon('symbol-function');
        node.command = {
            command: 'cosecha.openJsonPayload',
            title: 'Open Definition Payload',
            arguments: [
                `Cosecha Definition: ${descriptor.function_name}`,
                descriptor,
                workspacePath,
            ],
        };
        return node;
    });

    const group = new CosechaTreeNode(
        `Definiciones del fichero activo (${flattenedDescriptors.length})`,
        vscode.TreeItemCollapsibleState.Collapsed,
        children,
    );
    group.iconPath = new vscode.ThemeIcon('list-tree');
    return group;
}

function buildSessionNode(
    session: SessionArtifactRecord,
    workspaceFolder: vscode.WorkspaceFolder,
): CosechaTreeNode {
    const node = new CosechaTreeNode(shortSessionLabel(session.session_id));
    node.description = buildSessionDescription(session);
    node.iconPath = new vscode.ThemeIcon(getSessionIcon(session));
    node.contextValue = 'sessionArtifact';
    node.command = {
        command: 'cosecha.showSessionArtifact',
        title: 'Show Session Artifact',
        arguments: [session.session_id, workspaceFolder.uri.fsPath],
    };
    node.sessionTarget = {
        root_path: workspaceFolder.uri.fsPath,
        session_id: session.session_id,
    };
    node.tooltip = [
        `session_id: ${session.session_id}`,
        session.trace_id ? `trace_id: ${session.trace_id}` : undefined,
        `recorded_at: ${formatTimestamp(session.recorded_at)}`,
        buildSessionSummaryTooltip(session),
    ]
        .filter((value): value is string => Boolean(value))
        .join('\n');
    return node;
}

function buildInfoNode(message: string): CosechaTreeNode {
    const node = new CosechaTreeNode(message);
    node.iconPath = new vscode.ThemeIcon('info');
    return node;
}

function detectKnowledgeBaseAlignmentIssue(
    knowledgeBase: KnowledgeBaseInfo,
    workspaceInfo: WorkspaceInfo,
): string | undefined {
    const expectedRoot = workspaceInfo.workspace_root ?? workspaceInfo.project_path;
    const expectedAnchor = workspaceInfo.knowledge_anchor ?? workspaceInfo.root_path;
    if (
        path.normalize(knowledgeBase.project_path)
        !== path.normalize(expectedRoot)
    ) {
        return (
            'La KB esta asociada a un workspace_root distinto del resuelto '
            + `(${knowledgeBase.project_path} != ${expectedRoot}).`
        );
    }
    if (
        path.normalize(knowledgeBase.root_path)
        !== path.normalize(expectedAnchor)
    ) {
        return (
            'La KB apunta a un knowledge_anchor distinto del workspace activo '
            + `(${knowledgeBase.root_path} != ${expectedAnchor}).`
        );
    }

    const artifactFingerprint = (
        knowledgeBase.latest_session_artifact?.workspace_fingerprint
    );
    if (
        typeof artifactFingerprint === 'string'
        && artifactFingerprint.length > 0
        && typeof workspaceInfo.workspace_fingerprint === 'string'
        && workspaceInfo.workspace_fingerprint.length > 0
        && artifactFingerprint !== workspaceInfo.workspace_fingerprint
    ) {
        return (
            'La ultima sesion persistida pertenece a otro workspace_fingerprint '
            + `(${artifactFingerprint} != ${workspaceInfo.workspace_fingerprint}).`
        );
    }

    return undefined;
}

function buildActionNode(
    label: string,
    tooltip: string,
    iconId: string,
    command: string,
): CosechaTreeNode {
    const node = new CosechaTreeNode(label);
    node.tooltip = tooltip;
    node.iconPath = new vscode.ThemeIcon(iconId);
    node.command = {
        command,
        title: label,
    };
    return node;
}

function resolveSessionContextTarget(
    value: SessionContextTarget | CosechaTreeNode | undefined,
): SessionContextTarget | undefined {
    if (!value) {
        return undefined;
    }
    if (value instanceof CosechaTreeNode) {
        return value.sessionTarget;
    }

    return value;
}

function buildSessionDescription(session: SessionArtifactRecord): string {
    const statusCounts = readSessionStatusCounts(session);
    const summary = formatSessionStatusCounts(statusCounts);
    if (!summary) {
        return formatTimestamp(session.recorded_at);
    }

    return `${formatTimestamp(session.recorded_at)} · ${summary}`;
}

function buildSessionSummaryTooltip(session: SessionArtifactRecord): string | undefined {
    const statusCounts = readSessionStatusCounts(session);
    const lines: string[] = [];
    const summary = formatSessionStatusCounts(statusCounts);
    if (summary) {
        lines.push(`summary: ${summary}`);
    }
    const sessionSummary = readSessionSummary(session);
    if (typeof sessionSummary.coverage_total === 'number') {
        lines.push(`coverage: ${sessionSummary.coverage_total.toFixed(2)}%`);
    }
    if (typeof sessionSummary.total_tests === 'number') {
        lines.push(`total_tests: ${sessionSummary.total_tests}`);
    }

    return lines.length > 0 ? lines.join('\n') : undefined;
}

function getSessionIcon(session: SessionArtifactRecord): string {
    const statusCounts = readSessionStatusCounts(session);
    const failedCount = statusCounts.get('failed') ?? 0;
    const errorCount = statusCounts.get('error') ?? 0;
    const runningCount = statusCounts.get('running') ?? 0;
    const pendingCount = statusCounts.get('pending') ?? 0;
    const passedCount = statusCounts.get('passed') ?? 0;
    const skippedCount = statusCounts.get('skipped') ?? 0;

    if (failedCount > 0 || errorCount > 0 || sessionHasFailures(session)) {
        return 'testing-failed-icon';
    }
    if (runningCount > 0 || pendingCount > 0) {
        return 'loading';
    }
    if (passedCount > 0) {
        return 'testing-passed-icon';
    }
    if (skippedCount > 0) {
        return 'testing-skipped-icon';
    }
    return 'history';
}

function sessionHasFailures(session: SessionArtifactRecord): boolean {
    return session.has_failures === true;
}

function readSessionSummary(
    session: SessionArtifactRecord,
): SessionSummaryRecord {
    const summary = asRecord(session.report_summary);
    return (summary as SessionSummaryRecord | undefined) ?? {};
}

function readSessionFailedFiles(session: SessionArtifactRecord): string[] {
    const failedFiles = readSessionSummary(session).failed_files;
    if (!Array.isArray(failedFiles)) {
        return [];
    }

    return failedFiles.filter(
        (value): value is string =>
            typeof value === 'string' && value.trim().length > 0,
    );
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

function buildErrorNode(error: unknown): CosechaTreeNode {
    const detail = error instanceof Error ? error.message : String(error);
    const node = new CosechaTreeNode('Error cargando datos de Cosecha');
    node.description = detail;
    node.iconPath = new vscode.ThemeIcon('error');
    return node;
}

function buildTestDescription(
    test: TestRecord,
    latestSessionId?: string,
): string {
    const lastSessionStatus = resolveLastSessionStatus(test, latestSessionId);
    return [
        test.engine_name,
        test.source_line ? `L${test.source_line}` : undefined,
        lastSessionStatus
            ? `ultima sesion: ${formatStatusLabel(lastSessionStatus)}`
            : undefined,
    ]
        .filter((value): value is string => Boolean(value))
        .join(' · ');
}

async function getLatestSessionResult(
    bridge: CosechaBridge,
): Promise<LatestSessionResult> {
    const sessions = await bridge.listRecentSessions(1);
    return {
        sessionId: sessions[0]?.session_id,
    };
}

function buildLineStatusSummaries(
    tests: TestRecord[],
    latestSessionId?: string,
): LineStatusSummary[] {
    if (!latestSessionId) {
        return [];
    }

    const testsByLine = new Map<number, TestRecord[]>();
    for (const test of tests) {
        if (
            test.source_line == null
            || test.source_line <= 0
            || test.session_id !== latestSessionId
            || !test.status
        ) {
            continue;
        }

        const existing = testsByLine.get(test.source_line) ?? [];
        existing.push(test);
        testsByLine.set(test.source_line, existing);
    }

    return Array.from(testsByLine.entries())
        .sort(([leftLine], [rightLine]) => leftLine - rightLine)
        .map(([line, lineTests]) => {
            const statuses = lineTests
                .map((test) => normalizeStatus(test.status))
                .filter(
                    (status): status is LastSessionStatus => status !== undefined,
                );
            const status = summarizeStatuses(statuses);

            return {
                hoverMessage: buildLineStatusHover(lineTests, status),
                line: line - 1,
                status,
            };
        });
}

function buildLineStatusHover(
    tests: TestRecord[],
    status: LastSessionStatus,
): vscode.MarkdownString {
    const markdown = new vscode.MarkdownString(undefined, true);
    markdown.appendMarkdown(
        `**Cosecha · ultima sesion: ${formatStatusLabel(status)}**\n\n`,
    );
    for (const test of tests) {
        markdown.appendMarkdown(
            `- ${test.test_name}: ${formatStatusLabel(
                normalizeStatus(test.status) ?? status,
            )}\n`,
        );
    }
    markdown.isTrusted = false;
    return markdown;
}

function resolveLastSessionStatus(
    test: TestRecord,
    latestSessionId?: string,
): LastSessionStatus | undefined {
    if (
        !latestSessionId
        || !test.status
        || !test.session_id
        || test.session_id !== latestSessionId
    ) {
        return undefined;
    }

    return normalizeStatus(test.status);
}

function normalizeStatus(status: string | null | undefined): LastSessionStatus | undefined {
    if (!status) {
        return undefined;
    }

    if (status === 'passed') {
        return 'passed';
    }
    if (status === 'skipped') {
        return 'skipped';
    }
    if (status === 'running' || status === 'pending') {
        return 'running';
    }
    if (status === 'failed' || status === 'error') {
        return 'failed';
    }

    return 'mixed';
}

function summarizeStatuses(
    statuses: LastSessionStatus[],
): LastSessionStatus {
    if (statuses.length === 0) {
        return 'mixed';
    }

    const uniqueStatuses = new Set(statuses);
    if (uniqueStatuses.size === 1) {
        return statuses[0];
    }
    if (uniqueStatuses.has('failed')) {
        return 'failed';
    }
    if (uniqueStatuses.has('running')) {
        return 'running';
    }
    return 'mixed';
}

function formatLastSessionTitle(status: LastSessionStatus): string {
    if (status === 'passed') {
        return '✓ Last session passed';
    }
    if (status === 'failed') {
        return '✗ Last session failed';
    }
    if (status === 'skipped') {
        return '○ Last session skipped';
    }
    if (status === 'running') {
        return '◌ Last session running';
    }
    return '◐ Last session mixed';
}

function formatStatusLabel(status: LastSessionStatus): string {
    if (status === 'passed') {
        return 'passed';
    }
    if (status === 'failed') {
        return 'failed';
    }
    if (status === 'skipped') {
        return 'skipped';
    }
    if (status === 'running') {
        return 'running';
    }
    return 'mixed';
}

function getThemeIconForStatus(status: LastSessionStatus): string {
    if (status === 'passed') {
        return 'testing-passed-icon';
    }
    if (status === 'failed') {
        return 'testing-failed-icon';
    }
    if (status === 'skipped') {
        return 'testing-skipped-icon';
    }
    if (status === 'running') {
        return 'loading';
    }
    return 'warning';
}

function shortSessionLabel(sessionId: string): string {
    return sessionId.length > 12
        ? `${sessionId.slice(0, 12)}…`
        : sessionId;
}

function shortFingerprint(fingerprint: string): string {
    return fingerprint.length > 10
        ? `${fingerprint.slice(0, 10)}…`
        : fingerprint;
}

function formatTimestamp(timestamp: number): string {
    return new Date(timestamp * 1000).toLocaleString();
}

function appendOutput(message: string): void {
    outputChannel?.appendLine(`[${new Date().toISOString()}] ${message}`);
}

function renderSessionSummaryWebview(
    title: string,
    artifact: SessionArtifactRecord,
    summary: SessionSummaryRecord,
    serializedPayload: string,
): string {
    const escapedTitle = escapeHtml(title);
    const escapedPayload = escapeForScript(serializedPayload);
    const overviewCards = [
        renderMetricCard('Session', shortSessionLabel(artifact.session_id)),
        renderMetricCard(
            'Recorded at',
            formatTimestamp(artifact.recorded_at),
        ),
        renderMetricCard(
            'Total tests',
            String(summary.total_tests ?? 0),
        ),
        renderMetricCard(
            'Coverage',
            typeof summary.coverage_total === 'number'
                ? `${summary.coverage_total.toFixed(2)}%`
                : 'n/a',
        ),
        renderMetricCard(
            'Failed files',
            String(summary.failed_file_count ?? readSessionFailedFiles(artifact).length),
        ),
        renderMetricCard(
            'Snapshots',
            String(summary.live_snapshot_count ?? 0),
        ),
    ].join('');
    const statusList = renderKeyValueList(summary.status_counts);
    const failureList = renderKeyValueList(summary.failure_kind_counts);
    const failedFilesList = renderStringList(summary.failed_files);
    const engineSummaries = Array.isArray(summary.engine_summaries)
        ? summary.engine_summaries
            .map((engineSummary) => renderJsonValue(engineSummary, 'engine'))
            .join('')
        : '<p class="empty">Sin resumen por engine.</p>';
    const instrumentationSections = renderInstrumentationSections(
        summary.instrumentation_summaries,
    );

    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${escapedTitle}</title>
<style>
  :root {
    color-scheme: light dark;
    --bg: #0b1220;
    --panel: #111827;
    --panel-soft: #172033;
    --text: #e5eefb;
    --muted: #93a7c6;
    --accent: #38bdf8;
    --accent-soft: rgba(56, 189, 248, 0.15);
    --border: rgba(148, 163, 184, 0.2);
    --danger: #f87171;
    --success: #4ade80;
  }
  body {
    margin: 0;
    font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, sans-serif;
    background: radial-gradient(circle at top, #13203a 0%, #0b1220 60%);
    color: var(--text);
  }
  .toolbar {
    position: sticky;
    top: 0;
    z-index: 2;
    display: flex;
    gap: 0.75rem;
    align-items: center;
    padding: 0.9rem 1rem;
    border-bottom: 1px solid var(--border);
    background: rgba(11, 18, 32, 0.94);
    backdrop-filter: blur(10px);
  }
  .title {
    flex: 1;
    font-size: 0.95rem;
    font-weight: 700;
    letter-spacing: 0.03em;
  }
  button {
    border: 1px solid var(--border);
    background: #172554;
    color: var(--text);
    padding: 0.45rem 0.75rem;
    border-radius: 999px;
    cursor: pointer;
  }
  button:hover {
    border-color: var(--accent);
  }
  main {
    padding: 1rem;
    display: grid;
    gap: 1rem;
  }
  .card-grid {
    display: grid;
    gap: 0.75rem;
    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  }
  .card, .section {
    background: rgba(17, 24, 39, 0.88);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 0.9rem 1rem;
    box-shadow: 0 10px 24px rgba(0, 0, 0, 0.18);
  }
  .metric-label, h2 {
    color: var(--muted);
    font-size: 0.76rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    margin: 0 0 0.4rem 0;
  }
  .metric-value {
    font-size: 1.1rem;
    font-weight: 700;
  }
  .section h2 {
    margin-bottom: 0.75rem;
  }
  ul {
    margin: 0;
    padding-left: 1.1rem;
  }
  li + li {
    margin-top: 0.3rem;
  }
  .empty {
    color: var(--muted);
    margin: 0;
  }
  .pill {
    display: inline-flex;
    align-items: center;
    gap: 0.35rem;
    padding: 0.2rem 0.55rem;
    border-radius: 999px;
    background: var(--accent-soft);
    border: 1px solid var(--border);
    margin: 0 0.5rem 0.5rem 0;
  }
  .danger {
    color: var(--danger);
  }
  .success {
    color: var(--success);
  }
  details {
    margin-left: 1rem;
    border-left: 1px dashed var(--border);
    padding-left: 0.75rem;
  }
  summary {
    cursor: pointer;
    list-style: none;
    padding: 0.18rem 0;
  }
  summary::-webkit-details-marker {
    display: none;
  }
  .key {
    color: var(--accent);
  }
  .meta {
    color: var(--muted);
    margin-left: 0.35rem;
  }
  .value {
    padding: 0.15rem 0;
  }
  .string { color: #86efac; }
  .number { color: #f9a8d4; }
  .boolean { color: #fcd34d; }
  .null { color: #c4b5fd; }
</style>
</head>
<body>
  <div class="toolbar">
    <div class="title">${escapedTitle}</div>
    <button id="openRawJson">Open Raw JSON</button>
    <button id="copyJson">Copy JSON</button>
    <button id="exportJson">Export JSON</button>
  </div>
  <main>
    <section class="card-grid">${overviewCards}</section>
    <section class="section">
      <h2>Statuses</h2>
      ${statusList}
    </section>
    <section class="section">
      <h2>Failures</h2>
      ${failureList}
      ${failedFilesList}
    </section>
    <section class="section">
      <h2>Instrumentation</h2>
      ${instrumentationSections}
    </section>
    <section class="section">
      <h2>Engine summaries</h2>
      ${engineSummaries}
    </section>
  </main>
  <script>
    const vscode = acquireVsCodeApi();
    const payload = ${escapedPayload};
    document.getElementById('copyJson').addEventListener('click', () => {
      vscode.postMessage({ command: 'copyJson', payload });
    });
    document.getElementById('exportJson').addEventListener('click', () => {
      vscode.postMessage({ command: 'exportJson', payload });
    });
    document.getElementById('openRawJson').addEventListener('click', () => {
      vscode.postMessage({ command: 'openRawJson', payload });
    });
  </script>
</body>
</html>`;
}

function renderMetricCard(label: string, value: string): string {
    return (
        '<div class="card">'
        + `<div class="metric-label">${escapeHtml(label)}</div>`
        + `<div class="metric-value">${escapeHtml(value)}</div>`
        + '</div>'
    );
}

function renderKeyValueList(
    entries: Record<string, number> | Array<[string, number]> | undefined,
): string {
    const normalizedEntries = Array.isArray(entries)
        ? entries
        : entries
            ? Object.entries(entries)
            : [];
    if (normalizedEntries.length === 0) {
        return '<p class="empty">Sin datos.</p>';
    }

    return [
        '<ul>',
        ...normalizedEntries.map(
            ([key, value]) =>
                `<li><span class="pill"><strong>${escapeHtml(key)}</strong> ${escapeHtml(String(value))}</span></li>`,
        ),
        '</ul>',
    ].join('');
}

function renderStringList(values: string[] | undefined): string {
    if (!Array.isArray(values) || values.length === 0) {
        return '<p class="empty">Sin ficheros fallidos.</p>';
    }

    return [
        '<ul>',
        ...values.map((value) => `<li>${escapeHtml(value)}</li>`),
        '</ul>',
    ].join('');
}

function renderInstrumentationSections(
    summaries: Record<string, Record<string, unknown>> | undefined,
): string {
    if (!summaries || Object.keys(summaries).length === 0) {
        return '<p class="empty">Sin instrumentacion persistida.</p>';
    }

    return Object.entries(summaries)
        .map(([name, summary]) =>
            [
                `<div class="card"><div class="metric-label">${escapeHtml(name)}</div>`,
                renderJsonValue(summary, name),
                '</div>',
            ].join(''),
        )
        .join('');
}

function renderJsonPayloadWebview(
    webview: vscode.Webview,
    title: string,
    payload: object,
    serializedPayload: string,
): string {
    const body = renderJsonValue(payload, 'root');
    const escapedTitle = escapeHtml(title);
    const escapedPayload = escapeForScript(serializedPayload);

    return `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>${escapedTitle}</title>
<style>
  :root {
    color-scheme: light dark;
    --bg: #111827;
    --panel: #0f172a;
    --muted: #94a3b8;
    --text: #e2e8f0;
    --accent: #38bdf8;
    --border: rgba(148, 163, 184, 0.25);
    --string: #86efac;
    --number: #f9a8d4;
    --boolean: #fcd34d;
    --null: #c4b5fd;
  }
  body {
    margin: 0;
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    background: linear-gradient(180deg, #0b1220 0%, #111827 100%);
    color: var(--text);
  }
  .toolbar {
    position: sticky;
    top: 0;
    z-index: 2;
    display: flex;
    gap: 0.75rem;
    align-items: center;
    padding: 0.9rem 1rem;
    border-bottom: 1px solid var(--border);
    background: rgba(15, 23, 42, 0.95);
    backdrop-filter: blur(8px);
  }
  .title {
    flex: 1;
    font-size: 0.95rem;
    font-weight: 700;
    letter-spacing: 0.03em;
  }
  button {
    border: 1px solid var(--border);
    background: #172554;
    color: var(--text);
    padding: 0.45rem 0.75rem;
    border-radius: 999px;
    cursor: pointer;
  }
  button:hover {
    border-color: var(--accent);
  }
  .content {
    padding: 1rem;
  }
  details {
    margin-left: 1rem;
    border-left: 1px dashed var(--border);
    padding-left: 0.75rem;
  }
  details.root {
    margin-left: 0;
    padding-left: 0;
    border-left: 0;
  }
  summary {
    cursor: pointer;
    list-style: none;
    padding: 0.18rem 0;
  }
  summary::-webkit-details-marker {
    display: none;
  }
  .key {
    color: var(--accent);
  }
  .meta {
    color: var(--muted);
    margin-left: 0.35rem;
  }
  .value {
    padding: 0.15rem 0;
  }
  .string { color: var(--string); }
  .number { color: var(--number); }
  .boolean { color: var(--boolean); }
  .null { color: var(--null); }
</style>
</head>
<body>
  <div class="toolbar">
    <div class="title">${escapedTitle}</div>
    <button id="copyJson">Copy JSON</button>
    <button id="exportJson">Export JSON</button>
  </div>
  <div class="content">${body}</div>
  <script>
    const vscode = acquireVsCodeApi();
    const payload = ${escapedPayload};
    document.getElementById('copyJson').addEventListener('click', () => {
      vscode.postMessage({ command: 'copyJson', payload });
    });
    document.getElementById('exportJson').addEventListener('click', () => {
      vscode.postMessage({ command: 'exportJson', payload });
    });
  </script>
</body>
</html>`;
}

function renderJsonValue(value: unknown, key: string): string {
    if (Array.isArray(value)) {
        const children = value
            .map((item, index) => renderJsonEntry(String(index), item))
            .join('');
        return (
            `<details class="${key === 'root' ? 'root' : ''}" open>`
            + `<summary><span class="key">${escapeHtml(key)}</span>`
            + `<span class="meta">array[${value.length}]</span></summary>`
            + children
            + '</details>'
        );
    }

    const record = asRecord(value);
    if (record) {
        const entries = Object.entries(record);
        const children = entries
            .map(([entryKey, entryValue]) => renderJsonEntry(entryKey, entryValue))
            .join('');
        return (
            `<details class="${key === 'root' ? 'root' : ''}" open>`
            + `<summary><span class="key">${escapeHtml(key)}</span>`
            + `<span class="meta">object{${entries.length}}</span></summary>`
            + children
            + '</details>'
        );
    }

    return renderJsonScalar(key, value);
}

function renderJsonEntry(key: string, value: unknown): string {
    if (Array.isArray(value) || asRecord(value)) {
        return renderJsonValue(value, key);
    }

    return renderJsonScalar(key, value);
}

function renderJsonScalar(key: string, value: unknown): string {
    let className = 'null';
    let formattedValue = 'null';

    if (typeof value === 'string') {
        className = 'string';
        formattedValue = `"${escapeHtml(value)}"`;
    } else if (typeof value === 'number') {
        className = 'number';
        formattedValue = String(value);
    } else if (typeof value === 'boolean') {
        className = 'boolean';
        formattedValue = value ? 'true' : 'false';
    } else if (value === null || value === undefined) {
        className = 'null';
        formattedValue = 'null';
    } else {
        className = 'string';
        formattedValue = escapeHtml(JSON.stringify(value));
    }

    return (
        `<div class="value"><span class="key">${escapeHtml(key)}</span>: `
        + `<span class="${className}">${formattedValue}</span></div>`
    );
}

function escapeHtml(value: string): string {
    return value
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
}

function escapeForScript(value: string): string {
    return JSON.stringify(value);
}

function slugifyFileName(value: string): string {
    return value
        .toLowerCase()
        .replaceAll(/[^a-z0-9]+/g, '-')
        .replaceAll(/^-+|-+$/g, '');
}

async function showCosechaOperationError(error: unknown): Promise<void> {
    const message = error instanceof Error ? error.message : String(error);
    appendOutput(`Error: ${message}`);

    if (message.includes('Rebuild Knowledge Base')) {
        const selection = await vscode.window.showErrorMessage(
            message,
            'Rebuild Knowledge Base',
        );
        if (selection === 'Rebuild Knowledge Base') {
            await runCliCommand('knowledge rebuild', {
                refreshAfterSuccess: true,
                refreshScope: 'all',
                title: 'Cosecha Knowledge Rebuild',
            });
        }
        return;
    }

    void vscode.window.showErrorMessage(message);
}
