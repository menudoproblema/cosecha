import test from 'node:test';
import assert from 'node:assert/strict';
import * as fs from 'node:fs';
import * as os from 'node:os';
import * as path from 'node:path';
import { spawn } from 'node:child_process';
import { spawnSync } from 'node:child_process';


const extensionRoot = path.resolve(__dirname, '..');
const repoRoot = path.resolve(extensionRoot, '../..');
const bridgeScriptPath = path.join(extensionRoot, 'python', 'cosecha_bridge.py');
const pythonPath = path.join(repoRoot, '.venv', 'bin', 'python');
const canRunBridge = fs.existsSync(bridgeScriptPath) && fs.existsSync(pythonPath);

type BridgeResult = {
    knowledge_anchor?: string;
    knowledge_base_path?: string;
    manifest_path?: string | null;
    project_path?: string;
    root_path?: string;
    workspace_fingerprint?: string | null;
    workspace_root?: string;
    workspace?: BridgeResult;
    exists?: boolean;
    tests?: unknown[];
    artifacts?: unknown[];
};

async function createWorkspaceFixture(): Promise<string> {
    const rawFixtureRoot = await fs.promises.mkdtemp(
        path.join(os.tmpdir(), 'cosecha-vscode-bridge-'),
    );
    const fixtureRoot = await fs.promises.realpath(rawFixtureRoot);
    await fs.promises.mkdir(path.join(fixtureRoot, 'tests'), { recursive: true });
    await fs.promises.mkdir(path.join(fixtureRoot, 'src'), { recursive: true });
    await fs.promises.writeFile(
        path.join(fixtureRoot, 'cosecha.toml'),
        [
            '[manifest]',
            'schema_version = 1',
            '',
            '[workspace]',
            'root = "."',
            'knowledge_anchor = "tests"',
            '',
        ].join('\n'),
        'utf8',
    );
    return fixtureRoot;
}

async function runBridge(
    request: Record<string, unknown>,
): Promise<BridgeResult> {
    return await new Promise((resolve, reject) => {
        const child = spawn(
            pythonPath,
            [bridgeScriptPath],
            {
                cwd: repoRoot,
                stdio: ['pipe', 'pipe', 'pipe'],
            },
        );
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
        child.on('error', reject);
        child.on('close', (code) => {
            if (code !== 0) {
                reject(new Error(stderr.trim() || stdout.trim() || `bridge exited with ${code}`));
                return;
            }
            resolve(JSON.parse(stdout) as BridgeResult);
        });

        child.stdin.write(JSON.stringify(request));
        child.stdin.end();
    });
}

test(
    'bridge describe_workspace uses canonical workspace resolution',
    { skip: !canRunBridge },
    async () => {
        const workspaceRoot = await createWorkspaceFixture();
        try {
            const result = await runBridge({
                operation: 'describe_workspace',
                start_path: path.join(workspaceRoot, 'src'),
            });

            assert.equal(result.project_path, workspaceRoot);
            assert.equal(result.workspace_root, workspaceRoot);
            assert.equal(result.root_path, path.join(workspaceRoot, 'tests'));
            assert.equal(
                result.knowledge_anchor,
                path.join(workspaceRoot, 'tests'),
            );
            assert.equal(
                result.knowledge_base_path,
                path.join(workspaceRoot, '.cosecha', 'kb.db'),
            );
            assert.equal(
                result.manifest_path,
                path.join(workspaceRoot, 'cosecha.toml'),
            );
            assert.match(result.workspace_fingerprint ?? '', /^[a-f0-9]{64}$/);
        } finally {
            await fs.promises.rm(workspaceRoot, { recursive: true, force: true });
        }
    },
);

test(
    'bridge returns empty payloads when the KB is absent',
    { skip: !canRunBridge },
    async () => {
        const workspaceRoot = await createWorkspaceFixture();
        try {
            const knowledgeBase = await runBridge({
                operation: 'describe_knowledge_base',
                start_path: workspaceRoot,
            });
            assert.equal(knowledgeBase.exists, false);
            assert.equal(
                knowledgeBase.knowledge_base_path,
                path.join(workspaceRoot, '.cosecha', 'kb.db'),
            );

            const testsPayload = await runBridge({
                operation: 'query_tests',
                start_path: workspaceRoot,
                test_path: path.join(workspaceRoot, 'tests', 'sample.feature'),
                limit: 16,
            });
            assert.deepEqual(testsPayload.tests, []);
            assert.match(
                testsPayload.workspace?.workspace_fingerprint ?? '',
                /^[a-f0-9]{64}$/,
            );

            const sessionsPayload = await runBridge({
                operation: 'list_recent_sessions',
                start_path: workspaceRoot,
                limit: 8,
            });
            assert.deepEqual(sessionsPayload.artifacts, []);
            assert.equal(
                sessionsPayload.workspace?.knowledge_anchor,
                path.join(workspaceRoot, 'tests'),
            );
        } finally {
            await fs.promises.rm(workspaceRoot, { recursive: true, force: true });
        }
    },
);

test(
    'bridge serializes slotted dataclasses without using __dict__',
    { skip: !canRunBridge },
    () => {
        const probe = spawnSync(
            pythonPath,
            [
                '-c',
                [
                    'import importlib.util, json',
                    `spec = importlib.util.spec_from_file_location("cosecha_bridge", ${JSON.stringify(bridgeScriptPath)})`,
                    'module = importlib.util.module_from_spec(spec)',
                    'assert spec.loader is not None',
                    'spec.loader.exec_module(module)',
                    'from cosecha.core.knowledge_base import PlanKnowledge, SessionKnowledge',
                    'session = SessionKnowledge(root_path=".", workspace_fingerprint="abc", concurrency=2, session_id="s1", trace_id="t1", started_at=1.5)',
                    'plan = PlanKnowledge(mode="run", executable=True, node_count=3, issue_count=0, plan_id="p1", correlation_id="c1", session_id="s1", trace_id="t1", analyzed_at=2.5)',
                    'payload = {"session": module.serialize_structured_value(session), "plan": module.serialize_structured_value(plan)}',
                    'print(json.dumps(payload, sort_keys=True))',
                ].join('; '),
            ],
            {
                cwd: repoRoot,
                encoding: 'utf8',
            },
        );

        assert.equal(probe.status, 0, probe.stderr || probe.stdout);
        const payload = JSON.parse(probe.stdout) as {
            session: Record<string, unknown>;
            plan: Record<string, unknown>;
        };
        assert.equal(payload.session.session_id, 's1');
        assert.equal(payload.session.workspace_fingerprint, 'abc');
        assert.equal(payload.plan.plan_id, 'p1');
        assert.equal(payload.plan.node_count, 3);
    },
);
