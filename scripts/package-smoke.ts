import { mkdtempSync, readFileSync, rmSync, writeSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { WsTransport } from 'mud-framework-connector-ws';
import {
  ConnectionLayer,
  createJsonLogger,
  IpcExecutor,
  spawnBunServer,
} from 'mud-framework-core';
import type { Logger } from 'mud-framework-core';
import { SqliteBackend } from 'mud-framework-storage-sqlite';
import {
  isProcessAlive,
  isRecord,
  openClient,
  parseFrame,
  sleep,
} from 'mud-framework-test-support';

/**
 * Manual end-to-end check of the path a consumer actually takes; it is not
 * wired into CI. Every framework import below uses a package name, never a
 * relative path into `packages/*`. It starts a real Portal in this process,
 * which spawns a real Bun Server, then sends one request over a real WebSocket
 * and proves the write reached a real SQLite file before shutting everything
 * down and checking that no child was left behind.
 *
 * Run with `bun run smoke:package`. The root devDependencies exist so Bun's
 * default isolated linker creates the package links this file imports; a
 * `bun install --production` omits them and does not create those links. See
 * docs/INTEGRATION.md for the assembly.
 *
 * The post-stop child check reads Linux `/proc/<pid>/task/<pid>/children`.
 * Without `/proc` it is reported as skipped, never as "no children".
 *
 * `PACKAGE_SMOKE_HANG=1` stops after the Server is ready and never continues,
 * which exercises the watchdog and cleanup path. `PACKAGE_SMOKE_TIMEOUT_MS`
 * shortens the watchdog for that test. `PACKAGE_SMOKE_LEFTOVER_CHILD=1` spawns
 * an untracked child after `stop()`, so the success path must detect it instead
 * of letting cleanup hide it.
 */

const SERVER_ENTRY = join(import.meta.dir, 'package-smoke', 'server.ts');
const REQUEST_TIMEOUT_MS = 5000;
const DEFAULT_OVERALL_TIMEOUT_MS = 30_000;
const CLEANUP_STOP_TIMEOUT_MS = 5000;
const CLEANUP_KILL_WAIT_MS = 2000;
const KILL_POLL_MS = 25;
const PORTAL_LOG_TAIL = 20;
const SERVER_LOG_TAIL = 30;
const HANG_ENV = 'PACKAGE_SMOKE_HANG';
const LEFTOVER_ENV = 'PACKAGE_SMOKE_LEFTOVER_CHILD';
const OVERALL_TIMEOUT_ENV = 'PACKAGE_SMOKE_TIMEOUT_MS';

interface CapturedLogs {
  readonly logger: Logger;
  readonly lines: Record<string, unknown>[];
}

interface ProcessSnapshot {
  readonly pids: readonly { readonly pid: number; readonly alive: boolean }[];
  readonly children: number[] | undefined;
}

type StopOutcome = 'settled' | 'failed' | 'timeout';

interface StopResult {
  readonly outcome: StopOutcome;
  readonly error: string | undefined;
}

interface CleanupResult {
  /** Pids the cleanup attempted to kill, including ones already gone. */
  readonly killed: readonly number[];
  /** Pids still alive after the bounded wait; non-empty means cleanup failed. */
  readonly remaining: readonly number[];
  readonly stopOutcome: StopOutcome;
  readonly stopError: string | undefined;
  /** Children seen after the cleanup stop, before the kill. */
  readonly childrenBefore: number[] | undefined;
  /** Children seen after the bounded wait; `undefined` means /proc is absent. */
  readonly childrenAfter: number[] | undefined;
  /** True only when no known pid and no listed child can still write. */
  readonly canRemoveTempDir: boolean;
}

interface Diagnostics {
  readonly databasePath: string;
  readonly directory: string;
  readonly logs: CapturedLogs;
  readonly serverLogTail: readonly string[];
  readonly snapshot: ProcessSnapshot;
  readonly cleanup: CleanupResult;
  readonly tempDirRemoved: boolean;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function writeStdout(text: string): void {
  writeSync(1, text);
}

function writeStderr(text: string): void {
  writeSync(2, text);
}

/**
 * `process.exitCode` alone can leave a failed run waiting on a handle the
 * cleanup could not close, so the smoke exits explicitly after its bounded
 * cleanup instead.
 */
function exitFailed(): never {
  process.exitCode = 1;
  // oxlint-disable-next-line unicorn/no-process-exit -- a failed smoke must not hang on a handle cleanup could not close
  process.exit(1);
}

/**
 * The success line is written with `writeSync`, so it is already flushed when
 * this exits; the explicit exit keeps a stray handle from hanging a passed run.
 */
function exitSucceeded(): never {
  process.exitCode = 0;
  // oxlint-disable-next-line unicorn/no-process-exit -- a passed smoke must not hang on a handle layer.stop() did not close
  process.exit(0);
}

function readPositiveInteger(name: string, fallback: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(
      `${name} must be a positive integer, got ${JSON.stringify(raw)}`,
    );
  }
  return value;
}

function captureLogs(): CapturedLogs {
  const lines: Record<string, unknown>[] = [];
  const logger = createJsonLogger({
    level: 'debug',
    sink: (line) => {
      const parsed: unknown = JSON.parse(line);
      if (isRecord(parsed)) {
        lines.push(parsed);
      }
    },
  });
  return { logger, lines };
}

/** Every `server ready` line, in order; a restart adds another generation. */
function serverPids(lines: readonly Record<string, unknown>[]): number[] {
  const pids: number[] = [];
  for (const line of lines) {
    if (line.msg === 'server ready' && typeof line.pid === 'number') {
      pids.push(line.pid);
    }
  }
  return pids;
}

function recordServerPids(
  pids: number[],
  lines: readonly Record<string, unknown>[],
): void {
  for (const pid of serverPids(lines)) {
    if (!pids.includes(pid)) {
      pids.push(pid);
    }
  }
}

/**
 * Linux exposes the Portal's direct children here. `undefined` means the child
 * list could not be read; it must never be reported as "no children".
 */
function processChildren(): number[] | undefined {
  try {
    const raw = readFileSync(
      `/proc/${process.pid}/task/${process.pid}/children`,
      'utf8',
    ).trim();
    return raw.length === 0 ? [] : raw.split(/\s+/).map(Number);
  } catch {
    return undefined;
  }
}

function captureSnapshot(pids: readonly number[]): ProcessSnapshot {
  return {
    pids: pids.map((pid) => ({ pid, alive: isProcessAlive(pid) })),
    children: processChildren(),
  };
}

function formatPids(pids: readonly number[]): string {
  return pids.length === 0 ? 'none' : pids.join(', ');
}

function formatChildren(children: number[] | undefined): string {
  if (children === undefined) {
    return 'check skipped (no /proc)';
  }
  return children.length === 0 ? 'none' : children.join(', ');
}

function nextMessage(
  socket: WebSocket,
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<string | Uint8Array> {
  return new Promise((resolve, reject) => {
    let timer: ReturnType<typeof setTimeout> | undefined;

    function cleanup(): void {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      socket.removeEventListener('message', onMessage);
      socket.removeEventListener('error', onError);
      socket.removeEventListener('close', onClose);
    }

    function fail(error: Error): void {
      cleanup();
      reject(error);
    }

    function onMessage(event: MessageEvent): void {
      const data: unknown = event.data;
      if (typeof data === 'string' || data instanceof Uint8Array) {
        cleanup();
        resolve(data);
        return;
      }
      fail(new Error(`unexpected message type: ${typeof data}`));
    }

    function onError(): void {
      fail(new Error('socket errored before a reply arrived'));
    }

    function onClose(): void {
      fail(new Error('socket closed before a reply arrived'));
    }

    socket.addEventListener('message', onMessage);
    socket.addEventListener('error', onError);
    socket.addEventListener('close', onClose);
    timer = setTimeout(() => {
      fail(new Error(`reply timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
}

async function call(
  url: string,
  request: { id: string; method: string; payload: unknown },
): Promise<Record<string, unknown>> {
  const socket = await openClient(url, { timeoutMs: REQUEST_TIMEOUT_MS });
  try {
    const reply = nextMessage(socket);
    try {
      socket.send(JSON.stringify(request));
    } catch (error) {
      socket.close();
      await reply.catch(() => undefined);
      throw error;
    }
    return parseFrame(await reply);
  } finally {
    socket.close();
  }
}

/** Reads through a second backend, so the row is not the Server's own view. */
async function readStoredValues(databasePath: string): Promise<string[]> {
  const reader = SqliteBackend.open({ filename: databasePath });
  try {
    const rows = await reader.transaction((tx) =>
      tx.query<{ value: string }>('SELECT value FROM smoke_rows'),
    );
    return rows.map((row) => row.value);
  } finally {
    await reader.close();
  }
}

async function waitUntilGone(
  targets: Set<number>,
  deadline: number,
): Promise<void> {
  for (const pid of targets) {
    if (!isProcessAlive(pid)) {
      targets.delete(pid);
    }
  }
  if (targets.size === 0 || Date.now() >= deadline) {
    return;
  }
  await sleep(KILL_POLL_MS);
  await waitUntilGone(targets, deadline);
}

/**
 * Bounded shutdown: give `layer.stop()` a budget, then SIGKILL every known pid
 * and any child the Portal still lists. `canRemoveTempDir` is only true when the
 * cleanup can prove no writer remains; the caller keeps the directory otherwise.
 */
async function stopQuietly(
  layer: ConnectionLayer,
  pids: readonly number[],
): Promise<CleanupResult> {
  const stop = layer.stop().then(
    (): StopResult => ({ outcome: 'settled', error: undefined }),
    (error: unknown): StopResult => ({
      outcome: 'failed',
      error: messageOf(error),
    }),
  );
  let stopTimer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<StopResult>((resolve) => {
    stopTimer = setTimeout(() => {
      resolve({ outcome: 'timeout', error: undefined });
    }, CLEANUP_STOP_TIMEOUT_MS);
  });
  const stopResult = await Promise.race([stop, timeout]);
  if (stopTimer !== undefined) {
    clearTimeout(stopTimer);
  }

  const childrenBefore = processChildren();
  const targets = new Set<number>(pids);
  if (childrenBefore !== undefined) {
    for (const pid of childrenBefore) {
      targets.add(pid);
    }
  }
  const killed = [...targets];
  for (const pid of killed) {
    try {
      process.kill(pid, 'SIGKILL');
    } catch {
      // Already gone, which is the expected case.
    }
  }
  await waitUntilGone(targets, Date.now() + CLEANUP_KILL_WAIT_MS);

  const remaining = [...targets];
  const childrenAfter = processChildren();
  const childrenEmpty = childrenAfter?.length === 0;
  const canRemoveTempDir =
    remaining.length === 0 &&
    (childrenEmpty ||
      (childrenAfter === undefined && stopResult.outcome !== 'timeout'));

  return {
    killed,
    remaining,
    stopOutcome: stopResult.outcome,
    stopError: stopResult.error,
    childrenBefore,
    childrenAfter,
    canRemoveTempDir,
  };
}

function logTail(path: string, count: number): string[] {
  try {
    return readFileSync(path, 'utf8')
      .split('\n')
      .filter((line) => line.length > 0)
      .slice(-count);
  } catch {
    return [`<no log at ${path}>`];
  }
}

function printDiagnostics(error: unknown, diagnostics: Diagnostics): void {
  const lines: string[] = [];
  lines.push(`package smoke failed: ${messageOf(error)}`);
  lines.push(`database: ${diagnostics.databasePath}`);
  lines.push('process snapshot before cleanup:');
  if (diagnostics.snapshot.pids.length === 0) {
    lines.push('  no server pid recorded');
  }
  for (const entry of diagnostics.snapshot.pids) {
    lines.push(`  server pid ${entry.pid}: ${entry.alive ? 'alive' : 'gone'}`);
  }
  lines.push(`  children: ${formatChildren(diagnostics.snapshot.children)}`);
  lines.push('cleanup:');
  lines.push(`  killed: ${formatPids(diagnostics.cleanup.killed)}`);
  lines.push(`  remaining: ${formatPids(diagnostics.cleanup.remaining)}`);
  lines.push(`  stop: ${diagnostics.cleanup.stopOutcome}`);
  if (diagnostics.cleanup.stopError !== undefined) {
    lines.push(`  stop error: ${diagnostics.cleanup.stopError}`);
  }
  lines.push(
    `  children after cleanup: ${formatChildren(diagnostics.cleanup.childrenAfter)}`,
  );
  lines.push(
    `  temp dir: ${diagnostics.tempDirRemoved ? 'removed' : `kept at ${diagnostics.directory}`}`,
  );
  lines.push('portal log tail:');
  for (const line of diagnostics.logs.lines.slice(-PORTAL_LOG_TAIL)) {
    lines.push(`  ${JSON.stringify(line)}`);
  }
  lines.push('server log tail:');
  for (const line of diagnostics.serverLogTail) {
    lines.push(`  ${line}`);
  }
  writeStderr(`${lines.join('\n')}\n`);
}

/** Spawns a child deliberately outside `pids`, for the leftover-child mode. */
async function spawnUntrackedChild(): Promise<void> {
  const spawned = Bun.spawn({
    cmd: [process.execPath, '-e', 'setInterval(() => {}, 1000)'],
    stdin: 'ignore',
    stdout: 'ignore',
    stderr: 'ignore',
  });
  await sleep(100);
  if (!isProcessAlive(spawned.pid)) {
    throw new Error(
      'leftover child mode: the spawned child exited before the check',
    );
  }
  if (processChildren() === undefined) {
    spawned.kill();
    throw new Error('leftover child mode requires /proc to observe the child');
  }
}

async function runSmoke(options: {
  readonly layer: ConnectionLayer;
  readonly transport: WsTransport;
  readonly databasePath: string;
  readonly logs: CapturedLogs;
  readonly pids: number[];
  readonly hang: boolean;
  readonly leftover: boolean;
}): Promise<void> {
  const { layer, transport, databasePath, logs, pids, hang, leftover } =
    options;
  await layer.start();
  recordServerPids(pids, logs.lines);
  if (pids.length === 0) {
    throw new Error('Portal reached ready without reporting a Server pid');
  }

  const port = transport.port;
  if (port === undefined) {
    throw new Error('the transport did not bind a port');
  }

  if (hang) {
    await new Promise<never>(() => undefined);
  }

  const value = `smoke-${process.pid}-${Date.now()}`;
  const response = await call(`ws://127.0.0.1:${port}/`, {
    id: 'smoke-1',
    method: 'smoke.store',
    payload: { value },
  });

  if (response.id !== 'smoke-1' || response.ok !== true) {
    throw new Error(`unexpected response: ${JSON.stringify(response)}`);
  }
  const result = response.result;
  if (!isRecord(result) || result.stored !== value) {
    throw new Error(`unexpected result: ${JSON.stringify(result)}`);
  }

  const stored = await readStoredValues(databasePath);
  if (stored.length !== 1 || stored[0] !== value) {
    throw new Error(`unexpected rows: ${JSON.stringify(stored)}`);
  }

  // A request can cross a restart, so re-read after it instead of trusting the
  // generation recorded before the call.
  recordServerPids(pids, logs.lines);

  await layer.stop();
  recordServerPids(pids, logs.lines);

  for (const pid of pids) {
    if (isProcessAlive(pid)) {
      throw new Error(`Server pid ${pid} is still alive after stop()`);
    }
  }
  if (transport.port !== undefined) {
    throw new Error('the transport still reports a bound port after stop()');
  }

  if (leftover) {
    await spawnUntrackedChild();
  }

  // This is the success-path assertion cleanup must not satisfy on its behalf:
  // a child still present after framework shutdown is a failure even though the
  // cleanup below will SIGKILL it.
  const childrenAfterStop = processChildren();
  if (childrenAfterStop !== undefined) {
    const aliveChildren = childrenAfterStop.filter((pid) =>
      isProcessAlive(pid),
    );
    if (aliveChildren.length > 0) {
      throw new Error(
        `leftover child process(es) after stop(): ${aliveChildren.join(', ')}`,
      );
    }
  }
}

async function main(): Promise<void> {
  const startedAt = performance.now();
  const overallTimeoutMs = readPositiveInteger(
    OVERALL_TIMEOUT_ENV,
    DEFAULT_OVERALL_TIMEOUT_MS,
  );
  const hang = process.env[HANG_ENV] === '1';
  const leftover = process.env[LEFTOVER_ENV] === '1';
  const directory = mkdtempSync(join(tmpdir(), 'mud-package-smoke-'));
  const databasePath = join(directory, 'world.sqlite');
  const logs = captureLogs();
  const pids: number[] = [];

  const transport = new WsTransport({ port: 0, logger: logs.logger });
  const executor = new IpcExecutor({
    // `process.execPath` is this Bun binary: the IPC serialization requires the
    // child to be Bun, not Node (docs/INTEGRATION.md).
    spawn: () =>
      spawnBunServer({
        command: [process.execPath, 'run', SERVER_ENTRY, databasePath],
        logger: logs.logger,
      }),
    timeoutMs: REQUEST_TIMEOUT_MS,
    startTimeoutMs: 10_000,
    restartBackoffMs: 50,
    logger: logs.logger,
  });
  const layer = new ConnectionLayer({
    transport,
    executor,
    logger: logs.logger,
    shutdownTimeoutMs: 5000,
  });

  let failure: unknown;
  let snapshot: ProcessSnapshot;
  let cleanup: CleanupResult;
  let serverLogTail: readonly string[];
  let tempDirRemoved = false;

  let watchdogTimer: ReturnType<typeof setTimeout> | undefined;
  const watchdog = new Promise<never>((_resolve, reject) => {
    watchdogTimer = setTimeout(() => {
      reject(new Error(`package smoke exceeded ${overallTimeoutMs}ms`));
    }, overallTimeoutMs);
  });

  try {
    await Promise.race([
      runSmoke({
        layer,
        transport,
        databasePath,
        logs,
        pids,
        hang,
        leftover,
      }),
      watchdog,
    ]);
  } catch (error) {
    failure = error;
  } finally {
    if (watchdogTimer !== undefined) {
      clearTimeout(watchdogTimer);
    }
    snapshot = captureSnapshot(pids);
    cleanup = await stopQuietly(layer, pids);
    serverLogTail = logTail(`${databasePath}.log`, SERVER_LOG_TAIL);
    if (cleanup.canRemoveTempDir) {
      try {
        rmSync(directory, { recursive: true, force: true });
        tempDirRemoved = true;
      } catch (error) {
        failure ??= error;
      }
    }
  }

  if (failure === undefined && cleanup.stopOutcome !== 'settled') {
    failure = new Error(
      `cleanup stop ${cleanup.stopOutcome}${cleanup.stopError === undefined ? '' : `: ${cleanup.stopError}`}`,
    );
  }
  // A child that appeared after the success-path check must still fail the run;
  // cleanup killing it does not make the framework's shutdown correct.
  if (
    failure === undefined &&
    cleanup.childrenBefore !== undefined &&
    cleanup.childrenBefore.length > 0
  ) {
    failure = new Error(
      `leftover child process(es) after stop(): ${cleanup.childrenBefore.join(', ')}`,
    );
  }
  if (failure === undefined && cleanup.remaining.length > 0) {
    failure = new Error(
      `cleanup left processes alive: ${cleanup.remaining.join(', ')}`,
    );
  }
  if (failure === undefined && !cleanup.canRemoveTempDir) {
    failure = new Error('cleanup could not confirm no live writers');
  }
  if (failure !== undefined) {
    printDiagnostics(failure, {
      databasePath,
      directory,
      logs,
      serverLogTail,
      snapshot,
      cleanup,
      tempDirRemoved,
    });
    exitFailed();
  }

  const elapsedMs = Math.round(performance.now() - startedAt);
  const childCheck =
    cleanup.childrenAfter === undefined
      ? 'child check skipped (no /proc)'
      : 'no children';
  writeStdout(
    `package smoke ok: response and SQLite row verified, server pid ${pids.join(', ')} gone, ${childCheck}, ${elapsedMs}ms\n`,
  );
  exitSucceeded();
}

try {
  await main();
} catch (error) {
  writeStderr(`package smoke failed: ${messageOf(error)}\n`);
  exitFailed();
}
