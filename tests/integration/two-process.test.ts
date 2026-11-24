import { describe, expect, test } from 'bun:test';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { WsTransport } from '../../packages/connector-ws/src/index.ts';
import {
  ConnectionLayer,
  createJsonLogger,
  IpcExecutor,
  spawnBunServer,
} from '../../packages/core/src/index.ts';
import type { Logger } from '../../packages/core/src/index.ts';
import { SqliteBackend } from '../../packages/storage-sqlite/src/index.ts';

/**
 * The only integration coverage that uses a real child process. Everything else
 * runs against the in-memory channel pair, so a defect that only exists on the
 * deployment path (spawn, handshake, exit, restart) is invisible there. The
 * Server entry is `fixtures/server-entry.ts`; its behaviour is selected by argv.
 */
const FIXTURE = join(import.meta.dir, 'fixtures', 'server-entry.ts');

type Behavior = 'echo' | 'crash' | 'boot-fail';

interface TempDatabase {
  readonly path: string;
  remove(): void;
}

interface LogCapture {
  readonly logger: Logger;
  readonly lines: Record<string, unknown>[];
}

interface Portal {
  readonly executor: IpcExecutor;
  readonly transport: WsTransport;
  readonly layer: ConnectionLayer;
  readonly logs: LogCapture;
  /** How many times the executor called `spawn`, so a restart loop is visible. */
  spawns: () => number;
}

function tempDatabase(): TempDatabase {
  const directory = mkdtempSync(join(tmpdir(), 'mud-two-process-'));
  return {
    path: join(directory, 'server.sqlite'),
    remove(): void {
      rmSync(directory, { recursive: true, force: true });
    },
  };
}

function capturingLogger(): LogCapture {
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

function createPortal(options: {
  databasePath: string;
  behavior: Behavior;
  timeoutMs?: number;
  startTimeoutMs?: number;
}): Portal {
  const logs = capturingLogger();
  // `process.execPath` is the running Bun binary; the child must be Bun because
  // the IPC serialization is Bun-only (docs/INTEGRATION.md).
  const command = [
    process.execPath,
    'run',
    FIXTURE,
    options.behavior,
    options.databasePath,
  ];
  const spawnCounter = { count: 0 };
  const executor = new IpcExecutor({
    spawn: () => {
      spawnCounter.count += 1;
      return spawnBunServer({ command });
    },
    timeoutMs: options.timeoutMs ?? 2000,
    startTimeoutMs: options.startTimeoutMs,
    logger: logs.logger,
  });
  const transport = new WsTransport({ port: 0 });
  const layer = new ConnectionLayer({
    transport,
    executor,
    logger: logs.logger,
  });
  return {
    executor,
    transport,
    layer,
    logs,
    spawns: () => spawnCounter.count,
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });
}

function parseFrame(raw: string | Uint8Array): Record<string, unknown> {
  const text = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
  const parsed: unknown = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error(`frame is not a JSON object: ${text}`);
  }
  return parsed;
}

function openClient(url: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    socket.addEventListener(
      'open',
      () => {
        resolve(socket);
      },
      { once: true },
    );
    socket.addEventListener(
      'error',
      () => {
        reject(new Error(`failed to connect to ${url}`));
      },
      { once: true },
    );
  });
}

function nextMessage(socket: WebSocket): Promise<string | Uint8Array> {
  return new Promise((resolve, reject) => {
    socket.addEventListener(
      'message',
      (event: MessageEvent) => {
        const data: unknown = event.data;
        if (typeof data === 'string' || data instanceof Uint8Array) {
          resolve(data);
          return;
        }
        reject(new Error(`unexpected message type: ${typeof data}`));
      },
      { once: true },
    );
    socket.addEventListener(
      'error',
      () => {
        reject(new Error('socket errored before a reply arrived'));
      },
      { once: true },
    );
  });
}

/** One request over a fresh socket, so each call also crosses a new connection. */
async function call(
  url: string,
  request: { id: string; method: string; payload?: unknown },
): Promise<Record<string, unknown>> {
  const socket = await openClient(url);
  try {
    const reply = nextMessage(socket);
    socket.send(JSON.stringify(request));
    return parseFrame(await reply);
  } finally {
    socket.close();
  }
}

function urlOf(portal: Portal): string {
  const port = portal.transport.port;
  if (port === undefined) {
    throw new Error('expected the transport to bind a port');
  }
  return `ws://127.0.0.1:${port}/`;
}

/** Every Server process pid the Portal saw reach `ready`, in order. */
function serverPids(logs: LogCapture): number[] {
  const pids: number[] = [];
  for (const line of logs.lines) {
    const pid = line.pid;
    if (line.msg === 'server ready' && typeof pid === 'number') {
      pids.push(pid);
    }
  }
  return pids;
}

function replayCounts(logs: LogCapture): number[] {
  const counts: number[] = [];
  for (const line of logs.lines) {
    const replayCount = line.replayCount;
    if (line.msg === 'request replayed' && typeof replayCount === 'number') {
      counts.push(replayCount);
    }
  }
  return counts;
}

/**
 * `process.kill(pid, 0)` is the only portable liveness probe: it throws ESRCH
 * for a reaped process. Anything else (EPERM) means it is still alive.
 */
function isProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    const code = isRecord(error) ? error.code : undefined;
    return code !== 'ESRCH';
  }
}

/**
 * Always releases the Portal, then any Server pid it ever reported. A test that
 * fails its assertions must not leave a child behind for the rest of the suite.
 */
async function stopQuietly(portal: Portal): Promise<void> {
  try {
    await portal.layer.stop();
  } catch {
    // The failure being reported is the test's own; a shutdown error here would
    // replace it with a less useful one.
  }
  for (const pid of serverPids(portal.logs)) {
    try {
      process.kill(pid, 'SIGKILL');
    } catch {
      // Already gone, which is the expected case.
    }
  }
}

/** `start()` either settles or it does not; a hang is the failure under test. */
async function settleWithin(
  promise: Promise<unknown>,
  timeoutMs: number,
): Promise<'resolved' | 'rejected' | 'timeout'> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<'timeout'>((resolve) => {
    timer = setTimeout(() => {
      resolve('timeout');
    }, timeoutMs);
  });
  const settled = promise.then(
    (): 'resolved' => 'resolved',
    (): 'rejected' => 'rejected',
  );
  try {
    return await Promise.race([settled, timeout]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

describe('two-process integration', () => {
  test('a WebSocket request is executed by the Server process and its write lands in SQLite', async () => {
    const database = tempDatabase();
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'echo',
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);

      const echoed = await call(url, {
        id: 'r1',
        method: 'echo',
        payload: { hello: 'world' },
      });
      expect(echoed).toEqual({
        id: 'r1',
        ok: true,
        result: { hello: 'world' },
      });

      const stored = await call(url, {
        id: 'r2',
        method: 'store',
        payload: 'note-1',
      });
      expect(stored).toEqual({
        id: 'r2',
        ok: true,
        result: { stored: 'note-1' },
      });
    } finally {
      await stopQuietly(portal);
    }

    // The Portal never opened the file: this handle is a separate reader, so the
    // row proves the Server's commit outlived its own process.
    const reader = SqliteBackend.open({ filename: database.path });
    try {
      const rows = await reader.transaction((tx) =>
        tx.query<{ body: string }>('SELECT body FROM notes'),
      );
      expect(rows).toEqual([{ body: 'note-1' }]);
    } finally {
      await reader.close();
      database.remove();
    }
  });

  test('a Server that dies mid-request is replayed to exhaustion while the Portal keeps serving', async () => {
    const database = tempDatabase();
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'crash',
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);

      const failed = await call(url, { id: 'boom', method: 'crash' });
      expect(failed.id).toBe('boom');
      expect(failed.ok).toBe(false);
      expect(failed.error).toMatchObject({
        code: 'FRAMEWORK_REPLAY_EXHAUSTED',
      });
      // One replay per restart, counted on the Portal side; the sequence is what
      // makes "which attempt of which input" answerable from the logs.
      expect(replayCounts(portal.logs)).toEqual([1, 2, 3]);
      // One process per attempt plus the one that finds the budget spent. The
      // exact count is an implementation detail; the bound is the contract: a
      // bad input must not keep spawning processes.
      expect(portal.spawns()).toBeGreaterThanOrEqual(4);
      expect(portal.spawns()).toBeLessThanOrEqual(6);
      expect(serverPids(portal.logs).length).toBe(portal.spawns());

      // The Portal itself survived every crash: the same executor, on a new
      // connection, still answers.
      const after = await call(url, {
        id: 'after',
        method: 'echo',
        payload: 'still here',
      });
      expect(after).toEqual({ id: 'after', ok: true, result: 'still here' });
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('stop() returns only after the Server process has exited', async () => {
    const database = tempDatabase();
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'echo',
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);
      await call(url, { id: 'r1', method: 'echo', payload: 'ping' });

      const pids = serverPids(portal.logs);
      const pid = pids.at(-1);
      if (pid === undefined) {
        throw new Error('the Server never reported a pid');
      }

      await portal.layer.stop();

      // A Server still running here would keep writing to the storage it no
      // longer exclusively owns (INV-002).
      expect(isProcessAlive(pid)).toBe(false);
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('start() fails within a bounded time when the Server cannot boot', async () => {
    const database = tempDatabase();
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'boot-fail',
      startTimeoutMs: 1500,
    });
    const started = portal.layer.start();
    try {
      const outcome = await settleWithin(started, 4000);
      // A readiness barrier that never lifts must fail the start, not leave the
      // caller waiting forever behind a restart loop.
      expect(outcome).toBe('rejected');
      const attemptsAtFailure = portal.spawns();
      expect(attemptsAtFailure).toBeGreaterThan(0);

      // The restart loop must stop with the failure: continuing to spawn on a
      // configuration error is the "unbounded restart" this scenario is for.
      await sleep(500);
      expect(portal.spawns()).toBe(attemptsAtFailure);
    } finally {
      // `close()` rejects a start still waiting for `ready`, so the race above
      // must not leave an unhandled rejection behind.
      started.catch(() => undefined);
      await stopQuietly(portal);
      database.remove();
    }
  });
});
