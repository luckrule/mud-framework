import { describe, expect, test } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

import {
  isProcessAlive,
  isRecord,
  nextMessage,
  openClient,
  parseFrame,
  sleep,
  tempDatabase,
} from 'mud-framework-test-support';

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

type Behavior =
  | 'echo'
  | 'crash'
  | 'crash-once'
  | 'boot-fail'
  | 'extension-trial'
  | 'required-extension-fail';

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
  restartBackoffMs?: number;
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
    restartBackoffMs: options.restartBackoffMs,
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

/** One request on an already-open socket, which survives a Server restart. */
async function callOnSocket(
  socket: WebSocket,
  request: { id: string; method: string; payload?: unknown },
): Promise<Record<string, unknown>> {
  const reply = nextMessage(socket);
  socket.send(JSON.stringify(request));
  return parseFrame(await reply);
}

/** One request over a fresh socket, so each call also crosses a new connection. */
async function call(
  url: string,
  request: { id: string; method: string; payload?: unknown },
): Promise<Record<string, unknown>> {
  const socket = await openClient(url);
  try {
    return await callOnSocket(socket, request);
  } finally {
    socket.close();
  }
}

/** One line per `store-once` handler execution, written by the fixture. */
function attemptLog(databasePath: string): string[] {
  return readFileSync(`${databasePath}.attempts`, 'utf8')
    .split('\n')
    .filter((line) => line.length > 0);
}

/**
 * The fixture writes the startup failure reason next to the database because
 * its stderr is inherited and therefore unreadable here. `start()` rejects on
 * its own deadline, which can fire while the last child is still exiting, so
 * poll briefly instead of assuming the file is already there.
 */
async function startupFailure(databasePath: string): Promise<string> {
  const path = `${databasePath}.startup-error`;
  const deadline = Date.now() + 2000;
  while (!existsSync(path)) {
    if (Date.now() >= deadline) {
      throw new Error(`no startup failure reason was written to ${path}`);
    }
    await sleep(10);
  }
  return readFileSync(path, 'utf8');
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
    const database = tempDatabase({ prefix: 'mud-two-process-' });
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

      // `stopQuietly` is `stop()` plus a SIGKILL for every pid that reported
      // ready: `stop()` alone can time out and return while the Server is still
      // alive, and the reader below must not race a writer.
      await stopQuietly(portal);

      // The Portal never opened the file: this handle is a separate reader, so
      // the row proves the Server's commit outlived its own process.
      const reader = SqliteBackend.open({ filename: database.path });
      try {
        const rows = await reader.transaction((tx) =>
          tx.query<{ body: string }>('SELECT body FROM notes'),
        );
        expect(rows).toEqual([{ body: 'note-1' }]);
      } finally {
        await reader.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('a Server that dies mid-request is replayed to exhaustion while the Portal keeps serving', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
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

  test('a crash on the first execution is replayed and the same request succeeds', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'crash-once',
      timeoutMs: 5000,
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);

      // One socket for the whole exchange: the connection survives the Server
      // crash, so the reply comes from the replay rather than a reconnect
      // (INV-012).
      const socket = await openClient(url);
      try {
        const frame = await callOnSocket(socket, {
          id: 'once',
          method: 'store-once',
          payload: 'note-once',
        });
        expect(frame).toEqual({
          id: 'once',
          ok: true,
          result: { stored: 'note-once' },
        });

        // The fixture appends the framework `requestId` before crashing, so the
        // two lines are the same Request seen by the first attempt and by the
        // replay: `requestId` is what replay preserves.
        const attempts = attemptLog(database.path);
        expect(attempts).toHaveLength(2);
        expect(attempts[0]).toBe(attempts[1]);
        expect(replayCounts(portal.logs)).toEqual([1]);
        const replayed = portal.logs.lines.filter(
          (line) => line.msg === 'request replayed',
        );
        expect(replayed).toHaveLength(1);
        expect(replayed.at(0)).toMatchObject({
          requestId: attempts[0],
          clientRequestId: 'once',
          replayCount: 1,
        });
        // The first process crashed; the second is the replayer that answered.
        // The exact count is an implementation detail, as in the crash test
        // above: `replayCounts` having one entry is what proves there was
        // exactly one restart, not a retry loop.
        expect(portal.spawns()).toBeGreaterThanOrEqual(2);
        expect(serverPids(portal.logs).length).toBeGreaterThanOrEqual(2);
      } finally {
        socket.close();
      }

      await stopQuietly(portal);

      // Two handler executions, one committed effect. `once_executions` is the
      // evidence: it is not idempotent, so its single row means the interrupted
      // attempt's transaction never reached COMMIT and the replay committed
      // once. The interrupted attempt rolled back, so the replay does not hit
      // `ON CONFLICT DO NOTHING` either; that guard is for a client retry,
      // which keeps `clientRequestId` but arrives with a new `requestId`. A
      // non-transactional side effect can still run twice on replay (INV-011).
      const reader = SqliteBackend.open({ filename: database.path });
      try {
        const stored = await reader.transaction(async (tx) => ({
          notes: await tx.query<{ client_request_id: string; body: string }>(
            'SELECT client_request_id, body FROM once_notes',
          ),
          executions: await tx.query<{ client_request_id: string }>(
            'SELECT client_request_id FROM once_executions',
          ),
        }));
        expect(stored.notes).toEqual([
          { client_request_id: 'once', body: 'note-once' },
        ]);
        expect(stored.executions).toEqual([{ client_request_id: 'once' }]);
      } finally {
        await reader.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('an extension whose requirements SQLite meets registers and can bind a Session', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'extension-trial',
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);

      // One socket for both requests: `bindSession` commits the route only
      // after the first request succeeds, and the second request on the same
      // connection must see it as `ctx.sessionId` (SPEC 2).
      const socket = await openClient(url);
      try {
        const created = await callOnSocket(socket, {
          id: 'create',
          method: 'player.create',
          payload: { name: 'alice', sessionId: 'session-alice' },
        });
        expect(created).toEqual({
          id: 'create',
          ok: true,
          result: { created: 'alice', sessionId: 'session-alice' },
        });

        // The wire result carries only the handler value, so the Session id
        // here comes from `ctx.sessionId` of the second request, not from the
        // first request's reply.
        const whoami = await callOnSocket(socket, {
          id: 'whoami',
          method: 'player.whoami',
        });
        expect(whoami).toEqual({
          id: 'whoami',
          ok: true,
          result: { sessionId: 'session-alice' },
        });
      } finally {
        socket.close();
      }

      await stopQuietly(portal);

      // Separate reader, as in the write test above: the row proves the
      // extension's transaction committed on the Server.
      const reader = SqliteBackend.open({ filename: database.path });
      try {
        const rows = await reader.transaction((tx) =>
          tx.query<{ name: string }>('SELECT name FROM players'),
        );
        expect(rows).toEqual([{ name: 'alice' }]);
      } finally {
        await reader.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('an optional extension skipped for unmet capabilities leaves no handlers behind, and the Server keeps serving', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'extension-trial',
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);

      const socket = await openClient(url);
      try {
        // SQLite declares `concurrency: single`, so the pooled-only extension
        // was never registered and its handler does not exist (SPEC 4.3).
        const pooled = await callOnSocket(socket, {
          id: 'pooled',
          method: 'pooled.ping',
        });
        expect(pooled).toMatchObject({
          id: 'pooled',
          ok: false,
          error: { code: 'UNKNOWN_METHOD' },
        });

        // The skipped extension does not take the process down: the same
        // connection, on the next request, still reaches the satisfied one.
        const created = await callOnSocket(socket, {
          id: 'create',
          method: 'player.create',
          payload: { name: 'bob', sessionId: 'session-bob' },
        });
        expect(created).toEqual({
          id: 'create',
          ok: true,
          result: { created: 'bob', sessionId: 'session-bob' },
        });
      } finally {
        socket.close();
      }

      // A restart would mean the skip was treated as a crash; it is not.
      expect(portal.spawns()).toBe(1);
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('a required extension whose requirements are unmet refuses startup', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'required-extension-fail',
      // Same bounded-start shape as the boot-fail case: short deadline and
      // backoff so the retries before the failure complete quickly.
      startTimeoutMs: 500,
      restartBackoffMs: 25,
    });
    const started = portal.layer.start();
    try {
      const outcome = await settleWithin(started, 2000);
      // SPEC 6.2 step 2 refuses the process before `ready`, so Portal must
      // never see a ready Server and must fail the start.
      expect(outcome).toBe('rejected');
      expect(serverPids(portal.logs)).toEqual([]);
      const attemptsAtFailure = portal.spawns();
      expect(attemptsAtFailure).toBeGreaterThan(0);

      // A configuration error must not become an unbounded restart loop.
      await sleep(300);
      expect(portal.spawns()).toBe(attemptsAtFailure);

      // The refusal is provable, not inferred: the child's stderr goes to the
      // inherited terminal, so the file the fixture writes next to the database
      // is the only evidence of why startup was refused. It must name the
      // required extension and the capability the backend could not provide.
      const failure = await startupFailure(database.path);
      expect(failure).toContain('required extension "pooled-only"');
      expect(failure).toContain('concurrency');
    } finally {
      started.catch(() => undefined);
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('stop() returns only after the Server process has exited', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
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
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'boot-fail',
      // Shortened from 1500/100 only to cut the fixed wait: with a 25ms base
      // the retries before the 500ms deadline still step through the immediate
      // restart and several backoffs (0, 25, 50, 100, 200, 400), so the same
      // bounded-start path is exercised, not bypassed.
      startTimeoutMs: 500,
      restartBackoffMs: 25,
    });
    const started = portal.layer.start();
    try {
      const outcome = await settleWithin(started, 2000);
      // A readiness barrier that never lifts must fail the start, not leave the
      // caller waiting forever behind a restart loop.
      expect(outcome).toBe('rejected');
      const attemptsAtFailure = portal.spawns();
      expect(attemptsAtFailure).toBeGreaterThan(0);

      // The restart loop must stop with the failure: continuing to spawn on a
      // configuration error is the "unbounded restart" this scenario is for.
      // 300ms is many retry cycles at this backoff base, so any restart that
      // survived the failure would still be seen here.
      await sleep(300);
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
