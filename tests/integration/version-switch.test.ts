import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

import {
  isProcessAlive,
  isRecord,
  nextMessage,
  openClient,
  parseFrame,
  rejectionOf,
  tempDatabase,
  withDeadline,
} from 'mud-framework-test-support';

import { WsTransport } from '../../packages/connector-ws/src/index.ts';
import {
  ConnectionLayer,
  createJsonLogger,
  IpcExecutor,
  spawnBunServer,
} from '../../packages/core/src/index.ts';
import type { Logger, SpawnedServer } from '../../packages/core/src/index.ts';
import { SqliteBackend } from '../../packages/storage-sqlite/src/index.ts';

/**
 * `IpcExecutor.switchTo` against real processes, a real WebSocket and a real
 * SQLite file. The unit tests drive it through the in-memory channel pair, so
 * the parts only the deployment path can break — spawning a second Bun child,
 * waiting for the old one to actually exit, `activate` reaching a process that
 * has not opened storage yet — are not covered there (ADR-0007).
 *
 * Both "builds" are the same fixture file started with a different
 * `codeVersion` argument. That argument is what the Server reports in
 * `prepared`/`ready`, what the `version` handler echoes, and what a row in the
 * `version` table records when the backend opens, so one file can stand in for
 * two builds without duplicating the entry.
 */
const FIXTURE = join(import.meta.dir, 'fixtures', 'server-entry.ts');

const BUILD_1 = 'build-1';
const BUILD_2 = 'build-2';
const REQUEST_TIMEOUT_MS = 10_000;
const START_TIMEOUT_MS = 10_000;
const SWITCH_TIMEOUT_MS = 15_000;
const STOP_TIMEOUT_MS = 20_000;
const TEST_TIMEOUT_MS = 45_000;
/**
 * How long the build-2 replacement withholds `ready` after `activate`. It is the
 * window the in-flight request is sent into, so it has to outlast one round trip
 * on the socket by a wide margin; 300ms is ~10x the observed spawn latency.
 */
const SWITCH_HOLD_MS = 300;

/** `boot-fail` exits before the handshake; `echo` is the ordinary fixture. */
type Behavior = 'echo' | 'boot-fail';

interface Portal {
  readonly executor: IpcExecutor;
  readonly transport: WsTransport;
  readonly layer: ConnectionLayer;
  /** Every log line the Portal and its executor emitted, in order. */
  readonly lines: Record<string, unknown>[];
}

function serverCommand(
  databasePath: string,
  codeVersion: string,
  behavior: Behavior = 'echo',
  startDelayMs = 0,
): string[] {
  // `process.execPath` is the running Bun binary: the IPC serialization is
  // Bun-only, so a Node child would never complete the handshake.
  return [
    process.execPath,
    'run',
    FIXTURE,
    behavior,
    databasePath,
    codeVersion,
    String(startDelayMs),
  ];
}

function spawnFor(
  databasePath: string,
  codeVersion: string,
  behavior: Behavior = 'echo',
  startDelayMs = 0,
): () => SpawnedServer {
  const command = serverCommand(
    databasePath,
    codeVersion,
    behavior,
    startDelayMs,
  );
  return () => spawnBunServer({ command });
}

function createPortal(databasePath: string): Portal {
  const lines: Record<string, unknown>[] = [];
  const logger: Logger = createJsonLogger({
    level: 'debug',
    sink: (line) => {
      const parsed: unknown = JSON.parse(line);
      if (isRecord(parsed)) {
        lines.push(parsed);
      }
    },
  });
  const executor = new IpcExecutor({
    spawn: spawnFor(databasePath, BUILD_1),
    codeVersion: BUILD_1,
    timeoutMs: REQUEST_TIMEOUT_MS,
    startTimeoutMs: START_TIMEOUT_MS,
    logger,
  });
  const transport = new WsTransport({ port: 0 });
  const layer = new ConnectionLayer({ transport, executor, logger });
  return { executor, transport, layer, lines };
}

function urlOf(portal: Portal): string {
  const port = portal.transport.port;
  if (port === undefined) {
    throw new Error('expected the transport to bind a port');
  }
  return `ws://127.0.0.1:${port}/`;
}

function sendRequest(
  socket: WebSocket,
  request: { id: string; method: string; payload?: unknown },
): Promise<Record<string, unknown>> {
  const reply = nextMessage(socket);
  socket.send(JSON.stringify(request));
  return withDeadline(reply, REQUEST_TIMEOUT_MS, `reply to ${request.id}`).then(
    (raw) => parseFrame(raw),
  );
}

/**
 * True once the socket is gone. An error leaves an open client unusable, so it
 * counts as a drop even when the close event has not arrived yet.
 */
function trackDrop(socket: WebSocket): () => boolean {
  let dropped = false;
  const mark = (): void => {
    dropped = true;
  };
  socket.addEventListener('close', mark, { once: true });
  socket.addEventListener('error', mark, { once: true });
  return () => dropped;
}

/** Pids from the given `ready` log line, in the order the lines appeared. */
function pidsOf(
  lines: readonly Record<string, unknown>[],
  msg: string,
): number[] {
  const pids: number[] = [];
  for (const line of lines) {
    const pid = line.pid;
    if (line.msg === msg && typeof pid === 'number') {
      pids.push(pid);
    }
  }
  return pids;
}

function requirePid(
  lines: readonly Record<string, unknown>[],
  msg: string,
): number {
  const pid = pidsOf(lines, msg).at(-1);
  if (pid === undefined) {
    throw new Error(`no "${msg}" log line carried a pid`);
  }
  return pid;
}

/** Every Server pid this Portal ever reported as ready, for cleanup. */
function allServerPids(lines: readonly Record<string, unknown>[]): number[] {
  return [
    ...pidsOf(lines, 'server ready'),
    ...pidsOf(lines, 'replacement server ready'),
  ];
}

function lineIndex(
  lines: readonly Record<string, unknown>[],
  msg: string,
): number {
  return lines.findIndex((line) => line.msg === msg);
}

/** The single line with this message; a missing one is a test error, not a pass. */
function lineOf(
  lines: readonly Record<string, unknown>[],
  msg: string,
): Record<string, unknown> {
  const found = lines.filter((line) => line.msg === msg);
  if (found.length !== 1) {
    throw new Error(`expected exactly one "${msg}" line, got ${found.length}`);
  }
  const [only] = found;
  if (only === undefined) {
    throw new Error(`no "${msg}" line`);
  }
  return only;
}

/** `process.kill(pid, 0)` must throw ESRCH, not merely return false. */
function expectGone(pid: number): void {
  let code: unknown;
  try {
    process.kill(pid, 0);
  } catch (error) {
    code = isRecord(error) ? error.code : undefined;
  }
  expect(code).toBe('ESRCH');
}

interface VersionRow {
  readonly code_version: string;
  readonly pid: number;
}

/** Read through a second backend, so the rows are not the Server's own view. */
async function readVersions(databasePath: string): Promise<VersionRow[]> {
  const reader = SqliteBackend.open({ filename: databasePath });
  try {
    return await reader.transaction((tx) =>
      tx.query<VersionRow>('SELECT code_version, pid FROM version ORDER BY id'),
    );
  } finally {
    await reader.close();
  }
}

/** Always releases the Portal and every Server pid it reported. */
async function stopQuietly(portal: Portal): Promise<void> {
  try {
    await withDeadline(portal.layer.stop(), STOP_TIMEOUT_MS, 'layer stop');
  } catch {
    // The test's own failure is the one to report; a shutdown error here would
    // replace it with a less useful one.
  }
  for (const pid of allServerPids(portal.lines)) {
    try {
      process.kill(pid, 'SIGKILL');
    } catch {
      // Already gone, which is the expected case.
    }
  }
}

describe('version switch with real processes', () => {
  test(
    'switchTo replaces the Server without dropping a live WebSocket, and the next request is served by the new build',
    async () => {
      const database = tempDatabase({ prefix: 'mud-version-switch-' });
      const portal = createPortal(database.path);
      let socket: WebSocket | undefined;
      try {
        await portal.layer.start();
        socket = await openClient(urlOf(portal), { timeoutMs: 5000 });
        const dropped = trackDrop(socket);

        const before = await sendRequest(socket, {
          id: 'before',
          method: 'version',
        });
        expect(before).toEqual({
          id: 'before',
          ok: true,
          result: { codeVersion: BUILD_1 },
        });
        const oldPid = requirePid(portal.lines, 'server ready');

        let settled = false;
        const switchStartedAt = performance.now();
        const switching = portal.executor.switchTo({
          // The replacement holds `activate` for a moment before it can report
          // `ready`, so the request below lands inside the switching window on
          // every machine instead of racing a fast spawn.
          spawn: spawnFor(database.path, BUILD_2, 'echo', SWITCH_HOLD_MS),
          codeVersion: BUILD_2,
        });
        // Observed, not awaited: the assertion below proves the switch was still
        // in flight when the request was accepted. `catch` first so a rejection
        // before the await further down cannot become an unhandled one.
        void switching
          .catch(() => undefined)
          .finally(() => {
            settled = true;
          });

        // Accepted while `status === 'switching'`: it must stay in the
        // unacknowledged buffer and be delivered to the new generation.
        const during = sendRequest(socket, { id: 'during', method: 'version' });
        expect(settled).toBe(false);

        await withDeadline(switching, SWITCH_TIMEOUT_MS, 'switchTo');
        // The hold really happened, so the request above was accepted during the
        // switch rather than after it.
        expect(performance.now() - switchStartedAt).toBeGreaterThanOrEqual(
          SWITCH_HOLD_MS,
        );

        // An `ok` reply rules out FRAMEWORK_TIMEOUT, which is how a request
        // lost in the switch would end.
        expect(await during).toEqual({
          id: 'during',
          ok: true,
          result: { codeVersion: BUILD_2 },
        });

        const after = await sendRequest(socket, {
          id: 'after',
          method: 'version',
        });
        expect(after).toEqual({
          id: 'after',
          ok: true,
          result: { codeVersion: BUILD_2 },
        });
        expect(dropped()).toBe(false);

        // The order the switch promises: the old process is retired before the
        // replacement is activated, and the replacement reports the target
        // version (ADR-0007 step 2 before step 3). Both lines are required, so a
        // missing one cannot turn the ordering check into a tautology.
        const retiredAt = lineIndex(portal.lines, 'previous server retired');
        const replacementReadyAt = lineIndex(
          portal.lines,
          'replacement server ready',
        );
        expect(retiredAt).toBeGreaterThanOrEqual(0);
        expect(replacementReadyAt).toBeGreaterThan(retiredAt);
        expect(lineOf(portal.lines, 'server switch completed')).toMatchObject({
          codeVersion: BUILD_2,
        });
        // The in-flight request waited in the buffer instead of timing out or
        // being replayed onto a generation it never reached.
        expect(
          portal.lines.filter((line) => line.msg === 'request timed out'),
        ).toEqual([]);
        expect(
          portal.lines.filter((line) => line.msg === 'request replayed'),
        ).toEqual([]);

        const newPid = requirePid(portal.lines, 'replacement server ready');
        expect(newPid).not.toBe(oldPid);
        // The pid reported for the replacement is the one its own row recorded,
        // so "the new process is alive" refers to the build-2 writer.
        expect(
          lineOf(portal.lines, 'replacement server ready').codeVersion,
        ).toBe(BUILD_2);
        // The switch waits for the old process's exit before it activates the
        // new one (INV-002), so by the time it resolves the pid must be gone.
        expectGone(oldPid);
        expect(isProcessAlive(newPid)).toBe(true);

        await stopQuietly(portal);

        // One row per process that actually opened storage, in start order: the
        // build-2 row exists only because `activate` reached it after build-1
        // exited.
        expect(await readVersions(database.path)).toEqual([
          { code_version: BUILD_1, pid: oldPid },
          { code_version: BUILD_2, pid: newPid },
        ]);
      } finally {
        socket?.close();
        await stopQuietly(portal);
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );

  test(
    'a replacement that fails to boot is rejected, leaves the old build serving, and never opens storage',
    async () => {
      const database = tempDatabase({ prefix: 'mud-version-switch-' });
      const portal = createPortal(database.path);
      let socket: WebSocket | undefined;
      try {
        await portal.layer.start();
        socket = await openClient(urlOf(portal), { timeoutMs: 5000 });
        const dropped = trackDrop(socket);

        const before = await sendRequest(socket, {
          id: 'before',
          method: 'version',
        });
        expect(before).toEqual({
          id: 'before',
          ok: true,
          result: { codeVersion: BUILD_1 },
        });
        const oldPid = requirePid(portal.lines, 'server ready');

        const error = await withDeadline(
          rejectionOf(
            portal.executor.switchTo({
              spawn: spawnFor(database.path, BUILD_2, 'boot-fail'),
              codeVersion: BUILD_2,
            }),
          ),
          SWITCH_TIMEOUT_MS,
          'switchTo rejection',
        );
        // The failure is the replacement dying before `prepared`, not a timeout:
        // a boot-fail entry exits before the handshake, so either the staged
        // process is reported lost or the barrier names the step it never
        // reached. A `within Nms` message would mean the switch sat out its
        // budget instead.
        expect(error.message).toMatch(
          /^replacement server (lost|exited before)/,
        );
        expect(error.message).not.toContain('within');

        // The old process was never drained: it is still the current Server and
        // still answers on the connection that stayed open across the attempt.
        expect(isProcessAlive(oldPid)).toBe(true);
        expect(dropped()).toBe(false);
        const after = await sendRequest(socket, {
          id: 'after',
          method: 'version',
        });
        expect(after).toEqual({
          id: 'after',
          ok: true,
          result: { codeVersion: BUILD_1 },
        });
        // No crash restart: the failed replacement is not a lost current Server.
        expect(pidsOf(portal.lines, 'server ready')).toEqual([oldPid]);
        expect(pidsOf(portal.lines, 'replacement server ready')).toEqual([]);
        // The pre-drain failure path, not the rollback one: the old process was
        // never retired, so nothing had to be restarted.
        expect(
          lineOf(
            portal.lines,
            'server switch failed; the previous server keeps serving',
          ).phase,
        ).toBe('preparing');

        await stopQuietly(portal);

        // Only build-1 ever opened the storage. A row for build-2 would mean a
        // process that never became ready still wrote to the shared file.
        expect(await readVersions(database.path)).toEqual([
          { code_version: BUILD_1, pid: oldPid },
        ]);
      } finally {
        socket?.close();
        await stopQuietly(portal);
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );
});
