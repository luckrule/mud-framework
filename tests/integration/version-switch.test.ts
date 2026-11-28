import { describe, expect, test } from 'bun:test';

import {
  isProcessAlive,
  isRecord,
  openClient,
  rejectionOf,
  tempDatabase,
  withDeadline,
} from 'mud-framework-test-support';

import {
  callOnSocket,
  createPortal,
  spawnFor,
  stopQuietly,
  urlOf,
} from './support/two-process-harness.ts';

/**
 * `IpcExecutor.switchTo` against real processes, a real WebSocket and a real
 * SQLite file. The unit tests drive it through the in-memory channel pair, so
 * the parts only the deployment path can break — spawning a second Bun child,
 * waiting for the old one to actually exit, `activate` reaching a process that
 * has not opened storage yet — are not covered there (ADR-0007).
 */
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

function portalFor(databasePath: string) {
  return createPortal({
    databasePath,
    behavior: 'echo',
    timeoutMs: REQUEST_TIMEOUT_MS,
    startTimeoutMs: START_TIMEOUT_MS,
  });
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

describe('version switch with real processes', () => {
  test(
    'switchTo replaces the Server without dropping a live WebSocket',
    async () => {
      const database = tempDatabase({ prefix: 'mud-version-switch-' });
      const portal = portalFor(database.path);
      let socket: WebSocket | undefined;
      try {
        await portal.layer.start();
        socket = await openClient(urlOf(portal), { timeoutMs: 5000 });
        const dropped = trackDrop(socket);

        const before = await callOnSocket(
          socket,
          {
            id: 'before',
            method: 'echo',
            payload: 'before',
          },
          REQUEST_TIMEOUT_MS,
        );
        expect(before).toEqual({ id: 'before', ok: true, result: 'before' });
        const oldPid = requirePid(portal.logs.lines, 'server ready');

        let settled = false;
        const switchStartedAt = performance.now();
        const switching = portal.executor.switchTo({
          // The replacement holds `activate` for a moment before it can report
          // `ready`, so the request below lands inside the switching window on
          // every machine instead of racing a fast spawn.
          spawn: spawnFor(database.path, 'echo', SWITCH_HOLD_MS),
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
        const during = callOnSocket(
          socket,
          {
            id: 'during',
            method: 'echo',
            payload: 'during',
          },
          REQUEST_TIMEOUT_MS,
        );
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
          result: 'during',
        });

        const after = await callOnSocket(
          socket,
          {
            id: 'after',
            method: 'echo',
            payload: 'after',
          },
          REQUEST_TIMEOUT_MS,
        );
        expect(after).toEqual({ id: 'after', ok: true, result: 'after' });
        expect(dropped()).toBe(false);

        // The order the switch promises: the old process is retired before the
        // replacement is activated (ADR-0007 step 2 before step 3). Both lines
        // are required, so a missing one cannot turn the ordering check into a
        // tautology.
        const retiredAt = lineIndex(
          portal.logs.lines,
          'previous server retired',
        );
        const replacementReadyAt = lineIndex(
          portal.logs.lines,
          'replacement server ready',
        );
        expect(retiredAt).toBeGreaterThanOrEqual(0);
        expect(replacementReadyAt).toBeGreaterThan(retiredAt);
        expect(
          portal.logs.lines.some(
            (line) => line.msg === 'server switch completed',
          ),
        ).toBe(true);
        // The in-flight request waited in the buffer instead of timing out or
        // being replayed onto a generation it never reached.
        expect(
          portal.logs.lines.filter((line) => line.msg === 'request timed out'),
        ).toEqual([]);
        expect(
          portal.logs.lines.filter((line) => line.msg === 'request replayed'),
        ).toEqual([]);

        const newPid = requirePid(
          portal.logs.lines,
          'replacement server ready',
        );
        expect(newPid).not.toBe(oldPid);
        // The switch waits for the old process's exit before it activates the
        // new one (INV-002), so by the time it resolves the pid must be gone.
        expectGone(oldPid);
        expect(isProcessAlive(newPid)).toBe(true);
      } finally {
        socket?.close();
        await stopQuietly(portal, { stopTimeoutMs: STOP_TIMEOUT_MS });
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );

  test(
    'a replacement that fails to boot is rejected and leaves the old process serving',
    async () => {
      const database = tempDatabase({ prefix: 'mud-version-switch-' });
      const portal = portalFor(database.path);
      let socket: WebSocket | undefined;
      try {
        await portal.layer.start();
        socket = await openClient(urlOf(portal), { timeoutMs: 5000 });
        const dropped = trackDrop(socket);

        const before = await callOnSocket(
          socket,
          {
            id: 'before',
            method: 'echo',
            payload: 'before',
          },
          REQUEST_TIMEOUT_MS,
        );
        expect(before).toEqual({ id: 'before', ok: true, result: 'before' });
        const oldPid = requirePid(portal.logs.lines, 'server ready');

        const error = await withDeadline(
          rejectionOf(
            portal.executor.switchTo({
              spawn: spawnFor(database.path, 'boot-fail'),
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
        const after = await callOnSocket(
          socket,
          {
            id: 'after',
            method: 'echo',
            payload: 'after',
          },
          REQUEST_TIMEOUT_MS,
        );
        expect(after).toEqual({ id: 'after', ok: true, result: 'after' });
        // No crash restart: the failed replacement is not a lost current Server.
        expect(pidsOf(portal.logs.lines, 'server ready')).toEqual([oldPid]);
        expect(pidsOf(portal.logs.lines, 'replacement server ready')).toEqual(
          [],
        );
        // The pre-drain failure path, not the rollback one: the old process was
        // never retired, so nothing had to be restarted.
        expect(
          lineOf(
            portal.logs.lines,
            'server switch failed; the previous server keeps serving',
          ).phase,
        ).toBe('preparing');
      } finally {
        socket?.close();
        await stopQuietly(portal, { stopTimeoutMs: STOP_TIMEOUT_MS });
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );
});
