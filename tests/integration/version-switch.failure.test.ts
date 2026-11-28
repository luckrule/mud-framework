import { describe, expect, test } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';

import {
  isProcessAlive,
  openClient,
  rejectionOf,
  sleep,
  tempDatabase,
  withDeadline,
} from 'mud-framework-test-support';

import { isServerSwitchError } from '../../packages/core/src/index.ts';
import type {
  ServerSwitchError,
  SpawnedServer,
} from '../../packages/core/src/index.ts';
import {
  call,
  callOnSocket,
  createPortal,
  killPids,
  pidsInLogs,
  serverPids,
  spawnFor,
  spawnInline,
  spawnWithCommand,
  startupFailure,
  stopQuietly,
  urlOf,
} from './support/two-process-harness.ts';
import type { LogCapture, Portal } from './support/two-process-harness.ts';

/**
 * Every `switchTo` failure path against real processes: the spawn can fail, the
 * replacement can die before or after `activate`, it can never become ready,
 * and `close()` can interrupt it. The unit tests drive these through the
 * in-memory pair; here the staged process is a real Bun child whose exit, stderr
 * tail and pid are observable from outside, which is the only way to prove the
 * cleanup actually leaves nothing behind (INV-002).
 */
const REQUEST_TIMEOUT_MS = 10_000;
const SWITCH_TIMEOUT_MS = 15_000;
const STOP_TIMEOUT_MS = 20_000;
const WAIT_MS = 5_000;
const POLL_MS = 20;
/** Bun's default per-test budget is shorter than a spawn plus a rollback. */
const TEST_TIMEOUT_MS = 45_000;

const SWITCH_ROLLBACK =
  'server switch failed after the previous server retired; restarting the previous version';
const READY_TIMEOUT_WARN =
  'replacement server did not reach its barrier before the switch timeout';

/**
 * Replies `prepared` and then never answers `activate`. The interval keeps the
 * process alive so the switch has to abandon and kill it. The stderr line is
 * the diagnostic the rejection must still carry on this path, where the exit is
 * recorded only after the kill.
 */
const PREPARED_ONLY = [
  "console.error('stub: prepared but never activating');",
  "process.on('message', (message) => {",
  "  if (message?.type === 'hello') {",
  "    process.send({ type: 'prepared', pid: process.pid });",
  '  }',
  '});',
  'setInterval(() => {}, 1000);',
].join('\n');

/**
 * Records `activated <pid>` outside the process when `activate` arrives, then
 * stays alive without ever reporting `ready`. The file is evidence the switch
 * got past the previous process's retirement, which is the point of aborting it
 * from `close()` rather than assuming the phase.
 */
const PREPARE_THEN_MARK = [
  "const { appendFileSync } = require('node:fs');",
  "process.on('message', (message) => {",
  "  if (message?.type === 'hello') {",
  "    process.send({ type: 'prepared', pid: process.pid });",
  '    return;',
  '  }',
  "  if (message?.type === 'activate') {",
  '    appendFileSync(process.argv[1], `activated ${process.pid}\\n`);',
  '  }',
  '});',
  'setInterval(() => {}, 1000);',
].join('\n');

/**
 * Polls until `check` passes. Every wait in this file is a condition rather
 * than a fixed delay: the readiness and exit orderings are what the production
 * code promises, so a sleep long enough to pass on this machine would not test
 * them.
 */
async function until(
  check: () => boolean | Promise<boolean>,
  timeoutMs: number,
  what: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await check()) {
      return;
    }
    if (Date.now() >= deadline) {
      throw new Error(`${what} did not happen within ${timeoutMs}ms`);
    }
    await sleep(POLL_MS);
  }
}

/** Waits for a process the Portal never reported as ready. */
async function gone(pid: number): Promise<void> {
  await until(
    () => !isProcessAlive(pid),
    WAIT_MS,
    `pid ${String(pid)} to exit`,
  );
}

function pidsOf(logs: LogCapture, msg: string): number[] {
  const pids: number[] = [];
  for (const line of logs.lines) {
    const pid = line.pid;
    if (line.msg === msg && typeof pid === 'number') {
      pids.push(pid);
    }
  }
  return pids;
}

function lastPid(logs: LogCapture, msg: string): number {
  const pid = pidsOf(logs, msg).at(-1);
  if (pid === undefined) {
    throw new Error(`no "${msg}" log line carried a pid`);
  }
  return pid;
}

/** The single line with this message; a missing one is a test error, not a pass. */
function lineOf(logs: LogCapture, msg: string): Record<string, unknown> {
  const found = logs.lines.filter((line) => line.msg === msg);
  const [only] = found;
  if (found.length !== 1 || only === undefined) {
    throw new Error(`expected exactly one "${msg}" line, got ${found.length}`);
  }
  return only;
}

/**
 * Arms the rejection wait at once, so no path can leave the switch promise
 * unobserved while `close()` runs, and fails the test unless the failure is the
 * structured one callers are documented to receive.
 */
function switchFailureOf(switching: Promise<void>): Promise<ServerSwitchError> {
  return withDeadline(
    rejectionOf(switching),
    SWITCH_TIMEOUT_MS,
    'switchTo rejection',
  ).then((error) => {
    if (!isServerSwitchError(error)) {
      throw new Error(`expected a ServerSwitchError, got: ${error.message}`);
    }
    return error;
  });
}

function portalFor(
  databasePath: string,
  options: { startTimeoutMs?: number; restartBackoffMs?: number } = {},
): Portal {
  return createPortal({
    databasePath,
    behavior: 'echo',
    timeoutMs: 5000,
    startTimeoutMs: options.startTimeoutMs ?? 10_000,
    shutdownTimeoutMs: 1000,
    ...(options.restartBackoffMs === undefined
      ? {}
      : { restartBackoffMs: options.restartBackoffMs }),
  });
}

describe('version switch failure paths with real processes', () => {
  test(
    'a switch to a replacement spawned with stderr capture still promotes and serves',
    async () => {
      const database = tempDatabase({ prefix: 'mud-switch-failure-' });
      const portal = portalFor(database.path);
      let socket: WebSocket | undefined;
      try {
        await portal.layer.start();
        socket = await openClient(urlOf(portal), { timeoutMs: 5000 });
        const oldPid = lastPid(portal.logs, 'server ready');
        expect(
          await callOnSocket(
            socket,
            { id: 'before', method: 'echo', payload: 'before' },
            REQUEST_TIMEOUT_MS,
          ),
        ).toEqual({ id: 'before', ok: true, result: 'before' });

        const startedAt = performance.now();
        await withDeadline(
          portal.executor.switchTo({
            // Capture changes the child's stdio; the success path must keep
            // working, and the delay proves the switch waited for the
            // replacement rather than reporting a dead process.
            spawn: spawnFor(database.path, 'echo', 200, {
              stderrTailLines: 5,
            }),
          }),
          SWITCH_TIMEOUT_MS,
          'switchTo',
        );
        expect(performance.now() - startedAt).toBeGreaterThanOrEqual(200);

        const newPid = lastPid(portal.logs, 'replacement server ready');
        expect(newPid).not.toBe(oldPid);
        expect(isProcessAlive(newPid)).toBe(true);
        // The switch waits for the old process's exit before it promotes
        // (INV-002), so a resolving switch means the pid is already gone.
        expect(isProcessAlive(oldPid)).toBe(false);

        expect(
          await callOnSocket(
            socket,
            { id: 'after', method: 'echo', payload: 'after' },
            REQUEST_TIMEOUT_MS,
          ),
        ).toEqual({ id: 'after', ok: true, result: 'after' });
      } finally {
        socket?.close();
        await stopQuietly(portal, { stopTimeoutMs: STOP_TIMEOUT_MS });
        killPids(pidsInLogs(portal.logs));
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );

  test(
    'without stderr capture the rejection carries no tail',
    async () => {
      const database = tempDatabase({ prefix: 'mud-switch-failure-' });
      const portal = portalFor(database.path);
      let socket: WebSocket | undefined;
      try {
        await portal.layer.start();
        socket = await openClient(urlOf(portal), { timeoutMs: 5000 });
        const oldPid = lastPid(portal.logs, 'server ready');

        const error = await switchFailureOf(
          portal.executor.switchTo({
            // Negative control for the tail: the same failure with the default
            // stdio must not grow a field that consumers would branch on.
            spawn: spawnFor(database.path, 'boot-fail'),
          }),
        );
        const details = error.details;
        expect(details.stage).toBe('preparing');
        expect(details.stderrTail).toBeUndefined();
        expect(error.message).not.toContain('server stderr');

        expect(isProcessAlive(oldPid)).toBe(true);
        expect(
          await callOnSocket(
            socket,
            { id: 'after', method: 'echo', payload: 'after' },
            REQUEST_TIMEOUT_MS,
          ),
        ).toEqual({ id: 'after', ok: true, result: 'after' });
      } finally {
        socket?.close();
        await stopQuietly(portal, { stopTimeoutMs: STOP_TIMEOUT_MS });
        killPids(pidsInLogs(portal.logs));
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );

  test(
    'a replacement that dies before prepared carries its real stderr tail into the rejection',
    async () => {
      const database = tempDatabase({ prefix: 'mud-switch-failure-' });
      const portal = portalFor(database.path);
      let socket: WebSocket | undefined;
      try {
        await portal.layer.start();
        socket = await openClient(urlOf(portal), { timeoutMs: 5000 });
        const oldPid = lastPid(portal.logs, 'server ready');

        const startedAt = performance.now();
        const replacementSpawn = spawnFor(database.path, 'boot-fail', 0, {
          stderrTailLines: 5,
        });
        let replacement: SpawnedServer | undefined;
        const error = await switchFailureOf(
          portal.executor.switchTo({
            // A real child that fails before the handshake: its stderr line is
            // the only description of the cause, so the rejection must carry it
            // rather than leaving it on the Portal's console.
            spawn: () => {
              replacement = replacementSpawn();
              return replacement;
            },
          }),
        );
        const elapsedMs = performance.now() - startedAt;
        const details = error.details;
        expect(details.stage).toBe('preparing');
        expect(details.previousServing).toBe(true);
        expect(details.rolledBack).toBe(false);
        expect(details.promoted).toBe(false);
        expect(details.closeAborted).toBe(false);
        expect(details.pendingExits).toBe(0);

        await until(
          () =>
            details.stderrTail?.some((line) =>
              line.includes('server process failed: boot failed'),
            ) === true,
          WAIT_MS,
          'stderr tail to carry the boot failure',
        );
        const tail = details.stderrTail;
        if (tail === undefined) {
          throw new Error('the rejection carried no stderr tail');
        }
        expect(tail.length).toBeLessThanOrEqual(5);
        expect(error.message).toContain('server process failed: boot failed');
        // Not the timeout path: the replacement died, which is a different
        // operational story from a slow start.
        expect(error.message).not.toContain('within');

        // The details hand out a snapshot. The spawn keeps the live tail
        // array, so a mutation arriving after the switch settled would
        // otherwise rewrite a rejection the caller is already handling.
        if (replacement === undefined) {
          throw new Error('the switch never spawned a replacement');
        }
        const liveTail = replacement.lastExit()?.stderrTail;
        expect(liveTail).toBeDefined();
        expect(tail).not.toBe(liveTail);
        const settledLength = tail.length;
        await sleep(200);
        expect(tail.length).toBe(settledLength);

        expect(isProcessAlive(oldPid)).toBe(true);
        expect(
          await callOnSocket(
            socket,
            { id: 'after', method: 'echo', payload: 'after' },
            REQUEST_TIMEOUT_MS,
          ),
        ).toEqual({ id: 'after', ok: true, result: 'after' });
        // The old process was never retired, so it is the only ready Server
        // and no rollback restarted anything.
        expect(pidsOf(portal.logs, 'server ready')).toEqual([oldPid]);
        expect(pidsOf(portal.logs, 'replacement server ready')).toEqual([]);
        expect(elapsedMs).toBeLessThan(3000);
      } finally {
        socket?.close();
        await stopQuietly(portal, { stopTimeoutMs: STOP_TIMEOUT_MS });
        killPids(pidsInLogs(portal.logs));
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );

  test(
    'switchTo before start and after close rejects with a plain Error',
    async () => {
      const database = tempDatabase({ prefix: 'mud-switch-failure-' });
      const portal = portalFor(database.path);
      try {
        const idle = await rejectionOf(
          portal.executor.switchTo({ spawn: spawnFor(database.path) }),
        );
        expect(idle.message).toBe('ipc executor cannot switch while idle');
        expect(isServerSwitchError(idle)).toBe(false);
        // Neither refusal may reach a spawn: there is no Server to switch away
        // from, and starting one here would be a new lifecycle step.
        expect(portal.spawns()).toBe(0);

        await portal.layer.start();
        await withDeadline(portal.layer.stop(), STOP_TIMEOUT_MS, 'layer stop');

        const closed = await rejectionOf(
          portal.executor.switchTo({ spawn: spawnFor(database.path) }),
        );
        expect(closed.message).toBe('ipc executor cannot switch while closed');
        expect(isServerSwitchError(closed)).toBe(false);
        expect(portal.spawns()).toBe(1);
      } finally {
        await stopQuietly(portal, { stopTimeoutMs: STOP_TIMEOUT_MS });
        killPids(pidsInLogs(portal.logs));
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );

  test(
    'a second switch while one is in flight is refused with a plain Error',
    async () => {
      const database = tempDatabase({ prefix: 'mud-switch-failure-' });
      const portal = portalFor(database.path);
      try {
        await portal.layer.start();
        const spawnsAfterStart = portal.spawns();
        const first = portal.executor.switchTo({
          // The hold keeps the first switch in flight while the second is
          // issued synchronously, so the refusal cannot lose the race.
          spawn: spawnFor(database.path, 'echo', 300),
        });
        let secondSpawnCalls = 0;
        const second = portal.executor.switchTo({
          spawn: () => {
            secondSpawnCalls += 1;
            return spawnFor(database.path, 'echo')();
          },
        });

        const error = await withDeadline(
          rejectionOf(second),
          WAIT_MS,
          'second switch rejection',
        );
        expect(error.message).toBe(
          'an ipc executor switch is already in progress',
        );
        expect('details' in error).toBe(false);
        // The refused switch must not have spawned a second replacement beside
        // the one the first switch is still staging.
        expect(secondSpawnCalls).toBe(0);
        expect(portal.spawns()).toBe(spawnsAfterStart);

        await withDeadline(first, SWITCH_TIMEOUT_MS, 'first switchTo');
      } finally {
        await stopQuietly(portal, { stopTimeoutMs: STOP_TIMEOUT_MS });
        killPids(pidsInLogs(portal.logs));
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );

  test(
    'a spawn that throws is a ServerSwitchError with previousServing true',
    async () => {
      const database = tempDatabase({ prefix: 'mud-switch-failure-' });
      const portal = portalFor(database.path);
      let socket: WebSocket | undefined;
      try {
        await portal.layer.start();
        socket = await openClient(urlOf(portal), { timeoutMs: 5000 });
        const oldPid = lastPid(portal.logs, 'server ready');

        const error = await switchFailureOf(
          portal.executor.switchTo({
            // `Bun.spawn` throws synchronously for a missing binary, which is
            // the failure the switch's own try/catch exists for.
            spawn: spawnWithCommand(['/nonexistent/bun-binary']),
          }),
        );
        const details = error.details;
        expect(details.stage).toBe('preparing');
        expect(details.previousServing).toBe(true);
        expect(details.rolledBack).toBe(false);
        expect(details.promoted).toBe(false);
        expect(details.pendingExits).toBe(0);
        expect(details.stderrTail).toBeUndefined();

        // A switch that never reached the handshake leaves the old process as
        // the current Server, so the same connection keeps working.
        expect(isProcessAlive(oldPid)).toBe(true);
        expect(
          await callOnSocket(
            socket,
            { id: 'after', method: 'echo', payload: 'after' },
            REQUEST_TIMEOUT_MS,
          ),
        ).toEqual({ id: 'after', ok: true, result: 'after' });
      } finally {
        socket?.close();
        await stopQuietly(portal, { stopTimeoutMs: STOP_TIMEOUT_MS });
        killPids(pidsInLogs(portal.logs));
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );

  test(
    'a replacement that fails after activate rolls back to the previous version without dropping the connection',
    async () => {
      const database = tempDatabase({ prefix: 'mud-switch-failure-' });
      const portal = portalFor(database.path, { restartBackoffMs: 25 });
      let socket: WebSocket | undefined;
      try {
        await portal.layer.start();
        socket = await openClient(urlOf(portal), { timeoutMs: 5000 });
        const oldPid = lastPid(portal.logs, 'server ready');

        let settled = false;
        const switching = portal.executor.switchTo({
          // The replacement passes the handshake but refuses to open storage,
          // so the failure lands after the old process was already retired.
          spawn: spawnFor(database.path, 'required-extension-fail', 0, {
            stderrTailLines: 5,
          }),
        });
        // Observed, not awaited: the assertion below proves the request was
        // accepted while the switch was still in flight.
        void switching
          .catch(() => undefined)
          .finally(() => {
            settled = true;
          });
        const during = callOnSocket(
          socket,
          { id: 'during', method: 'echo', payload: 'during' },
          REQUEST_TIMEOUT_MS,
        );
        expect(settled).toBe(false);

        const error = await switchFailureOf(switching);
        const details = error.details;
        expect(details.stage).toBe('activating');
        expect(details.previousServing).toBe(false);
        expect(details.rolledBack).toBe(true);
        expect(details.promoted).toBe(false);
        expect(details.closeAborted).toBe(false);
        // The previous Server's exit was confirmed before activate; only the
        // killed replacement can still be unconfirmed, so the count is 0 or 1.
        expect(details.pendingExits).toBeLessThanOrEqual(1);

        await until(
          async () =>
            (await startupFailure(database.path)).includes(
              'required extension "pooled-only"',
            ),
          WAIT_MS,
          'the replacement startup failure reason',
        );
        await until(
          () => serverPids(portal.logs).length >= 2,
          WAIT_MS,
          'the rollback Server to report ready',
        );
        const readyPids = serverPids(portal.logs);
        expect(readyPids).toContain(oldPid);
        const rollbackPid = readyPids.at(-1);
        if (rollbackPid === undefined) {
          throw new Error('the rollback Server reported no pid');
        }
        expect(rollbackPid).not.toBe(oldPid);
        // The replacement never reached `ready`, so it must not be in the list
        // the rollback would have come from.
        expect(pidsOf(portal.logs, 'replacement server ready')).toEqual([]);

        await gone(oldPid);
        expect(isProcessAlive(rollbackPid)).toBe(true);
        await until(
          () => portal.executor.health().ready,
          WAIT_MS,
          'the rollback Server to become healthy',
        );
        expect(portal.executor.health().state).toBe('ready');

        // The request accepted mid-switch was buffered, not failed, and the
        // same connection still serves after the rollback.
        expect(await during).toEqual({
          id: 'during',
          ok: true,
          result: 'during',
        });
        expect(
          await callOnSocket(
            socket,
            { id: 'after', method: 'echo', payload: 'after' },
            REQUEST_TIMEOUT_MS,
          ),
        ).toEqual({ id: 'after', ok: true, result: 'after' });
        expect(
          portal.logs.lines.filter((line) => line.msg === 'request timed out'),
        ).toEqual([]);
        expect(lineOf(portal.logs, SWITCH_ROLLBACK).phase).toBe('activating');
      } finally {
        socket?.close();
        // P7: the same test proves the rollback leaves nothing behind. The
        // stop is bounded because an unconfirmed exit must be reported as a
        // failure here, not hang the suite.
        const spawnsBeforeStop = portal.spawns();
        const auditedPids = pidsInLogs(portal.logs);
        try {
          await withDeadline(
            portal.layer.stop(),
            STOP_TIMEOUT_MS,
            'layer stop',
          );
          await until(
            () => auditedPids.every((pid) => !isProcessAlive(pid)),
            WAIT_MS,
            'every logged pid to exit',
          );
          expect(portal.executor.health().state).toBe('closed');
          await sleep(250);
          expect(portal.spawns()).toBe(spawnsBeforeStop);
        } finally {
          killPids(pidsInLogs(portal.logs));
          killPids(auditedPids);
          database.remove();
        }
      }
    },
    TEST_TIMEOUT_MS,
  );

  test(
    'a replacement that never becomes ready times out and rolls back',
    async () => {
      const database = tempDatabase({ prefix: 'mud-switch-failure-' });
      const portal = portalFor(database.path, {
        startTimeoutMs: 2000,
        restartBackoffMs: 25,
      });
      const evidencePids: number[] = [];
      try {
        await portal.layer.start();
        const oldPid = lastPid(portal.logs, 'server ready');

        const startedAt = performance.now();
        const error = await switchFailureOf(
          portal.executor.switchTo({
            // Prepared but never ready: the staged process is alive, so this is
            // the timeout branch rather than a lost process. Capture is on so
            // the rejection can still name the cause on this path.
            spawn: spawnInline(PREPARED_ONLY, { stderrTailLines: 5 }),
          }),
        );
        const elapsedMs = performance.now() - startedAt;
        expect(error.message).toContain(
          'replacement did not become ready within 2000ms',
        );
        const details = error.details;
        expect(details.stage).toBe('activating');
        expect(details.rolledBack).toBe(true);
        expect(details.promoted).toBe(false);
        expect(details.closeAborted).toBe(false);
        expect(details.pendingExits).toBeGreaterThanOrEqual(1);
        // The staged process is killed at this moment, so the exit record is
        // not written yet: the diagnostic has to come from the live capture,
        // and still reach the rejection the caller reads.
        await until(
          () =>
            details.stderrTail?.some((line) =>
              line.includes('stub: prepared but never activating'),
            ) === true,
          WAIT_MS,
          'stderr tail to carry the stub diagnostic',
        );
        expect(error.message).toContain('stub: prepared but never activating');

        const stagedPid = lastPid(portal.logs, READY_TIMEOUT_WARN);
        evidencePids.push(stagedPid);
        // The abandoned process must be killed, not merely abandoned: the
        // rollback may only start once its exit is confirmed (INV-002).
        await gone(stagedPid);

        await until(
          () => serverPids(portal.logs).length >= 2,
          WAIT_MS,
          'the rollback Server to report ready',
        );
        const rollbackPid = serverPids(portal.logs).at(-1);
        if (rollbackPid === undefined) {
          throw new Error('the rollback Server reported no pid');
        }
        expect(rollbackPid).not.toBe(oldPid);
        expect(isProcessAlive(rollbackPid)).toBe(true);
        await until(
          () => portal.executor.health().ready,
          WAIT_MS,
          'the rollback Server to become healthy',
        );
        expect(
          await call(urlOf(portal), {
            id: 'after',
            method: 'echo',
            payload: 'after',
          }),
        ).toEqual({ id: 'after', ok: true, result: 'after' });

        // The switch sat out its own ready budget instead of failing early or
        // waiting for something else.
        expect(elapsedMs).toBeGreaterThanOrEqual(2000);
        expect(elapsedMs).toBeLessThan(2000 + 5000);
      } finally {
        await stopQuietly(portal, {
          stopTimeoutMs: STOP_TIMEOUT_MS,
          extraPids: evidencePids,
        });
        killPids(pidsInLogs(portal.logs));
        killPids(evidencePids);
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );

  test(
    'close() aborts a switch waiting for ready and confirms the replacement exit',
    async () => {
      const database = tempDatabase({ prefix: 'mud-switch-failure-' });
      const portal = portalFor(database.path);
      const evidencePath = `${database.path}.switch-stub`;
      const evidencePids: number[] = [];
      let socket: WebSocket | undefined;
      try {
        await portal.layer.start();
        socket = await openClient(urlOf(portal), { timeoutMs: 5000 });
        const oldPid = lastPid(portal.logs, 'server ready');

        const switching = portal.executor.switchTo({
          spawn: spawnInline(PREPARE_THEN_MARK, { args: [evidencePath] }),
        });
        const failure = switchFailureOf(switching);

        // The evidence file is written by the stub when `activate` arrives, so
        // waiting for it proves the switch is past the old process's
        // retirement and parked on `ready`.
        await until(
          () =>
            existsSync(evidencePath) &&
            readFileSync(evidencePath, 'utf8').includes('activated'),
          WAIT_MS,
          'the stub to receive activate',
        );
        const activated = /activated (\d+)/.exec(
          readFileSync(evidencePath, 'utf8'),
        )?.[1];
        if (activated === undefined) {
          throw new Error('the stub activation evidence carried no pid');
        }
        const stubPid = Number(activated);
        evidencePids.push(stubPid);

        await withDeadline(portal.layer.stop(), STOP_TIMEOUT_MS, 'layer stop');

        const error = await failure;
        const details = error.details;
        expect(details.stage).toBe('activating');
        expect(details.closeAborted).toBe(true);
        expect(details.previousServing).toBe(false);
        expect(details.rolledBack).toBe(false);
        expect(details.promoted).toBe(false);
        expect(details.pendingExits).toBeGreaterThanOrEqual(1);

        expect(portal.executor.health().state).toBe('closed');
        // `close()` owns the cleanup after an abort: it must not return while
        // the activated replacement may still hold storage (INV-002).
        await gone(oldPid);
        await gone(stubPid);

        const afterClose = await rejectionOf(
          portal.executor.switchTo({ spawn: spawnFor(database.path) }),
        );
        expect(afterClose.message).toBe(
          'ipc executor cannot switch while closed',
        );
        expect(isServerSwitchError(afterClose)).toBe(false);
      } finally {
        socket?.close();
        await stopQuietly(portal, {
          stopTimeoutMs: STOP_TIMEOUT_MS,
          extraPids: evidencePids,
        });
        killPids(pidsInLogs(portal.logs));
        killPids(evidencePids);
        database.remove();
      }
    },
    TEST_TIMEOUT_MS,
  );
});
