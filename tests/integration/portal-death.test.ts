import { describe, expect, test } from 'bun:test';

import {
  isProcessAlive,
  LineReader,
  tempDatabase,
  withDeadline,
} from 'mud-framework-test-support';

import { SqliteBackend } from '../../packages/storage-sqlite/src/index.ts';

import {
  POLL_INTERVAL_MS,
  PORTAL_EXIT_TIMEOUT_MS,
  PORTAL_FIXTURE,
  assertMissing,
  captureStderr,
  cleanupPortal,
  describeStopLine,
  diedFromSignal,
  drainLines,
  errorFrom,
  parentPidOf,
  readReadyLine,
  READY_TIMEOUT_MS,
  reportStderrTail,
  runBrokenHandshake,
  SERVER_EXIT_TIMEOUT_MS,
  SERVER_READY_LOG_TIMEOUT_MS,
  serverReadyPids,
  serverStopLine,
  STREAM_CLOSE_TIMEOUT_MS,
  storeOneRow,
  waitForGone,
  waitForServerReadyPids,
} from './support/portal-death-harness.ts';
import type { CleanupResult } from './support/portal-death-harness.ts';

type PortalMode = 'graceful' | 'default';

interface SignalScenario {
  readonly label: string;
  readonly signal: NodeJS.Signals;
  readonly mode: PortalMode;
}

const SIGNAL_SCENARIOS: readonly SignalScenario[] = [
  {
    label: 'SIGTERM with the documented shutdown handler',
    signal: 'SIGTERM',
    mode: 'graceful',
  },
  {
    label: 'SIGTERM with the OS default action and no Portal handler',
    signal: 'SIGTERM',
    mode: 'default',
  },
  {
    label: 'SIGKILL, which no handler can catch',
    signal: 'SIGKILL',
    mode: 'default',
  },
];

async function runSignalScenario(scenario: SignalScenario): Promise<void> {
  const database = tempDatabase({ prefix: 'mud-portal-death-' });
  const observed: string[] = [];
  const graceful = scenario.mode === 'graceful';
  const portal = Bun.spawn({
    cmd: [
      process.execPath,
      'run',
      PORTAL_FIXTURE,
      database.path,
      scenario.mode,
    ],
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const reader = new LineReader(portal.stdout);
  const stderr = captureStderr(portal.stderr);
  let serverPid: number | undefined;
  let drain: Promise<void> | undefined;
  let failure: unknown;
  let cleanup: CleanupResult | undefined;
  try {
    const ready = await withDeadline(
      readReadyLine(reader, observed),
      READY_TIMEOUT_MS,
      'portal ready line',
    );
    serverPid = ready.serverPid;
    expect(ready.mode).toBe(scenario.mode);
    expect(ready.portalPid).toBe(portal.pid);

    // The pid under observation must be alive and actually a child of this
    // Portal; otherwise "it disappeared" could be a stale-pid artefact.
    expect(isProcessAlive(ready.portalPid)).toBe(true);
    expect(isProcessAlive(ready.serverPid)).toBe(true);
    if (process.platform === 'linux') {
      expect(parentPidOf(ready.serverPid)).toBe(ready.portalPid);
    }

    const runningDrain = drainLines(reader, observed);
    drain = runningDrain;
    runningDrain.catch(() => undefined);

    expect(
      await waitForServerReadyPids(observed, SERVER_READY_LOG_TIMEOUT_MS),
    ).toEqual([ready.serverPid]);

    const clientRequestId = `before-${scenario.signal}-${scenario.mode}`;
    await storeOneRow(ready.wsPort, clientRequestId);

    // A restart between the handshake and the signal changes which process is
    // under test; observing the old pid disappear must then fail, not pass.
    expect(serverReadyPids(observed).at(-1)).toBe(ready.serverPid);

    const signaledAt = performance.now();
    process.kill(ready.portalPid, scenario.signal);
    await withDeadline(
      portal.exited,
      PORTAL_EXIT_TIMEOUT_MS,
      `portal exit after ${scenario.signal}`,
    );
    const portalExitLatencyMs = Math.round(performance.now() - signaledAt);

    if (graceful) {
      expect(portal.exitCode).toBe(0);
      expect(portal.signalCode).toBeNull();
    } else {
      expect(diedFromSignal(portal, scenario.signal)).toBe(true);
    }
    expect(isProcessAlive(ready.portalPid)).toBe(false);

    const serverGoneMs = await waitForGone(
      ready.serverPid,
      SERVER_EXIT_TIMEOUT_MS,
      POLL_INTERVAL_MS,
    );
    if (serverGoneMs === undefined) {
      throw new Error(
        `server process ${ready.serverPid} survived ${SERVER_EXIT_TIMEOUT_MS}ms after the Portal died`,
      );
    }
    const serverExitLatencyMs = Math.round(performance.now() - signaledAt);
    assertMissing(ready.serverPid);

    // The Server's stdout is inherited from the Portal, so it survives the
    // Portal and carries the proof of which path stopped it. Waiting for EOF is
    // bounded: the assertion above already established that it exited.
    await withDeadline(
      runningDrain,
      STREAM_CLOSE_TIMEOUT_MS,
      'portal stdout to close',
    ).catch(() => undefined);
    const stopLine = serverStopLine(observed, graceful);
    expect(stopLine).toBeDefined();
    // Exactly one generation was observed: a second `server process ready`
    // line here would mean the pid assertions above tested the wrong process.
    expect(serverReadyPids(observed)).toEqual([ready.serverPid]);

    // A killed writer must not leave a lock behind. `transaction` opens with
    // BEGIN IMMEDIATE, so this fails rather than reads a stale snapshot if the
    // Server still held the write lock.
    const readerBackend = SqliteBackend.open({ filename: database.path });
    try {
      const rows = await readerBackend.transaction((tx) =>
        tx.query<{ body: string }>('SELECT body FROM notes'),
      );
      expect(rows).toEqual([{ body: clientRequestId }]);
    } finally {
      await readerBackend.close();
    }

    console.log(
      `[portal-death] ${scenario.signal} (${scenario.mode}): portal ${portalExitLatencyMs}ms ` +
        `exit=${String(portal.exitCode)} signal=${String(portal.signalCode)}; ` +
        `server ${serverExitLatencyMs}ms; ` +
        `stop=${describeStopLine(stopLine)}`,
    );
  } catch (error) {
    failure = error;
  } finally {
    cleanup = await cleanupPortal({
      portal,
      observed,
      stderr,
      reader,
      drain,
      database,
      knownServerPid: serverPid,
    });
  }

  if (failure !== undefined) {
    reportStderrTail(`${scenario.signal} (${scenario.mode})`, stderr.lines);
    if (cleanup !== undefined && !cleanup.databaseRemoved) {
      console.error(
        `[portal-death] ${scenario.signal} (${scenario.mode}): kept ` +
          `${database.directory}: ${cleanup.retainedReason ?? 'reason unknown'}`,
      );
    }
    throw errorFrom(failure);
  }
  if (cleanup === undefined) {
    throw new Error('cleanup produced no result');
  }
  if (!cleanup.databaseRemoved) {
    throw new Error(
      `cleanup kept ${database.directory}: ${cleanup.retainedReason ?? 'reason unknown'}`,
    );
  }
}

/**
 * The Portal is killed as a real OS process while a real Server child is
 * running. The Server must learn about the death (IPC disconnect or the
 * parent-liveness poll) and exit on its own; a surviving process would be a
 * second storage writer beside the next Server (INV-002). The fixture is
 * `fixtures/portal-entry.ts`, and the behaviour under test is the whole
 * deployment path, not a mocked channel.
 *
 * Every row gets its own temp database and an ephemeral WebSocket port. The
 * graceful row installs the SIGTERM shutdown pattern documented in
 * docs/INTEGRATION.md; the default rows install none, so the OS terminates the
 * Portal and the Server's orphan detection is the only thing that can stop it.
 * SIGINT/SIGHUP are left out: they take the same fixture paths as the SIGTERM
 * rows and each row costs a real Portal and Server spawn.
 *
 * The signal delivery under test is POSIX-only; the win32 exit-code branch in
 * `diedFromSignal` is kept for completeness but not exercised here.
 */
describe('Portal death does not orphan the Server', () => {
  for (const scenario of SIGNAL_SCENARIOS) {
    test(
      scenario.label,
      async () => {
        await runSignalScenario(scenario);
      },
      60_000,
    );
  }

  test('a failed handshake still kills the Server before the database is removed', async () => {
    await runBrokenHandshake();
  }, 30_000);
});
