import { describe, expect, test } from 'bun:test';
import { existsSync, readFileSync } from 'node:fs';
import { constants as osConstants } from 'node:os';
import { join } from 'node:path';

import {
  isProcessAlive,
  isRecord,
  LineReader,
  openClient,
  parseJsonLine,
  sleep,
  tempDatabase,
  withDeadline,
} from 'mud-framework-test-support';
import type { TempDatabase } from 'mud-framework-test-support';

import { SqliteBackend } from '../../packages/storage-sqlite/src/index.ts';

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
 * The signal delivery under test is POSIX-only. The win32 exit-code branch in
 * `diedFromSignal` is kept for completeness but has not been exercised here.
 */
const PORTAL_FIXTURE = join(import.meta.dir, 'fixtures', 'portal-entry.ts');

const READY_TIMEOUT_MS = 15_000;
/** Allows the layer drain, executor close budget and process-scheduling slack. */
const PORTAL_EXIT_TIMEOUT_MS = 30_000;
const SERVER_EXIT_TIMEOUT_MS = 10_000;
const CLEANUP_EXIT_TIMEOUT_MS = 3000;
const CLEANUP_PID_TIMEOUT_MS = 2000;
const STREAM_CLOSE_TIMEOUT_MS = 2000;
const STORE_TIMEOUT_MS = 5000;
const SERVER_READY_LOG_TIMEOUT_MS = 5000;
const POLL_INTERVAL_MS = 50;
const STDERR_TAIL_LINES = 20;

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

interface PortalReady {
  readonly portalPid: number;
  readonly serverPid: number;
  readonly wsPort: number;
  readonly mode: PortalMode;
}

type PortalProcess = Bun.Subprocess<'ignore', 'pipe', 'pipe'>;

function errorFrom(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

function assertMissing(pid: number): void {
  let code: unknown;
  try {
    process.kill(pid, 0);
  } catch (error) {
    code = isRecord(error) ? error.code : undefined;
  }
  expect(code).toBe('ESRCH');
}

/** Field 4 of `/proc/<pid>/stat`: the parent pid, checked only where it exists. */
function parentPidOf(pid: number): number | undefined {
  let stat: string;
  try {
    stat = readFileSync(`/proc/${pid}/stat`, 'utf8');
  } catch {
    return undefined;
  }
  const afterName = stat.slice(stat.lastIndexOf(')') + 2);
  const fields = afterName.split(' ');
  const rawPpid = fields[1];
  if (rawPpid === undefined) {
    return undefined;
  }
  const ppid = Number.parseInt(rawPpid, 10);
  return Number.isNaN(ppid) ? undefined : ppid;
}

/**
 * Bounded polling at the required 50ms interval. Returns how long the process
 * took to disappear, or undefined at the deadline, so the caller keeps a single
 * time assertion.
 */
async function waitForGone(
  pid: number,
  timeoutMs: number,
  intervalMs: number,
): Promise<number | undefined> {
  const startedAt = performance.now();
  for (;;) {
    if (!isProcessAlive(pid)) {
      return performance.now() - startedAt;
    }
    if (performance.now() - startedAt > timeoutMs) {
      return undefined;
    }
    await sleep(intervalMs);
  }
}

function requireReady(value: Record<string, unknown>): PortalReady {
  const mode = value.mode;
  if (mode !== 'graceful' && mode !== 'default') {
    throw new Error(`portal ready line has no mode: ${JSON.stringify(value)}`);
  }
  const { portalPid, serverPid, wsPort } = value;
  if (
    typeof portalPid !== 'number' ||
    typeof serverPid !== 'number' ||
    typeof wsPort !== 'number'
  ) {
    throw new Error(`portal ready line is malformed: ${JSON.stringify(value)}`);
  }
  return { portalPid, serverPid, wsPort, mode };
}

/**
 * `process.kill(0, …)` signals the caller's whole process group and a negative
 * pid addresses a group too, so only positive integers may reach a kill call.
 */
function isUsablePid(value: unknown): value is number {
  return typeof value === 'number' && Number.isInteger(value) && value > 0;
}

/**
 * Pids from the Server's own `server process ready` log line. The handshake
 * carries the pid too, but the handshake can be the line that never arrives;
 * this is the fallback cleanup uses to find the process anyway.
 */
function serverReadyPids(observed: readonly string[]): number[] {
  const pids: number[] = [];
  for (const line of observed) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      continue;
    }
    if (
      isRecord(parsed) &&
      parsed.msg === 'server process ready' &&
      isUsablePid(parsed.pid)
    ) {
      pids.push(parsed.pid);
    }
  }
  return pids;
}

/** The pid the fixture announces on stderr before it attempts the handshake. */
function serverPidFromStderr(lines: readonly string[]): number | undefined {
  const prefix = 'portal server pid ';
  let pid: number | undefined;
  for (const line of lines) {
    if (!line.startsWith(prefix)) {
      continue;
    }
    const parsed = Number.parseInt(line.slice(prefix.length), 10);
    if (isUsablePid(parsed)) {
      pid = parsed;
    }
  }
  return pid;
}

/**
 * The pid the fixture announces on stdout ahead of an unparsable handshake.
 * Same-stream order is what makes it reliable: a lone stderr chunk can be
 * delivered late, while a stdout line written before the broken handshake
 * cannot be read after it.
 */
function serverPidFromAnnouncements(
  observed: readonly string[],
): number | undefined {
  let pid: number | undefined;
  for (const line of observed) {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      continue;
    }
    if (
      isRecord(parsed) &&
      parsed.event === 'portal-server-pid' &&
      isUsablePid(parsed.serverPid)
    ) {
      pid = parsed.serverPid;
    }
  }
  return pid;
}

/**
 * The handshake is the primary source. The fixture's stdout announcement, the
 * Server's own log line and the fixture's stderr announcement cover the case
 * where it never arrived.
 */
async function findServerPid(
  knownPid: number | undefined,
  observed: readonly string[],
  stderrLines: readonly string[],
  timeoutMs: number,
): Promise<number | undefined> {
  if (knownPid !== undefined) {
    return knownPid;
  }
  const deadlineAt = performance.now() + timeoutMs;
  for (;;) {
    const pid =
      serverPidFromAnnouncements(observed) ??
      serverReadyPids(observed).at(-1) ??
      serverPidFromStderr(stderrLines);
    if (pid !== undefined) {
      return pid;
    }
    if (performance.now() > deadlineAt) {
      return undefined;
    }
    await sleep(POLL_INTERVAL_MS);
  }
}

/**
 * The Server writes its `server process ready` line after sending `ready` over
 * IPC, so it can trail the handshake by a scheduling hop.
 */
async function waitForServerReadyPids(
  observed: readonly string[],
  timeoutMs: number,
): Promise<number[]> {
  const deadlineAt = performance.now() + timeoutMs;
  for (;;) {
    const pids = serverReadyPids(observed);
    if (pids.length > 0) {
      return pids;
    }
    if (performance.now() > deadlineAt) {
      throw new Error(
        `no "server process ready" log line within ${timeoutMs}ms`,
      );
    }
    await sleep(POLL_INTERVAL_MS);
  }
}

interface StderrCapture {
  readonly lines: string[];
  readonly done: Promise<void>;
}

/** Collected in the background so the pipe can never fill. */
function captureStderr(
  stream: ReadableStream<Uint8Array<ArrayBuffer>>,
): StderrCapture {
  const lines: string[] = [];
  const reader = new LineReader(stream);
  const done = (async () => {
    let line = await reader.nextLine();
    while (line !== undefined) {
      lines.push(line);
      line = await reader.nextLine();
    }
  })().catch(() => undefined);
  return { lines, done };
}

async function readReadyLine(
  reader: LineReader,
  observed: string[],
): Promise<PortalReady> {
  let line = await reader.nextLine();
  while (line !== undefined) {
    observed.push(line);
    const parsed = parseJsonLine(line, 'portal stdout line');
    if (parsed.event === 'portal-ready') {
      return requireReady(parsed);
    }
    line = await reader.nextLine();
  }
  throw new Error('portal stdout closed before the ready line');
}

async function drainLines(
  reader: LineReader,
  observed: string[],
): Promise<void> {
  let line = await reader.nextLine();
  while (line !== undefined) {
    observed.push(line);
    line = await reader.nextLine();
  }
}

function nextFrame(socket: WebSocket): Promise<Record<string, unknown>> {
  return new Promise((resolve, reject) => {
    socket.addEventListener(
      'message',
      (event: MessageEvent) => {
        const data: unknown = event.data;
        if (typeof data !== 'string' && !(data instanceof Uint8Array)) {
          reject(new Error(`unexpected message type: ${typeof data}`));
          return;
        }
        const text =
          typeof data === 'string' ? data : new TextDecoder().decode(data);
        try {
          resolve(parseJsonLine(text, 'portal stdout line'));
        } catch (error) {
          reject(error instanceof Error ? error : new Error(String(error)));
        }
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

async function storeOneRow(
  wsPort: number,
  clientRequestId: string,
): Promise<void> {
  const socket = await withDeadline(
    openClient(`ws://127.0.0.1:${wsPort}/`),
    STORE_TIMEOUT_MS,
    'pre-kill store connection',
  );
  try {
    const reply = withDeadline(
      nextFrame(socket),
      STORE_TIMEOUT_MS,
      'pre-kill store was not acknowledged',
    );
    // `send` can throw before the await below claims `reply`; without a handler
    // the deadline rejection would surface as an unhandled rejection later.
    reply.catch(() => undefined);
    socket.send(
      JSON.stringify({
        id: clientRequestId,
        method: 'store',
        payload: clientRequestId,
      }),
    );
    expect(await reply).toEqual({
      id: clientRequestId,
      ok: true,
      result: { stored: clientRequestId },
    });
  } finally {
    socket.close();
  }
}

/**
 * On POSIX only `signalCode` proves the signal killed the Portal; a plain
 * `exitCode` would mean it exited normally instead. Windows has no POSIX
 * signal delivery and Bun reports the emulated kill as the shell convention
 * 128 + n, so that platform keeps the exit-code form.
 */
function diedFromSignal(
  portal: PortalProcess,
  signal: NodeJS.Signals,
): boolean {
  if (process.platform === 'win32') {
    const number = osConstants.signals[signal];
    return portal.exitCode === number + 128;
  }
  return portal.signalCode === signal;
}

/** The Server's own log line proving which shutdown path it took. */
function serverStopLine(
  observed: readonly string[],
  graceful: boolean,
): Record<string, unknown> | undefined {
  for (const line of observed) {
    let parsed: Record<string, unknown>;
    try {
      parsed = parseJsonLine(line, 'portal stdout line');
    } catch {
      continue;
    }
    if (graceful) {
      if (parsed.msg === 'server process stopped') {
        return parsed;
      }
      continue;
    }
    if (
      parsed.code === 'IPC_CHANNEL_CLOSED' ||
      parsed.code === 'IPC_PARENT_GONE'
    ) {
      return parsed;
    }
  }
  return undefined;
}

function describeStopLine(line: Record<string, unknown> | undefined): string {
  if (typeof line?.msg === 'string') {
    return line.msg;
  }
  if (typeof line?.code === 'string') {
    return line.code;
  }
  return 'not observed';
}

/**
 * Last captured stderr lines, so a Server-side diagnostic is still visible when
 * the row fails. The capture has stopped by the time callers report, because
 * cleanup waits for the pipe to close.
 */
function reportStderrTail(label: string, lines: readonly string[]): void {
  const tail = lines.slice(-STDERR_TAIL_LINES);
  if (tail.length === 0) {
    console.error(`[portal-death] ${label}: portal stderr was empty`);
    return;
  }
  console.error(
    `[portal-death] ${label}: last ${String(tail.length)} portal stderr line(s):`,
  );
  for (const line of tail) {
    console.error(`  ${line}`);
  }
}

interface RunningPortal {
  readonly portal: PortalProcess;
  readonly observed: string[];
  readonly stderr: StderrCapture;
  readonly reader: LineReader;
  readonly drain: Promise<void> | undefined;
  readonly database: TempDatabase;
  readonly knownServerPid: number | undefined;
}

interface CleanupResult {
  readonly serverPid: number | undefined;
  /**
   * True only when both processes are confirmed gone and the directory was
   * removed. False means the directory is kept and `retainedReason` says why.
   */
  readonly databaseRemoved: boolean;
  readonly retainedReason: string | undefined;
}

/**
 * Kills both processes and deletes the database directory only after both are
 * confirmed gone. The Server pid comes from the handshake when it arrived, and
 * otherwise from the fixture's stdout announcement, the Server's own log line
 * or the fixture's stderr announcement; without that fallback a failed
 * handshake would delete the directory under a live Server. An unconfirmed
 * process keeps the directory and is reported through the return value instead
 * of being thrown, so a cleanup failure cannot hide the test's own error.
 */
async function cleanupPortal(run: RunningPortal): Promise<CleanupResult> {
  const { portal, observed, stderr, reader, drain, database } = run;
  // Portal first: a live Portal would restart a Server killed underneath it,
  // and the cleanup would chase a moving pid.
  if (isProcessAlive(portal.pid)) {
    try {
      process.kill(portal.pid, 'SIGKILL');
    } catch {
      // Already gone, which is the case on every successful row.
    }
  }
  const portalGone = await withDeadline(
    portal.exited,
    CLEANUP_EXIT_TIMEOUT_MS,
    'portal exit during cleanup',
  ).then(
    () => true,
    () => false,
  );

  const serverPid = await findServerPid(
    run.knownServerPid,
    observed,
    stderr.lines,
    CLEANUP_PID_TIMEOUT_MS,
  );
  const retainedReasons: string[] = [];
  if (!portalGone) {
    retainedReasons.push(
      `portal ${String(portal.pid)} did not exit within ${String(CLEANUP_EXIT_TIMEOUT_MS)}ms of SIGKILL`,
    );
  }
  if (serverPid === undefined) {
    retainedReasons.push(
      'no Server pid was found, so its exit could not be confirmed',
    );
  } else if (isProcessAlive(serverPid)) {
    try {
      process.kill(serverPid, 'SIGKILL');
    } catch {
      // Already gone.
    }
    const goneMs = await waitForGone(
      serverPid,
      CLEANUP_EXIT_TIMEOUT_MS,
      POLL_INTERVAL_MS,
    );
    if (goneMs === undefined) {
      retainedReasons.push(
        `server process ${String(serverPid)} did not exit within ${String(CLEANUP_EXIT_TIMEOUT_MS)}ms of SIGKILL`,
      );
    }
  }
  await reader.cancel().catch(() => undefined);
  if (drain !== undefined) {
    await drain.catch(() => undefined);
  }
  await withDeadline(
    stderr.done,
    CLEANUP_EXIT_TIMEOUT_MS,
    'portal stderr to close',
  ).catch(() => undefined);

  if (retainedReasons.length > 0) {
    return {
      serverPid,
      databaseRemoved: false,
      retainedReason: retainedReasons.join('; '),
    };
  }
  try {
    database.remove();
  } catch (error) {
    return {
      serverPid,
      databaseRemoved: false,
      retainedReason: `removing ${database.directory} failed: ${errorFrom(error).message}`,
    };
  }
  return { serverPid, databaseRemoved: true, retainedReason: undefined };
}

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
 * A handshake the test can never parse, with the Server still running. The
 * fixture announces the Server pid on stdout ahead of the broken line, so the
 * test does not depend on a lone stderr chunk that Bun may deliver late. On
 * POSIX the Server is frozen before cleanup so it cannot exit on IPC
 * disconnect by itself: only the fallback pid lookup can remove it, and the
 * database must survive until then.
 */
async function runBrokenHandshake(): Promise<void> {
  const database = tempDatabase({ prefix: 'mud-portal-death-' });
  const observed: string[] = [];
  const portal = Bun.spawn({
    cmd: [process.execPath, 'run', PORTAL_FIXTURE, database.path, 'default'],
    env: { ...process.env, PORTAL_FIXTURE_BROKEN_HANDSHAKE: '1' },
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'pipe',
  });
  const reader = new LineReader(portal.stdout);
  const stderr = captureStderr(portal.stderr);
  let announcedPid: number | undefined;
  let failure: unknown;
  let cleanup: CleanupResult | undefined;
  try {
    const handshakeFailure = await withDeadline(
      readReadyLine(reader, observed),
      READY_TIMEOUT_MS,
      'broken portal handshake',
    ).catch((error: unknown) => error);
    expect(handshakeFailure).toBeInstanceOf(Error);
    if (handshakeFailure instanceof Error) {
      expect(handshakeFailure.message).toContain('not JSON');
      // The message quotes the offending line, so this text proves the reader
      // had already consumed a valid announcement and then hit the broken
      // line; if the announcement itself were unparsable it would be absent.
      expect(handshakeFailure.message).toContain('portal handshake failed');
    }
    expect(observed.at(-1)).toContain('portal handshake failed');

    // Same-stream ordering is the synchronization point: the fixture writes
    // this line to stdout before the broken handshake, so a reader that
    // reached the broken line necessarily observed it. stderr stays a cleanup
    // fallback only, because Bun can deliver a lone chunk late.
    announcedPid = serverPidFromAnnouncements(observed);
    expect(announcedPid).toBeDefined();
    if (announcedPid === undefined) {
      throw new Error('the fixture never announced the Server pid on stdout');
    }
    const stderrPid = serverPidFromStderr(stderr.lines);
    if (stderrPid !== undefined) {
      expect(stderrPid).toBe(announcedPid);
    }
    expect(isProcessAlive(announcedPid)).toBe(true);
    if (process.platform !== 'win32') {
      process.kill(announcedPid, 'SIGSTOP');
    }
  } catch (error) {
    failure = error;
  } finally {
    cleanup = await cleanupPortal({
      portal,
      observed,
      stderr,
      reader,
      drain: undefined,
      database,
      knownServerPid: undefined,
    });
  }

  if (failure !== undefined) {
    reportStderrTail('broken handshake', stderr.lines);
    if (cleanup !== undefined && !cleanup.databaseRemoved) {
      console.error(
        `[portal-death] broken handshake: kept ` +
          `${database.directory}: ${cleanup.retainedReason ?? 'reason unknown'}`,
      );
    }
    throw errorFrom(failure);
  }
  if (cleanup?.serverPid === undefined) {
    throw new Error('cleanup never found the Server pid');
  }
  if (!cleanup.databaseRemoved) {
    throw new Error(
      `cleanup kept ${database.directory}: ${cleanup.retainedReason ?? 'reason unknown'}`,
    );
  }
  if (announcedPid === undefined) {
    throw new Error('the fixture never announced the Server pid on stdout');
  }
  expect(cleanup.serverPid).toBe(announcedPid);
  assertMissing(portal.pid);
  assertMissing(announcedPid);
  expect(existsSync(database.path)).toBe(false);
  console.log(
    `[portal-death] broken handshake: cleaned server ${announcedPid}; database removed`,
  );
}

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
