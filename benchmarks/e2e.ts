import {
  mkdtempSync,
  readFileSync,
  rmSync,
  statfsSync,
  writeSync,
} from 'node:fs';
import { arch, cpus, platform, release, tmpdir } from 'node:os';
import { join } from 'node:path';

import type { Subprocess } from 'bun';

import { SqliteBackend } from 'mud-framework-storage-sqlite';
import {
  LineReader,
  isProcessAlive,
  isRecord,
  sleep,
} from 'mud-framework-test-support';

/**
 * End-to-end latency and throughput of the real two-process assembly: a Portal
 * child process (real `WsTransport` on an OS-assigned port, real `IpcExecutor`)
 * spawns a real Bun Server child (real `runServerProcess`, real SQLite file in
 * a temp directory). This process is the WebSocket client.
 *
 * `echo` measures the framework boundary: WS frame decode, per-connection
 * ordering, IPC round trip, an empty request transaction, encode. `store` adds
 * one INSERT to that same transaction. Requests are sequential (one in flight),
 * so the numbers are per-request latency, not saturating throughput.
 *
 * Each round uses a fresh temp directory and a fresh Portal+Server, and swaps
 * the mode order per round. The default 4 rounds let each mode run first and
 * second twice, so neither order is favoured. The committed row count is
 * verified after shutdown. Any failure kills the children and exits 1; the
 * temp directory is removed unless a process could not be killed, in which
 * case it is kept and its path printed.
 *
 * The report names the temp root's filesystem because tmpfs makes fsync nearly
 * free: on tmpfs the store numbers do not include the commit cost a real disk
 * pays, and pointing BENCH_DIR at a real disk is the only way to measure it.
 *
 * Run with `bun run bench:e2e`. Method and caveats: benchmarks/README.md.
 */

const PORTAL_ENTRY = join(import.meta.dir, 'fixture-portal.ts');

const DEFAULT_REQUESTS = 1000;
const DEFAULT_WARMUP = 100;
const DEFAULT_ROUNDS = 4;

/**
 * Where the per-round SQLite file is created. `/tmp` is tmpfs on many Linux
 * setups, where fsync is nearly free; point `BENCH_DIR` at a real disk to make
 * the store path pay the commit cost a deployment pays.
 */
const TEMP_ROOT = process.env.BENCH_DIR ?? tmpdir();

/** `statfs` magic numbers; the report names the temp root's filesystem. */
const TMPFS_MAGIC = 0x0102_1994;
const FILESYSTEM_NAMES: ReadonlyMap<number, string> = new Map([
  [TMPFS_MAGIC, 'tmpfs'],
  [0xef53, 'ext2/3/4'],
  [0x5846_5342, 'xfs'],
  [0x9123_683e, 'btrfs'],
  [0x794c_7630, 'overlayfs'],
  [0x2fc1_2fc1, 'zfs'],
  [0x6573_5546, 'fuse'],
]);

interface FilesystemInfo {
  readonly label: string;
  readonly tmpfs: boolean;
}

function filesystemInfo(path: string): FilesystemInfo {
  try {
    const { type } = statfsSync(path);
    const name = FILESYSTEM_NAMES.get(type);
    const hex = `0x${type.toString(16)}`;
    return {
      label: name === undefined ? `unknown (${hex})` : `${name} (${hex})`,
      tmpfs: type === TMPFS_MAGIC,
    };
  } catch (error) {
    return {
      label: `unknown (statfs failed: ${messageOf(error)})`,
      tmpfs: false,
    };
  }
}

const START_TIMEOUT_MS = 30_000;
const REQUEST_TIMEOUT_MS = 10_000;
const STOP_TIMEOUT_MS = 15_000;
const KILL_WAIT_MS = 2_000;
const KILL_POLL_MS = 25;

type Mode = 'echo' | 'store';

const MODES: readonly Mode[] = ['echo', 'store'];

type PortalProcess = Subprocess<'ignore', 'pipe', 'inherit'>;

interface Handshake {
  readonly wsPort: number;
  readonly serverPid: number;
  readonly portalPid: number;
}

interface ModeMeasurement {
  readonly mode: Mode;
  readonly durationsNs: readonly number[];
  readonly wallNs: number;
}

interface ModeStats {
  readonly mode: Mode;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly p99Ms: number;
  readonly opsPerSecond: number;
  readonly totalMs: number;
}

interface RoundResult {
  readonly round: number;
  readonly startupMs: number;
  readonly order: readonly Mode[];
  readonly echo: ModeStats;
  readonly store: ModeStats;
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

function exitFailed(): never {
  process.exitCode = 1;
  // oxlint-disable-next-line unicorn/no-process-exit -- a failed benchmark must not hang on a child handle cleanup could not close
  process.exit(1);
}

function exitSucceeded(): never {
  process.exitCode = 0;
  // oxlint-disable-next-line unicorn/no-process-exit -- a passed benchmark must not hang on a handle the framework did not close
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

async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  description: string,
): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_resolve, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`${description} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

function killQuietly(pid: number): void {
  try {
    process.kill(pid, 'SIGKILL');
  } catch {
    // Already gone, which is the expected case.
  }
}

/** Linux exposes a process's direct children here; `undefined` means no /proc. */
function childrenOf(pid: number): number[] | undefined {
  try {
    const raw = readFileSync(
      `/proc/${pid}/task/${pid}/children`,
      'utf8',
    ).trim();
    return raw.length === 0 ? [] : raw.split(/\s+/).map(Number);
  } catch {
    return undefined;
  }
}

function readPidFile(path: string): number | undefined {
  try {
    const pid = Number(readFileSync(path, 'utf8').trim());
    return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
  } catch {
    return undefined;
  }
}

async function waitUntilGone(
  targets: ReadonlySet<number>,
  deadline: number,
): Promise<number[]> {
  let remaining = [...targets].filter((pid) => isProcessAlive(pid));
  while (remaining.length > 0 && Date.now() < deadline) {
    // oxlint-disable-next-line eslint/no-await-in-loop -- polling a process exit is the loop body
    await sleep(KILL_POLL_MS);
    remaining = remaining.filter((pid) => isProcessAlive(pid));
  }
  return remaining;
}

function parseHandshake(line: string): Handshake | undefined {
  let parsed: unknown;
  try {
    parsed = JSON.parse(line);
  } catch {
    return undefined;
  }
  if (!isRecord(parsed) || parsed.event !== 'bench-portal-ready') {
    return undefined;
  }
  const { wsPort, serverPid, portalPid } = parsed;
  if (
    typeof wsPort !== 'number' ||
    !Number.isSafeInteger(wsPort) ||
    wsPort <= 0 ||
    typeof serverPid !== 'number' ||
    !Number.isSafeInteger(serverPid) ||
    serverPid <= 0 ||
    typeof portalPid !== 'number' ||
    !Number.isSafeInteger(portalPid) ||
    portalPid <= 0
  ) {
    return undefined;
  }
  return { wsPort, serverPid, portalPid };
}

async function readHandshake(
  lines: LineReader,
  transcript: string[],
): Promise<Handshake> {
  const deadline = Date.now() + START_TIMEOUT_MS;
  while (true) {
    const remaining = deadline - Date.now();
    if (remaining <= 0) {
      throw new Error(
        `Portal did not report readiness within ${START_TIMEOUT_MS}ms`,
      );
    }
    // oxlint-disable-next-line eslint/no-await-in-loop -- waiting for the next stdout line is the operation
    const line = await withTimeout(
      lines.nextLine(),
      remaining,
      'Portal handshake',
    );
    if (line === undefined) {
      throw new Error(
        `Portal stdout closed before the handshake; saw ${JSON.stringify(transcript)}`,
      );
    }
    transcript.push(line);
    const handshake = parseHandshake(line);
    if (handshake !== undefined) {
      return handshake;
    }
  }
}

/** Keeps draining stdout after the handshake so a late write cannot fill the pipe. */
async function drain(lines: LineReader): Promise<void> {
  try {
    while (true) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- draining the pipe is the loop body
      const line = await lines.nextLine();
      if (line === undefined) {
        return;
      }
    }
  } catch {
    // A closed pipe ends the drain.
  }
}

function spawnPortal(databasePath: string, pidPath: string): PortalProcess {
  return Bun.spawn({
    cmd: [process.execPath, 'run', PORTAL_ENTRY, databasePath, pidPath],
    stdin: 'ignore',
    stdout: 'pipe',
    stderr: 'inherit',
  });
}

/**
 * SIGTERM the Portal and wait for the framework's own bounded shutdown, then
 * make sure both pids are gone. A non-zero exit code means shutdown failed,
 * even if the processes did disappear.
 */
async function stopPortal(
  portal: PortalProcess,
  pids: readonly number[],
): Promise<void> {
  if (isProcessAlive(portal.pid)) {
    process.kill(portal.pid, 'SIGTERM');
  }
  const code = await withTimeout(
    portal.exited,
    STOP_TIMEOUT_MS,
    'Portal graceful shutdown',
  );
  if (code !== 0) {
    throw new Error(`Portal exited with code ${code} during shutdown`);
  }
  const remaining = await waitUntilGone(
    new Set(pids),
    Date.now() + KILL_WAIT_MS,
  );
  if (remaining.length > 0) {
    throw new Error(
      `processes still alive after Portal shutdown: ${remaining.join(', ')}`,
    );
  }
}

/**
 * Best effort and idempotent: children are collected before the Portal is
 * killed (a SIGKILLed parent cannot be asked for them afterwards), then every
 * known pid is killed and awaited. Returns whatever is still alive.
 */
async function forceCleanup(
  portal: PortalProcess,
  pidPath: string,
  serverPid: number | undefined,
): Promise<number[]> {
  const targets = new Set<number>();
  if (isProcessAlive(portal.pid)) {
    for (const child of childrenOf(portal.pid) ?? []) {
      targets.add(child);
    }
    killQuietly(portal.pid);
  }
  if (serverPid !== undefined) {
    targets.add(serverPid);
  }
  const fromFile = readPidFile(pidPath);
  if (fromFile !== undefined) {
    targets.add(fromFile);
  }
  for (const pid of targets) {
    killQuietly(pid);
  }
  return waitUntilGone(targets, Date.now() + KILL_WAIT_MS);
}

function parseFrame(data: unknown): Record<string, unknown> {
  const text =
    typeof data === 'string'
      ? data
      : data instanceof Uint8Array
        ? new TextDecoder().decode(data)
        : undefined;
  if (text === undefined) {
    throw new Error(`unexpected frame type: ${typeof data}`);
  }
  const parsed: unknown = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error(`frame is not a JSON object: ${text}`);
  }
  return parsed;
}

function openSocket(url: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    const timer = setTimeout(() => {
      socket.close();
      reject(
        new Error(
          `WebSocket connect to ${url} timed out after ${REQUEST_TIMEOUT_MS}ms`,
        ),
      );
    }, REQUEST_TIMEOUT_MS);
    socket.addEventListener(
      'open',
      () => {
        clearTimeout(timer);
        resolve(socket);
      },
      { once: true },
    );
    socket.addEventListener(
      'error',
      () => {
        clearTimeout(timer);
        socket.close();
        reject(new Error(`WebSocket connect to ${url} failed`));
      },
      { once: true },
    );
  });
}

function nextReply(
  socket: WebSocket,
  timeoutMs: number,
): Promise<Record<string, unknown>> {
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
      cleanup();
      try {
        resolve(parseFrame(data));
      } catch (error) {
        reject(error instanceof Error ? error : new Error(String(error)));
      }
    }

    function onError(): void {
      fail(new Error('socket errored before a reply arrived'));
    }

    function onClose(): void {
      fail(new Error('socket closed before a reply arrived'));
    }

    socket.addEventListener('message', onMessage, { once: true });
    socket.addEventListener('error', onError, { once: true });
    socket.addEventListener('close', onClose, { once: true });
    timer = setTimeout(() => {
      fail(new Error(`reply timed out after ${timeoutMs}ms`));
    }, timeoutMs);
  });
}

async function call(
  socket: WebSocket,
  mode: Mode,
  seq: number,
  phase: 'warmup' | 'measure',
): Promise<void> {
  const id = `${phase}-${mode}-${seq}`;
  const payload = mode === 'echo' ? { seq } : seq;
  const reply = nextReply(socket, REQUEST_TIMEOUT_MS);
  try {
    socket.send(JSON.stringify({ id, method: mode, payload }));
  } catch (error) {
    reply.catch(() => undefined);
    throw error;
  }
  const frame = await reply;
  if (frame.id !== id || frame.ok !== true) {
    throw new Error(`unexpected response to ${id}: ${JSON.stringify(frame)}`);
  }
  const result = frame.result;
  if (mode === 'echo') {
    if (!isRecord(result) || result.seq !== seq) {
      throw new Error(
        `echo result mismatch for ${id}: ${JSON.stringify(result)}`,
      );
    }
    return;
  }
  if (!isRecord(result) || result.stored !== seq) {
    throw new Error(
      `store result mismatch for ${id}: ${JSON.stringify(result)}`,
    );
  }
}

async function measureMode(
  wsPort: number,
  mode: Mode,
  requests: number,
  warmup: number,
): Promise<ModeMeasurement> {
  const socket = await openSocket(`ws://127.0.0.1:${wsPort}/`);
  try {
    for (let i = 0; i < warmup; i += 1) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- sequential requests are the measurement
      await call(socket, mode, i, 'warmup');
    }
    const durationsNs: number[] = [];
    const startedAt = Bun.nanoseconds();
    for (let i = 0; i < requests; i += 1) {
      const requestStart = Bun.nanoseconds();
      // oxlint-disable-next-line eslint/no-await-in-loop -- sequential requests are the measurement
      await call(socket, mode, warmup + i, 'measure');
      durationsNs.push(Bun.nanoseconds() - requestStart);
    }
    return { mode, durationsNs, wallNs: Bun.nanoseconds() - startedAt };
  } finally {
    socket.close();
  }
}

function percentileNs(sorted: readonly number[], percent: number): number {
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil((percent / 100) * sorted.length) - 1),
  );
  return sorted[index] ?? 0;
}

function statsOf(measurement: ModeMeasurement): ModeStats {
  const sorted = measurement.durationsNs.toSorted((a, b) => a - b);
  return {
    mode: measurement.mode,
    p50Ms: percentileNs(sorted, 50) / 1e6,
    p95Ms: percentileNs(sorted, 95) / 1e6,
    p99Ms: percentileNs(sorted, 99) / 1e6,
    opsPerSecond:
      measurement.durationsNs.length / (measurement.wallNs / 1_000_000_000),
    totalMs: measurement.wallNs / 1e6,
  };
}

/**
 * Reads the committed rows with a second backend after the Server is gone, so
 * the count proves the writes outlived the process and were not buffered.
 */
async function verifyStoredRows(
  databasePath: string,
  expected: number,
): Promise<void> {
  const backend = SqliteBackend.open({ filename: databasePath });
  try {
    const rows = await backend.transaction((tx) =>
      tx.query<{ total: number; maxSeq: number | null }>(
        'SELECT COUNT(*) AS total, MAX(seq) AS maxSeq FROM bench_rows',
      ),
    );
    const row = rows[0];
    if (row === undefined) {
      throw new Error('store mode committed no rows');
    }
    if (row.total !== expected) {
      throw new Error(
        `store mode committed ${row.total} rows, expected ${expected}`,
      );
    }
    if (row.maxSeq !== expected - 1) {
      throw new Error(
        `store mode max seq is ${String(row.maxSeq)}, expected ${expected - 1}`,
      );
    }
  } finally {
    await backend.close();
  }
}

function removeDirectory(directory: string): void {
  try {
    rmSync(directory, { recursive: true, force: true });
  } catch (error) {
    writeStderr(
      `bench: could not remove temp dir ${directory}: ${messageOf(error)}\n`,
    );
  }
}

async function runRound(
  round: number,
  requests: number,
  warmup: number,
): Promise<RoundResult> {
  const directory = mkdtempSync(join(TEMP_ROOT, 'mud-bench-e2e-'));
  const databasePath = join(directory, 'bench.sqlite');
  const pidPath = join(directory, 'server.pid');
  const transcript: string[] = [];
  const startedAt = Bun.nanoseconds();
  let portal: PortalProcess | undefined;
  let serverPid: number | undefined;

  try {
    // Everything that can throw lives inside this try so a failing spawn or
    // stream setup still removes the fresh temp directory.
    portal = spawnPortal(databasePath, pidPath);
    const lines = new LineReader(portal.stdout);
    const handshake = await readHandshake(lines, transcript);
    serverPid = handshake.serverPid;
    void drain(lines);
    const startupMs = (Bun.nanoseconds() - startedAt) / 1e6;

    // The mode measured second would otherwise always run with a warmed store
    // page cache; swapping the order per round cancels that.
    const order: readonly Mode[] = round % 2 === 0 ? MODES.toReversed() : MODES;
    const stats = new Map<Mode, ModeStats>();
    for (const mode of order) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- one mode at a time; concurrent modes would measure queueing, not latency
      const measurement = await measureMode(
        handshake.wsPort,
        mode,
        requests,
        warmup,
      );
      stats.set(mode, statsOf(measurement));
    }

    const echo = stats.get('echo');
    const store = stats.get('store');
    if (echo === undefined || store === undefined) {
      throw new Error('a mode was not measured');
    }

    await stopPortal(portal, [handshake.portalPid, handshake.serverPid]);
    await verifyStoredRows(databasePath, warmup + requests);

    return { round, startupMs, order, echo, store };
  } finally {
    if (portal === undefined) {
      removeDirectory(directory);
    } else {
      const remaining = await forceCleanup(portal, pidPath, serverPid);
      if (remaining.length === 0) {
        removeDirectory(directory);
      } else {
        writeStderr(
          `bench: round ${round} temp dir kept at ${directory} (still alive: ${remaining.join(', ')})\n`,
        );
      }
    }
  }
}

function median(values: readonly number[]): number {
  const sorted = values.toSorted((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle] ?? 0;
  }
  return ((sorted[middle - 1] ?? 0) + (sorted[middle] ?? 0)) / 2;
}

function medianStats(rounds: readonly RoundResult[], mode: Mode): ModeStats {
  const values = rounds.map((round) =>
    mode === 'echo' ? round.echo : round.store,
  );
  return {
    mode,
    p50Ms: median(values.map((value) => value.p50Ms)),
    p95Ms: median(values.map((value) => value.p95Ms)),
    p99Ms: median(values.map((value) => value.p99Ms)),
    opsPerSecond: median(values.map((value) => value.opsPerSecond)),
    totalMs: median(values.map((value) => value.totalMs)),
  };
}

function spreadText(values: readonly number[]): string {
  const centre = median(values);
  if (centre === 0) {
    return 'n/a';
  }
  const spread = ((Math.max(...values) - Math.min(...values)) / centre) * 100;
  return `${spread.toFixed(1)}%`;
}

const integerFormat = new Intl.NumberFormat('en-US');

function msText(value: number): string {
  return `${value.toFixed(3)}ms`;
}

function opsText(value: number): string {
  return `${integerFormat.format(Math.round(value))} ops/s`;
}

function modeLine(stats: ModeStats): string {
  return `  ${stats.mode.padEnd(5)}  p50 ${msText(stats.p50Ms)}  p95 ${msText(stats.p95Ms)}  p99 ${msText(stats.p99Ms)}  ${opsText(stats.opsPerSecond)}  total ${msText(stats.totalMs)}`;
}

function spreadLine(
  rounds: readonly RoundResult[],
  mode: Mode,
  label: string,
): string {
  const values =
    mode === 'echo'
      ? rounds.map((round) => round.echo)
      : rounds.map((round) => round.store);
  return `  ${label.padEnd(5)}  p50 ${spreadText(values.map((value) => value.p50Ms))}  p95 ${spreadText(values.map((value) => value.p95Ms))}  p99 ${spreadText(values.map((value) => value.p99Ms))}  ops/s ${spreadText(values.map((value) => value.opsPerSecond))}`;
}

function printReport(
  rounds: readonly RoundResult[],
  requests: number,
  warmup: number,
  elapsedSeconds: number,
): void {
  const lines: string[] = [];
  lines.push('mud-framework e2e benchmark');
  lines.push(
    `Bun ${process.versions.bun ?? 'unknown'}  ${platform()} ${arch()}  kernel ${release()}  ${cpus()[0]?.model ?? 'unknown CPU'}`,
  );
  lines.push(
    `${integerFormat.format(requests)} sequential requests per mode, warmup ${warmup}, ${rounds.length} rounds (one request in flight)`,
  );
  lines.push(
    'client: this process -> Portal child (WS, OS-assigned port) -> Server child (Bun IPC) -> SQLite (WAL, temp file)',
  );
  const filesystem = filesystemInfo(TEMP_ROOT);
  lines.push(`temp root: ${TEMP_ROOT}  fs ${filesystem.label}`);
  if (filesystem.tmpfs) {
    lines.push(
      '  tmpfs: fsync is nearly free here, so store does not pay the commit cost a real disk would; set BENCH_DIR to a real disk to measure it',
    );
  }
  lines.push('');
  for (const round of rounds) {
    lines.push(
      `round ${round.round}  order ${round.order.join(',')}  startup ${round.startupMs.toFixed(0)}ms`,
    );
    lines.push(modeLine(round.echo));
    lines.push(modeLine(round.store));
  }
  lines.push(`median of ${rounds.length} rounds`);
  lines.push(modeLine(medianStats(rounds, 'echo')));
  lines.push(modeLine(medianStats(rounds, 'store')));
  lines.push('round-to-round spread (max-min)/median');
  lines.push(spreadLine(rounds, 'echo', 'echo'));
  lines.push(spreadLine(rounds, 'store', 'store'));
  lines.push('');
  lines.push(
    `bench e2e ok: ${rounds.length} rounds, ${integerFormat.format(requests * 2 * rounds.length)} measured requests, ${elapsedSeconds.toFixed(1)}s total`,
  );
  writeStdout(`${lines.join('\n')}\n`);
}

async function main(): Promise<void> {
  const startedAt = Bun.nanoseconds();
  const requests = readPositiveInteger('BENCH_REQUESTS', DEFAULT_REQUESTS);
  const warmup = readPositiveInteger('BENCH_WARMUP', DEFAULT_WARMUP);
  const rounds = readPositiveInteger('BENCH_ROUNDS', DEFAULT_ROUNDS);
  if (rounds % 2 !== 0) {
    writeStderr(
      `bench: BENCH_ROUNDS=${rounds} is odd, so one mode runs first in one more round than the other; an even number keeps the order balanced\n`,
    );
  }

  const results: RoundResult[] = [];
  for (let round = 1; round <= rounds; round += 1) {
    // oxlint-disable-next-line eslint/no-await-in-loop -- rounds are sequential by design: one assembly at a time
    results.push(await runRound(round, requests, warmup));
  }
  printReport(
    results,
    requests,
    warmup,
    (Bun.nanoseconds() - startedAt) / 1_000_000_000,
  );
}

try {
  await main();
  exitSucceeded();
} catch (error) {
  writeStderr(`e2e benchmark failed: ${messageOf(error)}\n`);
  exitFailed();
}
