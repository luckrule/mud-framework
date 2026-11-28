import { writeSync } from 'node:fs';

import {
  isProcessAlive,
  LineReader,
  sleep,
  withDeadline,
} from 'mud-framework-test-support';

import { EXECUTOR_CLOSE_SLACK_MS } from '../../packages/core/src/portal/connection-layer-options.ts';
import {
  closeBudget,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
} from '../../packages/core/src/shared/budget.ts';
import { messageOf } from '../../packages/core/src/shared/guards.ts';

/** Portal lifecycle, stdout parsing and latency statistics for `index.ts`. */

const POLL_INTERVAL_MS = 25;
const FORCE_EXIT_TIMEOUT_MS = 5000;
const SERVER_EXIT_TIMEOUT_MS = 5000;

/** Headroom above the legal bound, so a slow stop is not read as a hang. */
export const STOP_DIAGNOSTIC_MARGIN_MS = 1000;

/**
 * ADR-0004 bounds an external `ConnectionLayer.stop()` by
 * `C + max(C, E + S) + C`: connection-layer drain, executor close (the
 * executor's declared budget plus slack), then transport close. `C` is the
 * `shutdownTimeoutMs` both benchmark Portals configure explicitly, which is
 * `DEFAULT_SHUTDOWN_TIMEOUT_MS` — not the connection layer's option default.
 * `E` is core's close budget for that same grace, and `S` is
 * `EXECUTOR_CLOSE_SLACK_MS`. Waiting less than this kills a Server that is
 * closing legally; a Portal whose configuration changes must change this too.
 */
export function portalStopBudgetMs(): number {
  const connectionMs = DEFAULT_SHUTDOWN_TIMEOUT_MS;
  const executorMs = Math.max(
    connectionMs,
    closeBudget(DEFAULT_SHUTDOWN_TIMEOUT_MS) + EXECUTOR_CLOSE_SLACK_MS,
  );
  return connectionMs + executorMs + connectionMs + STOP_DIAGNOSTIC_MARGIN_MS;
}

export function write(line: string): void {
  writeSync(1, `${line}\n`);
}

export function writeError(line: string): void {
  writeSync(2, `${line}\n`);
}

/** Calls `onLine` for every newline-terminated chunk plus the trailing line. */
export async function pumpLines(
  stream: ReadableStream<Uint8Array<ArrayBuffer>>,
  onLine: (line: string) => void,
): Promise<void> {
  const reader = new LineReader(stream);
  for (;;) {
    // oxlint-disable-next-line eslint/no-await-in-loop -- a stream yields chunks over time; reading the next one is the operation itself
    const line = await reader.nextLine();
    if (line === undefined) {
      return;
    }
    onLine(line);
  }
}

export interface LatencyStats {
  readonly minNs: number;
  readonly p50Ns: number;
  readonly p95Ns: number;
  readonly p99Ns: number;
  readonly maxNs: number;
}

export function latencyStats(samples: readonly number[]): LatencyStats {
  const sorted = samples.toSorted((a, b) => a - b);
  const at = (quantile: number): number =>
    sorted[
      Math.min(sorted.length - 1, Math.ceil(quantile * sorted.length) - 1)
    ] ?? 0;
  return {
    minNs: sorted[0] ?? 0,
    p50Ns: at(0.5),
    p95Ns: at(0.95),
    p99Ns: at(0.99),
    maxNs: sorted.at(-1) ?? 0,
  };
}

/** Milliseconds with three significant digits; the caller labels the unit. */
export function millisecondsText(ns: number): string {
  const ms = ns / 1e6;
  return ms >= 10 ? ms.toFixed(1) : ms >= 1 ? ms.toFixed(2) : ms.toFixed(3);
}

export function latencyText(stats: LatencyStats): string {
  return `min ${millisecondsText(stats.minNs)}  p50 ${millisecondsText(stats.p50Ns)}  p95 ${millisecondsText(stats.p95Ns)}  p99 ${millisecondsText(stats.p99Ns)}  max ${millisecondsText(stats.maxNs)}`;
}

const integerFormat = new Intl.NumberFormat('en-US');

export function integerText(value: number): string {
  return integerFormat.format(value);
}

export function throughputText(count: number, elapsedNs: number): string {
  return `${integerText(Math.round((count * 1e9) / elapsedNs))} ops/s`;
}

export function envInt(name: string, fallback: number): number {
  const raw = process.env[name]?.trim();
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} is not a positive integer: ${raw}`);
  }
  return value;
}

export function envIntList(
  name: string,
  fallback: readonly number[],
): readonly number[] {
  const raw = process.env[name]?.trim();
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  const values = raw.split(',').map((part) => {
    const value = Number(part.trim());
    if (!Number.isInteger(value) || value <= 0) {
      throw new Error(`${name} entry is not a positive integer: ${part}`);
    }
    return value;
  });
  return [...new Set(values)];
}

function killQuietly(pid: number): void {
  try {
    process.kill(pid, 'SIGKILL');
  } catch {
    // Already gone between the liveness check and the kill.
  }
}

async function waitForExit(
  child: PortalProcess,
  timeoutMs: number,
): Promise<number | null | undefined> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<undefined>((resolve) => {
    timer = setTimeout(() => {
      resolve(undefined);
    }, timeoutMs);
  });
  try {
    return await Promise.race([child.exited, timeout]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

type PortalProcess = Bun.Subprocess<'ignore', 'pipe', 'inherit'>;

export interface PortalHandle {
  readonly url: string;
  readonly pid: number;
  /** The newest Server pid from the Portal's log, while it is still alive. */
  liveServerPid(): number | undefined;
  stop(): Promise<PortalStopResult>;
}

export interface PortalStopResult {
  readonly exitCode: number | null | undefined;
  /** A SIGKILL fallback or a leftover Server: cleanup degraded, not silent. */
  readonly forced: boolean;
  readonly serverPids: readonly number[];
  readonly survivors: readonly number[];
}

/**
 * The Portal prints the listening line; the Server logs its own pid through
 * the inherited stdout, so one pump covers both.
 */
function serverPidFromLine(line: string): number | undefined {
  if (!line.startsWith('{')) {
    return undefined;
  }
  try {
    const parsed: unknown = JSON.parse(line);
    if (typeof parsed !== 'object' || parsed === null) {
      return undefined;
    }
    if (
      !('msg' in parsed) ||
      (parsed.msg !== 'server process ready' &&
        parsed.msg !== 'server ready') ||
      !('pid' in parsed)
    ) {
      return undefined;
    }
    const pid = parsed.pid;
    return typeof pid === 'number' && Number.isInteger(pid) && pid > 0
      ? pid
      : undefined;
  } catch {
    return undefined;
  }
}

export async function startPortal(options: {
  readonly entry: string;
  readonly dataDirectory: string;
  readonly requestTimeoutMs: number;
  readonly readyTimeoutMs: number;
}): Promise<PortalHandle> {
  const child = Bun.spawn({
    cmd: [process.execPath, 'run', options.entry],
    stdin: 'ignore',
    stdout: 'pipe',
    // Inherited so Server and Portal diagnostics stay visible without a pump.
    stderr: 'inherit',
    env: {
      ...process.env,
      PORTAL_PORT: '0',
      GAME_SLICE_DATA_DIR: options.dataDirectory,
      REQUEST_TIMEOUT_MS: String(options.requestTimeoutMs),
    },
  });

  const serverPids = new Set<number>();
  let newestServerPid: number | undefined;
  const listening = Promise.withResolvers<string>();
  const stdoutPump = pumpLines(child.stdout, (line) => {
    const pid = serverPidFromLine(line);
    if (pid !== undefined) {
      serverPids.add(pid);
      newestServerPid = pid;
    }
    const url = /^portal: listening on (ws:\/\/\S+)$/.exec(line)?.[1];
    if (url !== undefined) {
      listening.resolve(url);
    }
  }).catch((error: unknown) => {
    listening.reject(error instanceof Error ? error : new Error(String(error)));
  });

  let url: string;
  try {
    url = await withDeadline(
      listening.promise,
      options.readyTimeoutMs,
      'portal listening line',
    );
  } catch (error) {
    child.kill('SIGKILL');
    for (const pid of serverPids) {
      killQuietly(pid);
    }
    await waitForExit(child, FORCE_EXIT_TIMEOUT_MS);
    throw new Error(messageOf(error), { cause: error });
  }

  // The Server logs its pid just after the listening line can win the race; the
  // pump keeps filling `serverPids`, and stop() re-checks it much later.
  const liveServerPid = (): number | undefined =>
    newestServerPid !== undefined && isProcessAlive(newestServerPid)
      ? newestServerPid
      : undefined;

  let stopped: Promise<PortalStopResult> | undefined;
  const stop = (): Promise<PortalStopResult> => {
    stopped ??= (async () => {
      child.kill('SIGTERM');
      let exitCode = await waitForExit(child, portalStopBudgetMs());
      let forced = false;
      if (exitCode === undefined) {
        forced = true;
        child.kill('SIGKILL');
        exitCode = await waitForExit(child, FORCE_EXIT_TIMEOUT_MS);
      }
      for (const pid of serverPids) {
        if (isProcessAlive(pid)) {
          // A Server outliving the Portal must not become an orphan writer;
          // reap it and surface the degraded shutdown through `forced`.
          forced = true;
          killQuietly(pid);
        }
      }
      const exitDeadline = Date.now() + SERVER_EXIT_TIMEOUT_MS;
      while (
        [...serverPids].some((pid) => isProcessAlive(pid)) &&
        Date.now() < exitDeadline
      ) {
        // oxlint-disable-next-line eslint/no-await-in-loop -- bounded wait for the Server exits
        await sleep(POLL_INTERVAL_MS);
      }
      const survivors = [...serverPids].filter((pid) => isProcessAlive(pid));
      await Promise.race([Promise.allSettled([stdoutPump]), sleep(2000)]);
      return { exitCode, forced, serverPids: [...serverPids], survivors };
    })();
    return stopped;
  };

  return { url, pid: child.pid, liveServerPid, stop };
}
