import { rmSync } from 'node:fs';
import { resolve } from 'node:path';

import { portalStopFailed, shouldRemoveDataDir } from '../cleanup.ts';
import {
  integerText,
  isProcessAlive,
  latencyStats,
  latencyText,
  messageOf,
  pumpLines,
  sleep,
  withDeadline,
  write,
  writeError,
} from '../support.ts';
import type { LatencyStats, PortalStopResult } from '../support.ts';
import { PORTAL_STOP_BUDGET_MS } from './budget.ts';

/**
 * Configuration, phase statistics, Portal lifecycle and cleanup for
 * `bench:switch`. The measurement itself and the report live in `index.ts`.
 */

const DEFAULT_CLIENTS = 10;
const DEFAULT_INTERVAL_MS = 15;
const DEFAULT_DURATION_SEC = 15;
const DEFAULT_WARMUP_SEC = 3;
const DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
/** Minimum baseline observation before the switch is triggered. */
export const BASELINE_MS = 2000;
/** Extra time after `switchTo` resolves that still counts as the switch window. */
export const WINDOW_MARGIN_MS = 1000;
const CONTROL_TIMEOUT_MS = 60_000;
const PORTAL_READY_TIMEOUT_MS = 15_000;
const FORCE_EXIT_TIMEOUT_MS = 5000;
const SERVER_EXIT_TIMEOUT_MS = 5000;
const POLL_INTERVAL_MS = 25;

export interface SwitchConfig {
  readonly clients: number;
  readonly intervalMs: number;
  readonly durationSec: number;
  readonly warmupSec: number;
  readonly requestTimeoutMs: number;
  /** Run the two-client `say`/event probe. */
  readonly events: boolean;
  readonly dataDirectory: string | undefined;
  readonly keep: boolean;
  readonly externalUrl: string | undefined;
  readonly externalControlUrl: string | undefined;
}

function emptyToUndefined(value: string | undefined): string | undefined {
  return value !== undefined && value.trim().length > 0 ? value : undefined;
}

function envWhole(name: string, fallback: number, minimum: number): number {
  const raw = process.env[name]?.trim();
  if (raw === undefined || raw.length === 0) {
    return fallback;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum) {
    throw new Error(`${name} must be an integer >= ${minimum}, got ${raw}`);
  }
  return value;
}

export function readSwitchConfig(): SwitchConfig {
  const externalUrl = emptyToUndefined(process.env.SWITCH_URL);
  const externalControlUrl = emptyToUndefined(process.env.SWITCH_CONTROL_URL);
  if ((externalUrl === undefined) !== (externalControlUrl === undefined)) {
    throw new Error(
      'SWITCH_URL and SWITCH_CONTROL_URL must be set together; a switch needs the control endpoint',
    );
  }
  const config: SwitchConfig = {
    clients: envWhole('SWITCH_CLIENTS', DEFAULT_CLIENTS, 1),
    intervalMs: envWhole('SWITCH_INTERVAL_MS', DEFAULT_INTERVAL_MS, 1),
    durationSec: envWhole('SWITCH_DURATION_SEC', DEFAULT_DURATION_SEC, 1),
    warmupSec: envWhole('SWITCH_WARMUP_SEC', DEFAULT_WARMUP_SEC, 0),
    requestTimeoutMs: envWhole(
      'REQUEST_TIMEOUT_MS',
      DEFAULT_REQUEST_TIMEOUT_MS,
      1,
    ),
    events: process.env.SWITCH_EVENTS !== '0',
    dataDirectory: emptyToUndefined(process.env.SWITCH_DATA_DIR),
    keep: process.env.SWITCH_KEEP === '1',
    externalUrl,
    externalControlUrl,
  };
  if (config.warmupSec >= config.durationSec) {
    throw new Error(
      `SWITCH_WARMUP_SEC (${config.warmupSec}) must be less than SWITCH_DURATION_SEC (${config.durationSec})`,
    );
  }
  if (config.dataDirectory !== undefined) {
    return { ...config, dataDirectory: resolve(config.dataDirectory) };
  }
  return config;
}

/** The shared clock boundaries every sample and event is classified against. */
export interface SwitchMarkers {
  readonly measureStartNs: number;
  readonly endNs: number;
  triggerNs: number | undefined;
  resolvedNs: number | undefined;
  windowEndNs: number | undefined;
}

export type Phase = 'warmup' | 'baseline' | 'window' | 'recovery';

export interface RequestSample {
  readonly client: number;
  readonly startNs: number;
  readonly endNs: number;
  readonly ok: boolean;
  readonly code: string | undefined;
}

export function phaseAt(ns: number, markers: SwitchMarkers): Phase {
  if (ns < markers.measureStartNs) {
    return 'warmup';
  }
  if (markers.triggerNs === undefined) {
    return 'baseline';
  }
  if (markers.windowEndNs !== undefined && ns >= markers.windowEndNs) {
    return 'recovery';
  }
  return ns > markers.triggerNs ? 'window' : 'baseline';
}

/**
 * A request belongs to the switch window when it overlaps it at all: one that
 * was already in flight at the trigger waited through the switch just like one
 * started inside the window.
 */
export function phaseOfSample(
  sample: RequestSample,
  markers: SwitchMarkers,
): Phase {
  if (markers.triggerNs !== undefined && markers.windowEndNs !== undefined) {
    if (
      sample.startNs < markers.windowEndNs &&
      sample.endNs > markers.triggerNs
    ) {
      return 'window';
    }
    if (sample.startNs >= markers.windowEndNs) {
      return 'recovery';
    }
  }
  if (sample.endNs <= (markers.triggerNs ?? Number.POSITIVE_INFINITY)) {
    return sample.startNs >= markers.measureStartNs ? 'baseline' : 'warmup';
  }
  return phaseAt(sample.endNs, markers);
}

export interface PhaseSummary {
  readonly requests: number;
  readonly ok: number;
  /** Failures that are neither client-side nor framework timeouts. */
  readonly failed: number;
  /** Client-side deadline (`CLIENT_TIMEOUT`). */
  readonly clientTimeouts: number;
  /** Portal/Server deadline (`FRAMEWORK_TIMEOUT`). */
  readonly frameworkTimeouts: number;
  /** Both timeout kinds; broken out above, never folded into `failed`. */
  readonly timeouts: number;
  readonly disconnected: number;
  readonly latency: LatencyStats | undefined;
}

export function summarizePhase(
  samples: readonly RequestSample[],
  markers: SwitchMarkers,
  phase: Phase,
): PhaseSummary {
  let requests = 0;
  let ok = 0;
  let failed = 0;
  let clientTimeouts = 0;
  let frameworkTimeouts = 0;
  let disconnected = 0;
  const latencies: number[] = [];
  for (const sample of samples) {
    if (phaseOfSample(sample, markers) !== phase) {
      continue;
    }
    requests += 1;
    if (sample.ok) {
      ok += 1;
      latencies.push(sample.endNs - sample.startNs);
      continue;
    }
    if (sample.code === 'CLIENT_TIMEOUT') {
      clientTimeouts += 1;
      continue;
    }
    if (sample.code === 'FRAMEWORK_TIMEOUT') {
      frameworkTimeouts += 1;
      continue;
    }
    failed += 1;
    if (
      sample.code === 'CLIENT_CLOSED' ||
      sample.code === 'CLIENT_SEND_FAILED'
    ) {
      disconnected += 1;
    }
  }
  return {
    requests,
    ok,
    failed,
    clientTimeouts,
    frameworkTimeouts,
    timeouts: clientTimeouts + frameworkTimeouts,
    disconnected,
    latency: latencies.length === 0 ? undefined : latencyStats(latencies),
  };
}

export function phaseLine(label: string, summary: PhaseSummary): string {
  const latency =
    summary.latency === undefined
      ? 'latency: no successful samples'
      : `latency ms: ${latencyText(summary.latency)}`;
  return `${label.padEnd(9)} requests ${integerText(summary.requests).padStart(7)}  ok ${integerText(summary.ok).padStart(7)}  failed ${integerText(summary.failed).padStart(5)}  timeout ${integerText(summary.timeouts).padStart(5)} (client ${integerText(summary.clientTimeouts)}, framework ${integerText(summary.frameworkTimeouts)})  closed ${integerText(summary.disconnected).padStart(4)}  ${latency}`;
}

export interface SwitchReply {
  readonly ok: boolean;
  readonly durationNs: number;
  readonly error: string | undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export async function requestSwitch(controlUrl: string): Promise<SwitchReply> {
  const response = await withDeadline(
    fetch(controlUrl, { method: 'POST' }),
    CONTROL_TIMEOUT_MS,
    'POST /switch',
  );
  const text = await response.text();
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    throw new Error(
      `control endpoint returned non-JSON (${response.status}): ${messageOf(error)}`,
      { cause: error },
    );
  }
  if (!isRecord(parsed)) {
    throw new Error(`control endpoint returned an unexpected body: ${text}`);
  }
  const record = parsed;
  return {
    ok: record.ok === true,
    durationNs: typeof record.durationNs === 'number' ? record.durationNs : 0,
    error: typeof record.error === 'string' ? record.error : undefined,
  };
}

type PortalProcess = Bun.Subprocess<'ignore', 'pipe', 'inherit'>;

export interface SwitchPortal {
  readonly url: string;
  readonly controlUrl: string;
  readonly pid: number;
  serverPids(): readonly number[];
  liveServerPid(): number | undefined;
  logCount(msg: string): number;
  stop(): Promise<PortalStopResult>;
}

function serverPidFromLine(line: string): number | undefined {
  if (!line.startsWith('{')) {
    return undefined;
  }
  try {
    const parsed: unknown = JSON.parse(line);
    if (!isRecord(parsed)) {
      return undefined;
    }
    const record = parsed;
    if (
      record.msg !== 'server process ready' &&
      record.msg !== 'server ready'
    ) {
      return undefined;
    }
    const pid = record.pid;
    return typeof pid === 'number' && Number.isInteger(pid) && pid > 0
      ? pid
      : undefined;
  } catch {
    return undefined;
  }
}

function logMessageOf(line: string): string | undefined {
  if (!line.startsWith('{')) {
    return undefined;
  }
  try {
    const parsed: unknown = JSON.parse(line);
    if (!isRecord(parsed)) {
      return undefined;
    }
    const msg = parsed.msg;
    return typeof msg === 'string' ? msg : undefined;
  } catch {
    return undefined;
  }
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
  const timeout = new Promise<undefined>((resolveTimeout) => {
    timer = setTimeout(() => {
      resolveTimeout(undefined);
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

export async function startSwitchPortal(options: {
  readonly entry: string;
  readonly dataDirectory: string;
  readonly requestTimeoutMs: number;
}): Promise<SwitchPortal> {
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
  const logCounts = new Map<string, number>();
  const listening = Promise.withResolvers<string>();
  const control = Promise.withResolvers<string>();
  const stdoutPump = pumpLines(child.stdout, (line) => {
    const pid = serverPidFromLine(line);
    if (pid !== undefined) {
      serverPids.add(pid);
      newestServerPid = pid;
    }
    const msg = logMessageOf(line);
    if (msg !== undefined) {
      logCounts.set(msg, (logCounts.get(msg) ?? 0) + 1);
    }
    const url = /^portal: listening on (ws:\/\/\S+)$/.exec(line)?.[1];
    if (url !== undefined) {
      listening.resolve(url);
    }
    const controlUrl = /^portal: control on (http:\/\/\S+)$/.exec(line)?.[1];
    if (controlUrl !== undefined) {
      control.resolve(controlUrl);
    }
  }).catch((error: unknown) => {
    const wrapped = error instanceof Error ? error : new Error(String(error));
    listening.reject(wrapped);
    control.reject(wrapped);
  });

  let url: string;
  let controlUrl: string;
  try {
    [url, controlUrl] = await Promise.all([
      withDeadline(
        listening.promise,
        PORTAL_READY_TIMEOUT_MS,
        'portal listening line',
      ),
      withDeadline(
        control.promise,
        PORTAL_READY_TIMEOUT_MS,
        'portal control line',
      ),
    ]);
  } catch (error) {
    child.kill('SIGKILL');
    for (const pid of serverPids) {
      killQuietly(pid);
    }
    await waitForExit(child, FORCE_EXIT_TIMEOUT_MS);
    throw new Error(messageOf(error), { cause: error });
  }

  const liveServerPid = (): number | undefined =>
    newestServerPid !== undefined && isProcessAlive(newestServerPid)
      ? newestServerPid
      : undefined;

  let stopped: Promise<PortalStopResult> | undefined;
  const stop = (): Promise<PortalStopResult> => {
    stopped ??= (async () => {
      child.kill('SIGTERM');
      // ADR-0004's complete external stop bound plus a diagnostic margin, not a
      // fixed guess: a legal but slow drain/exit confirmation must finish.
      let exitCode = await waitForExit(child, PORTAL_STOP_BUDGET_MS);
      let forced = false;
      if (exitCode === undefined) {
        forced = true;
        child.kill('SIGKILL');
        exitCode = await waitForExit(child, FORCE_EXIT_TIMEOUT_MS);
      }
      for (const pid of serverPids) {
        if (isProcessAlive(pid)) {
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

  return {
    url,
    controlUrl,
    pid: child.pid,
    serverPids: () => [...serverPids],
    liveServerPid,
    logCount: (msg) => logCounts.get(msg) ?? 0,
    stop,
  };
}

export interface SwitchCleanupOptions {
  readonly portal: SwitchPortal | undefined;
  readonly dataDirectory: string | undefined;
  readonly userDataDirectory: boolean;
  readonly keep: boolean;
  /** Cleanup still runs, but the scene is kept for inspection. */
  readonly failed: boolean;
}

/**
 * Stops the Portal, confirms its Servers are gone, then removes the temporary
 * data directory only when the run was clean. A failed run keeps the directory
 * so the scene can be inspected.
 */
export async function cleanupSwitchRun(
  options: SwitchCleanupOptions,
): Promise<void> {
  let stopFailed =
    options.portal === undefined && options.dataDirectory !== undefined;
  let survivors: readonly number[] = [];
  if (options.portal !== undefined) {
    try {
      const result = await options.portal.stop();
      const exit =
        result.exitCode === undefined || result.exitCode === null
          ? 'no exit code'
          : `code ${result.exitCode}`;
      const pids =
        result.serverPids.length === 0
          ? 'none seen'
          : result.serverPids.join(', ');
      const state =
        result.survivors.length === 0
          ? 'all gone'
          : `still alive: ${result.survivors.join(', ')}`;
      write(
        `cleanup: portal exited (${exit}${result.forced ? ', forced' : ''}); Server pid(s) ${pids}: ${state}`,
      );
      survivors = result.survivors;
      stopFailed = portalStopFailed(result);
      if (stopFailed) {
        const exitNote =
          result.exitCode === undefined ? ' (exit not confirmed)' : '';
        writeError(
          `switch benchmark cleanup failed: ${state}; portal exit ${exit}${exitNote}`,
        );
        process.exitCode = 1;
      }
    } catch (error) {
      stopFailed = true;
      writeError(
        `switch benchmark cleanup failed: could not stop the Portal: ${messageOf(error)}`,
      );
      process.exitCode = 1;
    }
  }

  if (options.dataDirectory === undefined) {
    return;
  }
  const keep =
    options.userDataDirectory || options.keep || options.failed || stopFailed;
  if (!shouldRemoveDataDir({ stopFailed, survivors, keep })) {
    const why = options.userDataDirectory
      ? 'SWITCH_DATA_DIR'
      : options.keep
        ? 'SWITCH_KEEP=1'
        : options.failed
          ? 'the run failed'
          : stopFailed
            ? 'Portal stop was not graceful or not confirmed'
            : `Server pid(s) still alive: ${survivors.join(', ')}`;
    write(`cleanup: kept data dir ${options.dataDirectory} (${why})`);
    return;
  }
  try {
    rmSync(options.dataDirectory, { recursive: true, force: true });
    write(`cleanup: removed data dir ${options.dataDirectory}`);
  } catch (error) {
    writeError(
      `switch benchmark cleanup failed: could not remove ${options.dataDirectory}: ${messageOf(error)}`,
    );
    process.exitCode = 1;
  }
}
