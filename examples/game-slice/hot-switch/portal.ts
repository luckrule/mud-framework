import { fileURLToPath } from 'node:url';

import { parsePortalLine } from '../src/portal-log.ts';
import {
  CONTROL_PROBE_TIMEOUT_MS,
  FORCE_EXIT_TIMEOUT_MS,
  POLL_INTERVAL_MS,
  PORTAL_READY_TIMEOUT_MS,
  PORTAL_STOP_BUDGET_MS,
  SERVER_EXIT_TIMEOUT_MS,
  SERVER_PID_TIMEOUT_MS,
} from './config.ts';
import {
  isProcessAlive,
  killQuietly,
  messageOf,
  sleep,
  toError,
  withDeadline,
} from './support.ts';

/** Spawning, parsing and stopping the real Portal the drill drives. */

const PORTAL_ENTRY = fileURLToPath(
  new URL('../src/portal.ts', import.meta.url),
);

export interface PortalStopResult {
  readonly exitCode: number | null | undefined;
  readonly forced: boolean;
  readonly pids: readonly number[];
  readonly survivors: readonly number[];
}

/** The handle available immediately after `Bun.spawn`, before readiness. */
export interface PortalProcessHandle {
  readonly child: Bun.Subprocess<'ignore', 'pipe', 'inherit'>;
  readonly logPids: Set<number>;
  readonly ready: Promise<{
    readonly url: string;
    readonly controlUrl: string;
  }>;
  readonly pump: Promise<void>;
  killNow(): void;
  /** True when the child exited within the bound. */
  waitExited(timeoutMs: number): Promise<boolean>;
}

export interface RunningPortal {
  readonly url: string;
  readonly controlUrl: string;
  /** The pid of the Server the Portal started before any switch. */
  readonly initialPid: number;
  /** Adds a pid the control endpoint reported to the cleanup backstop list. */
  notePid(pid: number | undefined): void;
  stop(): Promise<PortalStopResult>;
  /** Synchronous reap for the abort paths; no waits, no logging. */
  killNow(): void;
}

type PortalProcess = Bun.Subprocess<'ignore', 'pipe', 'inherit'>;

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

function killChildAndPids(child: PortalProcess, pids: Iterable<number>): void {
  child.kill('SIGKILL');
  for (const pid of pids) {
    killQuietly(pid);
  }
}

/**
 * Spawns the Portal and starts reading its stdout synchronously. The handle is
 * returned before the child is ready, so a signal in the startup window can
 * still kill it instead of leaving an orphan.
 */
export function spawnPortal(dataDirectory: string): PortalProcessHandle {
  const child: PortalProcess = Bun.spawn({
    cmd: [process.execPath, 'run', PORTAL_ENTRY],
    stdin: 'ignore',
    stdout: 'pipe',
    // Inherited: Server and Portal diagnostics reach this terminal directly,
    // and only stdout has lines the drill must parse.
    stderr: 'inherit',
    env: {
      ...process.env,
      PORTAL_PORT: '0',
      GAME_SLICE_DATA_DIR: dataDirectory,
      // The control endpoint is off by default; only this drill turns it on.
      GAME_SLICE_CONTROL: '1',
    },
  });

  const logPids = new Set<number>();
  const ready = Promise.withResolvers<{
    readonly url: string;
    readonly controlUrl: string;
  }>();
  let url: string | undefined;
  let controlUrl: string | undefined;
  const pump = (async (): Promise<void> => {
    const reader = child.stdout.getReader();
    const decoder = new TextDecoder();
    let pending = '';
    for (;;) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- reading the next chunk is the operation itself
      const chunk = await reader.read();
      if (chunk.done) {
        break;
      }
      pending += decoder.decode(chunk.value, { stream: true });
      const parts = pending.split('\n');
      pending = parts.pop() ?? '';
      for (const part of parts) {
        const line = parsePortalLine(part);
        if (line === undefined) {
          continue;
        }
        if (line.kind === 'server-pid') {
          logPids.add(line.pid);
        } else if (line.kind === 'listening') {
          url = line.url;
        } else {
          controlUrl = line.url;
        }
        if (url !== undefined && controlUrl !== undefined) {
          ready.resolve({ url, controlUrl });
        }
      }
    }
    // The Portal's JSON log stream is parsed, not echoed: the report is the
    // interface. It only has to stay open until both addresses are known.
    ready.reject(
      new Error('portal stdout closed before it printed both addresses'),
    );
  })().catch((error: unknown) => {
    ready.reject(toError(error));
  });

  return {
    child,
    logPids,
    ready: ready.promise,
    pump,
    killNow: () => {
      killChildAndPids(child, logPids);
    },
    waitExited: async (timeoutMs) =>
      (await waitForExit(child, timeoutMs)) !== undefined,
  };
}

export async function awaitPortalReady(
  handle: PortalProcessHandle,
): Promise<RunningPortal> {
  let addresses: { readonly url: string; readonly controlUrl: string };
  try {
    addresses = await withDeadline(
      handle.ready,
      PORTAL_READY_TIMEOUT_MS,
      'portal listening/control lines',
    );
  } catch (error) {
    handle.killNow();
    await handle.waitExited(FORCE_EXIT_TIMEOUT_MS);
    throw new Error(messageOf(error), { cause: error });
  }

  // The first switch has no previous `toPid` to compare against, so the
  // initial Server's pid is required, not best-effort.
  const pidDeadline = Date.now() + SERVER_PID_TIMEOUT_MS;
  while (handle.logPids.size === 0 && Date.now() < pidDeadline) {
    // oxlint-disable-next-line eslint/no-await-in-loop -- bounded wait for the initial pid
    await sleep(POLL_INTERVAL_MS);
  }
  const initialPid = handle.logPids.values().next().value;
  if (initialPid === undefined) {
    handle.killNow();
    await handle.waitExited(FORCE_EXIT_TIMEOUT_MS);
    throw new Error(
      `no "server ready" line with a pid within ${SERVER_PID_TIMEOUT_MS}ms`,
    );
  }

  let stopped: Promise<PortalStopResult> | undefined;
  const stop = (): Promise<PortalStopResult> => {
    stopped ??= (async () => {
      const child = handle.child;
      child.kill('SIGTERM');
      let exitCode = await waitForExit(child, PORTAL_STOP_BUDGET_MS);
      let forced = false;
      if (exitCode === undefined) {
        forced = true;
        child.kill('SIGKILL');
        exitCode = await waitForExit(child, FORCE_EXIT_TIMEOUT_MS);
      }
      for (const pid of handle.logPids) {
        if (isProcessAlive(pid)) {
          // A Server outliving the Portal must not become an orphan writer.
          forced = true;
          killQuietly(pid);
        }
      }
      const deadline = Date.now() + SERVER_EXIT_TIMEOUT_MS;
      while (
        [...handle.logPids].some(isProcessAlive) &&
        Date.now() < deadline
      ) {
        // oxlint-disable-next-line eslint/no-await-in-loop -- bounded wait for the Server exits
        await sleep(POLL_INTERVAL_MS);
      }
      const survivors = [...handle.logPids].filter(isProcessAlive);
      await Promise.race([handle.pump, sleep(2000)]);
      return {
        exitCode,
        forced,
        pids: [...handle.logPids],
        survivors,
      };
    })();
    return stopped;
  };

  return {
    url: addresses.url,
    controlUrl: addresses.controlUrl,
    initialPid,
    notePid: (pid) => {
      if (pid !== undefined) {
        handle.logPids.add(pid);
      }
    },
    stop,
    killNow: () => {
      handle.killNow();
    },
  };
}

/** True when something still answers on the control URL after the Portal exits. */
export async function controlReachable(url: string): Promise<boolean> {
  try {
    await fetch(url, { signal: AbortSignal.timeout(CONTROL_PROBE_TIMEOUT_MS) });
    return true;
  } catch {
    return false;
  }
}
