import { existsSync, rmSync } from 'node:fs';

import { closeClients } from './clients.ts';
import {
  FORCE_EXIT_TIMEOUT_MS,
  POLL_INTERVAL_MS,
  SCRIPT_BUDGET_MS,
  SERVER_EXIT_TIMEOUT_MS,
} from './config.ts';
import { controlReachable } from './portal.ts';
import type { PortalProcessHandle, RunningPortal } from './portal.ts';
import { printResult } from './report.ts';
import {
  fail,
  formatMs,
  isProcessAlive,
  messageOf,
  sleep,
  write,
  writeError,
} from './support.ts';
import type { Run } from './types.ts';

/** Abort paths, run construction and the bounded post-run cleanup. */

export interface AbortContext {
  readonly dataDirectory: string;
  /** The spawned child, available before the Portal is ready. */
  process(): PortalProcessHandle | undefined;
  portal(): RunningPortal | undefined;
}

/**
 * Forced end for a watchdog or signal. It kills the child (and any Server pid
 * already reported), waits for them within a bound, and only then removes the
 * directory: removing it first would let a surviving Server recreate it. The
 * normal path runs `cleanUp` instead.
 */
export async function abortNow(
  context: AbortContext,
  reason: string,
  exitCode: number,
): Promise<never> {
  try {
    writeError(`FAIL ${reason}; aborting the drill`);
    const portal = context.portal();
    const spawned = context.process();
    portal?.killNow();
    spawned?.killNow();
    if (spawned !== undefined) {
      const exited = await spawned.waitExited(FORCE_EXIT_TIMEOUT_MS);
      if (!exited) {
        writeError('abort: the Portal did not exit within its bound');
      }
      const deadline = Date.now() + SERVER_EXIT_TIMEOUT_MS;
      while (
        [...spawned.logPids].some(isProcessAlive) &&
        Date.now() < deadline
      ) {
        // oxlint-disable-next-line eslint/no-await-in-loop -- bounded wait for the Server exits
        await sleep(POLL_INTERVAL_MS);
      }
      const survivors = [...spawned.logPids].filter(isProcessAlive);
      if (survivors.length > 0) {
        writeError(`abort: Server pid(s) still alive: ${survivors.join(', ')}`);
      }
    }
    try {
      rmSync(context.dataDirectory, { recursive: true, force: true });
    } catch {
      // Best effort: the abort must not stall on a directory it cannot remove.
    }
  } finally {
    // oxlint-disable-next-line unicorn/no-process-exit -- an abort must end a stuck run even if a child holds the event loop
    process.exit(exitCode);
  }
}

export function createRun(
  runTag: string,
  dataDirectory: string,
  portal: RunningPortal,
): Run {
  return {
    runTag,
    dataDirectory,
    portal,
    clients: [],
    failures: [],
    requestSamples: [],
    eventSamples: [],
    windows: [],
    switches: [],
    stateChecks: [],
    anchorSendTimes: new Map(),
    anchorExpected: new Set(),
    anchorObserved: new Set(),
    activeSwitch: 0,
    probes: 0,
    stopping: false,
    windowWake: undefined,
  };
}

export interface CleanupOptions {
  readonly run: Run | undefined;
  readonly portal: RunningPortal | undefined;
  readonly dataDirectory: string;
}

/**
 * Closes clients, stops the Portal and removes the temporary directory,
 * returning every problem instead of throwing so a failed cleanup still ends
 * with a reported result.
 */
export async function cleanUp(options: CleanupOptions): Promise<string[]> {
  const problems: string[] = [];
  if (options.run !== undefined) {
    problems.push(...(await closeClients(options.run.clients)));
  }
  if (options.portal !== undefined) {
    const { portal } = options;
    try {
      const result = await portal.stop();
      const exit =
        result.exitCode === undefined || result.exitCode === null
          ? 'no exit code'
          : `code ${result.exitCode}`;
      const pids =
        result.pids.length === 0 ? 'none seen' : result.pids.join(', ');
      const state =
        result.survivors.length === 0
          ? 'all gone'
          : `still alive: ${result.survivors.join(', ')}`;
      write(
        `cleanup: portal exited (${exit}${result.forced ? ', forced' : ''}); Server pid(s) ${pids}: ${state}`,
      );
      if (result.exitCode !== 0) {
        problems.push(`cleanup: portal exit was not clean (${exit})`);
      }
      if (result.forced) {
        problems.push(
          'cleanup: the Portal had to be killed or its Server reaped',
        );
      }
      if (result.survivors.length > 0) {
        problems.push(
          `cleanup: Server pid(s) still alive: ${result.survivors.join(', ')}`,
        );
      }
    } catch (error) {
      problems.push(`cleanup: could not stop the Portal: ${messageOf(error)}`);
    }
    if (await controlReachable(portal.controlUrl)) {
      problems.push(
        'cleanup: the control endpoint still answers after shutdown',
      );
    }
  }
  try {
    rmSync(options.dataDirectory, { recursive: true, force: true });
  } catch (error) {
    problems.push(
      `cleanup: could not remove ${options.dataDirectory}: ${messageOf(error)}`,
    );
  }
  if (existsSync(options.dataDirectory)) {
    problems.push(
      `cleanup: temporary directory still exists: ${options.dataDirectory}`,
    );
  } else {
    write(`cleanup: removed ${options.dataDirectory}`);
  }
  return problems;
}

export interface FinishOptions {
  readonly run: Run | undefined;
  readonly portal: RunningPortal | undefined;
  readonly dataDirectory: string;
  readonly elapsedMs: number;
  readonly preRunFailure: string | undefined;
}

/**
 * Cleanup, the total line and the final result. Failures from the run, the
 * cleanup and the budget all land in the same count, which becomes the exit
 * code, so nothing can be reported as a pass after a failed teardown.
 */
export async function finish(options: FinishOptions): Promise<void> {
  const { run } = options;
  let preRunFailure = options.preRunFailure;
  const record = (problem: string): void => {
    if (run === undefined) {
      preRunFailure ??= problem;
      writeError(`FAIL ${problem}`);
    } else {
      fail(run, problem);
    }
  };
  let problems: string[];
  try {
    problems = await cleanUp({
      run,
      portal: options.portal,
      dataDirectory: options.dataDirectory,
    });
  } catch (error) {
    problems = [`cleanup threw: ${messageOf(error)}`];
  }
  for (const problem of problems) {
    record(problem);
  }
  if (options.elapsedMs >= SCRIPT_BUDGET_MS) {
    record(
      `total ${formatMs(options.elapsedMs)} ms exceeds ${SCRIPT_BUDGET_MS} ms`,
    );
  }
  write(`total: ${formatMs(options.elapsedMs)} ms`);
  const failures =
    (run?.failures.length ?? 0) + (preRunFailure === undefined ? 0 : 1);
  printResult(failures);
  process.exitCode = failures === 0 ? 0 : 1;
}
