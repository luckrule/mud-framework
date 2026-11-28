import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { SCRIPT_BUDGET_MS } from './hot-switch/config.ts';
import { abortNow, createRun, finish } from './hot-switch/lifecycle.ts';
import { aggregate, sayAccounting } from './hot-switch/metrics.ts';
import { spawnPortal, awaitPortalReady } from './hot-switch/portal.ts';
import type {
  PortalProcessHandle,
  RunningPortal,
} from './hot-switch/portal.ts';
import { judge, reportMetrics } from './hot-switch/report.ts';
import { scenario } from './hot-switch/scenario.ts';
import {
  asMs,
  fail,
  messageOf,
  nowNs,
  write,
  writeError,
} from './hot-switch/support.ts';
import type { Run } from './hot-switch/types.ts';

/**
 * Manual hot-switch drill for the game slice: real Portal, real Server
 * processes, real WebSocket clients, real SQLite. See the game-slice README;
 * the modules under `hot-switch/` own the load, the switches and the report.
 */
async function main(): Promise<void> {
  const startedAtNs = nowNs();
  const runTag = `${process.pid.toString(36)}${Date.now().toString(36).slice(-6)}`;
  const dataDirectory = mkdtempSync(join(tmpdir(), 'game-slice-hot-switch-'));
  let spawned: PortalProcessHandle | undefined;
  let portal: RunningPortal | undefined;
  let run: Run | undefined;
  let preRunFailure: string | undefined;
  // Set once an abort owns the teardown: the normal finally must then not
  // clean up concurrently or print a result the abort already decided.
  let aborted = false;

  const abortContext = {
    dataDirectory,
    process: () => spawned,
    portal: () => portal,
  };
  const abort = (reason: string, exitCode: number): void => {
    aborted = true;
    void abortNow(abortContext, reason, exitCode);
  };
  const watchdog = setTimeout(() => {
    abort(`script exceeded its ${SCRIPT_BUDGET_MS} ms budget`, 1);
  }, SCRIPT_BUDGET_MS);
  const onSignal = (signal: NodeJS.Signals): void => {
    abort(`interrupted by ${signal}`, 130);
  };
  process.once('SIGINT', onSignal);
  process.once('SIGTERM', onSignal);

  try {
    // Spawn synchronously, then publish the handle: a signal after this
    // point can kill the child even before it is ready.
    spawned = spawnPortal(dataDirectory);
    // Print the addresses as soon as they are parsed, before the initial pid
    // wait: a signal in that window must still show which ports to audit.
    void spawned.ready
      .then((addresses) => {
        write(`portal: ${addresses.url}`);
        write(`portal: control ${addresses.controlUrl}`);
        return undefined;
      })
      .catch(() => undefined);
    portal = await awaitPortalReady(spawned);
    const active = createRun(runTag, dataDirectory, portal);
    run = active;
    await scenario(active);
    const elapsedMs = asMs(nowNs() - startedAtNs);
    const stats = aggregate(active);
    judge(active, stats, elapsedMs);
    reportMetrics(active, stats, sayAccounting(active), elapsedMs);
  } catch (error) {
    if (run === undefined) {
      preRunFailure = messageOf(error);
      writeError(`FAIL ${preRunFailure}`);
    } else {
      fail(run, messageOf(error));
    }
  } finally {
    process.off('SIGINT', onSignal);
    process.off('SIGTERM', onSignal);
    clearTimeout(watchdog);
    // The abort path kills the child, waits for it, removes the directory and
    // owns the exit code; running the normal cleanup beside it would race.
    if (!aborted) {
      await finish({
        run,
        portal,
        dataDirectory,
        elapsedMs: asMs(nowNs() - startedAtNs),
        preRunFailure,
      });
    }
  }
}

try {
  await main();
} catch (error) {
  writeError(`FAIL ${messageOf(error)}`);
  process.exitCode = 1;
}
