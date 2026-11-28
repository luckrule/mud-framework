import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { withDeadline } from 'mud-framework-test-support';

import { messageOf } from '../../../packages/core/src/shared/guards.ts';
import { portalStopFailed } from '../cleanup.ts';
import { pumpLines, startPortal, write, writeError } from '../support.ts';
import type { PortalHandle } from '../support.ts';
import {
  clientSessionId,
  mixText,
  parseLoadResultJson,
  senderClientsFor,
} from './support.ts';
import type {
  LoadResultJson,
  MainConfig,
  SharedLoadConfig,
} from './support.ts';

/**
 * Coordinator plumbing: the Portal process, generator plans and their
 * lifecycle, result collection, and bounded cleanup. Reporting lives in
 * `report.ts`; the measurement itself lives in `client.ts`.
 */

const CLIENT_ENTRY = join(import.meta.dir, 'client.ts');
const BENCH_SERVER_ENTRY = join(import.meta.dir, 'bench-server.ts');
const PORTAL_ENTRY = join(
  import.meta.dir,
  '..',
  '..',
  '..',
  'examples',
  'game-slice',
  'src',
  'portal.ts',
);

const READY_TIMEOUT_MS = 15_000;
const SETUP_TIMEOUT_MS = 180_000;
const MEASUREMENT_SLACK_MS = 30_000;
const GENERATOR_EXIT_TIMEOUT_MS = 10_000;
const GENERATOR_TERM_WAIT_MS = 3000;
const GENERATOR_KILL_WAIT_MS = 3000;

type GeneratorProcess = Bun.Subprocess<'pipe', 'pipe', 'inherit'>;

export type LoadPortalOptions =
  | {
      readonly serverStats: true;
      /** Benchmark-local Server writes its frame/handler counters here. */
      readonly serverStatsFile: string;
      /** Session ids of the sender clients, for the handler duration filter. */
      readonly senderSessions: readonly string[];
    }
  | { readonly serverStats: false };

/**
 * The Portal reads `GAME_SLICE_SERVER_ENTRY` for its Server child, so the
 * counting entry and the two stats variables only travel together. Disabled
 * runs explicitly blank all three: `startPortal` merges `process.env`, and an
 * inherited value must not select the counting entry when the run claims the
 * production one. The Portal treats an empty entry as its default.
 */
export function loadPortalExtraEnv(
  options: LoadPortalOptions,
): Readonly<Record<string, string>> {
  if (!options.serverStats) {
    return {
      GAME_SLICE_SERVER_ENTRY: '',
      BENCH_SERVER_STATS: '',
      BENCH_SENDER_SESSIONS: '',
    };
  }
  return {
    GAME_SLICE_SERVER_ENTRY: BENCH_SERVER_ENTRY,
    BENCH_SERVER_STATS: options.serverStatsFile,
    BENCH_SENDER_SESSIONS: options.senderSessions.join(','),
  };
}

export async function startLoadPortal(
  dataDirectory: string,
  shared: SharedLoadConfig,
  options: LoadPortalOptions,
): Promise<PortalHandle> {
  return startPortal({
    entry: PORTAL_ENTRY,
    dataDirectory,
    requestTimeoutMs: shared.requestTimeoutMs,
    readyTimeoutMs: READY_TIMEOUT_MS,
    extraEnv: loadPortalExtraEnv(options),
  });
}

export interface GeneratorPlan {
  readonly index: number;
  readonly clients: number;
  /** Clients of this generator that send; always its first `senders`. */
  readonly senders: number;
  readonly offset: number;
  readonly resultFile: string;
}

export function planGenerators(
  config: MainConfig,
  resultsDir: string,
): readonly GeneratorPlan[] {
  const base = Math.floor(config.clients / config.generators);
  const extra = config.clients % config.generators;
  const plans: GeneratorPlan[] = [];
  let offset = 0;
  for (let index = 0; index < config.generators; index += 1) {
    const clients = base + (index < extra ? 1 : 0);
    plans.push({
      index,
      clients,
      senders: senderClientsFor(offset, clients, config.senders),
      offset,
      resultFile: join(resultsDir, `generator-${index}.json`),
    });
    offset += clients;
  }
  return plans;
}

/** Session ids of the sender clients, in global client order. */
export function senderSessionIds(
  plans: readonly GeneratorPlan[],
): readonly string[] {
  const sessions: string[] = [];
  for (const plan of plans) {
    for (let index = 0; index < plan.senders; index += 1) {
      sessions.push(clientSessionId(plan.index, index));
    }
  }
  return sessions;
}

export interface GeneratorHandle {
  readonly plan: GeneratorPlan;
  readonly child: GeneratorProcess;
  readonly ready: Promise<void>;
  readonly done: Promise<void>;
  readonly exited: Promise<number>;
  readonly status: { exited: boolean; exitCode: number | undefined };
}

export function spawnGenerator(
  plan: GeneratorPlan,
  url: string,
  shared: SharedLoadConfig,
): GeneratorHandle {
  const child = Bun.spawn({
    cmd: [process.execPath, 'run', CLIENT_ENTRY],
    stdin: 'pipe',
    stdout: 'pipe',
    // Generator diagnostics stay visible without a second pump.
    stderr: 'inherit',
    env: {
      ...process.env,
      LOAD_URL: url,
      LOAD_ROOM_SIZE: String(shared.roomSize),
      LOAD_DURATION_SEC: String(shared.durationSec),
      LOAD_WARMUP_SEC: String(shared.warmupSec),
      LOAD_THINK_MS: String(shared.thinkMs),
      LOAD_MODE: shared.mode,
      LOAD_MIX: mixText(shared.mix),
      LOAD_RUN_TAG: shared.runTag,
      REQUEST_TIMEOUT_MS: String(shared.requestTimeoutMs),
      LOAD_GENERATOR_INDEX: String(plan.index),
      LOAD_GENERATOR_CLIENTS: String(plan.clients),
      LOAD_GENERATOR_SENDERS: String(plan.senders),
      LOAD_GENERATOR_OFFSET: String(plan.offset),
      LOAD_GENERATOR_RESULT: plan.resultFile,
    },
  });

  const ready = Promise.withResolvers<void>();
  const done = Promise.withResolvers<void>();
  const status: { exited: boolean; exitCode: number | undefined } = {
    exited: false,
    exitCode: undefined,
  };
  const extraLines: string[] = [];
  let sawDone = false;
  const stdoutClosed = pumpLines(child.stdout, (line) => {
    const trimmed = line.trim();
    if (trimmed === 'ready') {
      ready.resolve();
      return;
    }
    if (trimmed === 'done') {
      sawDone = true;
      done.resolve();
      return;
    }
    if (trimmed.length > 0) {
      extraLines.push(trimmed);
      if (extraLines.length > 5) {
        extraLines.shift();
      }
    }
  }).catch((error: unknown) => {
    const wrapped = new Error(
      `generator ${plan.index} stdout pump failed: ${messageOf(error)}`,
    );
    ready.reject(wrapped);
    done.reject(wrapped);
  });
  // A crash during setup would otherwise surface as an unhandled rejection
  // because `ready` and `done` are awaited at different points.
  void ready.promise.catch(() => undefined);
  void done.promise.catch(() => undefined);
  // Wait for stdout to close before deciding that `done` is missing: the
  // child may have written it just before exiting, and its exit event can be
  // observed before the pump has drained the final chunk.
  void Promise.all([child.exited, stdoutClosed]).then(([code]) => {
    status.exited = true;
    status.exitCode = code;
    if (code === 0 && sawDone) {
      return undefined;
    }
    const detail =
      extraLines.length === 0 ? '' : `; stdout: ${extraLines.join(' | ')}`;
    const error =
      code === 0
        ? new Error(
            `generator ${plan.index} exited without the done signal${detail}`,
          )
        : new Error(
            `generator ${plan.index} exited with code ${code}${detail}`,
          );
    ready.reject(error);
    done.reject(error);
    return undefined;
  });

  return {
    plan,
    child,
    ready: ready.promise,
    done: done.promise,
    exited: child.exited,
    status,
  };
}

export async function waitForReady(
  generators: readonly GeneratorHandle[],
): Promise<void> {
  await Promise.all(
    generators.map((handle) =>
      withDeadline(
        handle.ready,
        SETUP_TIMEOUT_MS,
        `generator ${handle.plan.index} setup`,
      ),
    ),
  );
}

export async function sendGo(
  generators: readonly GeneratorHandle[],
): Promise<number> {
  // One timestamp for every generator: deriving the window from each process's
  // local stdin receipt would offset windows by their scheduling delay.
  const measurementStartMs = Date.now();
  await Promise.all(
    generators.map(async (handle) => {
      await handle.child.stdin.write(`go ${measurementStartMs}\n`);
      await handle.child.stdin.end();
    }),
  );
  return measurementStartMs;
}

export async function collectResults(
  generators: readonly GeneratorHandle[],
  shared: SharedLoadConfig,
): Promise<readonly LoadResultJson[]> {
  const deadlineMs =
    (shared.warmupSec + shared.durationSec) * 1000 + MEASUREMENT_SLACK_MS;
  return Promise.all(
    generators.map(async (handle) => {
      await withDeadline(
        handle.done,
        deadlineMs,
        `generator ${handle.plan.index} to finish its measurement`,
      );
      const exitCode = await withDeadline(
        handle.exited,
        GENERATOR_EXIT_TIMEOUT_MS,
        `generator ${handle.plan.index} to exit`,
      );
      if (exitCode !== 0) {
        throw new Error(
          `generator ${handle.plan.index} exited with code ${exitCode}`,
        );
      }
      const text = readFileSync(handle.plan.resultFile, 'utf8');
      let parsed: unknown;
      try {
        parsed = JSON.parse(text);
      } catch (error) {
        throw new Error(
          `generator ${handle.plan.index} result is not JSON: ${messageOf(error)}`,
          { cause: error },
        );
      }
      const result = parseLoadResultJson(parsed);
      if (
        result.generator !== handle.plan.index ||
        result.clients !== handle.plan.clients ||
        result.senders !== handle.plan.senders
      ) {
        throw new Error(
          `generator ${handle.plan.index} result says generator=${result.generator} clients=${result.clients} senders=${result.senders}`,
        );
      }
      return result;
    }),
  );
}

async function waitForExit(
  handle: GeneratorHandle,
  timeoutMs: number,
): Promise<void> {
  if (handle.status.exited) {
    return;
  }
  await withDeadline(
    handle.exited.then(() => undefined),
    timeoutMs,
    `generator ${handle.plan.index} to exit`,
  ).catch(() => undefined);
}

export async function stopGenerators(
  generators: readonly GeneratorHandle[],
): Promise<void> {
  const pending = generators.filter((handle) => !handle.status.exited);
  if (pending.length > 0) {
    write(
      `cleanup: stopping ${pending.length} generator process(es) with SIGTERM`,
    );
    for (const handle of pending) {
      try {
        handle.child.kill('SIGTERM');
      } catch {
        // Already gone between the status check and the signal.
      }
    }
    await Promise.all(
      pending.map((handle) => waitForExit(handle, GENERATOR_TERM_WAIT_MS)),
    );
    const survivors = pending.filter((handle) => !handle.status.exited);
    for (const handle of survivors) {
      try {
        handle.child.kill('SIGKILL');
      } catch {
        // Already gone.
      }
    }
    if (survivors.length > 0) {
      write(
        `cleanup: SIGKILLed ${survivors.length} generator process(es) after the SIGTERM wait`,
      );
      await Promise.all(
        survivors.map((handle) => waitForExit(handle, GENERATOR_KILL_WAIT_MS)),
      );
      const leftovers = survivors.filter((handle) => !handle.status.exited);
      if (leftovers.length > 0) {
        writeError(
          `load benchmark cleanup failed: generator pid(s) ${leftovers
            .map((handle) => handle.child.pid)
            .join(', ')} did not exit`,
        );
        process.exitCode = 1;
      }
    }
  }
  const exited = generators.filter((handle) => handle.status.exited).length;
  write(
    `cleanup: generators ${generators.length === 0 ? 'none' : `${exited}/${generators.length} exited`}`,
  );
}

export interface PortalStopSummary {
  /** The stop path did not confirm a clean exit (throw, non-zero, no exit). */
  readonly stopFailed: boolean;
  /** Server pids still alive after the bounded stop wait. */
  readonly survivors: readonly number[];
}

export async function stopPortal(
  portal: PortalHandle | undefined,
  externalUrl: boolean,
): Promise<PortalStopSummary> {
  if (portal === undefined) {
    write(
      `cleanup: portal not spawned${externalUrl ? ' (LOAD_URL reuses an external Portal)' : ''}`,
    );
    // A spawn path that never produced a Portal cannot confirm any exit.
    return { stopFailed: !externalUrl, survivors: [] };
  }
  try {
    const result = await portal.stop();
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
    const stopFailed = portalStopFailed(result);
    if (stopFailed) {
      const exitNote =
        result.exitCode === undefined ? ' (exit not confirmed)' : '';
      writeError(
        `load benchmark cleanup failed: ${state}; portal exit ${exit}${exitNote}`,
      );
      process.exitCode = 1;
    }
    return { stopFailed, survivors: result.survivors };
  } catch (error) {
    writeError(
      `load benchmark cleanup failed: could not stop the Portal: ${messageOf(error)}`,
    );
    process.exitCode = 1;
    return { stopFailed: true, survivors: [] };
  }
}
