import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  isProcessAlive,
  sleep,
  withDeadline,
} from 'mud-framework-test-support';

import { GameClient } from '../../examples/game-slice/src/client-lib.ts';
import type { GameEvent } from '../../examples/game-slice/src/client-lib.ts';
import { messageOf } from '../../packages/core/src/shared/validation.ts';
import { environmentLine } from '../environment.ts';
import { portalStopFailed, shouldRemoveDataDir } from './cleanup.ts';
import {
  envInt,
  envIntList,
  integerText,
  latencyStats,
  latencyText,
  millisecondsText,
  startPortal,
  throughputText,
  write,
  writeError,
} from './support.ts';
import type { LatencyStats, PortalHandle } from './support.ts';

/**
 * Hot-path and idle-recovery measurements over the real game-slice assembly:
 * this process spawns `examples/game-slice/src/portal.ts`, which spawns the
 * Server, so every number includes the WebSocket hop, the Portal's routing, the
 * IPC hop and the SQLite transaction. Nothing asserts a threshold; performance
 * never fails the run. A scenario's own wire failure still does, because then
 * the benchmark did not measure what it claims to.
 */

const PORTAL_ENTRY = join(
  import.meta.dir,
  '..',
  '..',
  'examples',
  'game-slice',
  'src',
  'portal.ts',
);
const DEFAULT_REQUESTS = 500;
const DEFAULT_WARMUP = 100;
const DEFAULT_CLIENTS = [10, 50];
const DEFAULT_PER_CLIENT = 100;
/** Larger than a framework default: restart polling must not race this value. */
const DEFAULT_REQUEST_TIMEOUT_MS = 20_000;
const READY_TIMEOUT_MS = 10_000;
const RESTART_POLL_INTERVAL_MS = 25;
const RESTART_TIMEOUT_MS = 30_000;

/**
 * Names must stay unique across runs: a reused Portal may still hold rows from
 * an earlier run, and the move scenario depends on a fresh player's start room.
 */
const RUN_TAG = `${process.pid.toString(36)}${Date.now().toString(36).slice(-6)}`;

function emptyToUndefined(value: string | undefined): string | undefined {
  return value !== undefined && value.length > 0 ? value : undefined;
}

function readConfig() {
  return {
    requests: envInt('BENCH_REQUESTS', DEFAULT_REQUESTS),
    warmup: envInt('BENCH_WARMUP', DEFAULT_WARMUP),
    clients: envIntList('BENCH_CLIENTS', DEFAULT_CLIENTS),
    perClient: envInt('BENCH_PER_CLIENT', DEFAULT_PER_CLIENT),
    requestTimeoutMs: envInt('REQUEST_TIMEOUT_MS', DEFAULT_REQUEST_TIMEOUT_MS),
    dataDirectory: emptyToUndefined(process.env.BENCH_DATA_DIR),
    keep: process.env.BENCH_KEEP === '1',
    externalUrl: emptyToUndefined(
      process.env.BENCH_URL ?? process.env.GAME_SLICE_URL,
    ),
  };
}

type Config = ReturnType<typeof readConfig>;

function writeSection(heading: string, ...lines: readonly string[]): void {
  write(`\n${heading}\n  ${lines.join('\n  ')}`);
}

/**
 * Sequential by design: each client has one request in flight, which is what
 * makes the concurrent runs measure server-side queueing rather than client
 * pipelining.
 */
async function runCalls<T>(
  count: number,
  action: (index: number) => Promise<T>,
): Promise<T[]> {
  const results: T[] = [];
  for (let index = 0; index < count; index += 1) {
    // oxlint-disable-next-line eslint/no-await-in-loop -- one in-flight request per client is the measured model
    results.push(await action(index));
  }
  return results;
}

async function timeCall(action: () => Promise<unknown>): Promise<number> {
  const started = Bun.nanoseconds();
  await action();
  return Bun.nanoseconds() - started;
}

/**
 * Connects and logs in the named clients, then logs each out before closing:
 * a bare close would leave the player a broadcast target that drops events.
 */
async function withClients<T>(
  url: string,
  names: readonly string[],
  timeoutMs: number,
  action: (clients: readonly GameClient[]) => Promise<T>,
): Promise<T> {
  const clients: GameClient[] = [];
  try {
    for (const name of names) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- logins are one write at a time on the Server
      const client = await GameClient.connect({ url, timeoutMs });
      clients.push(client);
      try {
        // oxlint-disable-next-line eslint/no-await-in-loop -- the login belongs to the client just connected
        await client.call('game.login', { name });
      } catch (error) {
        // oxlint-disable-next-line eslint/no-await-in-loop -- close before surfacing the failed login
        await client.close();
        throw error;
      }
    }
    return await action(clients);
  } finally {
    for (const client of clients) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- bounded per client, in the order they opened
      await withDeadline(client.call('game.logout'), 1000, 'logout').catch(
        () => undefined,
      );
      // oxlint-disable-next-line eslint/no-await-in-loop -- same order as the logouts
      await client.close();
    }
  }
}

/**
 * One client is the sequential scenario; several are the concurrency levels.
 * The measurement shape is the same (per-client sequential, clients parallel),
 * so both report through one path.
 */
async function runLook(url: string, config: Config, clientCount: number) {
  const perClient = clientCount === 1 ? config.requests : config.perClient;
  const names = Array.from(
    { length: clientCount },
    (_, index) => `bench-look-${clientCount}-${RUN_TAG}-${index}`,
  );

  return withClients(url, names, config.requestTimeoutMs, async (clients) => {
    await Promise.all(
      clients.map((client) =>
        runCalls(config.warmup, () => client.call('game.look')),
      ),
    );

    const buckets: number[][] = clients.map(() => []);
    const started = Bun.nanoseconds();
    await Promise.all(
      clients.map((client, index) =>
        runCalls(perClient, async () => {
          buckets[index]?.push(await timeCall(() => client.call('game.look')));
        }),
      ),
    );
    const wallNs = Bun.nanoseconds() - started;

    const total = clientCount * perClient;
    const stats = latencyStats(buckets.flat());
    const heading =
      clientCount === 1
        ? 'look, 1 client, sequential (per-call latency):'
        : `look, ${clientCount} clients x ${perClient} calls (each client sequential, clients concurrent):`;
    const note =
      clientCount === 1
        ? ''
        : '; note: the single Server and its single SQLite connection serialize the requests, so each call waits behind the others';
    writeSection(
      heading,
      `calls ${integerText(total)}, warmup ${config.warmup}/client, wall ${(wallNs / 1e9).toFixed(2)} s, ${throughputText(total, wallNs)}, latency ms: ${latencyText(stats)}${note}`,
    );
    return { clients: clientCount, stats, wallNs };
  });
}

async function runMove(url: string, config: Config): Promise<LatencyStats> {
  const names = [`bench-move-${RUN_TAG}`, `bench-watch-${RUN_TAG}`];
  return withClients(url, names, config.requestTimeoutMs, async (clients) => {
    const [mover, observer] = clients;
    if (mover === undefined || observer === undefined) {
      throw new Error('move scenario needs two clients');
    }

    // One real recipient in the market: north delivers room.entered there and
    // south delivers room.left, so every move pays for event delivery. The
    // listener drains them instead of letting the client queue grow.
    await observer.call('game.move', { direction: 'north' });
    observer.onEvent(() => undefined);
    await runCalls(config.warmup * 2, (index) =>
      mover.call('game.move', {
        direction: index % 2 === 0 ? 'north' : 'south',
      }),
    );

    const started = Bun.nanoseconds();
    const samples = await runCalls(config.requests * 2, (index) =>
      timeCall(() =>
        mover.call('game.move', {
          direction: index % 2 === 0 ? 'north' : 'south',
        }),
      ),
    );
    const elapsedNs = Bun.nanoseconds() - started;

    const moves = config.requests * 2;
    const stats = latencyStats(samples);
    writeSection(
      `move, 1 client, ${integerText(config.requests)} round trips square<->market (write + room broadcast):`,
      `moves ${integerText(moves)}, warmup ${config.warmup * 2}, wall ${(elapsedNs / 1e9).toFixed(2)} s, ${throughputText(moves, elapsedNs)}, latency ms: ${latencyText(stats)}`,
    );
    return stats;
  });
}

function eventText(event: GameEvent): string | undefined {
  const payload = event.payload;
  if (
    typeof payload !== 'object' ||
    payload === null ||
    !('text' in payload) ||
    typeof payload.text !== 'string'
  ) {
    return undefined;
  }
  return payload.text;
}

/**
 * The event time comes from the delivery callback, not from awaiting the
 * sender's response: the two connections have no ordering guarantee.
 */
async function sayRound(
  sender: GameClient,
  receiver: GameClient,
  text: string,
  timeoutMs: number,
): Promise<{ responseNs: number; eventNs: number }> {
  const arrival = Promise.withResolvers<number>();
  const unsubscribe = receiver.onEvent((event) => {
    if (event.event === 'room.say' && eventText(event) === text) {
      arrival.resolve(Bun.nanoseconds());
    }
  });
  try {
    const started = Bun.nanoseconds();
    await sender.call('game.say', { text });
    const responseNs = Bun.nanoseconds() - started;
    const arrivedAt = await withDeadline(
      arrival.promise,
      timeoutMs,
      'room.say delivery at the receiving client',
    );
    return { responseNs, eventNs: arrivedAt - started };
  } finally {
    unsubscribe();
  }
}

async function runSay(url: string, config: Config) {
  const names = [`bench-say-a-${RUN_TAG}`, `bench-say-b-${RUN_TAG}`];
  return withClients(url, names, config.requestTimeoutMs, async (clients) => {
    const [sender, receiver] = clients;
    if (sender === undefined || receiver === undefined) {
      throw new Error('say scenario needs two clients');
    }

    const send = (text: string) =>
      sayRound(sender, receiver, text, config.requestTimeoutMs);
    await runCalls(config.warmup, (index) => send(`bench-warmup-${index}`));
    const rounds = await runCalls(config.requests, (index) =>
      send(`bench-say-${index}`),
    );

    const response = latencyStats(rounds.map((round) => round.responseNs));
    const event = latencyStats(rounds.map((round) => round.eventNs));
    writeSection(
      `say + event, 2 clients in the same room (${integerText(config.requests)} rounds, ${config.warmup} warmup):`,
      `A response ms: ${latencyText(response)}; B send->room.say ms: ${latencyText(event)}; note: both clocks start just before A issues the call, and the receiver arrival is stamped on delivery`,
    );
    return { response, event };
  });
}

async function restartOnce(pid: number, client: GameClient) {
  const heading = 'server restart recovery (idle):';

  // Warm the session binding: recovery measures a request on an established
  // connection, not a fresh login.
  await client.call('game.look');
  const killedAt = Bun.nanoseconds();
  try {
    process.kill(pid, 'SIGKILL');
  } catch (error) {
    // The pid disappeared between the liveness check and the signal.
    writeSection(
      heading,
      `skipped: could not SIGKILL Server pid ${pid}: ${messageOf(error)}`,
    );
    return { recoveryNs: undefined, attempts: 0 };
  }

  let attempts = 0;
  let recoveryNs: number | undefined;
  while (
    recoveryNs === undefined &&
    Bun.nanoseconds() - killedAt < RESTART_TIMEOUT_MS * 1e6
  ) {
    attempts += 1;
    try {
      // oxlint-disable-next-line eslint/no-await-in-loop -- polling is the measurement
      await client.call('game.look');
      recoveryNs = Bun.nanoseconds() - killedAt;
    } catch {
      // The Server is restarting; the next poll is the measurement.
      // oxlint-disable-next-line eslint/no-await-in-loop -- bounded poll interval between attempts
      await sleep(RESTART_POLL_INTERVAL_MS);
    }
  }

  const outcome =
    recoveryNs === undefined
      ? `no successful look within ${RESTART_TIMEOUT_MS} ms; recovery not observed`
      : `first successful look ${millisecondsText(recoveryNs)} ms after kill, ${integerText(attempts)} ${attempts === 1 ? 'attempt' : 'attempts'}`;
  writeSection(
    heading,
    `killed Server pid ${pid} with SIGKILL, polled game.look every ${RESTART_POLL_INTERVAL_MS} ms; ${outcome}`,
    `killed pid ${pid}: ${isProcessAlive(pid) ? 'still alive' : 'gone'}`,
    'note: only idle recovery is measured; in-flight replay is covered by tests/integration, not by this benchmark',
  );
  return { recoveryNs, attempts };
}

async function runRestart(portal: PortalHandle, url: string, config: Config) {
  const pid = portal.liveServerPid();
  if (pid === undefined) {
    writeSection(
      'server restart recovery (idle):',
      'skipped: no Server pid in the Portal log',
    );
    return undefined;
  }

  return withClients(
    url,
    [`bench-restart-${RUN_TAG}`],
    config.requestTimeoutMs,
    async (clients) => {
      const client = clients[0];
      if (client === undefined) {
        throw new Error('restart scenario needs a client');
      }
      return restartOnce(pid, client);
    },
  );
}

async function cleanup(
  portal: PortalHandle | undefined,
  dataDirectory: string | undefined,
  config: Config,
): Promise<void> {
  // A spawn path that never produced a Portal cannot confirm any exit; an
  // external URL has no `dataDirectory` and never reaches the removal below.
  let stopFailed = portal === undefined && dataDirectory !== undefined;
  let survivors: readonly number[] = [];
  if (portal !== undefined) {
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
      survivors = result.survivors;
      stopFailed = portalStopFailed(result);
      if (stopFailed) {
        const exitNote =
          result.exitCode === undefined ? ' (exit not confirmed)' : '';
        writeError(
          `game benchmark cleanup failed: ${state}; portal exit ${exit}${exitNote}`,
        );
        process.exitCode = 1;
      }
    } catch (error) {
      stopFailed = true;
      writeError(
        `game benchmark cleanup failed: could not stop the Portal: ${messageOf(error)}`,
      );
      process.exitCode = 1;
    }
  }

  if (dataDirectory === undefined) {
    return;
  }
  const keep = config.dataDirectory !== undefined || config.keep;
  if (!shouldRemoveDataDir({ stopFailed, survivors, keep })) {
    const why =
      config.dataDirectory !== undefined
        ? 'BENCH_DATA_DIR'
        : config.keep
          ? 'BENCH_KEEP=1'
          : stopFailed
            ? 'Portal stop was not graceful or not confirmed'
            : `Server pid(s) still alive: ${survivors.join(', ')}`;
    write(`cleanup: kept data dir ${dataDirectory} (${why})`);
    return;
  }
  try {
    rmSync(dataDirectory, { recursive: true, force: true });
    write(`cleanup: removed data dir ${dataDirectory}`);
  } catch (error) {
    writeError(
      `game benchmark cleanup failed: could not remove ${dataDirectory}: ${messageOf(error)}`,
    );
    process.exitCode = 1;
  }
}

async function main(): Promise<void> {
  const config = readConfig();

  write('mud-framework game hot-path benchmark');
  write(environmentLine());
  write(
    `config: requests=${integerText(config.requests)} warmup=${config.warmup} clients=${config.clients.join(',')} per-client=${config.perClient} request-timeout=${config.requestTimeoutMs}ms`,
  );

  let dataDirectory: string | undefined;
  let portal: PortalHandle | undefined;
  let summary: string | undefined;
  try {
    let url: string;
    if (config.externalUrl !== undefined) {
      url = config.externalUrl;
      write('data dir: not used (external Portal)');
      write(
        `portal: reusing ${url}; spawn, Server kill and restart recovery are skipped`,
      );
    } else {
      dataDirectory =
        config.dataDirectory ?? mkdtempSync(join(tmpdir(), 'mud-bench-game-'));
      const kept =
        config.dataDirectory !== undefined
          ? ' (BENCH_DATA_DIR; never removed by the benchmark)'
          : config.keep
            ? ' (kept: BENCH_KEEP=1)'
            : '';
      write(`data dir: ${dataDirectory}${kept}`);
      portal = await startPortal({
        entry: PORTAL_ENTRY,
        dataDirectory,
        requestTimeoutMs: config.requestTimeoutMs,
        readyTimeoutMs: READY_TIMEOUT_MS,
      });
      url = portal.url;
      write(`portal: spawned pid ${portal.pid}, listening on ${url}`);
    }

    const sequence = await runLook(url, config, 1);
    const concurrent: Awaited<ReturnType<typeof runLook>>[] = [];
    for (const clientCount of config.clients) {
      // 1 client repeats the sequential section; skip it rather than print twice.
      if (clientCount === 1) {
        continue;
      }
      // oxlint-disable-next-line eslint/no-await-in-loop -- levels run one after another so their latencies do not mix
      concurrent.push(await runLook(url, config, clientCount));
    }
    const move = await runMove(url, config);
    const say = await runSay(url, config);
    const restart =
      portal === undefined ? undefined : await runRestart(portal, url, config);
    if (portal === undefined) {
      writeSection(
        'server restart recovery (idle):',
        'skipped: the benchmark reuses an external Portal and does not kill its Server',
      );
    }

    const restartText =
      portal === undefined
        ? 'restart recovery skipped (external portal)'
        : restart?.recoveryNs === undefined
          ? 'restart recovery not observed'
          : `restart recovery ${millisecondsText(restart.recoveryNs)} ms (${integerText(restart.attempts)} ${restart.attempts === 1 ? 'attempt' : 'attempts'})`;
    const parts = [
      `look p50 ${millisecondsText(sequence.stats.p50Ns)} ms`,
      ...concurrent.map(
        (run) =>
          `look@${run.clients} p50 ${millisecondsText(run.stats.p50Ns)} ms (${throughputText(run.clients * config.perClient, run.wallNs)})`,
      ),
      `move p50 ${millisecondsText(move.p50Ns)} ms`,
      `say A p50 ${millisecondsText(say.response.p50Ns)} ms / B event p50 ${millisecondsText(say.event.p50Ns)} ms`,
      restartText,
    ];
    summary = `summary: ${parts.join('; ')}; numbers are observations, not assertions`;
  } finally {
    await cleanup(portal, dataDirectory, config);
  }

  if (summary !== undefined) {
    write(summary);
  }
}

await main().catch((error: unknown) => {
  writeError(`game benchmark failed: ${messageOf(error)}`);
  process.exitCode = 1;
});

if (process.exitCode !== undefined && process.exitCode !== 0) {
  // oxlint-disable-next-line unicorn/no-process-exit -- a failed run must not hang on a handle it cannot close
  process.exit(1);
}
