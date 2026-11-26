import { mkdtempSync } from 'node:fs';
import { cpus, release, tmpdir } from 'node:os';
import { join } from 'node:path';

import {
  GameClient,
  GameClientError,
} from '../../../examples/game-slice/src/client-lib.ts';
import type { GameEvent } from '../../../examples/game-slice/src/client-lib.ts';

import {
  integerText,
  sleep,
  withDeadline,
  write,
  writeError,
} from '../support.ts';
import {
  BASELINE_MS,
  WINDOW_MARGIN_MS,
  cleanupSwitchRun,
  phaseAt,
  phaseLine,
  readSwitchConfig,
  requestSwitch,
  startSwitchPortal,
  summarizePhase,
} from './support.ts';
import type {
  Phase,
  PhaseSummary,
  RequestSample,
  SwitchConfig,
  SwitchMarkers,
  SwitchPortal,
  SwitchReply,
} from './support.ts';

/**
 * Observes one live `switchTo` over the real Portal + Server + WebSocket
 * assembly. Clients keep issuing `game.look` at a fixed interval while the
 * coordinator triggers the switch through the benchmark Portal's control
 * endpoint, so every number is a real request's wait: WebSocket, Portal
 * routing, the switch's unacknowledged buffer, the IPC hop to the new Server
 * and its SQLite read. No threshold is asserted; a wire or assembly failure
 * still fails the run because it would mean the benchmark did not measure what
 * it claims to.
 */

const PORTAL_ENTRY = join(import.meta.dir, 'portal.ts');
const RUN_TAG = `${process.pid.toString(36)}${Date.now().toString(36).slice(-6)}`;
const EVENT_DRAIN_MS = 1000;
const LOGOUT_TIMEOUT_MS = 1000;
const EVENT_PREFIX = 'switchevent';

function environmentLine(): string {
  const cpu = cpus()[0]?.model ?? 'unknown CPU';
  return `Bun ${process.versions.bun ?? 'unknown'}  ${process.platform} ${process.arch}  kernel ${release()}  ${cpu}`;
}

function writeSection(heading: string, ...lines: readonly string[]): void {
  write(`\n${heading}\n  ${lines.join('\n  ')}`);
}

function errorCodeOf(error: unknown): string {
  return error instanceof GameClientError ? error.code : 'UNKNOWN';
}

function closeCode(code: string | undefined): boolean {
  return code === 'CLIENT_CLOSED' || code === 'CLIENT_SEND_FAILED';
}

async function connectAndLogin(
  url: string,
  name: string,
  timeoutMs: number,
): Promise<GameClient> {
  const client = await GameClient.connect({ url, timeoutMs });
  try {
    await client.call('game.login', { name });
  } catch (error) {
    await client.close();
    throw error;
  }
  return client;
}

async function closeClients(clients: readonly GameClient[]): Promise<void> {
  for (const client of clients) {
    // oxlint-disable-next-line eslint/no-await-in-loop -- bounded per client, in the order they opened
    await withDeadline(client.call('game.logout'), LOGOUT_TIMEOUT_MS, 'logout')
      .catch(() => undefined)
      .then(() => client.close());
  }
}

interface ClientRun {
  readonly index: number;
  readonly requests: number;
  readonly disconnected: boolean;
}

/**
 * Fixed start-to-start interval: a slow request does not add its latency to
 * the next gap, so the offered rate stays constant through the switch.
 */
async function runLookClient(
  client: GameClient,
  index: number,
  intervalMs: number,
  endNs: number,
  samples: RequestSample[],
): Promise<ClientRun> {
  let requests = 0;
  let disconnected = false;
  while (Bun.nanoseconds() < endNs) {
    const startNs = Bun.nanoseconds();
    try {
      // oxlint-disable-next-line eslint/no-await-in-loop -- one in-flight request per client is the measured model
      await client.call('game.look');
      samples.push({
        client: index,
        startNs,
        endNs: Bun.nanoseconds(),
        ok: true,
        code: undefined,
      });
    } catch (error) {
      const code = errorCodeOf(error);
      samples.push({
        client: index,
        startNs,
        endNs: Bun.nanoseconds(),
        ok: false,
        code,
      });
      if (closeCode(code)) {
        disconnected = true;
        break;
      }
    }
    requests += 1;
    const waitNs = startNs + intervalMs * 1e6 - Bun.nanoseconds();
    if (waitNs > 0) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- the interval is the measurement
      await sleep(waitNs / 1e6);
    }
  }
  return { index, requests, disconnected };
}

function emptyPhases(): Record<Phase, number> {
  return { warmup: 0, baseline: 0, window: 0, recovery: 0 };
}

interface EventProbeResult {
  readonly attempted: number;
  readonly sent: number;
  readonly failed: number;
  readonly received: number;
  readonly gaps: number;
  readonly duplicates: number;
  readonly gapsByPhase: Record<Phase, number>;
  readonly duplicatesByPhase: Record<Phase, number>;
  readonly senderDisconnected: boolean;
}

function eventText(payload: unknown): string | undefined {
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
 * A two-client `say` probe. A sequence number advances only when the Server
 * accepts the say, so rejected or timed-out attempts are retried under the same
 * number; the receiver sees a contiguous run unless an accepted say's event was
 * lost. A timed-out attempt may still have been broadcast, so its retry can
 * arrive twice under one number; that is counted as a duplicate, not as loss.
 */
async function runEventProbe(
  sender: GameClient,
  receiver: GameClient,
  markers: SwitchMarkers,
  intervalMs: number,
): Promise<EventProbeResult> {
  const result = {
    attempted: 0,
    sent: 0,
    failed: 0,
    received: 0,
    gaps: 0,
    duplicates: 0,
    gapsByPhase: emptyPhases(),
    duplicatesByPhase: emptyPhases(),
    senderDisconnected: false,
  };
  let lastSeq = -1;
  let nextSeq = 0;
  const prefix = `${EVENT_PREFIX}|`;
  const unsubscribe = receiver.onEvent((event: GameEvent) => {
    if (event.event !== 'room.say') {
      return;
    }
    const text = eventText(event.payload);
    if (text === undefined || !text.startsWith(prefix)) {
      return;
    }
    const seq = Number(text.slice(prefix.length));
    if (!Number.isInteger(seq) || seq < 0) {
      return;
    }
    const arrivedAt = Bun.nanoseconds();
    const phase = phaseAt(arrivedAt, markers);
    result.received += 1;
    if (seq > lastSeq + 1) {
      const gap = seq - lastSeq - 1;
      result.gaps += gap;
      result.gapsByPhase[phase] += gap;
    } else if (seq <= lastSeq) {
      result.duplicates += 1;
      result.duplicatesByPhase[phase] += 1;
    }
    if (seq > lastSeq) {
      lastSeq = seq;
    }
  });

  try {
    while (Bun.nanoseconds() < markers.endNs) {
      const startNs = Bun.nanoseconds();
      const seq = nextSeq;
      result.attempted += 1;
      try {
        // oxlint-disable-next-line eslint/no-await-in-loop -- one in-flight say per sender is the measured model
        await sender.call('game.say', { text: `${prefix}${seq}` });
        nextSeq += 1;
        result.sent += 1;
      } catch (error) {
        result.failed += 1;
        if (closeCode(errorCodeOf(error))) {
          result.senderDisconnected = true;
          break;
        }
      }
      const waitNs = startNs + intervalMs * 1e6 - Bun.nanoseconds();
      if (waitNs > 0) {
        // oxlint-disable-next-line eslint/no-await-in-loop -- the interval is the measurement
        await sleep(waitNs / 1e6);
      }
    }
    // Accepted event frames may still be in flight when the sender stops.
    await sleep(EVENT_DRAIN_MS);
  } finally {
    unsubscribe();
  }
  return result;
}

interface ScenarioResult {
  readonly samples: readonly RequestSample[];
  readonly markers: SwitchMarkers;
  readonly clientCount: number;
  readonly lookRuns: readonly ClientRun[];
  readonly events: EventProbeResult | undefined;
  readonly reply: SwitchReply;
  readonly controlRoundtripNs: number;
}

function sleepUntil(ns: number): Promise<void> {
  const remaining = ns - Bun.nanoseconds();
  return remaining > 0 ? sleep(remaining / 1e6) : Promise.resolve();
}

async function runScenario(
  url: string,
  controlUrl: string,
  config: SwitchConfig,
): Promise<ScenarioResult> {
  const lookNames = Array.from(
    { length: config.clients },
    (_, index) => `sw-${RUN_TAG}-l${index}`,
  );
  const lookClients: GameClient[] = [];
  let sender: GameClient | undefined;
  let receiver: GameClient | undefined;
  try {
    for (const name of lookNames) {
      // oxlint-disable-next-line eslint/no-await-in-loop -- logins are one write at a time on the Server
      const client = await connectAndLogin(url, name, config.requestTimeoutMs);
      lookClients.push(client);
    }
    // The event probe's room is the same square, so these clients also receive
    // room.say. Drain instead of letting each client's event queue fill.
    for (const client of lookClients) {
      client.onEvent(() => undefined);
    }
    if (config.events) {
      receiver = await connectAndLogin(
        url,
        `sw-${RUN_TAG}-r`,
        config.requestTimeoutMs,
      );
      sender = await connectAndLogin(
        url,
        `sw-${RUN_TAG}-s`,
        config.requestTimeoutMs,
      );
    }

    const runStartNs = Bun.nanoseconds();
    const markers: SwitchMarkers = {
      measureStartNs: runStartNs + config.warmupSec * 1e9,
      endNs: runStartNs + config.durationSec * 1e9,
      triggerNs: undefined,
      resolvedNs: undefined,
      windowEndNs: undefined,
    };
    const samples: RequestSample[] = [];
    const lookRuns = Promise.all(
      lookClients.map((client, index) =>
        runLookClient(client, index, config.intervalMs, markers.endNs, samples),
      ),
    );
    const eventProbe =
      sender === undefined || receiver === undefined
        ? undefined
        : runEventProbe(sender, receiver, markers, config.intervalMs);

    await sleepUntil(markers.measureStartNs);
    const measurementNs = markers.endNs - markers.measureStartNs;
    const baselineNs = Math.min(BASELINE_MS * 1e6, measurementNs / 3);
    await sleepUntil(markers.measureStartNs + baselineNs);

    markers.triggerNs = Bun.nanoseconds();
    const roundtripStarted = Bun.nanoseconds();
    let reply: SwitchReply;
    try {
      reply = await requestSwitch(controlUrl);
    } catch (error) {
      reply = {
        ok: false,
        durationNs: 0,
        error: error instanceof Error ? error.message : String(error),
      };
    }
    const controlRoundtripNs = Bun.nanoseconds() - roundtripStarted;
    markers.resolvedNs = Bun.nanoseconds();
    markers.windowEndNs = markers.resolvedNs + WINDOW_MARGIN_MS * 1e6;

    await sleepUntil(markers.endNs);
    const runs = await lookRuns;
    const events = eventProbe === undefined ? undefined : await eventProbe;
    return {
      samples,
      markers,
      clientCount: lookClients.length,
      lookRuns: runs,
      events,
      reply,
      controlRoundtripNs,
    };
  } finally {
    const all = [
      ...lookClients,
      ...(sender === undefined ? [] : [sender]),
      ...(receiver === undefined ? [] : [receiver]),
    ];
    await closeClients(all);
  }
}

function errorCounts(samples: readonly RequestSample[]): readonly string[] {
  const counts = new Map<string, number>();
  for (const sample of samples) {
    if (sample.ok || sample.code === undefined) {
      continue;
    }
    counts.set(sample.code, (counts.get(sample.code) ?? 0) + 1);
  }
  return [...counts.entries()]
    .toSorted((left, right) => right[1] - left[1])
    .map(([code, count]) => `${code} ${integerText(count)}`);
}

function phaseText(summary: PhaseSummary): string {
  const latency = summary.latency;
  if (latency === undefined) {
    return 'no successful samples';
  }
  return `p50 ${(latency.p50Ns / 1e6).toFixed(1)}  p95 ${(latency.p95Ns / 1e6).toFixed(1)}  p99 ${(latency.p99Ns / 1e6).toFixed(1)}  max ${(latency.maxNs / 1e6).toFixed(1)} ms`;
}

function report(
  result: ScenarioResult,
  config: SwitchConfig,
  portal: SwitchPortal | undefined,
): void {
  const { markers, samples } = result;
  const baseline = summarizePhase(samples, markers, 'baseline');
  const window = summarizePhase(samples, markers, 'window');
  const recovery = summarizePhase(samples, markers, 'recovery');
  const warmup = summarizePhase(samples, markers, 'warmup');
  const disconnected = result.lookRuns.filter((run) => run.disconnected).length;
  const phaseSpanNs =
    markers.windowEndNs !== undefined && markers.triggerNs !== undefined
      ? markers.windowEndNs - markers.triggerNs
      : 0;

  writeSection(
    'switch timing:',
    `server-measured trigger -> switchTo resolve: ${(result.reply.durationNs / 1e6).toFixed(1)} ms`,
    `POST /switch round trip: ${(result.controlRoundtripNs / 1e6).toFixed(1)} ms (control hop included)`,
    `switch window (trigger -> resolve + margin): ${(phaseSpanNs / 1e6).toFixed(1)} ms; margin ${WINDOW_MARGIN_MS} ms`,
    `scheduled run: duration ${config.durationSec} s, warmup ${config.warmupSec} s, look interval ${config.intervalMs} ms, look clients ${result.clientCount}, event probe ${config.events ? 'on' : 'off'}`,
  );

  writeSection(
    'look latency by phase:',
    phaseLine('baseline', baseline),
    phaseLine('window', window),
    phaseLine('recovery', recovery),
    `warmup requests excluded: ${integerText(warmup.requests)}`,
    'window includes requests already in flight at the trigger; latency is request start to response',
  );

  writeSection(
    'connections and failures:',
    `look clients ${config.clients}, disconnected during run ${integerText(disconnected)}`,
    `errors by wire code: ${errorCounts(samples).length === 0 ? 'none' : errorCounts(samples).join(', ')}`,
    `portal log: request replayed ${integerText(portal?.logCount('request replayed') ?? 0)}, request timed out ${integerText(portal?.logCount('request timed out') ?? 0)}, server switch completed ${integerText(portal?.logCount('server switch completed') ?? 0)}`,
  );

  if (portal !== undefined) {
    const pids = portal.serverPids();
    const current = portal.liveServerPid();
    const retired = pids.filter((pid) => pid !== current);
    const retiredAlive = retired.filter((pid) => {
      try {
        process.kill(pid, 0);
        return true;
      } catch {
        return false;
      }
    });
    writeSection(
      'server processes:',
      `pids seen ${pids.length === 0 ? 'none' : pids.join(', ')}; current ${current ?? 'unknown'}`,
      `retired pids ${retired.length === 0 ? 'none' : retired.join(', ')}: ${retiredAlive.length === 0 ? 'all gone' : `still alive: ${retiredAlive.join(', ')}`}`,
    );
  }

  if (result.events !== undefined) {
    const events = result.events;
    const gapPhases = [
      `warmup ${integerText(events.gapsByPhase.warmup)}`,
      `baseline ${integerText(events.gapsByPhase.baseline)}`,
      `window ${integerText(events.gapsByPhase.window)}`,
      `recovery ${integerText(events.gapsByPhase.recovery)}`,
    ].join(', ');
    writeSection(
      'room.say event probe:',
      `attempted ${integerText(events.attempted)}, accepted ${integerText(events.sent)}, failed ${integerText(events.failed)}; received ${integerText(events.received)}`,
      `within-epoch sequence gaps ${integerText(events.gaps)} (by arrival phase: ${gapPhases}); duplicates ${integerText(events.duplicates)} (a retried timed-out say, ambiguous with double delivery, not loss)`,
      `sender disconnected ${events.senderDisconnected ? 'yes' : 'no'}`,
    );
  }

  const eventSummary =
    result.events === undefined
      ? 'event probe off'
      : `event gaps ${integerText(result.events.gaps)}, duplicates ${integerText(result.events.duplicates)}`;
  const summary =
    `summary: switch ${(result.reply.durationNs / 1e6).toFixed(1)} ms; ` +
    `window ${integerText(window.requests)} requests, ${phaseText(window)}; ` +
    `baseline ${phaseText(baseline)}; ` +
    `failures ${integerText(window.failed)} timeout ${integerText(window.timeouts)} (client ${integerText(window.clientTimeouts)}, framework ${integerText(window.frameworkTimeouts)}) disconnected ${integerText(window.disconnected)} in window, ${integerText(disconnected)} look clients detached; ` +
    `${eventSummary}; ` +
    'numbers are observations, not assertions';
  write(`\n${summary}`);

  if (!result.reply.ok) {
    writeError(
      `switch benchmark failed: switchTo was rejected: ${result.reply.error ?? 'unknown error'}`,
    );
    process.exitCode = 1;
  }
  if (window.requests === 0) {
    writeError(
      'switch benchmark did not measure: no request was in the switch window',
    );
    process.exitCode = 1;
  }
  if (baseline.requests === 0) {
    writeError(
      'switch benchmark did not measure: no baseline request before the switch',
    );
    process.exitCode = 1;
  }
}

async function main(): Promise<void> {
  const config = readSwitchConfig();
  write('mud-framework version-switch latency benchmark');
  write(environmentLine());
  write(
    `config: clients=${config.clients} interval=${config.intervalMs}ms duration=${config.durationSec}s warmup=${config.warmupSec}s events=${config.events ? 'on' : 'off'} request-timeout=${config.requestTimeoutMs}ms`,
  );

  const userDataDirectory = config.dataDirectory !== undefined;
  let dataDirectory: string | undefined;
  let portal: SwitchPortal | undefined;
  let runFailed = false;
  try {
    let url: string;
    let controlUrl: string;
    if (config.externalUrl !== undefined) {
      if (config.externalControlUrl === undefined) {
        throw new Error('SWITCH_CONTROL_URL is required with SWITCH_URL');
      }
      url = config.externalUrl;
      controlUrl = config.externalControlUrl;
      write(
        `portal: reusing ${url}; control ${controlUrl}; spawn and cleanup skipped`,
      );
    } else {
      dataDirectory =
        config.dataDirectory ??
        mkdtempSync(join(tmpdir(), 'mud-bench-switch-'));
      const kept =
        config.dataDirectory !== undefined
          ? ' (SWITCH_DATA_DIR; never removed by the benchmark)'
          : config.keep
            ? ' (kept: SWITCH_KEEP=1)'
            : '';
      write(`data dir: ${dataDirectory}${kept}`);
      portal = await startSwitchPortal({
        entry: PORTAL_ENTRY,
        dataDirectory,
        requestTimeoutMs: config.requestTimeoutMs,
      });
      url = portal.url;
      controlUrl = portal.controlUrl;
      write(
        `portal: spawned pid ${portal.pid}, listening on ${url}, control ${controlUrl}`,
      );
      write(
        `portal: executor close budget ${portal.executorCloseBudgetMs} ms (${portal.closeBudgetReported ? 'reported by the Portal' : 'snapshot fallback'})`,
      );
    }

    const result = await runScenario(url, controlUrl, config);
    report(result, config, portal);
  } catch (error) {
    runFailed = true;
    throw error;
  } finally {
    const failed =
      runFailed || (process.exitCode !== undefined && process.exitCode !== 0);
    await cleanupSwitchRun({
      portal,
      dataDirectory,
      userDataDirectory,
      keep: config.keep,
      failed,
    });
  }
}

await main().catch((error: unknown) => {
  writeError(
    `switch benchmark failed: ${error instanceof Error ? error.message : String(error)}`,
  );
  process.exitCode = 1;
});

if (process.exitCode !== undefined && process.exitCode !== 0) {
  // oxlint-disable-next-line unicorn/no-process-exit -- a failed run must not hang on a handle it cannot close
  process.exit(1);
}
