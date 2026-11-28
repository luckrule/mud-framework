import {
  integerText,
  latencyText,
  millisecondsText,
  throughputText,
  write,
} from '../support.ts';
import type { PidTraceReport, ResourceReport } from './resources.ts';
import type { MainConfig } from './support.ts';
import { COMMAND_NAMES, Histogram, mixText } from './support.ts';
import type { CommandName, LoadResultJson } from './support.ts';

/**
 * Result aggregation and console reporting. Command histograms are merged
 * bucket-by-bucket across generator processes; the printed percentiles are
 * bucket upper bounds (see `support.ts`), never raw samples.
 */

export interface AggregateCommand {
  count: number;
  errors: number;
  readonly histogram: Histogram;
}

export interface AggregateEvents {
  saysAttempted: number;
  saysSent: number;
  saysFailed: number;
  received: number;
  gaps: number;
  duplicates: number;
  staleRoom: number;
  lateEpoch: number;
  unparsed: number;
  readonly histogram: Histogram;
}

export interface Aggregate {
  clients: number;
  measuredFromMs: number;
  measuredUntilMs: number;
  readonly commands: Record<CommandName, AggregateCommand>;
  readonly events: AggregateEvents;
  readonly errors: Record<string, number>;
  readonly rooms: Record<string, number>;
}

export function aggregateResults(
  results: readonly LoadResultJson[],
): Aggregate {
  const commands: Record<CommandName, AggregateCommand> = {
    look: { count: 0, errors: 0, histogram: new Histogram() },
    who: { count: 0, errors: 0, histogram: new Histogram() },
    move: { count: 0, errors: 0, histogram: new Histogram() },
    say: { count: 0, errors: 0, histogram: new Histogram() },
  };
  const events: AggregateEvents = {
    saysAttempted: 0,
    saysSent: 0,
    saysFailed: 0,
    received: 0,
    gaps: 0,
    duplicates: 0,
    staleRoom: 0,
    lateEpoch: 0,
    unparsed: 0,
    histogram: new Histogram(),
  };
  const aggregate: Aggregate = {
    clients: 0,
    measuredFromMs: Number.POSITIVE_INFINITY,
    measuredUntilMs: Number.NEGATIVE_INFINITY,
    commands,
    events,
    errors: {},
    rooms: {},
  };
  for (const result of results) {
    aggregate.clients += result.clients;
    aggregate.measuredFromMs = Math.min(
      aggregate.measuredFromMs,
      result.measuredFromMs,
    );
    aggregate.measuredUntilMs = Math.max(
      aggregate.measuredUntilMs,
      result.measuredUntilMs,
    );
    for (const command of COMMAND_NAMES) {
      const bucket = commands[command];
      const part = result.commands[command];
      bucket.count += part.count;
      bucket.errors += part.errors;
      bucket.histogram.mergeJson(part.histogram);
    }
    const partEvents = result.events;
    events.saysAttempted += partEvents.saysAttempted;
    events.saysSent += partEvents.saysSent;
    events.saysFailed += partEvents.saysFailed;
    events.received += partEvents.received;
    events.gaps += partEvents.gaps;
    events.duplicates += partEvents.duplicates;
    events.staleRoom += partEvents.staleRoom;
    events.lateEpoch += partEvents.lateEpoch;
    events.unparsed += partEvents.unparsed;
    events.histogram.mergeJson(partEvents.histogram);
    for (const [code, count] of Object.entries(result.errors)) {
      aggregate.errors[code] = (aggregate.errors[code] ?? 0) + count;
    }
    for (const [room, count] of Object.entries(result.rooms)) {
      aggregate.rooms[room] = (aggregate.rooms[room] ?? 0) + count;
    }
  }
  if (
    !Number.isFinite(aggregate.measuredFromMs) ||
    !Number.isFinite(aggregate.measuredUntilMs)
  ) {
    throw new Error('no generator reported a measurement window');
  }
  return aggregate;
}

function successCount(aggregate: Aggregate): number {
  return COMMAND_NAMES.reduce(
    (sum, command) => sum + aggregate.commands[command].count,
    0,
  );
}

function errorCount(counts: Readonly<Record<string, number>>): number {
  return Object.values(counts).reduce((sum, value) => sum + value, 0);
}

export function printPerGenerator(results: readonly LoadResultJson[]): void {
  write('\ngenerators (setup and measurement window per process):');
  for (const result of results) {
    const successes = COMMAND_NAMES.reduce(
      (sum, command) => sum + result.commands[command].count,
      0,
    );
    write(
      `  ${result.generator}: clients ${integerText(result.clients)}, setup ${(result.setupMs / 1000).toFixed(2)} s, window ${((result.measuredUntilMs - result.measuredFromMs) / 1000).toFixed(2)} s, ops ${integerText(successes)}, errors ${integerText(errorCount(result.errors))}`,
    );
  }
}

export function printAggregate(aggregate: Aggregate, config: MainConfig): void {
  const windowMs = aggregate.measuredUntilMs - aggregate.measuredFromMs;
  const windowNs = windowMs * 1e6;
  const successes = successCount(aggregate);
  const totalErrors = errorCount(aggregate.errors);

  write(
    `\nresults (warmup excluded, union measurement window ${(windowMs / 1000).toFixed(2)} s):`,
  );
  write(
    `  successful ops ${integerText(successes)}, ${throughputText(successes, windowNs)}, errors ${integerText(totalErrors)}`,
  );
  for (const command of COMMAND_NAMES) {
    const bucket = aggregate.commands[command];
    const summary = bucket.histogram.summary();
    const overflow =
      summary.overflow === 0
        ? ''
        : `  overflow ${integerText(summary.overflow)}`;
    write(
      `  ${command.padEnd(4)}  n ${integerText(summary.count)}  errors ${integerText(bucket.errors)}  latency ms: ${latencyText(
        {
          minNs: summary.minNs,
          p50Ns: summary.p50Ns,
          p95Ns: summary.p95Ns,
          p99Ns: summary.p99Ns,
          maxNs: summary.maxNs,
        },
      )}${overflow}`,
    );
  }

  const events = aggregate.events;
  const eventSummary = events.histogram.summary();
  // Successful responses are the confirmable broadcast set, so they are the
  // delivery denominator; a timed-out attempt may also have been broadcast, so
  // deliveries/say can exceed 1.
  const perSay =
    events.saysSent === 0
      ? 'n/a'
      : (events.received / events.saysSent).toFixed(1);
  write(
    `  room.say  attempted ${integerText(events.saysAttempted)}  sent ${integerText(events.saysSent)}  failed ${integerText(events.saysFailed)}  received ${integerText(events.received)}  deliveries/say ${perSay}  latency ms: ${latencyText(
      {
        minNs: eventSummary.minNs,
        p50Ns: eventSummary.p50Ns,
        p95Ns: eventSummary.p95Ns,
        p99Ns: eventSummary.p99Ns,
        maxNs: eventSummary.maxNs,
      },
    )}${eventSummary.overflow === 0 ? '' : `  overflow ${integerText(eventSummary.overflow)}`}`,
  );
  write(
    `  event checks: within-epoch gaps ${integerText(events.gaps)}, duplicates ${integerText(events.duplicates)} (a retried timed-out say, ambiguous with double delivery, not loss), receiver-moved ${integerText(events.staleRoom)}, late-epoch ${integerText(events.lateEpoch)}, unparsed ${integerText(events.unparsed)}`,
  );

  const queueFull = aggregate.errors.FRAMEWORK_QUEUE_FULL ?? 0;
  const timeouts = aggregate.errors.CLIENT_TIMEOUT ?? 0;
  const dropText =
    events.gaps === 0
      ? 'no event drops detected (no within-epoch sequence gaps)'
      : `${integerText(events.gaps)} event gaps (drops detected)`;
  write(
    `  stress: queue-full ${integerText(queueFull)}, client timeouts ${integerText(timeouts)}; ${dropText}`,
  );

  const codes = Object.entries(aggregate.errors).toSorted(
    (left, right) => right[1] - left[1],
  );
  write(
    `  errors by code: ${codes.length === 0 ? 'none' : codes.map(([code, count]) => `${code} ${integerText(count)}`).join(', ')}`,
  );
  const rooms = Object.entries(aggregate.rooms).toSorted(
    (left, right) => right[1] - left[1],
  );
  write(
    `  final rooms: ${rooms.length === 0 ? 'none' : rooms.map(([room, count]) => `${room} ${integerText(count)}`).join(', ')}`,
  );
  write(
    `  latency percentiles are histogram bucket upper bounds (5% relative width); room.say times use Date.now (1 ms resolution); mix ${mixText(config.shared.mix)}`,
  );
}

function pidTraceText(trace: PidTraceReport): string {
  const rssPeak =
    trace.peakRssKb === undefined
      ? 'n/a'
      : `${(trace.peakRssKb / 1024).toFixed(1)} MB`;
  const rssLast =
    trace.lastRssKb === undefined
      ? 'n/a'
      : `${(trace.lastRssKb / 1024).toFixed(1)} MB`;
  const fdsPeak =
    trace.peakFds === undefined ? 'n/a' : integerText(trace.peakFds);
  const fdsLast =
    trace.lastFds === undefined ? 'n/a' : integerText(trace.lastFds);
  return `${trace.label}: rss peak ${rssPeak}, last ${rssLast}; fds peak ${fdsPeak}, last ${fdsLast}; samples ${integerText(trace.samples)}, misses ${integerText(trace.misses)}`;
}

export function printResources(report: ResourceReport): void {
  if (!report.supported) {
    write('resources: skipped (RSS/fd sampling reads /proc on Linux only)');
    return;
  }
  write('resources (1 s /proc samples, peak / last):');
  if (report.portalPid === undefined) {
    write('  portal: not spawned (LOAD_URL); RSS/fd skipped');
    write('  server: not sampled with an external Portal');
    return;
  }
  write(`  ${pidTraceText(report.portal)}`);
  if (!report.serverSeen || report.serverPid === undefined) {
    write(
      '  server: no "server process ready" pid in the Portal log; RSS/fd skipped',
    );
  } else {
    write(`  ${pidTraceText(report.server)} (pid ${report.serverPid})`);
  }
}

export function buildSummary(aggregate: Aggregate, config: MainConfig): string {
  const windowMs = aggregate.measuredUntilMs - aggregate.measuredFromMs;
  const successes = successCount(aggregate);
  const look = aggregate.commands.look.histogram.summary();
  const move = aggregate.commands.move.histogram.summary();
  const say = aggregate.commands.say.histogram.summary();
  const event = aggregate.events.histogram.summary();
  const drops =
    aggregate.events.gaps === 0
      ? 'no event drops detected (sequence gaps 0)'
      : `${integerText(aggregate.events.gaps)} event gaps`;
  return `summary: ${integerText(config.clients)} clients, ${config.shared.mode}, ${integerText(successes)} successful ops in ${(windowMs / 1000).toFixed(1)} s = ${throughputText(successes, windowMs * 1e6)}; look p50/p95 ${millisecondsText(look.p50Ns)}/${millisecondsText(look.p95Ns)} ms; move p50/p95 ${millisecondsText(move.p50Ns)}/${millisecondsText(move.p95Ns)} ms; say p50/p95 ${millisecondsText(say.p50Ns)}/${millisecondsText(say.p95Ns)} ms; room.say p50/p95 ${millisecondsText(event.p50Ns)}/${millisecondsText(event.p95Ns)} ms; errors ${integerText(errorCount(aggregate.errors))}; ${drops}; numbers are observations, not assertions`;
}
