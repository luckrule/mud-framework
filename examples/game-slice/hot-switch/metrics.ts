import { formatMs } from './support.ts';
import type { Run, SwitchWindow } from './types.ts';

/** Latency summaries and the per-run aggregates the report and judge share. */

export interface LatencySummary {
  readonly count: number;
  readonly p50Ms: number;
  readonly p95Ms: number;
  readonly maxMs: number;
}

/** Nearest-rank percentiles over all attempts, failed ones included. */
export function summarizeLatency(
  latenciesNs: readonly number[],
): LatencySummary | undefined {
  if (latenciesNs.length === 0) {
    return undefined;
  }
  const sorted = latenciesNs.toSorted((left, right) => left - right);
  const at = (fraction: number): number =>
    sorted[
      Math.min(sorted.length - 1, Math.ceil(fraction * sorted.length) - 1)
    ];
  return {
    count: sorted.length,
    p50Ms: at(0.5) / 1e6,
    p95Ms: at(0.95) / 1e6,
    maxMs: (sorted.at(-1) ?? 0) / 1e6,
  };
}

export function latencyText(summary: LatencySummary | undefined): string {
  if (summary === undefined) {
    return 'no samples';
  }
  return `p50 ${formatMs(summary.p50Ms)} ms, p95 ${formatMs(summary.p95Ms)} ms, max ${formatMs(summary.maxMs)} ms (n=${summary.count})`;
}

export interface Aggregate {
  attempts: number;
  ok: number;
  clientTimeouts: number;
  frameworkTimeouts: number;
  failures: number;
  disconnects: number;
  retried: number;
  retrySucceeded: number;
  recoveredMoves: number;
  repeats: number;
  repeatsInSwitches: number;
  maxRepeatsPerWindow: number;
  looks: number;
  says: number;
  moves: number;
  readonly latenciesNs: number[];
  readonly codes: Map<string, number>;
}

export function aggregate(run: Run): Aggregate {
  const total: Aggregate = {
    attempts: 0,
    ok: 0,
    clientTimeouts: 0,
    frameworkTimeouts: 0,
    failures: 0,
    disconnects: 0,
    retried: 0,
    retrySucceeded: 0,
    recoveredMoves: 0,
    repeats: 0,
    repeatsInSwitches: 0,
    maxRepeatsPerWindow: 0,
    looks: 0,
    says: 0,
    moves: 0,
    latenciesNs: [],
    codes: new Map(),
  };
  for (const state of run.clients) {
    const { stats } = state;
    total.attempts += stats.attempts;
    total.ok += stats.ok;
    total.clientTimeouts += stats.clientTimeouts;
    total.frameworkTimeouts += stats.frameworkTimeouts;
    total.failures += stats.failures;
    total.disconnects += stats.disconnects;
    total.retried += stats.retried;
    total.retrySucceeded += stats.retrySucceeded;
    total.recoveredMoves += stats.recoveredMoves;
    total.looks += stats.looks;
    total.says += stats.says;
    total.moves += stats.moves;
    total.latenciesNs.push(...stats.latencyNs);
    for (const [code, count] of stats.codes) {
      total.codes.set(code, (total.codes.get(code) ?? 0) + count);
    }
    for (const [window, count] of stats.repeatsByWindow) {
      total.repeats += count;
      if (window > 0) {
        total.repeatsInSwitches += count;
      }
      total.maxRepeatsPerWindow = Math.max(total.maxRepeatsPerWindow, count);
    }
  }
  return total;
}

/** Every attempt overlapping the window, timed-out and failed ones included. */
export function requestLatenciesInWindow(
  run: Run,
  window: SwitchWindow,
): number[] {
  return run.requestSamples
    .filter(
      (sample) =>
        sample.startNs < window.endNs && sample.endNs > window.startNs,
    )
    .map((sample) => sample.endNs - sample.startNs);
}

/** Events whose say was submitted inside the window. */
export function eventLatenciesInWindow(
  run: Run,
  window: SwitchWindow,
): number[] {
  return run.eventSamples
    .filter(
      (sample) =>
        sample.sentAtNs >= window.startNs && sample.sentAtNs <= window.endNs,
    )
    .map((sample) => sample.latencyNs);
}

export interface SayAccounting {
  readonly missing: readonly string[];
  readonly unconfirmed: readonly string[];
}

export function sayAccounting(run: Run): SayAccounting {
  return {
    missing: [...run.anchorExpected].filter(
      (marker) => !run.anchorObserved.has(marker),
    ),
    unconfirmed: [...run.anchorObserved].filter(
      (marker) => !run.anchorExpected.has(marker),
    ),
  };
}
