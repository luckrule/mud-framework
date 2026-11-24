import { writeSync } from 'node:fs';
import { cpus, release } from 'node:os';

import { createJsonLogger, safeLogger } from 'mud-framework-core';
import type { LogFields, Logger } from 'mud-framework-core';

/**
 * Per-call cost of `safeLogger` and of JSON serialization, measured with real
 * calls: the wrapper's cost is one closure plus one try/catch per call, which
 * only a call loop can observe.
 *
 * Every measured logger increments a counter that is asserted after each
 * sample, so a runtime that elides the calls fails the run instead of
 * reporting a meaningless time. `info` is used for the loop; the wrapper's
 * cost does not depend on the level.
 *
 * Each measurement pair runs ABBA (raw, safe, safe, raw; the next pair
 * reverses it), so drift inside a pair reaches both sides. The comparison uses
 * per-pair deltas and reports their median and range rather than two
 * independent medians. A cost is only reported when the median and the whole
 * range stay above zero; otherwise the result is reported as indistinguishable
 * from noise at this sample size, never as a negative cost or a negative share
 * of a JSON line.
 *
 * Run with `bun run bench:logger`. Method and caveats: benchmarks/README.md.
 */

const ITERATIONS = 1_000_000;
const WARMUP_ITERATIONS = 1_000_000;
const INFO_PAIRS = 6;

const CHILD_ITERATIONS = 100_000;
const CHILD_WARMUP_ITERATIONS = 200_000;
const CHILD_PAIRS = 4;

const JSON_ITERATIONS = 50_000;
const JSON_WARMUP_ITERATIONS = 50_000;
const JSON_PAIRS = 4;

const FIELDS: LogFields = {
  requestId: 'bench-request',
  connectionId: 'bench-connection',
  extension: 'bench',
};
const MESSAGE = 'bench log line';

interface Counter {
  calls: number;
}

/**
 * Stands in for a level-filtered logger: no formatting, no sink, no I/O. The
 * counter keeps the calls observable to both the JIT and the harness.
 */
function countingNoop(counter: Counter): Logger {
  return {
    debug() {
      counter.calls += 1;
    },
    info() {
      counter.calls += 1;
    },
    warn() {
      counter.calls += 1;
    },
    error() {
      counter.calls += 1;
    },
    child() {
      counter.calls += 1;
      // A fresh logger per call, like `JsonLogger.child`, so the wrapper's
      // idempotency check and WeakSet insertion are part of what is measured.
      return countingNoop(counter);
    },
  };
}

function callLoop(logger: Logger, iterations: number): void {
  for (let i = 0; i < iterations; i += 1) {
    logger.info(FIELDS, MESSAGE);
  }
}

/**
 * Module-level so the JIT cannot prove the returned child unused and elide the
 * allocation: the raw baseline must pay for a fresh logger per call, as a real
 * logger's `child()` does. Without this the raw side collapses to a counter
 * increment and the wrapper delta absorbs an allocation it does not own.
 */
let heldChild: Logger | undefined;

function childLoop(logger: Logger, iterations: number): void {
  for (let i = 0; i < iterations; i += 1) {
    heldChild = logger.child(FIELDS);
  }
}

function timed(
  run: () => void,
  expectedCalls: number,
  calls: () => number,
): number {
  const before = calls();
  const started = Bun.nanoseconds();
  run();
  const elapsed = Bun.nanoseconds() - started;
  const observed = calls() - before;
  if (observed !== expectedCalls) {
    throw new Error(
      `expected ${expectedCalls} calls but observed ${observed}: the runtime elided the loop, so the timing is meaningless`,
    );
  }
  return elapsed / expectedCalls;
}

interface GroupSamples {
  readonly raw: readonly number[];
  readonly safe: readonly number[];
  /** One per ABBA pair: mean(safe) - mean(raw) inside that pair. */
  readonly deltas: readonly number[];
  /** One per ABBA pair, used only when the delta is reported as a cost. */
  readonly ratios: readonly number[];
}

function pairMean(values: readonly number[]): number {
  return ((values[0] ?? 0) + (values[1] ?? 0)) / 2;
}

/**
 * ABBA within each pair and a reversed ABBA for the next pair: both sides get
 * the outer and inner slots equally often, and linear drift inside a pair
 * cancels out of the paired delta.
 */
function pairedRuns(
  pairs: number,
  measureRaw: () => number,
  measureSafe: () => number,
): GroupSamples {
  const raw: number[] = [];
  const safe: number[] = [];
  const deltas: number[] = [];
  const ratios: number[] = [];
  for (let pair = 0; pair < pairs; pair += 1) {
    const pairRaw: number[] = [];
    const pairSafe: number[] = [];
    if (pair % 2 === 0) {
      pairRaw.push(measureRaw());
      pairSafe.push(measureSafe());
      pairSafe.push(measureSafe());
      pairRaw.push(measureRaw());
    } else {
      pairSafe.push(measureSafe());
      pairRaw.push(measureRaw());
      pairRaw.push(measureRaw());
      pairSafe.push(measureSafe());
    }
    raw.push(pairRaw[0] ?? 0, pairRaw[1] ?? 0);
    safe.push(pairSafe[0] ?? 0, pairSafe[1] ?? 0);
    const rawMean = pairMean(pairRaw);
    const safeMean = pairMean(pairSafe);
    deltas.push(safeMean - rawMean);
    ratios.push(safeMean / rawMean);
  }
  return { raw, safe, deltas, ratios };
}

function median(values: readonly number[]): number {
  const sorted = values.toSorted((a, b) => a - b);
  const middle = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) {
    return sorted[middle] ?? 0;
  }
  return ((sorted[middle - 1] ?? 0) + (sorted[middle] ?? 0)) / 2;
}

const integerFormat = new Intl.NumberFormat('en-US');

/**
 * Two to three significant digits: enough to show a difference, not enough to
 * look like the run measured more precision than it did.
 */
function approximate(value: number): string {
  const magnitude = Math.abs(value);
  if (magnitude >= 100) {
    return Math.round(value).toString();
  }
  if (magnitude >= 1) {
    return value.toFixed(1);
  }
  return value.toFixed(2);
}

function throughputText(nsPerCall: number): string {
  return `${integerFormat.format(Math.round(1e9 / nsPerCall))} ops/s`;
}

function sampleText(values: readonly number[]): string {
  return values.map((value) => approximate(value)).join(', ');
}

function deltaRangeText(samples: GroupSamples): string {
  return `${approximate(Math.min(...samples.deltas))} to ${approximate(
    Math.max(...samples.deltas),
  )} ns/call`;
}

/**
 * A cost is only reported when the median pair delta and the whole range sit
 * above zero. A median above zero with a range that crosses it means the pairs
 * disagree on the sign, which is noise at this sample size, not a small cost.
 */
function distinguishable(samples: GroupSamples): boolean {
  return median(samples.deltas) > 0 && Math.min(...samples.deltas) > 0;
}

function deltaLine(samples: GroupSamples, withRatio: boolean): string {
  const delta = median(samples.deltas);
  const pairs = samples.deltas.length;
  if (!distinguishable(samples)) {
    const signNote =
      delta < 0
        ? '; a negative paired delta is run-to-run noise, not a speed-up'
        : '';
    return `  wrapper delta: not distinguishable from noise at this sample size (median ${approximate(delta)} ns/call, range ${deltaRangeText(samples)} across ${pairs} ABBA pairs${signNote}); no ratio or share reported`;
  }
  const ratio = withRatio
    ? `; ratio ~${approximate(median(samples.ratios))}x`
    : '';
  return `  wrapper delta: ~${approximate(delta)} ns/call median, range ${deltaRangeText(samples)} across ${pairs} ABBA pairs${ratio}`;
}

function assertPositive(samples: GroupSamples, label: string): void {
  for (const value of [...samples.raw, ...samples.safe]) {
    if (!Number.isFinite(value) || value <= 0) {
      throw new Error(`${label}: non-positive sample ${String(value)}`);
    }
  }
}

function environmentLine(): string {
  const cpu = cpus()[0]?.model ?? 'unknown CPU';
  return `Bun ${process.versions.bun ?? 'unknown'}  ${process.platform} ${process.arch}  kernel ${release()}  ${cpu}`;
}

interface GroupLabels {
  readonly raw: string;
  readonly safe: string;
}

interface GroupOptions {
  readonly showSamples: boolean;
  readonly showRatio: boolean;
}

function appendGroup(
  lines: string[],
  heading: string,
  samples: GroupSamples,
  labels: GroupLabels,
  options: GroupOptions,
): void {
  const rawMedian = median(samples.raw);
  const safeMedian = median(samples.safe);
  lines.push(heading);
  lines.push(
    `  ${labels.raw.padEnd(16)} ~${approximate(rawMedian)} ns/call  ~${throughputText(rawMedian)}`,
  );
  lines.push(
    `  ${labels.safe.padEnd(16)} ~${approximate(safeMedian)} ns/call  ~${throughputText(safeMedian)}`,
  );
  lines.push(deltaLine(samples, options.showRatio));
  if (options.showSamples) {
    lines.push(`  direct samples: ${sampleText(samples.raw)}`);
    lines.push(`  safe samples:   ${sampleText(samples.safe)}`);
    lines.push(`  pair deltas:    ${sampleText(samples.deltas)}`);
  }
  lines.push('');
}

function conclusionText(
  callSamples: GroupSamples,
  jsonSamples: GroupSamples,
): string {
  const callDelta = median(callSamples.deltas);
  const jsonLineMedian = median(jsonSamples.safe);
  const jsonText = `one JSON-serialized line through the same wrapper costs ~${approximate(jsonLineMedian)} ns/call`;
  if (!distinguishable(callSamples)) {
    return `conclusion: the wrapper's info() cost is not distinguishable from noise at this sample size (median paired delta ${approximate(callDelta)} ns/call, range ${deltaRangeText(callSamples)} across ${callSamples.deltas.length} ABBA pairs), so no share of a JSON line is reported; ${jsonText}`;
  }
  const share = (callDelta / jsonLineMedian) * 100;
  return `conclusion: the wrapper costs ~${approximate(callDelta)} ns per info() call on a level-filtered logger (median paired delta, range ${deltaRangeText(callSamples)} across ${callSamples.deltas.length} ABBA pairs), ~${approximate(median(callSamples.ratios))}x the raw call; ${jsonText}, so the wrapper is ~${approximate(share)}% of it`;
}

function main(): void {
  const counter: Counter = { calls: 0 };
  const raw = countingNoop(counter);
  const safe = safeLogger(raw);

  callLoop(raw, WARMUP_ITERATIONS);
  callLoop(safe, WARMUP_ITERATIONS);

  const calls = (): number => counter.calls;
  const callSamples = pairedRuns(
    INFO_PAIRS,
    () => timed(() => callLoop(raw, ITERATIONS), ITERATIONS, calls),
    () => timed(() => callLoop(safe, ITERATIONS), ITERATIONS, calls),
  );
  assertPositive(callSamples, 'info()');

  childLoop(raw, CHILD_WARMUP_ITERATIONS);
  childLoop(safe, CHILD_WARMUP_ITERATIONS);
  const childSamples = pairedRuns(
    CHILD_PAIRS,
    () =>
      timed(() => childLoop(raw, CHILD_ITERATIONS), CHILD_ITERATIONS, calls),
    () =>
      timed(() => childLoop(safe, CHILD_ITERATIONS), CHILD_ITERATIONS, calls),
  );
  assertPositive(childSamples, 'child()');
  if (heldChild === undefined) {
    throw new Error('child() loop returned no logger');
  }

  const jsonCounter: Counter = { calls: 0 };
  const json = createJsonLogger({
    level: 'info',
    sink: () => {
      jsonCounter.calls += 1;
    },
  });
  const safeJson = safeLogger(json);
  const jsonCalls = (): number => jsonCounter.calls;
  callLoop(json, JSON_WARMUP_ITERATIONS);
  callLoop(safeJson, JSON_WARMUP_ITERATIONS);
  const jsonSamples = pairedRuns(
    JSON_PAIRS,
    () =>
      timed(() => callLoop(json, JSON_ITERATIONS), JSON_ITERATIONS, jsonCalls),
    () =>
      timed(
        () => callLoop(safeJson, JSON_ITERATIONS),
        JSON_ITERATIONS,
        jsonCalls,
      ),
  );
  assertPositive(jsonSamples, 'JsonLogger.info()');

  const lines: string[] = [];
  lines.push('mud-framework logging benchmark');
  lines.push(environmentLine());
  lines.push(
    'method: ABBA per pair (raw,safe,safe,raw then safe,raw,raw,safe); delta = mean(safe) - mean(raw) in each pair; a cost needs a median and range above zero; values are approximate',
  );
  lines.push('');
  appendGroup(
    lines,
    `safeLogger wrapper, info() on a counting no-op logger, ${integerFormat.format(ITERATIONS)} calls/sample, ${INFO_PAIRS} ABBA pairs (${INFO_PAIRS * 2} samples per side)`,
    callSamples,
    { raw: 'direct noop', safe: 'safeLogger(noop)' },
    { showSamples: true, showRatio: true },
  );
  appendGroup(
    lines,
    `safeLogger wrapper, child() on a counting no-op logger, ${integerFormat.format(CHILD_ITERATIONS)} calls/sample, ${CHILD_PAIRS} ABBA pairs (${CHILD_PAIRS * 2} samples per side)`,
    childSamples,
    { raw: 'direct noop', safe: 'safeLogger(noop)' },
    { showSamples: false, showRatio: true },
  );
  appendGroup(
    lines,
    `JsonLogger.info() with a no-op sink (serialization only), ${integerFormat.format(JSON_ITERATIONS)} calls/sample, ${JSON_PAIRS} ABBA pairs (${JSON_PAIRS * 2} samples per side)`,
    jsonSamples,
    { raw: 'direct logger', safe: 'safeLogger(json)' },
    { showSamples: true, showRatio: false },
  );
  lines.push(conclusionText(callSamples, jsonSamples));

  writeSync(1, `${lines.join('\n')}\n`);
}

try {
  main();
} catch (error) {
  const message = error instanceof Error ? error.message : String(error);
  writeSync(2, `logger benchmark failed: ${message}\n`);
  process.exitCode = 1;
  // oxlint-disable-next-line unicorn/no-process-exit -- a failed benchmark must not hang on a handle it cannot close
  process.exit(1);
}
