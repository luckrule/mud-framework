import { describe, expect, test } from 'bun:test';

import {
  recordWindowedEvent,
  serializeEventWindows,
  withinMeasuredWindow,
} from './event-windows.ts';
import { aggregateResults, printEventWindows } from './report.ts';
import type { Aggregate } from './report.ts';
import { EVENT_WINDOW_MS, Histogram } from './support.ts';
import type {
  EventWindowJson,
  HistogramJson,
  LoadResultJson,
} from './support.ts';

const BASE_MS = 1_000_000;

function histogramJson(samplesNs: readonly number[]): HistogramJson {
  const histogram = new Histogram();
  for (const sample of samplesNs) {
    histogram.record(sample);
  }
  return histogram.toJson();
}

function mergedHistogramJson(
  windows: readonly EventWindowJson[],
): HistogramJson {
  const histogram = new Histogram();
  for (const window of windows) {
    histogram.mergeJson(window.histogram);
  }
  return histogram.toJson();
}

function eventWindow(
  index: number,
  samplesNs: readonly number[],
  baseMs = BASE_MS,
): EventWindowJson {
  const fromMs = baseMs + index * EVENT_WINDOW_MS;
  return {
    index,
    fromMs,
    untilMs: fromMs + EVENT_WINDOW_MS,
    histogram: histogramJson(samplesNs),
  };
}

function loadResult(options: {
  readonly generator: number;
  readonly measuredFromMs?: number;
  readonly windows?: readonly EventWindowJson[];
}): LoadResultJson {
  const measuredFromMs = options.measuredFromMs ?? BASE_MS;
  const empty = histogramJson([]);
  const windowSamples =
    options.windows?.reduce((sum, window) => sum + window.histogram.count, 0) ??
    0;
  const command = (): {
    count: number;
    errors: number;
    histogram: HistogramJson;
  } => ({
    count: 0,
    errors: 0,
    histogram: empty,
  });
  return {
    version: 1,
    generator: options.generator,
    clients: 2,
    senders: 1,
    setupMs: 1,
    measuredFromMs,
    measuredUntilMs: measuredFromMs + 900_000,
    commands: {
      look: command(),
      who: command(),
      move: command(),
      say: command(),
    },
    events: {
      saysAttempted: 0,
      saysSent: 0,
      saysFailed: 0,
      received: windowSamples,
      gaps: 0,
      duplicates: 0,
      staleRoom: 0,
      lateEpoch: 0,
      unparsed: 0,
      histogram: mergedHistogramJson(options.windows ?? []),
      ...(options.windows === undefined ? {} : { windows: options.windows }),
    },
    errors: {},
    rooms: { square: 2 },
  };
}

function expectWindowCoverage(aggregate: Aggregate): void {
  const total =
    aggregate.events.eventWindows?.windows.reduce(
      (sum, window) => sum + window.histogram.count,
      0,
    ) ?? 0;
  expect(total).toBe(aggregate.events.received);
}

describe('client event windows', () => {
  const window = { fromMs: BASE_MS, untilMs: BASE_MS + 300_000 };

  test('the measurement window is half-open', () => {
    expect(withinMeasuredWindow(BASE_MS - 1, window)).toBe(false);
    expect(withinMeasuredWindow(BASE_MS, window)).toBe(true);
    expect(withinMeasuredWindow(BASE_MS + 299_999, window)).toBe(true);
    expect(withinMeasuredWindow(BASE_MS + 300_000, window)).toBe(false);
  });

  test('records samples into send-time windows and serializes them', () => {
    const windows = new Map<number, Histogram>();
    recordWindowedEvent(windows, BASE_MS, BASE_MS, 1_000_000);
    recordWindowedEvent(windows, BASE_MS + 299_999, BASE_MS, 2_000_000);
    recordWindowedEvent(windows, BASE_MS + 300_000, BASE_MS, 3_000_000);

    const serialized = serializeEventWindows(windows, BASE_MS);
    expect(serialized.map((entry) => entry.index)).toEqual([0, 1]);
    expect(serialized.map((entry) => entry.histogram.count)).toEqual([2, 1]);
    expect(serialized[0]?.fromMs).toBe(BASE_MS);
    expect(serialized[0]?.untilMs).toBe(BASE_MS + EVENT_WINDOW_MS);
    expect(serialized[1]?.fromMs).toBe(BASE_MS + EVENT_WINDOW_MS);
    expect(
      serialized.reduce((sum, entry) => sum + entry.histogram.count, 0),
    ).toBe(3);
  });
});

describe('windowed event aggregation', () => {
  test('merges the same window across generators', () => {
    const aggregate = aggregateResults([
      loadResult({ generator: 0, windows: [eventWindow(0, [1_000_000])] }),
      loadResult({ generator: 1, windows: [eventWindow(0, [3_000_000])] }),
    ]);

    const eventWindows = aggregate.events.eventWindows;
    expect(eventWindows?.resultsWithWindows).toBe(2);
    expect(eventWindows?.windows).toHaveLength(1);
    const first = eventWindows?.windows[0];
    expect(first?.index).toBe(0);
    expect(first?.histogram.count).toBe(2);
    expect(first?.histogram.summary().maxNs).toBe(3_000_000);
    expectWindowCoverage(aggregate);
  });

  test('fills empty windows between samples with fixed boundaries', () => {
    const aggregate = aggregateResults([
      loadResult({
        generator: 0,
        windows: [eventWindow(0, [1_000_000]), eventWindow(2, [2_000_000])],
      }),
      loadResult({ generator: 1, windows: [eventWindow(2, [4_000_000])] }),
    ]);

    const windows = aggregate.events.eventWindows?.windows ?? [];
    expect(windows.map((window) => window.index)).toEqual([0, 1, 2]);
    expect(windows.map((window) => window.fromMs)).toEqual([
      BASE_MS,
      BASE_MS + EVENT_WINDOW_MS,
      BASE_MS + 2 * EVENT_WINDOW_MS,
    ]);
    expect(windows.map((window) => window.histogram.count)).toEqual([1, 0, 2]);
    expect(windows[1]?.histogram.summary().p50Ns).toBe(0);
    expectWindowCoverage(aggregate);
  });

  test('an explicit empty window is kept and merged', () => {
    const aggregate = aggregateResults([
      loadResult({ generator: 0, windows: [eventWindow(1, [])] }),
      loadResult({ generator: 1, windows: [eventWindow(1, [5_000_000])] }),
    ]);

    const windows = aggregate.events.eventWindows?.windows ?? [];
    expect(windows.map((window) => window.index)).toEqual([0, 1]);
    expect(windows[0]?.histogram.count).toBe(0);
    expect(windows[1]?.histogram.count).toBe(1);
    expectWindowCoverage(aggregate);
  });

  test('results without windows keep the aggregate field undefined', () => {
    const aggregate = aggregateResults([
      loadResult({ generator: 0 }),
      loadResult({ generator: 1 }),
    ]);

    expect(aggregate.events.eventWindows).toBeUndefined();
  });

  test('partial coverage is reported without inventing samples', () => {
    const aggregate = aggregateResults([
      loadResult({ generator: 0, windows: [eventWindow(0, [1_000_000])] }),
      loadResult({ generator: 1 }),
    ]);

    expect(aggregate.events.eventWindows?.resultsWithWindows).toBe(1);
    expect(aggregate.events.eventWindows?.windows[0]?.histogram.count).toBe(1);
    expectWindowCoverage(aggregate);
  });

  test('window samples must match the generator received count', () => {
    const result = loadResult({
      generator: 0,
      windows: [eventWindow(0, [1_000_000])],
    });
    expect(() =>
      aggregateResults([
        { ...result, events: { ...result.events, received: 2 } },
      ]),
    ).toThrow('samples but it reported');
  });

  test('an out-of-range window index is rejected before filling', () => {
    const result = loadResult({
      generator: 0,
      windows: [eventWindow(100_000, [1_000_000])],
    });

    expect(() => aggregateResults([result])).toThrow('exceeds');
  });

  test('misaligned generator windows are rejected', () => {
    expect(() =>
      aggregateResults([
        loadResult({ generator: 0, windows: [eventWindow(0, [1_000_000])] }),
        loadResult({
          generator: 1,
          measuredFromMs: BASE_MS + 1,
          windows: [eventWindow(0, [1_000_000], BASE_MS + 1)],
        }),
      ]),
    ).toThrow('boundaries differ across generators');
  });

  test('empty window data is distinguishable from missing data', () => {
    const aggregate = aggregateResults([
      loadResult({ generator: 0, windows: [] }),
    ]);

    expect(aggregate.events.eventWindows).toEqual({
      windows: [],
      resultsWithWindows: 1,
    });
  });
});

describe('window report lines', () => {
  test('old results report a skip', () => {
    const lines: string[] = [];
    printEventWindows(undefined, 2, BASE_MS, (line) => lines.push(line));

    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain('skipped');
  });

  test('a run without samples mentions partial coverage', () => {
    const lines: string[] = [];
    printEventWindows(
      { windows: [], resultsWithWindows: 1 },
      2,
      BASE_MS,
      (line) => lines.push(line),
    );

    expect(lines).toHaveLength(1);
    expect(lines[0]).toContain('no room.say samples');
    expect(lines[0]).toContain('1/2');
  });

  test('a complete table carries every window and its latency', () => {
    const aggregate = aggregateResults([
      loadResult({
        generator: 0,
        windows: [eventWindow(0, [1_000_000]), eventWindow(1, [])],
      }),
    ]);
    const lines: string[] = [];
    printEventWindows(
      aggregate.events.eventWindows,
      aggregate.generators,
      aggregate.measuredFromMs,
      (line) => lines.push(line),
    );

    expect(lines[0]).toContain('fixed 5-minute window');
    expect(lines[1]).toContain('window');
    expect(lines).toHaveLength(4);
    expect(lines[2]).toContain('1.00');
    expect(lines[3]).toContain('n/a');
  });
});
