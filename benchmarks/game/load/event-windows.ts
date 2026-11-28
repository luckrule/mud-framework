import { EVENT_WINDOW_MS, Histogram, eventWindowIndex } from './support.ts';
import type { EventWindowJson } from './support.ts';

/**
 * Client-side event window bookkeeping, kept out of `client.ts` so the
 * half-open measurement filter and the sparse window serialization can be
 * tested without starting a generator process.
 */

export interface MeasuredWindow {
  readonly fromMs: number;
  readonly untilMs: number;
}

/**
 * Measurement is half-open: a token sent exactly at `untilMs` belongs to the
 * next 5-minute bucket but outside the run, so it is not a sample.
 */
export function withinMeasuredWindow(
  sentAtMs: number,
  window: MeasuredWindow,
): boolean {
  return sentAtMs >= window.fromMs && sentAtMs < window.untilMs;
}

export function recordWindowedEvent(
  windows: Map<number, Histogram>,
  sentAtMs: number,
  measuredFromMs: number,
  latencyNs: number,
): void {
  const index = eventWindowIndex(sentAtMs, measuredFromMs);
  const existing = windows.get(index);
  if (existing === undefined) {
    const histogram = new Histogram();
    histogram.record(latencyNs);
    windows.set(index, histogram);
  } else {
    existing.record(latencyNs);
  }
}

export function serializeEventWindows(
  windows: ReadonlyMap<number, Histogram>,
  measuredFromMs: number,
): readonly EventWindowJson[] {
  return [...windows.entries()]
    .toSorted((left, right) => left[0] - right[0])
    .map(([index, histogram]) => ({
      index,
      fromMs: measuredFromMs + index * EVENT_WINDOW_MS,
      untilMs: measuredFromMs + (index + 1) * EVENT_WINDOW_MS,
      histogram: histogram.toJson(),
    }));
}
