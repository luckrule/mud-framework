import type { SpawnedServer } from '../../ipc/spawn-port.ts';
import type { Logger } from '../../logging/types.ts';
import { DEFAULT_MAX_RESTART_BACKOFF_MS } from '../../shared/budget.ts';
import { describeValue } from '../../shared/guards.ts';
import {
  assertTimerDelay,
  MAX_TIMER_DELAY_MS,
} from '../connection-layer-options.ts';

/** The executor's timer options, checked as whole milliseconds at construction. */
export function assertExecutorTimerMs(
  value: unknown,
  name: string,
): asserts value is number {
  assertTimerDelay(value, name);
  if (!Number.isInteger(value)) {
    // A fractional delay is truncated by `setTimeout`, so the bound the caller
    // configured would not be the bound the executor waits out.
    throw new RangeError(
      `${name} must be an integer number of milliseconds, got ${String(value)}`,
    );
  }
}

/**
 * Resolves the restart backoff cap at construction. An omitted cap keeps the
 * historical default even when `baseBackoffMs` is larger, so a configuration
 * that predates this option behaves as it did. An explicit cap must be a safe
 * integer from 1 to `MAX_TIMER_DELAY_MS` and at least the base: above the timer
 * ceiling Bun clamps the delay to 1ms, and below the base the exponential
 * growth has no range.
 */
export function resolveMaxRestartBackoffMs(
  value: unknown,
  baseBackoffMs: number,
): number {
  if (value === undefined) {
    return DEFAULT_MAX_RESTART_BACKOFF_MS;
  }
  if (typeof value !== 'number') {
    throw new TypeError(
      `maxRestartBackoffMs must be a safe integer between 1 and ${MAX_TIMER_DELAY_MS} (2^31-1), got ${describeValue(value)}`,
    );
  }
  if (!Number.isSafeInteger(value) || value < 1 || value > MAX_TIMER_DELAY_MS) {
    throw new RangeError(
      `maxRestartBackoffMs must be a safe integer between 1 and ${MAX_TIMER_DELAY_MS} (2^31-1), got ${String(value)}`,
    );
  }
  if (value < baseBackoffMs) {
    throw new RangeError(
      `maxRestartBackoffMs must be at least restartBackoffMs (${String(baseBackoffMs)}), got ${String(value)}`,
    );
  }
  return value;
}

export interface IpcExecutorOptions {
  spawn: () => SpawnedServer;
  /** Portal-side per-request ceiling; effective = min(this, backend's declared max). */
  timeoutMs: number;
  /** Max replays of one request before it reaches a terminal state. Default 3. */
  maxReplays?: number;
  /** Base for exponential restart backoff in ms. Default 100. */
  restartBackoffMs?: number;
  /**
   * Ceiling for exponential restart backoff in ms. Default 5000.
   *
   * An explicit value must be a safe integer from 1 to `MAX_TIMER_DELAY_MS`
   * (2^31−1) and at least `restartBackoffMs`, checked at construction. The
   * upper bound is the largest `setTimeout` delay; Bun clamps anything above
   * it to 1ms, inverting a longer wait into the shortest. Omission keeps the
   * default ceiling, including when `restartBackoffMs` is larger.
   */
  maxRestartBackoffMs?: number;
  /**
   * How long a Server must stay up before its crash streak is considered over.
   * Defaults to `resolveStableResetMs(timeoutMs, maxRestartBackoffMs)`, i.e.
   * `Math.max(30_000, timeoutMs, maxRestartBackoffMs)`.
   *
   * The window must be at least as long as both the backoff cap and the
   * longest a single request can live: a Server that crashes while
   * handling its first request must be counted as a crash, not as a healthy
   * run that reset the backoff. Otherwise "ready, then die on the first
   * request" restarts at `attempt === 0` (immediate) forever.
   *
   * An explicit value must be an integer number of milliseconds from 1 to
   * `MAX_TIMER_DELAY_MS`, checked at construction. That range check does not
   * cover the relation to the cap or the request timeout, and an explicit
   * value bypasses the default formula: the caller still owns it (ADR-0004).
   */
  stableResetMs?: number;
  /**
   * Grace period for a graceful shutdown before killing. Default 5000ms.
   * An explicit value must be an integer number of milliseconds from 1 to
   * `MAX_TIMER_DELAY_MS`, checked at construction.
   */
  shutdownTimeoutMs?: number;
  /**
   * Overall readiness budget for the first `start()`, including crash retries,
   * and the handshake budget for each ordinary recovery generation. Each
   * generation's watchdog starts before `hello` and ends at `ready`; expiry is
   * terminal, with unconfirmed processes retained for close and late exit.
   * Manual switches use this bound separately for `prepared` and `ready`.
   * Defaults to `Math.max(10_000, restartBackoffMs * 10)`.
   */
  startTimeoutMs?: number;
  /**
   * Requests buffered but not yet acknowledged by a Server. Default 10000.
   *
   * Portal-side deadlines settle requests even while the Server is unavailable.
   * This caps concurrent buffer occupancy within those deadlines; the connection
   * layer's per-connection cap no longer applies after a request leaves its queue.
   */
  maxPending?: number;
  /**
   * Defaults to a JSON logger writing to stdout with
   * `component: 'ipc-executor'` bindings (SPEC §7.1). Supplying one only
   * redirects where the events go; observability does not depend on it.
   */
  logger?: Logger;
}
