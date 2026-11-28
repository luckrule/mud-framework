import type { SpawnedServer } from '../../ipc/spawn-port.ts';
import type { Logger } from '../../logging/types.ts';
import { assertTimerDelay } from '../connection-layer-options.ts';

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

export interface IpcExecutorOptions {
  spawn: () => SpawnedServer;
  /** Portal-side per-request ceiling; effective = min(this, backend's declared max). */
  timeoutMs: number;
  /** Max replays of one request before it reaches a terminal state. Default 3. */
  maxReplays?: number;
  /** Base for exponential restart backoff in ms. Default 100, cap 5000. */
  restartBackoffMs?: number;
  /**
   * How long a Server must stay up before its crash streak is considered over.
   * Defaults to `Math.max(30_000, timeoutMs)`.
   *
   * The window must be at least as long as both the backoff cap (5000ms) and
   * the longest a single request can live: a Server that crashes while
   * handling its first request must be counted as a crash, not as a healthy
   * run that reset the backoff. Otherwise "ready, then die on the first
   * request" restarts at `attempt === 0` (immediate) forever.
   *
   * An explicit value must be an integer number of milliseconds from 1 to
   * `MAX_TIMER_DELAY_MS`, checked at construction. That range check does not
   * cover this relation: the caller still owns it (ADR-0004).
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
