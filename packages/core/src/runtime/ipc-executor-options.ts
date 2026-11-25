import type { SpawnedServer } from '../ipc/spawn-port.ts';
import type { Logger } from '../logging/types.ts';

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
   * The window must exceed both the backoff cap (5000ms) and the longest a
   * single request can live: a Server that crashes while handling its first
   * request must be counted as a crash, not as a healthy run that reset the
   * backoff. Otherwise "ready, then die on the first request" restarts at
   * `attempt === 0` (immediate) forever.
   */
  stableResetMs?: number;
  /** Grace period for a graceful shutdown before killing. Default 5000ms. */
  shutdownTimeoutMs?: number;
  /**
   * How long `start()` waits for the readiness barrier before failing.
   * Defaults to `Math.max(10_000, restartBackoffMs * 10)`.
   *
   * The value has to cover several restart attempts (a first crash may be a
   * one-off and must not fail the start) while still making a genuinely broken
   * Server visible in seconds rather than never: with the default backoff the
   * restarts inside 10s are immediate, 100, 200, 400, 800, 1600, 3200 and 5000
   * ms, so the default covers eight attempts.
   */
  startTimeoutMs?: number;
  /**
   * Requests buffered but not yet acknowledged by a Server. Default 10000.
   *
   * A pending request lives at most one deadline, so this is not a throughput
   * cap; it is the brake for the one case the deadline cannot bound: a Server
   * that stays unavailable while clients keep writing. Without it the buffer
   * grows until the Portal runs out of memory, and the connection layer's own
   * per-connection cap does not apply once a request left its queue.
   */
  maxPending?: number;
  /**
   * Opaque application code version. Stamped on every Request accepted here and
   * kept across replays; compared only to warn when a `ready` reports another
   * version. The framework never parses it or makes a decision from it
   * (SPEC 6.4). Must be a non-empty string when set.
   *
   * A caller-supplied `ExecutorRequest.codeVersion` must also be a non-empty
   * string; an invalid one fails that request with `INTERNAL_ERROR` instead of
   * being sent.
   */
  codeVersion?: string;
  /**
   * Defaults to a JSON logger writing to stdout with
   * `component: 'ipc-executor'` bindings (SPEC §7.1). Supplying one only
   * redirects where the events go; observability does not depend on it.
   */
  logger?: Logger;
}
