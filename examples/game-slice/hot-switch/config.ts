import type { GameVersion } from '../src/game.ts';

/** Everything the drill's timing, bounds and pass/fail criteria are made of. */

export const CLIENT_COUNT = 20;
/** Offered cadence per client. One command at a time, so a switch overlaps one. */
export const INTERVAL_MS = 120;
/**
 * Clients start a few ms apart. Started together they send one synchronised
 * burst per interval and leave the rest idle, so a ~30ms switch can complete
 * inside a gap and never see a request.
 */
export const STAGGER_MS = INTERVAL_MS / CLIENT_COUNT;
export const REQUEST_TIMEOUT_MS = 10_000;
export const CONTROL_TIMEOUT_MS = 30_000;
export const PROBE_TIMEOUT_MS = 5000;
/**
 * How long after `POST /switch` the in-window says are issued: late enough that
 * the Portal is already buffering for the replacement, early enough to stay
 * inside a switch that takes tens of ms.
 */
export const CROSS_PROBE_DELAY_MS = 5;
export const PORTAL_READY_TIMEOUT_MS = 15_000;
export const SERVER_PID_TIMEOUT_MS = 5000;
/**
 * ADR-0004's bound for an external `ConnectionLayer.stop()` at the Portal's
 * 5s shutdown grace: C + max(C, core close budget + slack) + C ~= 35s. Waiting
 * less would kill a Portal that is closing legally.
 */
export const PORTAL_STOP_BUDGET_MS = 36_000;
export const FORCE_EXIT_TIMEOUT_MS = 5000;
export const SERVER_EXIT_TIMEOUT_MS = 5000;
export const POLL_INTERVAL_MS = 25;
export const WARMUP_MS = 1500;
export const BETWEEN_SWITCHES_MS = 2000;
export const SETTLE_AFTER_LAST_SWITCH_MS = 1000;
export const EVENT_DRAIN_MS = 1000;
export const LOGOUT_TIMEOUT_MS = 1000;
export const CONTROL_PROBE_TIMEOUT_MS = 1500;
export const SCRIPT_BUDGET_MS = 90_000;
/** Client repeats (same clientRequestId) allowed per client per window. */
export const MAX_REPEATS_PER_WINDOW = 1;
/** UX ceiling for request/event p95, not a capacity promise. */
export const P95_LIMIT_MS = 500;
export const SWITCH_TARGETS: readonly GameVersion[] = ['v2', 'v1'];
/** Seed item in the start room, taken before the first switch. */
export const ANCHOR_ITEM = 'sword';
/** The quiet room the observation clients hold; nothing else enters it. */
export const TARGET_ROOM = 'smithy';
export const ROUTE_TO_TARGET = ['north', 'east'] as const;
/** The two rooms the roaming clients bounce between, one hop each way. */
export const FLEET_DOORS: Readonly<Record<string, string>> = {
  square: 'north',
  market: 'south',
};
export const FLEET_TARGETS: Readonly<Record<string, string>> = {
  north: 'market',
  south: 'square',
};
export const RETRYABLE_CODES: ReadonlySet<string> = new Set([
  'CLIENT_TIMEOUT',
  'FRAMEWORK_TIMEOUT',
]);
/**
 * Floors that prove the load actually ran: a broken loop cannot pass by
 * looking like a quiet but healthy run. The anchor's and watcher's extra
 * setup/verification commands are below these, so the loop itself must have
 * produced the rest.
 */
export const MIN_STATIONARY_LOOKS = 12;
export const MIN_STATIONARY_SAYS = 12;
export const MIN_FLEET_LOOKS = 6;
export const MIN_FLEET_SAYS = 6;
export const MIN_FLEET_MOVES = 6;
