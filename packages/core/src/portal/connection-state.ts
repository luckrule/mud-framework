import type { TransportConnection } from '../transport/types.ts';
import type { ConnectionWriter } from './connection-writer.ts';

/**
 * Why work that never started was dropped after the layer closed a connection
 * for an overflowing event buffer (SPEC 3.8, 7.1). Shared by request admission
 * and event delivery so one phrase covers both, and an operator can tell "the
 * player left" from "we kicked the player" without reading the code path.
 */
export const BACKLOG_CLOSED_REASON =
  'the connection was closed after an event backlog overflow';

export interface ConnectionState {
  readonly connection: TransportConnection;
  /**
   * The connection's single writer (SPEC 3.8): request work runs first, one
   * queued event follows each completed request, and events fill the idle
   * moments between requests. Ordering state is per connection.
   */
  readonly writer: ConnectionWriter;
  /**
   * Requests accepted on this connection that are waiting for their turn,
   * excluding the one executing. The SPEC 3.5 cap is about this backlog: a
   * client that keeps writing while the handler lags is the case it exists for.
   *
   * Written only through `acquireSeat`, so a seat cannot be released twice.
   */
  queued: number;
  /**
   * Requests accepted on this connection that have not settled yet. A
   * disconnect cancels exactly these, so no work outlives the only client that
   * could read its result (SPEC 3.6) — unless the layer closed it for an event
   * backlog, in which case executing requests are kept so their outcome still
   * settles (SPEC 3.8).
   */
  readonly inFlight: Set<string>;
  /**
   * Aborts only waiting work; an unfinished Session barrier survives it.
   *
   * Aborted by `onDisconnect` and by `closeForBacklog`. The backlog close aborts
   * it at the decision, not when the transport reports the close: the writer's
   * drain continuation is a microtask while a transport's close event is a
   * macrotask, so waiting for the report leaves a window in which a queued
   * request would still start (SPEC 3.8).
   */
  readonly closed: AbortController;
  /**
   * Set when this layer closed the connection itself because an event buffer
   * overflowed (SPEC 3.8). The transport's `onDisconnect` still removes the
   * route, but it must not cancel requests already executing: the contract
   * keeps them running so their outcome is settled rather than turned into a
   * cancellation nobody asked for. Their result simply fails to write.
   *
   * It is also the fence for work that never started: a request and a queued
   * event frame both check it, and `closeForBacklog` sets it synchronously, so
   * nothing new begins on a connection this layer has decided to close.
   */
  backlogClosed: boolean;
}

/**
 * One request's claim on a connection's bounded backlog (SPEC 3.5).
 *
 * A seat is acquired when the frame is admitted and released on exactly one of
 * three paths: the work starts, the wait is abandoned (Session barrier or
 * closed connection), or shutdown drops the seat before the work ever runs. The
 * three are mutually exclusive today, but the release is idempotent so a future
 * path cannot decrement the count twice and silently widen the bound.
 */
export interface RequestSeat {
  release(): void;
}

export function acquireSeat(state: ConnectionState): RequestSeat {
  state.queued += 1;
  let released = false;
  return {
    release(): void {
      if (released) {
        return;
      }
      released = true;
      state.queued -= 1;
    },
  };
}
