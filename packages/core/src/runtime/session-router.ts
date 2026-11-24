import type { Logger } from '../logging/types.ts';
import { settleWithin } from './timeout.ts';

/**
 * Session routing: which connection holds a Session, and the wait that keeps a
 * reconnect from overlapping the previous connection's work (SPEC 3.2,
 * INV-004).
 *
 * Apart from the connection layer because it is the one piece of it that spans
 * connections and carries its own bound and refusal policy. It holds routing
 * information only — a Session's business state belongs to the business process
 * (SPEC 6.1).
 */

/**
 * Upper bound on the Session routes kept (SPEC 6.1). A route is removed when its
 * connection goes away, so this is not a steady-state target; it is a brake for
 * a business that never ends a Session and keeps minting ids, which would
 * otherwise grow the table until the process runs out of memory. 10k is the same
 * order as the executor's pending-buffer brake: one route per plausible
 * concurrent player rather than a capacity promise.
 */
export const DEFAULT_MAX_SESSIONS = 10_000;

/**
 * How long a reconnecting connection waits for the previous connection's work
 * to settle (SPEC 3.2, INV-004). A handler can hang forever — JS cannot preempt
 * running code (SPEC 3.4) — so this wait MUST be bounded: a late release makes
 * two processing lines overlap visibly, a permanent wait would stall the
 * Session. 10s matches the layer's other wait for in-flight work and covers a
 * healthy commit or rollback. Deliberately independent of the shutdown timeout:
 * one bounds a runtime routing wait, the other a shutdown step, and tuning one
 * must not silently change the other.
 */
export const DEFAULT_SESSION_HANDOFF_TIMEOUT_MS = 10_000;

/**
 * What the router needs to know about a connection it does not own.
 *
 * Methods rather than properties because both answers are read at call time:
 * `chain()` returns the tail as it stands now, and the tail advances with every
 * request the connection accepts. A property would freeze whichever promise
 * existed when the holder object was built, so a handoff would wait on work the
 * previous connection had already finished and miss the work it accepted after
 * the rebind. The router keeps the holder object, never the connection itself.
 */
export interface SessionHolder {
  readonly connectionId: string;
  /** Tail of that connection's work chain; the handoff waits on it. */
  chain(): Promise<unknown>;
  /** Resolves when the connection closes, so a handoff wait can stop early. */
  closed(): Promise<void>;
}

/**
 * Set on the connection that rebinds a Session another connection still holds
 * (a reconnect). The next request there waits for the previous holder's chain to
 * settle before it starts, so one Session never has two processing lines
 * (SPEC 3.2, INV-004). The wait is bounded by the router's handoff timeout: a
 * handler nobody can preempt would otherwise stall the Session forever
 * (SPEC 3.4).
 */
export interface SessionGate {
  readonly sessionId: string;
  readonly previousConnectionId: string;
  /**
   * The previous holder's chain tail as it stood when the rebind was recorded;
   * already caught, so it never rejects. A fallback: the wait prefers the live
   * tail when the caller still tracks that connection.
   */
  readonly settled: Promise<unknown>;
}

export interface SessionRouterOptions {
  readonly maxSessions: number;
  readonly handoffTimeoutMs: number;
  readonly logger: Logger;
}

/**
 * Which connection currently holds a Session, and the tail of that connection's
 * chain at the time it bound. Only routing information: the Session's business
 * state belongs to the business process (SPEC 6.1).
 */
interface SessionRoute {
  connectionId: string;
  chain: Promise<unknown>;
}

export class SessionRouter {
  private readonly maxSessions: number;
  private readonly handoffTimeoutMs: number;
  private readonly logger: Logger;
  /** Session ID → the connection that holds it. */
  private readonly routes = new Map<string, SessionRoute>();
  /**
   * The reverse of `routes`: which Session each connection holds. A connection
   * holds one at a time, so this answers "what does this connection hold"
   * without scanning the table — a disconnect must stay O(1) (SPEC 6.1).
   */
  private readonly heldBy = new Map<string, string>();
  /**
   * The holder per routed connection, so a handoff can read the live chain tail
   * of the previous connection. Dropped with the route: a holder the layer no
   * longer tracks must not be waited on.
   */
  private readonly holders = new Map<string, SessionHolder>();

  constructor(options: SessionRouterOptions) {
    this.maxSessions = options.maxSessions;
    this.handoffTimeoutMs = options.handoffTimeoutMs;
    this.logger = options.logger;
  }

  /**
   * Records that `holder` holds `sessionId` (SPEC 2, 3.2), returning the gate to
   * apply when the binding landed on a Session another connection still holds
   * (INV-004). Refusing a binding never fails the request that carried it —
   * routing is not the business result — and leaves the holder with whatever it
   * held before, which the caller reads back through `sessionOf`.
   *
   * `logger` defaults to the router's own; the connection layer passes the
   * request-scoped one so these lines carry the request id (SPEC 7.1).
   */
  bind(
    holder: SessionHolder,
    sessionId: string,
    logger: Logger = this.logger,
  ): SessionGate | undefined {
    const connectionId = holder.connectionId;
    const route = this.routes.get(sessionId);
    if (route?.connectionId === connectionId) {
      // Same connection, same Session, still the holder: only the chain tail
      // moved. Not logged, or a long-lived connection would emit one line per
      // request.
      this.routes.set(sessionId, { connectionId, chain: holder.chain() });
      this.holders.set(connectionId, holder);
      return undefined;
    }
    if (route === undefined && this.routes.size >= this.maxSessions) {
      // Refused rather than evicting: evicting a live Session would send its
      // next reconnect to the wrong connection. The request itself still
      // succeeds, because a routing limit is not a business failure.
      logger.warn(
        { sessionId, connectionId, limit: this.maxSessions },
        'session binding refused: session table is full',
      );
      return undefined;
    }
    // A connection holds one Session at a time; binding a new one drops the old
    // route so the table does not accumulate ids this connection has left.
    this.release(holder);
    // A route held by another connection is a reconnect, including the case
    // where this connection used to hold the Session and another one has taken
    // it over since.
    let gate: SessionGate | undefined;
    if (route !== undefined) {
      logger.warn(
        {
          sessionId,
          previousConnectionId: route.connectionId,
          connectionId,
        },
        'session rebound to a new connection',
      );
      // The previous connection no longer holds this Session. Dropping its
      // reverse entry keeps `sessionOf` honest for a connection that has been
      // taken over but has not rebound yet.
      if (this.heldBy.get(route.connectionId) === sessionId) {
        this.heldBy.delete(route.connectionId);
      }
      // The binding request itself is not delayed: it already ran. Only the
      // next request on this connection waits for the old chain to settle.
      gate = {
        sessionId,
        previousConnectionId: route.connectionId,
        settled: this.holders.get(route.connectionId)?.chain() ?? route.chain,
      };
    }
    this.routes.set(sessionId, { connectionId, chain: holder.chain() });
    this.heldBy.set(connectionId, sessionId);
    this.holders.set(connectionId, holder);
    logger.info({ sessionId, connectionId }, 'session bound');
    return gate;
  }

  /** The Session `connectionId` holds, if the router still routes one to it. */
  sessionOf(connectionId: string): string | undefined {
    return this.heldBy.get(connectionId);
  }

  /**
   * Drops the route this connection holds, if it is still the holder. Safe for
   * a connection the router does not know: a disconnect after a refused binding
   * has nothing to release.
   *
   * The holder is always forgotten, even when another connection has taken the
   * Session over: a handoff must read the live chain of a connection the layer
   * still tracks, and this connection may be gone.
   */
  release(holder: SessionHolder): void {
    const { connectionId } = holder;
    this.holders.delete(connectionId);
    const sessionId = this.heldBy.get(connectionId);
    if (sessionId === undefined) {
      return;
    }
    this.heldBy.delete(connectionId);
    // Only if this connection is still the holder: another one may have taken
    // the Session over since, and its route must survive.
    if (this.routes.get(sessionId)?.connectionId === connectionId) {
      this.routes.delete(sessionId);
    }
  }

  /**
   * Waits until the connection that held `gate.sessionId` before `holder` has no
   * work left, or until the bound expires (SPEC 3.2, INV-004).
   *
   * `holder` is the connection that will start next: racing its closure releases
   * the wait as soon as nobody can read the result, so a reconnect-then-drop does
   * not pin shutdown's drain for the whole handoff timeout.
   */
  async waitForHandoff(
    gate: SessionGate,
    holder: SessionHolder,
  ): Promise<void> {
    // The live tail, not the one captured at bind time: the previous connection
    // may have accepted more requests since, and those are exactly the work that
    // must reach a terminal state first. The captured tail is the fallback for a
    // holder the router no longer tracks.
    const previous = this.holders.get(gate.previousConnectionId);
    const settled = previous?.chain() ?? gate.settled;
    const waited = await settleWithin(
      Promise.race([settled, holder.closed()]),
      this.handoffTimeoutMs,
    );
    if (waited.status === 'timeout') {
      // The previous holder may be stuck on a handler nobody can preempt
      // (SPEC 3.4). Releasing is the lesser evil: a permanent wait would stall
      // the Session, and the overlap is visible in this line.
      this.logger.warn(
        {
          sessionId: gate.sessionId,
          previousConnectionId: gate.previousConnectionId,
          timeoutMs: this.handoffTimeoutMs,
        },
        'session handoff timed out; releasing the next request',
      );
    }
  }

  /**
   * Drops every route (SPEC 6.1). Routes outlive neither their connections nor
   * the router: leaving them behind would make a restarted layer route a
   * reconnect to a connection id that no longer exists.
   */
  clear(): void {
    this.routes.clear();
    this.heldBy.clear();
    this.holders.clear();
  }
}
