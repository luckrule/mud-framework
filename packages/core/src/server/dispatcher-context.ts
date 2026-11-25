import type { Logger } from '../logging/types.ts';
import type { SessionChange } from '../shared/executor.ts';
import type { RequestIdentity } from '../shared/identity.ts';
import type { StorageTransaction } from '../storage/types.ts';

/**
 * What the caller knows about a request before it is dispatched. All optional:
 * a direct `dispatch` has no connection, and a replayed request supplies the
 * original `requestId` so the id survives the replay (SPEC 7.3).
 *
 * `sessionId` here means "this request already belongs to a Session", which is
 * not the same thing as the `SessionChange` a successful dispatch may report
 * ("the handler bound or unbound one").
 */
export type DispatchMeta = Partial<RequestIdentity> & {
  /**
   * A tighter deadline for this request only. It can shorten a request but
   * never extend one: the effective timeout is still
   * `min(this, configured timeoutMs, backend max)`, because the configured
   * value is the promise this dispatcher made to the rest of the framework and
   * a caller (or a Portal relaying its own budget) must not be able to relax it
   * (SPEC 3.4).
   */
  timeoutMs?: number;
};

export interface RequestContext extends RequestIdentity {
  /**
   * Aborts when the request reaches a terminal state before the handler
   * returns: timeout, cancellation, or disconnect (SPEC 3.6). It is a
   * notification, not a preemption — JS cannot stop running code, so a handler
   * that ignores it keeps running and only loses its transaction (SPEC 3.4).
   * Pass it to `fetch`, `Bun.sleep`, or anything else that accepts one.
   */
  readonly signal: AbortSignal;
  /**
   * Binds the current connection to a Session. The framework stores the mapping
   * and routes by it; it does not validate the id — who a player is, and when a
   * session starts or ends, belongs to the business (SPEC 2).
   *
   * Only a successful request commits the binding: a handler that throws or
   * times out leaves the connection unbound, so a failed login cannot leave a
   * half-created session behind. Calls after the handler settles are rejected.
   * Requires `mayChangeSession: true` on the handler's registration.
   */
  bindSession(sessionId: string): void;
  /**
   * Ends the current connection's Session binding. Like `bindSession`, it is
   * recorded for the request and applied only if the request succeeds, so a
   * failed unbind leaves the route in place. Requires `mayChangeSession: true`
   * and shares the one-change-per-request budget with `bindSession`.
   */
  unbindSession(): void;
  /** The Extension that registered this handler, when one did (SPEC 7.1). */
  readonly extension?: string;
  /** Already bound to the request's ids; handlers never build one. */
  logger: Logger;
  /** Ends with the handler's scope, or earlier on timeout or cancellation. */
  storage: StorageTransaction;
}

/**
 * What a successful dispatch yields: the handler's value plus the Session
 * change it declared, if any. Failures are still thrown — `instanceof` only
 * works in process, and the Server's dispatcher-backed executor is the one
 * place that turns them into the cross-process `ExecutorOutcome` union.
 *
 * The change was recorded, not applied: the caller owns the routing table and
 * commits it only after this result arrives (SPEC 2).
 */
export interface DispatchResult {
  /** The handler's return value. */
  result: unknown;
  /** Set when the handler bound or unbound a Session (SPEC 2). */
  sessionChange?: SessionChange;
}

export type Handler = (
  ctx: RequestContext,
  payload: unknown,
) => Promise<unknown>;

/** Whether a handler needs a Session on entry (SPEC 3.7). */
export type HandlerAccess = 'public' | 'session';

export interface HandlerOptions {
  /** The Extension that owns this handler, for request logs (SPEC 7.1). */
  readonly extension?: string;
  /**
   * Defaults to `'session'`: a handler is assumed to need an authenticated
   * Session unless it explicitly opts out with `'public'` (SPEC 3.7). Must be
   * exactly one of those two values.
   */
  readonly access?: HandlerAccess;
  /**
   * Defaults to false. Only a handler that declares true may call
   * `bindSession`/`unbindSession`; the declaration makes the mutation an
   * explicit part of the handler's contract rather than an accident. Must be a
   * boolean when provided.
   */
  readonly mayChangeSession?: boolean;
}

/**
 * What an access policy sees. Deliberately narrower than `RequestContext`: the
 * policy runs before the transaction exists, so it has no storage to write and
 * no Session change to declare (SPEC 3.7).
 */
export interface AccessContext extends RequestIdentity {
  /** The Extension that registered the handler, when one did (SPEC 7.1). */
  readonly extension?: string;
  /** Aborts at the request deadline, so a slow policy can stop early. */
  readonly signal: AbortSignal;
  readonly logger: Logger;
}
