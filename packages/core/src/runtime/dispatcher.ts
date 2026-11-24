import { randomUUID } from 'node:crypto';

import { createJsonLogger } from '../logging/json-logger.ts';
import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { StorageBackend, StorageTransaction } from '../storage/types.ts';
import type { RequestIdentity } from './identity.ts';
import { tightenTimeout } from './timeout.ts';
import {
  FrameworkTimeoutError,
  RequestCancelledError,
  UnknownHandlerError,
} from './wire-codes.ts';

/*
 * The error types live in `wire-codes.ts`, beside the codes they carry: a code
 * module that imported its own error classes from here would be a cycle. They
 * are re-exported so the existing `import ... from './dispatcher.ts'` call
 * sites (and the package surface in `index.ts`) keep working.
 */
export { FrameworkTimeoutError, RequestCancelledError, UnknownHandlerError };

/**
 * What the caller knows about a request before it is dispatched. All optional:
 * a direct `dispatch` has no connection, and a replayed request supplies the
 * original `requestId` so the id survives the replay (SPEC 7.3).
 *
 * `sessionId` here means "this request already belongs to a Session", which is
 * not the same thing as `DispatchResult.boundSessionId` ("the handler bound
 * this request to one").
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
   * half-created session behind.
   */
  bindSession(sessionId: string): void;
  /** The Extension that registered this handler, when one did (SPEC 7.1). */
  readonly extension?: string;
  /** Already bound to the request's ids; handlers never build one. */
  logger: Logger;
  storage: StorageTransaction;
}

/**
 * What a successful dispatch yields: the handler's value plus the Session it
 * bound, if any. Failures are still thrown — `instanceof` only works in
 * process, and `LocalExecutor` is the one place that turns them into the
 * cross-process `ExecutorOutcome` union.
 *
 * `boundSessionId` rather than `sessionId`: the input `DispatchMeta.sessionId`
 * says "this request already belongs to a Session", and reusing the name for
 * the output would suggest the result merely echoes the input.
 */
export interface DispatchResult {
  /** The handler's return value. */
  result: unknown;
  /** Set when the handler bound the connection to a Session (SPEC 2). */
  boundSessionId?: string;
}

export type Handler = (
  ctx: RequestContext,
  payload: unknown,
) => Promise<unknown>;

export interface HandlerOptions {
  /** The Extension that owns this handler, for request logs (SPEC 7.1). */
  readonly extension?: string;
}

export interface DispatcherOptions {
  backend: StorageBackend;
  timeoutMs: number;
  /** Defaults to `randomUUID`; injectable so tests get deterministic ids. */
  createRequestId?: () => string;
  /** Defaults to the JSON logger; tests inject a sink-backed one. */
  logger?: Logger;
}

/**
 * A request's terminal-state flag plus the signal that announces it. Both live
 * here because every set point must do both: `expired` is what `guard` enforces
 * (a write after the deadline MUST fail), while the abort is only a
 * notification that lets a cooperative handler stop early (SPEC 3.6).
 */
interface Expiry {
  expired: boolean;
  readonly controller: AbortController;
}

/** What `register` remembers about a handler: the function and its Extension. */
interface HandlerEntry {
  readonly handler: Handler;
  /** The owning Extension, if any, for request logs (SPEC 7.1). */
  readonly extension?: string;
}

/**
 * One request that has not reached a terminal state yet. `cancel` needs the
 * expiry (to invalidate later writes) and the resolver (to settle the dispatch
 * promise), neither of which is reachable from outside `dispatch` otherwise.
 */
interface InFlight {
  readonly expiry: Expiry;
  /** Request-bound, so the cancellation line carries the same ids as the rest. */
  readonly logger: Logger;
  readonly startedAt: number;
  /** Settles the dispatch race as failed. A second call is a no-op. */
  readonly fail: (error: Error) => void;
}

const EXPIRED_MESSAGE =
  'storage transaction expired: the request already reached a terminal state';

export class Dispatcher {
  private readonly backend: StorageBackend;
  private readonly timeoutMs: number;
  private readonly createRequestId: () => string;
  private readonly logger: Logger;
  private readonly handlers = new Map<string, HandlerEntry>();
  /** Requests that have not reached a terminal state, by framework request id. */
  private readonly inFlight = new Map<string, InFlight>();
  private closed = false;

  constructor(options: DispatcherOptions) {
    // SPEC 3.4 makes a per-request timeout mandatory; Infinity would also make
    // setTimeout fire on the next tick rather than never.
    if (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0) {
      throw new Error(
        `timeoutMs must be a positive finite number, got ${options.timeoutMs}`,
      );
    }
    this.backend = options.backend;
    this.timeoutMs = options.timeoutMs;
    this.createRequestId = options.createRequestId ?? randomUUID;
    // A child rather than a default-only binding: an injected logger must still
    // carry `component: 'dispatcher'`, so every dispatcher line is identifiable
    // whatever the caller bound.
    this.logger = safeLogger(options.logger ?? createJsonLogger()).child({
      component: 'dispatcher',
    });
  }

  register(name: string, handler: Handler, options?: HandlerOptions): void {
    this.assertOpen();
    this.handlers.set(name, {
      handler,
      ...(options?.extension === undefined
        ? {}
        : { extension: options.extension }),
    });
  }

  /**
   * The Extension that registered `method`, when one did (SPEC 7.1). Used by the
   * caller that reports request completion, which logs outside this class.
   */
  extensionOf(method: string): string | undefined {
    return this.handlers.get(method)?.extension;
  }

  async dispatch(
    name: string,
    payload: unknown,
    meta: DispatchMeta = {},
  ): Promise<DispatchResult> {
    this.assertOpen();
    // The supplied id wins: a replay must keep the id the original request
    // got on entry (SPEC 7.3), and generating a fresh one would split one
    // player input across two unrelated log trails.
    const requestId = meta.requestId ?? this.createRequestId();
    const connectionId = meta.connectionId ?? '';
    const clientRequestId = meta.clientRequestId ?? '';
    const registered = this.handlers.get(name);
    const bindings: Record<string, unknown> = {
      requestId,
      connectionId,
      clientRequestId,
    };
    if (meta.sessionId !== undefined) {
      bindings.sessionId = meta.sessionId;
    }
    // Only when a handler has one: a request no Extension owns must not carry
    // the field at all (SPEC 7.1).
    if (registered?.extension !== undefined) {
      bindings.extension = registered.extension;
    }
    const logger = this.logger.child(bindings);

    if (registered === undefined) {
      logger.warn({ method: name }, 'dispatch rejected: unknown method');
      throw new UnknownHandlerError(name);
    }

    const effectiveTimeoutMs = tightenTimeout(
      Math.min(
        this.timeoutMs,
        this.backend.capabilities().maxTransactionDurationMs,
      ),
      meta.timeoutMs,
    );
    const expiry: Expiry = {
      expired: false,
      controller: new AbortController(),
    };
    // Both the timer and `cancel` settle this, and only the first settles it:
    // a request has exactly one terminal state (SPEC 2).
    const terminal = Promise.withResolvers<never>();
    const entry: InFlight = {
      expiry,
      logger,
      startedAt: performance.now(),
      fail: (error) => terminal.reject(error),
    };
    // Registered before the handler can run so a cancel in the same batch finds
    // it. Ids are unique per request (SPEC 7.3) and the callers that can
    // duplicate one drop the duplicate before dispatching, so a same-id
    // overwrite would mean the caller broke that contract.
    this.inFlight.set(requestId, entry);

    const timer = setTimeout(() => {
      // Same guard as `finally`: a second dispatch of the same id overwrote the
      // map entry, and deleting it here would make that running request
      // uncancellable.
      if (this.inFlight.get(requestId) === entry) {
        this.inFlight.delete(requestId);
      }
      expiry.expired = true;
      // A reason, not just the fact: a handler that reacts to the abort may
      // want to tell a timeout from a disconnect, and a string keeps the two
      // cases distinguishable without the handler matching on error types.
      // `abort` is idempotent, so a later cancel for the same request needs no
      // guard here.
      expiry.controller.abort('timeout');
      logger.warn(
        { method: name, timeoutMs: effectiveTimeoutMs },
        'request timed out',
      );
      terminal.reject(new FrameworkTimeoutError(requestId, effectiveTimeoutMs));
    }, effectiveTimeoutMs);

    // Recorded, not applied: the binding is committed only when the handler
    // succeeds, so a failed login leaves the connection unbound (SPEC 2). A
    // holder object rather than a `let`, because the handler closure assigns it
    // and control-flow narrowing would otherwise treat it as still undefined.
    const binding: { sessionId?: string } = {};

    try {
      const work = this.backend.transaction(async (tx) => {
        const context: RequestContext = {
          requestId,
          connectionId,
          clientRequestId,
          sessionId: meta.sessionId,
          signal: expiry.controller.signal,
          extension: registered.extension,
          bindSession: (sessionId) => {
            assertActive(expiry);
            if (sessionId.length === 0) {
              // An empty id cannot be routed, and the framework does not
              // invent one: the business owns what a session is (SPEC 2).
              throw new Error('bindSession requires a non-empty session id');
            }
            if (binding.sessionId !== undefined) {
              // Two ids in one request have no defined meaning, and letting the
              // last call win would make the binding depend on call order while
              // looking successful. Failing the request keeps it diagnosable and
              // leaves the connection unbound.
              throw new Error(
                `bindSession called twice in one request: "${binding.sessionId}" then "${sessionId}"`,
              );
            }
            binding.sessionId = sessionId;
          },
          logger,
          storage: guard(tx, expiry),
        };
        const result = await registered.handler(context, payload);
        // The handler outlived its terminal state: force the backend to roll
        // back instead of committing effects nobody is waiting for
        // (SPEC 3.3, 3.4).
        if (expiry.expired) {
          throw new Error(EXPIRED_MESSAGE);
        }
        return result;
      });
      // After a timeout or cancel the abandoned transaction settles later, and
      // its rejection must not surface as an unhandled rejection. The commit
      // side is not silent, though: a backend commits after the handler returns,
      // so a cancel or timeout that landed in that window still commits, and
      // without a line here that outcome is indistinguishable from a rollback.
      void work.then(
        () =>
          expiry.expired ? logLostCommit(logger, entry.startedAt) : undefined,
        () => undefined,
      );
      const result = await Promise.race([work, terminal.promise]);
      const boundSessionId = binding.sessionId;
      if (boundSessionId === undefined) {
        return { result };
      }
      logger.debug({ sessionId: boundSessionId }, 'session bound');
      return { result, boundSessionId };
    } finally {
      clearTimeout(timer);
      if (this.inFlight.get(requestId) === entry) {
        this.inFlight.delete(requestId);
      }
    }
  }

  /**
   * Invalidates a request's transaction and settles it as cancelled. The
   * handler keeps running (JS cannot preempt it) but every later write is
   * rejected, and the transaction rolls back when the handler returns
   * (SPEC 3.3, 3.6). `ctx.signal` also aborts with `code` as its reason, so a
   * handler that listens can stop early instead of only discovering the
   * terminal state at its next write. Returns false when the request already
   * reached a terminal state, or never existed here.
   *
   * `true` means the cancellation was accepted, NOT that the transaction rolled
   * back. `StorageBackend.transaction` commits after its callback resolves, so a
   * handler that had already returned can still commit in the window before the
   * backend's COMMIT finishes; that outcome is logged at warn with
   * `code: 'CANCEL_LOST_TO_COMMIT'` instead of being silently swallowed.
   */
  cancel(requestId: string, code: string): boolean {
    const entry = this.inFlight.get(requestId);
    if (entry === undefined) {
      return false;
    }
    // Removed before the race continuation runs, so a second cancel for the
    // same id already sees a terminal request.
    this.inFlight.delete(requestId);
    entry.expiry.expired = true;
    // The caller's code is the reason: the handler sees the same value the
    // cancel carried (`TRANSPORT_CLOSED`, `FRAMEWORK_TIMEOUT`, ...), so a
    // disconnect and a Portal-side timeout stay distinguishable (SPEC 3.6).
    entry.expiry.controller.abort(code);
    entry.fail(new RequestCancelledError(requestId, code));
    // warn, not error: the framework kept running and nothing failed
    // internally, but someone should know a request was dropped mid-flight
    // (SPEC 7.1).
    entry.logger.warn(
      { code, durationMs: Math.round(performance.now() - entry.startedAt) },
      'request cancelled',
    );
    return true;
  }

  async close(): Promise<void> {
    if (this.closed) {
      return;
    }
    this.closed = true;
    await this.backend.close();
  }

  private assertOpen(): void {
    if (this.closed) {
      throw new Error('dispatcher is closed');
    }
  }
}

/**
 * Records a commit that landed after the request already had a terminal state
 * (a cancel or a timeout). Both are the same defect from the operator's side —
 * a write nobody is waiting for — so the line is named for the outcome, not for
 * one of the two causes.
 */
function logLostCommit(logger: Logger, startedAt: number): void {
  logger.warn(
    {
      code: 'CANCEL_LOST_TO_COMMIT',
      durationMs: Math.round(performance.now() - startedAt),
    },
    'transaction committed after the request reached a terminal state',
  );
}

function guard(tx: StorageTransaction, expiry: Expiry): StorageTransaction {
  return {
    async query<T>(sql: string, params?: readonly unknown[]): Promise<T[]> {
      assertActive(expiry);
      return tx.query<T>(sql, params);
    },
    async execute(sql: string, params?: readonly unknown[]): Promise<void> {
      assertActive(expiry);
      return tx.execute(sql, params);
    },
  };
}

function assertActive(expiry: Expiry): void {
  if (expiry.expired) {
    throw new Error(EXPIRED_MESSAGE);
  }
}
