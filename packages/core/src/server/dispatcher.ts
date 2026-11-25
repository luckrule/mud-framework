import { randomUUID } from 'node:crypto';

import { createJsonLogger } from '../logging/json-logger.ts';
import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { SessionChange } from '../shared/executor.ts';
import { describeValue, messageOf } from '../shared/guards.ts';
import type { RequestIdentity } from '../shared/identity.ts';
import { elapsedMs, tightenTimeout } from '../shared/timeout.ts';
import {
  AUTH_REQUIRED,
  AuthenticationRequiredError,
  FORBIDDEN,
  ForbiddenError,
  FrameworkTimeoutError,
  RequestCancelledError,
  UnknownHandlerError,
} from '../shared/wire-codes.ts';
import type { StorageBackend, StorageTransaction } from '../storage/types.ts';

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
 * works in process, and `LocalExecutor` is the one place that turns them into
 * the cross-process `ExecutorOutcome` union.
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

export interface DispatcherOptions {
  backend: StorageBackend;
  timeoutMs: number;
  /**
   * Optional access policy, called for every registered handler after the
   * Session check and before the transaction (SPEC 3.7). Returning false
   * refuses the request with `FORBIDDEN`; throwing is an internal failure.
   * Runs under the request deadline, so it cannot outlive the request.
   */
  authorize?: (
    ctx: AccessContext,
    method: string,
    payload: unknown,
  ) => boolean | Promise<boolean>;
  /** Defaults to `randomUUID`; injectable so tests get deterministic ids. */
  createRequestId?: () => string;
  /** Defaults to the JSON logger; tests inject a sink-backed one. */
  logger?: Logger;
}

/** Abnormal termination, separate from the storage handle's lifetime. */
interface Expiry {
  expired: boolean;
  readonly controller: AbortController;
}

interface StorageScope {
  active: boolean;
  /**
   * Set after a write fails on a backend that declares `transactions: 'none'`.
   * Such a backend cannot roll back, so later writes would land on top of a
   * half-applied request (SPEC 4.3). Scoped per request, never shared.
   */
  writeFailed: boolean;
}

/** What `register` remembers about a handler: the function and its contract. */
interface HandlerEntry {
  readonly handler: Handler;
  /** The owning Extension, if any, for request logs (SPEC 7.1). */
  readonly extension?: string;
  readonly access: HandlerAccess;
  readonly mayChangeSession: boolean;
}

/** A pending request: cancellation ends its scope and rejects dispatch. */
interface InFlight {
  readonly expiry: Expiry;
  readonly storageScope: StorageScope;
  /** Request-bound, so the cancellation line carries the same ids as the rest. */
  readonly logger: Logger;
  readonly startedAt: number;
  /** Settles the dispatch race as failed. A second call is a no-op. */
  readonly fail: (error: Error) => void;
}

const EXPIRED_MESSAGE =
  'storage transaction expired: the request already reached a terminal state';

const WRITE_ABORTED_MESSAGE =
  'write rejected: an earlier write in this request failed and the backend declares transactions "none" (no rollback), so later writes in this request are aborted';

export class Dispatcher {
  private readonly backend: StorageBackend;
  private readonly timeoutMs: number;
  private readonly createRequestId: () => string;
  private readonly logger: Logger;
  private readonly authorize: DispatcherOptions['authorize'];
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
    this.authorize = options.authorize;
    // A child rather than a default-only binding: an injected logger must still
    // carry `component: 'dispatcher'`, so every dispatcher line is identifiable
    // whatever the caller bound.
    this.logger = safeLogger(options.logger ?? createJsonLogger()).child({
      component: 'dispatcher',
    });
  }

  register(name: string, handler: Handler, options?: HandlerOptions): void {
    this.assertOpen();
    const access = options?.access;
    if (access !== undefined && access !== 'public' && access !== 'session') {
      throw new TypeError(
        `access must be 'public' or 'session', got ${describeValue(access)}`,
      );
    }
    const mayChangeSession = options?.mayChangeSession;
    if (
      mayChangeSession !== undefined &&
      typeof mayChangeSession !== 'boolean'
    ) {
      throw new TypeError(
        `mayChangeSession must be a boolean, got ${describeValue(mayChangeSession)}`,
      );
    }
    this.handlers.set(name, {
      handler,
      access: access ?? 'session',
      mayChangeSession: mayChangeSession ?? false,
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

    // SPEC 3.7: the Session gate runs before any request state is created, so
    // a refusal cannot open a transaction, execute the handler or affect the
    // routing table. Unknown methods were already answered above: adding a
    // Session must not turn "no such method" into AUTH_REQUIRED.
    if (
      registered.access !== 'public' &&
      (meta.sessionId === undefined || meta.sessionId.length === 0)
    ) {
      logger.warn(
        { code: AUTH_REQUIRED, method: name },
        'dispatch rejected: session required',
      );
      throw new AuthenticationRequiredError(name);
    }

    const capabilities = this.backend.capabilities();
    const effectiveTimeoutMs = tightenTimeout(
      Math.min(this.timeoutMs, capabilities.maxTransactionDurationMs),
      meta.timeoutMs,
    );
    const expiry: Expiry = {
      expired: false,
      controller: new AbortController(),
    };
    const storageScope: StorageScope = { active: true, writeFailed: false };
    // Both the timer and `cancel` settle this, and only the first settles it:
    // a request has exactly one terminal state (SPEC 2).
    const terminal = Promise.withResolvers<never>();
    const entry: InFlight = {
      expiry,
      storageScope,
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
      storageScope.active = false;
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

    // Recorded, not applied: the change is committed only when the handler
    // succeeds, so a failed request leaves the route untouched (SPEC 2). A
    // holder object rather than a `let`, because the handler closure assigns it
    // and control-flow narrowing would otherwise treat it as still undefined.
    const change: { value?: SessionChange } = {};

    try {
      if (this.authorize !== undefined) {
        await this.decideAccess(
          this.authorize,
          {
            requestId,
            connectionId,
            clientRequestId,
            sessionId: meta.sessionId,
            extension: registered.extension,
            signal: expiry.controller.signal,
            logger,
          },
          name,
          payload,
          terminal.promise,
        );
      }
      const work = this.backend.transaction(async (tx) => {
        const context: RequestContext = {
          requestId,
          connectionId,
          clientRequestId,
          sessionId: meta.sessionId,
          signal: expiry.controller.signal,
          extension: registered.extension,
          bindSession: (sessionId) => {
            assertSessionChangeAllowed(registered.mayChangeSession);
            assertActive(storageScope);
            if (sessionId.length === 0) {
              // An empty id cannot be routed, and the framework does not
              // invent one: the business owns what a session is (SPEC 2).
              throw new Error('bindSession requires a non-empty session id');
            }
            recordSessionChange(change, { type: 'bind', sessionId });
          },
          unbindSession: () => {
            assertSessionChangeAllowed(registered.mayChangeSession);
            assertActive(storageScope);
            recordSessionChange(change, { type: 'unbind' });
          },
          logger,
          storage: guard(
            tx,
            storageScope,
            capabilities.transactions === 'none',
          ),
        };
        try {
          const result = await registered.handler(context, payload);
          // The handler outlived its terminal state: force the backend to roll
          // back instead of committing effects nobody is waiting for
          // (SPEC 3.3, 3.4).
          if (expiry.expired) {
            throw new Error(EXPIRED_MESSAGE);
          }
          return result;
        } finally {
          storageScope.active = false;
        }
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
      const sessionChange = change.value;
      if (sessionChange === undefined) {
        return { result };
      }
      // The request's `sessionId` binding (if any) is the entry Session; the
      // change is always the object, never a replacement id.
      logger.debug({ sessionChange }, 'session change recorded');
      return { result, sessionChange };
    } finally {
      storageScope.active = false;
      clearTimeout(timer);
      if (this.inFlight.get(requestId) === entry) {
        this.inFlight.delete(requestId);
      }
    }
  }

  /**
   * Runs the injected access policy under the request deadline (SPEC 3.7). The
   * decision is raced against the terminal promise rather than given its own
   * timer: the request timer already started, so timeout and cancellation end
   * the request through the same path as any other terminal transition. The
   * `.then(onFulfilled, onRejected)` below consumes a policy rejection even
   * when the terminal promise wins the race, so a late rejection cannot surface
   * as an unhandled one and look like a crash.
   */
  private async decideAccess(
    authorize: NonNullable<DispatcherOptions['authorize']>,
    context: AccessContext,
    method: string,
    payload: unknown,
    terminal: Promise<never>,
  ): Promise<void> {
    const decision = Promise.resolve().then(() =>
      authorize(context, method, payload),
    );
    const raced = await Promise.race([
      decision.then(
        (allowed) => ({ type: 'decision' as const, allowed }),
        (error: unknown) => ({ type: 'policy-error' as const, error }),
      ),
      terminal.catch((error: unknown) => ({
        type: 'terminal' as const,
        error,
      })),
    ]);
    if (raced.type === 'terminal') {
      throw raced.error;
    }
    if (raced.type === 'policy-error') {
      // A policy failure is an internal failure, not a business refusal: its
      // code must not reach the wire even when it looks like a `BusinessError`
      // (SPEC 3.7, 7.2).
      throw new Error(
        `access policy failed for "${method}": ${messageOf(raced.error)}`,
        { cause: raced.error },
      );
    }
    const allowed: unknown = raced.allowed;
    if (allowed !== true) {
      if (allowed === false) {
        context.logger.warn(
          { code: FORBIDDEN, method },
          'dispatch rejected: access denied',
        );
        throw new ForbiddenError(method);
      }
      // A consumer policy is outside code: a truthy non-boolean is not an
      // allow, and treating it as one would turn a policy defect into access.
      throw new Error(
        `access policy for "${method}" returned ${describeValue(allowed)}; expected a boolean`,
      );
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
    entry.storageScope.active = false;
    // The caller's code is the reason: the handler sees the same value the
    // cancel carried (`TRANSPORT_CLOSED`, `FRAMEWORK_TIMEOUT`, ...), so a
    // disconnect and a Portal-side timeout stay distinguishable (SPEC 3.6).
    entry.expiry.controller.abort(code);
    entry.fail(new RequestCancelledError(requestId, code));
    // warn, not error: the framework kept running and nothing failed
    // internally, but someone should know a request was dropped mid-flight
    // (SPEC 7.1).
    entry.logger.warn(
      { code, durationMs: elapsedMs(entry.startedAt) },
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

  /** Read-only view for an owner that reports health without owning the flag. */
  isClosed(): boolean {
    return this.closed;
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
      durationMs: elapsedMs(startedAt),
    },
    'transaction committed after the request reached a terminal state',
  );
}

function guard(
  tx: StorageTransaction,
  scope: StorageScope,
  abortOnWriteFailure: boolean,
): StorageTransaction {
  return {
    async query<T>(sql: string, params?: readonly unknown[]): Promise<T[]> {
      assertActive(scope);
      return tx.query<T>(sql, params);
    },
    async execute(sql: string, params?: readonly unknown[]): Promise<void> {
      // The terminal state wins: an ended request reports expiry whatever its
      // earlier writes did (SPEC 3.3, 3.4).
      assertActive(scope);
      if (abortOnWriteFailure && scope.writeFailed) {
        throw new Error(WRITE_ABORTED_MESSAGE);
      }
      try {
        await tx.execute(sql, params);
      } catch (error) {
        if (abortOnWriteFailure) {
          scope.writeFailed = true;
        }
        // The handler sees the original error; the framework never replaces or
        // swallows the cause of the first failure.
        throw error;
      }
    },
  };
}

function assertActive(scope: StorageScope): void {
  if (!scope.active) {
    throw new Error(EXPIRED_MESSAGE);
  }
}

function assertSessionChangeAllowed(mayChangeSession: boolean): void {
  if (!mayChangeSession) {
    throw new Error(
      'handler must be registered with mayChangeSession: true to change the session',
    );
  }
}

/**
 * At most one change per request. Two changes have no defined meaning, and
 * letting the last call win would make the outcome depend on call order while
 * looking successful. A rejected request commits neither change.
 */
function recordSessionChange(
  holder: { value?: SessionChange },
  next: SessionChange,
): void {
  if (holder.value !== undefined) {
    throw new Error(
      `a request may change its session at most once: ${describeSessionChange(holder.value)} then ${describeSessionChange(next)}`,
    );
  }
  holder.value = next;
}

function describeSessionChange(change: SessionChange): string {
  return change.type === 'bind'
    ? `bindSession("${change.sessionId}")`
    : 'unbindSession()';
}
