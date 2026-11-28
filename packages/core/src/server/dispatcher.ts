import { randomUUID } from 'node:crypto';

import { createJsonLogger } from '../logging/json-logger.ts';
import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import { describeValue, messageOf } from '../shared/guards.ts';
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
import type { StorageBackend } from '../storage/types.ts';
import type {
  AccessContext,
  DispatchMeta,
  DispatchResult,
  Handler,
  HandlerAccess,
  HandlerOptions,
  RequestContext,
} from './dispatcher-context.ts';
import {
  assertActive,
  assertSessionChangeAllowed,
  EXPIRED_MESSAGE,
  guard,
  logLostCommit,
  recordEvent,
  recordSessionChange,
  resolveMaxEventsPerRequest,
} from './dispatcher-scope.ts';
import type {
  EventBuffer,
  Expiry,
  InFlight,
  SessionChangeHolder,
  StorageScope,
} from './dispatcher-scope.ts';

export type {
  AccessContext,
  DispatchMeta,
  DispatchResult,
  Handler,
  HandlerAccess,
  HandlerOptions,
  RequestContext,
} from './dispatcher-context.ts';

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
  /**
   * How many events one request may buffer across `emit` and `emitToSession`
   * (SPEC 3.8). Defaults to 1024. Must be a safe integer of at least 1,
   * validated here at construction rather than per request; the cap is copied
   * into each request's buffer, so it cannot change while a request is in
   * flight.
   */
  maxEventsPerRequest?: number;
  /** Defaults to `randomUUID`; injectable so tests get deterministic ids. */
  createRequestId?: () => string;
  /** Defaults to the JSON logger; tests inject a sink-backed one. */
  logger?: Logger;
}

/** What `register` remembers about a handler: the function and its contract. */
interface HandlerEntry {
  readonly handler: Handler;
  /** The owning Extension, if any, for request logs (SPEC 7.1). */
  readonly extension?: string;
  readonly access: HandlerAccess;
  readonly mayChangeSession: boolean;
}

export class Dispatcher {
  private readonly backend: StorageBackend;
  private readonly timeoutMs: number;
  private readonly maxEventsPerRequest: number;
  private readonly createRequestId: () => string;
  private readonly logger: Logger;
  private readonly authorize: DispatcherOptions['authorize'];
  private readonly handlers = new Map<string, HandlerEntry>();
  /** Requests that have not reached a terminal state, by framework request id. */
  private readonly inFlight = new Map<string, InFlight>();
  private closed = false;
  private closePromise: Promise<void> | undefined;

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
    this.maxEventsPerRequest = resolveMaxEventsPerRequest(
      options.maxEventsPerRequest,
    );
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
    const startedAt = performance.now();
    const entry: InFlight = {
      expiry,
      storageScope,
      logger,
      startedAt,
      fail: (error) => terminal.reject(error),
    };
    // Registered before the handler can run so a cancel in the same batch finds
    // it. Ids are unique per request (SPEC 7.3) and the callers that can
    // duplicate one drop the duplicate before dispatching, so a same-id
    // overwrite would mean the caller broke that contract.
    this.inFlight.set(requestId, entry);

    // The flag alone is not enough: synchronous handler work holds the event
    // loop, so the timer cannot fire while the deadline passes. Both paths go
    // through `expire`, which settles the request exactly once.
    const deadlineAt = startedAt + effectiveTimeoutMs;
    const expire = (): void => {
      if (expiry.expired) {
        return;
      }
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
    };
    const timer = setTimeout(expire, effectiveTimeoutMs);

    // Ends the transaction callback (so the backend rolls back) whenever the
    // request is already terminal or outlived its deadline. A request that
    // waited for the backend's slot past its deadline must not start its
    // handler at all; one that blocked the timer must not commit its result.
    const assertNotExpired = (): void => {
      if (!expiry.expired && performance.now() >= deadlineAt) {
        expire();
      }
      if (expiry.expired) {
        throw new Error(EXPIRED_MESSAGE);
      }
    };

    // Recorded, not applied: the change is committed only when the handler
    // succeeds, so a failed request leaves the route untouched (SPEC 2). A
    // holder object rather than a `let`, because the handler closure assigns it
    // and control-flow narrowing would otherwise treat it as still undefined.
    const change: SessionChangeHolder = {};
    // Same rule for events (ADR-0008): buffered here, handed back only with a
    // committed result. A rejected dispatch drops the array with its frame.
    const eventBuffer: EventBuffer = {
      events: [],
      limit: this.maxEventsPerRequest,
    };

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
        // Waiting for the backend's slot can outlast the deadline (a 'single'
        // backend queues the callback). Starting the handler now would run
        // framework-external side effects for a request already settled.
        assertNotExpired();
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
          emit: (eventName, eventPayload) => {
            recordEvent(
              eventBuffer,
              storageScope,
              { kind: 'connection', connectionId },
              eventName,
              eventPayload,
            );
          },
          emitToSession: (targetSessionId, eventName, eventPayload) => {
            recordEvent(
              eventBuffer,
              storageScope,
              { kind: 'session', sessionId: targetSessionId },
              eventName,
              eventPayload,
            );
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
          // (SPEC 3.3, 3.4). Checked before this callback returns, so the
          // backend has not started its COMMIT yet; a synchronous handler that
          // blocked the timer is caught here as well.
          assertNotExpired();
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
      // The settlement time is recorded at the first dispatcher-visible moment:
      // a synchronous COMMIT can block past the deadline, and the check after
      // the race must use when the transaction actually settled rather than
      // when its continuation happened to run.
      let committedAt: number | undefined;
      void work.then(
        () => {
          committedAt = performance.now();
          if (expiry.expired) {
            logLostCommit(logger, entry.startedAt);
          }
          return undefined;
        },
        () => undefined,
      );
      // The terminal promise is listed first: when a pre-handler expiry and the
      // transaction rejection settle in the same turn, the request's timeout
      // (or cancel) must be the reported outcome, not the internal rollback
      // signal.
      const result = await Promise.race([terminal.promise, work]);
      // The deadline timer is a macrotask, so a synchronous COMMIT after the
      // transaction callback resolves keeps it from firing; the race above can
      // then report success for a request that already outlived its deadline.
      // The COMMIT cannot be aborted, but the request must still terminate as
      // a timeout: no result, session change or event may reach a caller that
      // stopped waiting (SPEC 3.4).
      if (
        !expiry.expired &&
        committedAt !== undefined &&
        committedAt >= deadlineAt
      ) {
        expire();
        // The transaction already committed, so the timeout terminal state must
        // not be read as a rollback (ADR-0004).
        logLostCommit(logger, entry.startedAt);
        throw new FrameworkTimeoutError(requestId, effectiveTimeoutMs);
      }
      const sessionChange = change.value;
      const events =
        eventBuffer.events.length === 0 ? undefined : eventBuffer.events;
      if (sessionChange === undefined && events === undefined) {
        return { result };
      }
      if (sessionChange !== undefined) {
        // The request's `sessionId` binding (if any) is the entry Session; the
        // change is always the object, never a replacement id.
        logger.debug({ sessionChange }, 'session change recorded');
      }
      return events === undefined
        ? { result, sessionChange }
        : {
            result,
            events,
            ...(sessionChange === undefined ? {} : { sessionChange }),
          };
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

  /**
   * Memoised so every caller observes the same close attempt, including a
   * failed one. A second call after a failed backend close must not return a
   * fresh resolved promise: that would report a shutdown that never happened.
   */
  close(): Promise<void> {
    this.closePromise ??= this.doClose();
    return this.closePromise;
  }

  private async doClose(): Promise<void> {
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
