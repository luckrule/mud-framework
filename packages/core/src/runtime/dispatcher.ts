import { randomUUID } from 'node:crypto';

import { jsonLoggerFactory } from '../logging/json-logger.ts';
import type { Logger, LoggerFactory } from '../logging/types.ts';
import type { StorageBackend, StorageTransaction } from '../storage/types.ts';
import { FRAMEWORK_TIMEOUT, UNKNOWN_METHOD } from './wire-codes.ts';

/**
 * What the caller knows about a request before it is dispatched. All optional:
 * a direct `dispatch` has no connection, and a replayed request supplies the
 * original `requestId` so the id survives the replay (SPEC 7.3).
 */
export interface DispatchMeta {
  requestId?: string;
  connectionId?: string;
  sessionId?: string;
  clientRequestId?: string;
  /**
   * A tighter deadline for this request only. It can shorten a request but
   * never extend one: the effective timeout is still
   * `min(this, configured timeoutMs, backend max)`, because the configured
   * value is the promise this dispatcher made to the rest of the framework and
   * a caller (or a Portal relaying its own budget) must not be able to relax it
   * (SPEC 3.4).
   */
  timeoutMs?: number;
}

export interface RequestContext {
  /** Framework-assigned, never client-controlled, stable across replay. */
  requestId: string;
  /** Empty when the request did not arrive over a transport. */
  connectionId: string;
  /** The client's correlation key; empty when the caller supplied none. */
  clientRequestId: string;
  sessionId?: string;
  /** Already bound to the request's ids; handlers never build one. */
  logger: Logger;
  storage: StorageTransaction;
}

export type Handler = (
  ctx: RequestContext,
  payload: unknown,
) => Promise<unknown>;

export interface DispatcherOptions {
  backend: StorageBackend;
  timeoutMs: number;
  /** Defaults to `randomUUID`; injectable so tests get deterministic ids. */
  createRequestId?: () => string;
  /** Defaults to the JSON logger; tests inject a sink-backed one. */
  loggerFactory?: LoggerFactory;
}

interface Expiry {
  expired: boolean;
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

export class FrameworkTimeoutError extends Error {
  readonly code = FRAMEWORK_TIMEOUT;
  readonly requestId: string;
  readonly timeoutMs: number;

  constructor(requestId: string, timeoutMs: number) {
    super(`request ${requestId} timed out after ${timeoutMs}ms`);
    this.name = 'FrameworkTimeoutError';
    this.requestId = requestId;
    this.timeoutMs = timeoutMs;
  }
}

/**
 * A request ended because someone asked it to, not because it ran out of time.
 * A distinct type so the code stays the caller's (a connection layer sends
 * `TRANSPORT_CLOSED`, a Portal sends `FRAMEWORK_TIMEOUT`), rather than being
 * flattened into the timeout code (SPEC 7.2).
 */
export class RequestCancelledError extends Error {
  readonly code: string;
  readonly requestId: string;

  constructor(requestId: string, code: string) {
    super(`request ${requestId} was cancelled: ${code}`);
    this.name = 'RequestCancelledError';
    this.code = code;
    this.requestId = requestId;
  }
}

/**
 * A distinct type rather than a bare Error: the connection layer maps it to a
 * stable wire code, and callers must not have to match on message text to tell
 * "no such method" apart from a handler that failed (INV-014).
 */
export class UnknownHandlerError extends Error {
  readonly code = UNKNOWN_METHOD;
  readonly method: string;

  constructor(method: string) {
    super(`no handler registered for "${method}"`);
    this.name = 'UnknownHandlerError';
    this.method = method;
  }
}

export class Dispatcher {
  private readonly backend: StorageBackend;
  private readonly timeoutMs: number;
  private readonly createRequestId: () => string;
  private readonly logger: Logger;
  private readonly handlers = new Map<string, Handler>();
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
    this.logger = (options.loggerFactory ?? jsonLoggerFactory).create({
      bindings: { component: 'dispatcher' },
    });
  }

  register(name: string, handler: Handler): void {
    this.assertOpen();
    this.handlers.set(name, handler);
  }

  async dispatch(
    name: string,
    payload: unknown,
    meta: DispatchMeta = {},
  ): Promise<unknown> {
    this.assertOpen();
    // The supplied id wins: a replay must keep the id the original request
    // got on entry (SPEC 7.3), and generating a fresh one would split one
    // player input across two unrelated log trails.
    const requestId = meta.requestId ?? this.createRequestId();
    const connectionId = meta.connectionId ?? '';
    const clientRequestId = meta.clientRequestId ?? '';
    const bindings: Record<string, unknown> = {
      requestId,
      connectionId,
      clientRequestId,
    };
    if (meta.sessionId !== undefined) {
      bindings.sessionId = meta.sessionId;
    }
    const logger = this.logger.child(bindings);

    const handler = this.handlers.get(name);
    if (handler === undefined) {
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
    const expiry: Expiry = { expired: false };
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
      logger.warn(
        { method: name, timeoutMs: effectiveTimeoutMs },
        'request timed out',
      );
      terminal.reject(new FrameworkTimeoutError(requestId, effectiveTimeoutMs));
    }, effectiveTimeoutMs);

    try {
      const work = this.backend.transaction(async (tx) => {
        const context: RequestContext = {
          requestId,
          connectionId,
          clientRequestId,
          sessionId: meta.sessionId,
          logger,
          storage: guard(tx, expiry),
        };
        const result = await handler(context, payload);
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
      return await Promise.race([work, terminal.promise]);
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
   * (SPEC 3.3, 3.6). Returns false when the request already reached a terminal
   * state, or never existed here.
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

/**
 * Applies a per-request deadline to the dispatcher's own ceiling. Only a
 * smaller value wins, and an unusable one (NaN, zero, negative) is ignored
 * rather than propagated: `setTimeout(NaN)` fires on the next tick, so a bad
 * override would otherwise cancel every request immediately.
 */
function tightenTimeout(
  ceilingMs: number,
  overrideMs: number | undefined,
): number {
  if (
    overrideMs === undefined ||
    !Number.isFinite(overrideMs) ||
    overrideMs <= 0
  ) {
    return ceilingMs;
  }
  return Math.min(ceilingMs, overrideMs);
}
