import { randomUUID } from 'node:crypto';

import { jsonLoggerFactory } from '../logging/json-logger.ts';
import type { Logger, LoggerFactory } from '../logging/types.ts';
import type { StorageBackend, StorageTransaction } from '../storage/types.ts';

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

const EXPIRED_MESSAGE =
  'storage transaction expired: the request already reached a terminal state';

export class FrameworkTimeoutError extends Error {
  readonly code = 'FRAMEWORK_TIMEOUT';
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
 * A distinct type rather than a bare Error: the connection layer maps it to a
 * stable wire code, and callers must not have to match on message text to tell
 * "no such method" apart from a handler that failed (INV-014).
 */
export class UnknownHandlerError extends Error {
  readonly code = 'UNKNOWN_METHOD';
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

    const effectiveTimeoutMs = Math.min(
      this.timeoutMs,
      this.backend.capabilities().maxTransactionDurationMs,
    );
    const expiry: Expiry = { expired: false };
    const timeout = Promise.withResolvers<never>();
    const timer = setTimeout(() => {
      expiry.expired = true;
      logger.warn(
        { method: name, timeoutMs: effectiveTimeoutMs },
        'request timed out',
      );
      timeout.reject(new FrameworkTimeoutError(requestId, effectiveTimeoutMs));
    }, effectiveTimeoutMs);

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
      // The handler outlived its timeout: force the backend to roll back
      // instead of committing effects nobody is waiting for (SPEC 3.4).
      if (expiry.expired) {
        throw new Error(EXPIRED_MESSAGE);
      }
      return result;
    });

    try {
      // After a timeout the abandoned transaction settles later; mark its
      // rejection handled so it cannot surface as an unhandled rejection.
      void work.catch(() => undefined);
      return await Promise.race([work, timeout.promise]);
    } finally {
      clearTimeout(timer);
    }
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
