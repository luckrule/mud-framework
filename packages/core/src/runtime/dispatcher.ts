import { randomUUID } from 'node:crypto';

import type { StorageBackend, StorageTransaction } from '../storage/types.ts';

export interface RequestContext {
  requestId: string;
  storage: StorageTransaction;
}

export type Handler = (
  ctx: RequestContext,
  payload: unknown,
) => Promise<unknown>;

export interface DispatcherOptions {
  backend: StorageBackend;
  timeoutMs: number;
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

export class Dispatcher {
  private readonly backend: StorageBackend;
  private readonly timeoutMs: number;
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
  }

  register(name: string, handler: Handler): void {
    this.assertOpen();
    this.handlers.set(name, handler);
  }

  async dispatch(name: string, payload: unknown): Promise<unknown> {
    this.assertOpen();
    const handler = this.handlers.get(name);
    if (handler === undefined) {
      throw new Error(`no handler registered for "${name}"`);
    }

    const requestId = randomUUID();
    const effectiveTimeoutMs = Math.min(
      this.timeoutMs,
      this.backend.capabilities().maxTransactionDurationMs,
    );
    const expiry: Expiry = { expired: false };
    const timeout = Promise.withResolvers<never>();
    const timer = setTimeout(() => {
      expiry.expired = true;
      timeout.reject(new FrameworkTimeoutError(requestId, effectiveTimeoutMs));
    }, effectiveTimeoutMs);

    const work = this.backend.transaction(async (tx) => {
      const context: RequestContext = {
        requestId,
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
