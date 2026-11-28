import type { Logger } from '../logging/types.ts';
import type {
  BufferedServerEvent,
  ServerEventTarget,
} from '../shared/events.ts';
import type { SessionChange } from '../shared/executor.ts';
import { elapsedMs } from '../shared/timeout.ts';
import type { StorageTransaction } from '../storage/types.ts';

/** Abnormal termination, separate from the storage handle's lifetime. */
export interface Expiry {
  expired: boolean;
  readonly controller: AbortController;
}

export interface StorageScope {
  active: boolean;
  /**
   * Set after a write fails on a backend that declares `transactions: 'none'`.
   * Such a backend cannot roll back, so later writes would land on top of a
   * half-applied request (SPEC 4.3). Scoped per request, never shared.
   */
  writeFailed: boolean;
}

/** A pending request: cancellation ends its scope and rejects dispatch. */
export interface InFlight {
  readonly expiry: Expiry;
  readonly storageScope: StorageScope;
  /** Request-bound, so the cancellation line carries the same ids as the rest. */
  readonly logger: Logger;
  readonly startedAt: number;
  /** Settles the dispatch race as failed. A second call is a no-op. */
  readonly fail: (error: Error) => void;
}

/** At most one change per request; `recordSessionChange` enforces it. */
export interface SessionChangeHolder {
  value?: SessionChange;
}

/**
 * Default number of events one request may buffer (ADR-0008), overridable per
 * Server via `ServerProcessOptions.maxEventsPerRequest`. Buffered events are
 * bounded like every other per-request accumulation: an unbounded buffer would
 * let one handler pin memory until the request ends, and a flood of events is
 * far easier to produce than a flood of requests. Exceeding the bound is a
 * plain handler error (catchable), not a framework terminal state, because the
 * handler that emits in a loop is the one that has to decide what to drop.
 */
export const DEFAULT_MAX_EVENTS_PER_REQUEST = 1024;

/** Events recorded by one request, in emission order, under one shared cap. */
export interface EventBuffer {
  readonly events: BufferedServerEvent[];
  /**
   * Carried by the buffer rather than read from module state, so the cap is
   * fixed at request creation and a change of configuration cannot alter a
   * request already in flight.
   */
  readonly limit: number;
}

export function recordEvent(
  buffer: EventBuffer,
  scope: StorageScope,
  target: ServerEventTarget,
  event: string,
  payload: unknown,
): void {
  // The same gate as bindSession/storage: a request past its terminal state
  // must not enqueue anything, because no result will carry it (ADR-0008).
  assertActive(scope);
  if (event.length === 0) {
    throw new Error('emit requires a non-empty event name');
  }
  if (target.kind === 'session' && target.sessionId.length === 0) {
    throw new Error('emitToSession requires a non-empty session id');
  }
  if (buffer.events.length >= buffer.limit) {
    throw new Error(`a request may emit at most ${buffer.limit} events`);
  }
  buffer.events.push({ target, event, payload });
}

export const EXPIRED_MESSAGE =
  'storage transaction expired: the request already reached a terminal state';

export const WRITE_ABORTED_MESSAGE =
  'write rejected: an earlier write in this request failed and the backend declares transactions "none" (no rollback), so later writes in this request are aborted';

/**
 * Records a commit that landed after the request already had a terminal state
 * (a cancel or a timeout). Both are the same defect from the operator's side —
 * a write nobody is waiting for — so the line is named for the outcome, not for
 * one of the two causes.
 */
export function logLostCommit(logger: Logger, startedAt: number): void {
  logger.warn(
    {
      code: 'CANCEL_LOST_TO_COMMIT',
      durationMs: elapsedMs(startedAt),
    },
    'transaction committed after the request reached a terminal state',
  );
}

export function guard(
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

export function assertActive(scope: StorageScope): void {
  if (!scope.active) {
    throw new Error(EXPIRED_MESSAGE);
  }
}

export function assertSessionChangeAllowed(mayChangeSession: boolean): void {
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
export function recordSessionChange(
  holder: SessionChangeHolder,
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
