import { messageOf } from '../internal/guards.ts';
import type { LogFields, Logger } from '../logging/types.ts';
import type { ExecutorOutcome, ExecutorRequest } from './executor.ts';
import { failure, requestMessage } from './ipc-messages.ts';
import type { PendingBuffer, PendingRequest } from './ipc-pending.ts';
import type { ServerState } from './server-slot.ts';
import { FRAMEWORK_REPLAY_EXHAUSTED, FRAMEWORK_TIMEOUT } from './wire-codes.ts';

/**
 * What the ledger needs from its owner. Everything here is a read or a
 * notification: the ledger never caches one of these values, so a decision it
 * makes is the owner's state at that moment.
 */
export interface DeliveryHost {
  readonly logger: Logger;
  /** Max replays of one request before it reaches a terminal state. */
  readonly maxReplays: number;
  /** True once no Server may receive work any more (shutdown, failed start). */
  readonly isTerminal: boolean;
  /** The Server a cancel may be addressed to; `undefined` between generations. */
  currentServer(): ServerState | undefined;
  /** A send failed: the owner decides whether that is a crash or a retirement. */
  onServerLost(
    server: ServerState,
    code: number | null,
    signal: string | null,
    reason: string,
  ): void;
}

export interface DeliveryLedgerOptions {
  buffer: PendingBuffer;
  host: DeliveryHost;
}

/**
 * Delivery and settlement for the unacknowledged buffer: the SPEC 6.5 delivery
 * loop, the replay budget, per-request deadlines, the cancel send, and the one
 * place a pending request's promise is resolved.
 *
 * Split from the executor because all of it is policy over the buffer and the
 * channel, none of it is the state machine: which Server may receive arrives as
 * an argument, whether the executor is shutting down and what a dead channel
 * means arrive through `DeliveryHost`. `status`, `server` and `switching` are
 * not visible here, so delivery cannot start a restart or promote a generation
 * on its own.
 */
export class DeliveryLedger {
  private readonly buffer: PendingBuffer;
  private readonly host: DeliveryHost;
  /** Woken when the buffer empties; the close path waits on it to drain. */
  private drainedWaiter: (() => void) | undefined;

  constructor(options: DeliveryLedgerOptions) {
    this.buffer = options.buffer;
    this.host = options.host;
  }

  /**
   * Delivers everything `undelivered` reports for `server`, in the order it
   * reports them (SPEC 6.5).
   *
   * `server` is the caller's decision: a switch is executor state, and
   * delivering to the process it is draining would race the retirement. The
   * terminal check defaults to the host, which reads it live, so a caller that
   * already resolved the same state may pass its own answer instead.
   *
   * A request that keeps killing the process starves the later requests on its
   * connection: every flush stops at it (its replay budget runs out, and the
   * flush returns), so those requests wait until it reaches a terminal state.
   * That is the necessary cost of the SPEC 6.5 ordering guarantee, not a drop.
   */
  flush(
    server: ServerState | undefined,
    isTerminal: boolean = this.host.isTerminal,
  ): void {
    if (server === undefined || !server.ready) {
      return;
    }
    if (isTerminal) {
      return;
    }
    for (const pending of this.buffer.undelivered(server.generation)) {
      if (!this.deliver(server, pending)) {
        return;
      }
    }
  }

  /** Returns false when the channel died and delivery must stop. */
  deliver(server: ServerState, pending: PendingRequest): boolean {
    if (this.buffer.get(pending.request.requestId) !== pending) {
      return true;
    }
    const isReplay = pending.sentOn !== undefined;
    // The count is only committed after a successful send: a send that dies on
    // a closing channel did not reach any Server and must not burn budget.
    const replayCount = isReplay ? pending.replayCount + 1 : 0;
    if (replayCount > this.host.maxReplays) {
      // SPEC 6.5: a request that keeps killing the process must stop being
      // retried, otherwise one bad input becomes a crash loop.
      this.host.logger.error(
        {
          ...requestFields(pending.request),
          replayCount: pending.replayCount,
          code: FRAMEWORK_REPLAY_EXHAUSTED,
        },
        'request replay exhausted',
      );
      this.settle(
        pending,
        failure(
          FRAMEWORK_REPLAY_EXHAUSTED,
          `request ${pending.request.requestId} exceeded ${this.host.maxReplays} replays`,
        ),
      );
      return true;
    }
    try {
      server.channel.send(
        requestMessage(
          pending.request,
          Math.max(1, pending.deadlineAt - Date.now()),
          replayCount,
        ),
      );
    } catch (error) {
      this.host.onServerLost(
        server,
        null,
        null,
        `send failed: ${messageOf(error)}`,
      );
      return false;
    }
    pending.replayCount = replayCount;
    pending.sentOn = server.generation;
    if (isReplay) {
      this.host.logger.warn(
        {
          ...requestFields(pending.request),
          replayCount,
          durationMs: Date.now() - pending.startedAt,
        },
        'request replayed',
      );
    }
    return true;
  }

  /** (Re)arms the deadline timer; a tightened deadline replaces the old one. */
  armTimer(pending: PendingRequest): void {
    if (pending.timer !== undefined) {
      clearTimeout(pending.timer);
    }
    const remaining = Math.max(0, pending.deadlineAt - Date.now());
    pending.timer = setTimeout(() => {
      this.onDeadline(pending);
    }, remaining);
  }

  private onDeadline(pending: PendingRequest): void {
    if (this.buffer.get(pending.request.requestId) !== pending) {
      return;
    }
    pending.timer = undefined;
    // warn, not error: the connection layer records the failed outcome at error
    // level, and one request failure should not produce two error lines.
    this.host.logger.warn(
      {
        ...requestFields(pending.request),
        code: FRAMEWORK_TIMEOUT,
        durationMs: Date.now() - pending.startedAt,
      },
      'request timed out',
    );
    // The Server may still be working on it; tell it to stop. Best effort: the
    // terminal state is decided here, and a result that arrives later is
    // discarded by requestId.
    this.cancelOnServer(pending, FRAMEWORK_TIMEOUT);
    this.settle(
      pending,
      failure(
        FRAMEWORK_TIMEOUT,
        `request ${pending.request.requestId} timed out after ${pending.effectiveTimeoutMs}ms`,
      ),
    );
  }

  /**
   * Tells the Server working on `pending` to stop. Best effort and only for the
   * generation that holds it: a request delivered to a process that is already
   * gone has no one left to tell.
   */
  cancelOnServer(pending: PendingRequest, code: string): void {
    const server = this.host.currentServer();
    if (
      server === undefined ||
      !server.ready ||
      pending.sentOn !== server.generation
    ) {
      return;
    }
    try {
      server.channel.send({
        type: 'cancel',
        requestId: pending.request.requestId,
        code,
      });
    } catch (error) {
      this.host.logger.warn(
        {
          ...requestFields(pending.request),
          code,
          reason: messageOf(error),
        },
        'cancel could not be sent',
      );
    }
  }

  /**
   * Ends `pending` with `outcome`. The entry is dropped first, so a late result
   * or a second deadline cannot settle it again, and an empty buffer wakes the
   * close path's drain wait.
   */
  settle(pending: PendingRequest, outcome: ExecutorOutcome): void {
    if (this.buffer.get(pending.request.requestId) !== pending) {
      return;
    }
    this.buffer.take(pending.request.requestId);
    if (pending.timer !== undefined) {
      clearTimeout(pending.timer);
      pending.timer = undefined;
    }
    pending.resolve(outcome);
    if (this.buffer.isEmpty) {
      const drained = this.drainedWaiter;
      this.drainedWaiter = undefined;
      drained?.();
    }
  }

  /**
   * Settles every buffered request with one failure. `message` is the outcome
   * text the caller sees; `reason` is log-only and names the call site, because
   * several paths share a code and would otherwise look identical in the logs.
   */
  failAllPending(code: string, message: string, reason: string): void {
    for (const pending of this.buffer.values()) {
      // warn, not error: the connection layer records the failed outcome at
      // error level, and one request failure should not produce two error lines.
      this.host.logger.warn(
        {
          ...requestFields(pending.request),
          code,
          reason,
          durationMs: Date.now() - pending.startedAt,
        },
        'pending request failed',
      );
      this.settle(pending, failure(code, message));
    }
  }

  /** Resolves once the buffer is empty; resolves at once when it already is. */
  awaitPendingSettled(): Promise<void> {
    if (this.buffer.isEmpty) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.drainedWaiter = resolve;
    });
  }
}

/**
 * Request-level log bindings (SPEC 7.1): the ids every request carries plus the
 * Session its connection already held on entry.
 *
 * `sessionId` is omitted, not written as `undefined` or an empty string, when
 * the connection holds no Session: absence is the contract for "unbound"
 * (SPEC 2, identity.ts).
 *
 * Lives here rather than in the executor because the ledger writes most of the
 * request-level lines; the executor's admission lines share the same fields.
 */
export function requestFields(request: ExecutorRequest): LogFields {
  const fields: LogFields = {
    requestId: request.requestId,
    connectionId: request.connectionId,
    clientRequestId: request.clientRequestId,
  };
  if (request.sessionId !== undefined) {
    fields.sessionId = request.sessionId;
  }
  return fields;
}
