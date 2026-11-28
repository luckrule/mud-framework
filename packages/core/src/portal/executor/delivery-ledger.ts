import { failure, requestMessage } from '../../ipc/outbound.ts';
import type { ResultMessage } from '../../ipc/protocol.ts';
import type { LogFields, Logger } from '../../logging/types.ts';
import type { ExecutorOutcome } from '../../shared/executor.ts';
import { requestLogFields } from '../../shared/identity.ts';
import { messageOf } from '../../shared/validation.ts';
import {
  FRAMEWORK_REPLAY_EXHAUSTED,
  FRAMEWORK_SHUTDOWN,
  FRAMEWORK_TIMEOUT,
} from '../../shared/wire-codes.ts';
import type { PendingBuffer, PendingRequest } from './ipc-pending.ts';
import type { ServerState } from './server-slot.ts';

interface DeliveryLedgerOptions {
  buffer: PendingBuffer;
  logger: Logger;
  /** Max replays of one request before it reaches a terminal state. */
  maxReplays: number;
  /** True once no Server may receive work any more (shutdown, failed start). */
  isTerminal: () => boolean;
  /** The Server a cancel may be addressed to; `undefined` between generations. */
  currentServer: () => ServerState | undefined;
  /** A send failed: the owner decides whether that is a crash or a retirement. */
  onServerLost: (
    server: ServerState,
    code: number | null,
    signal: string | null,
    reason: string,
  ) => void;
}

/**
 * Delivery and settlement for the unacknowledged buffer: the SPEC 6.5 delivery
 * loop, the replay budget, per-request deadlines, the cancel send, result
 * acceptance, and the one place a pending request's promise is resolved.
 *
 * Split from the executor because all of it is policy over the buffer and the
 * channel, none of it is the state machine: which Server may receive arrives as
 * an argument, whether the executor is shutting down and what a dead channel
 * means arrive as callbacks. `status`, `server` and `switching` are not visible
 * here, so delivery cannot start a restart or promote a generation on its own.
 */
export class DeliveryLedger {
  private readonly buffer: PendingBuffer;
  private readonly logger: Logger;
  private readonly maxReplays: number;
  private readonly isTerminal: () => boolean;
  private readonly currentServer: () => ServerState | undefined;
  private readonly onServerLost: (
    server: ServerState,
    code: number | null,
    signal: string | null,
    reason: string,
  ) => void;
  /** Woken when the buffer empties; the close path waits on it to drain. */
  private drainedWaiter: (() => void) | undefined;

  constructor(options: DeliveryLedgerOptions) {
    this.buffer = options.buffer;
    this.logger = options.logger;
    this.maxReplays = options.maxReplays;
    this.isTerminal = options.isTerminal;
    this.currentServer = options.currentServer;
    this.onServerLost = options.onServerLost;
  }

  /**
   * Delivers everything `undelivered` reports for `server`, in the order it
   * reports them (SPEC 6.5).
   *
   * `server` is the caller's decision: a switch is executor state, and
   * delivering to the process it is draining would race the retirement. Whether
   * the executor is shutting down is read live through `isTerminal`.
   *
   * A request that keeps killing the process starves the later requests on its
   * connection: until its replay budget runs out, every flush dies on the send
   * that kills the process, so those requests wait for the next flush. Once the
   * budget is exhausted the request is settled and the same flush continues;
   * only a send that throws ends the flush. That is the necessary cost of the
   * SPEC 6.5 ordering guarantee, not a drop.
   */
  flush(server: ServerState | undefined): void {
    // `ready` alone is not enough: during a rollback the slot can still name the
    // generation the switch just retired. Sending there would race its exit and
    // drop the request on a closed channel; it stays buffered for the restart
    // instead (SPEC 6.5, INV-002).
    if (
      server === undefined ||
      !server.ready ||
      server.closed ||
      server.exited
    ) {
      return;
    }
    if (this.isTerminal()) {
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
    // A deadline that has already passed is terminal. Rounding it up to the
    // protocol's 1ms floor would deliver work whose result nobody can accept
    // (SPEC 3.4, INV-007); settle it here instead, exactly like the timer.
    const remaining = pending.deadlineAt - Date.now();
    if (remaining <= 0) {
      this.expire(pending);
      return true;
    }
    const isReplay = pending.sentOn !== undefined;
    // The count is only committed after a successful send: a send that dies on
    // a closing channel did not reach any Server and must not burn budget.
    const replayCount = isReplay ? pending.replayCount + 1 : 0;
    if (replayCount > this.maxReplays) {
      // SPEC 6.5: a request that keeps killing the process must stop being
      // retried, otherwise one bad input becomes a crash loop.
      this.logger.error(
        {
          ...requestLogFields(pending.request),
          replayCount: pending.replayCount,
          code: FRAMEWORK_REPLAY_EXHAUSTED,
        },
        'request replay exhausted',
      );
      this.settle(
        pending,
        failure(
          FRAMEWORK_REPLAY_EXHAUSTED,
          `request ${pending.request.requestId} exceeded ${this.maxReplays} replays`,
        ),
      );
      return true;
    }
    try {
      server.channel.send(
        requestMessage(pending.request, remaining, replayCount),
      );
    } catch (error) {
      this.onServerLost(server, null, null, `send failed: ${messageOf(error)}`);
      return false;
    }
    pending.replayCount = replayCount;
    pending.sentOn = server.generation;
    if (isReplay) {
      this.logger.warn(
        {
          ...requestLogFields(pending.request),
          replayCount,
          // Absolute receive time so the two processes' logs can be aligned;
          // `durationMs` answers "how long" instead (SPEC 7.3).
          receivedAt: pending.startedAt,
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

  /**
   * Accepts a `result` message: it settles the buffered entry, or is discarded
   * when the request already reached a Portal-decided terminal state (SPEC 7.3
   * keeps the id stable, so the correlation still works — there is just nothing
   * left to settle).
   *
   * `requestSessionId` is envelope metadata for logging only: it must never
   * reach the settled outcome. Splitting it off with `type` and `requestId` is
   * what keeps the outcome equal to `ExecutorOutcome`.
   */
  acceptResult(message: ResultMessage): void {
    const {
      type: _type,
      requestId: _requestId,
      requestSessionId,
      ...outcome
    } = message;
    const pending = this.buffer.get(message.requestId);
    if (pending === undefined) {
      const fields: LogFields = {
        requestId: message.requestId,
        reason: 'request is no longer buffered',
      };
      if (requestSessionId !== undefined) {
        fields.sessionId = requestSessionId;
      }
      this.logger.debug(fields, 'late result discarded');
      return;
    }
    const fields: LogFields = {
      ...requestLogFields(pending.request),
      durationMs: Date.now() - pending.startedAt,
    };
    // `sessionId` stays the entry Session from `requestLogFields`; a routing
    // change is reported separately, never as a replacement id.
    if (message.ok && message.sessionChange !== undefined) {
      fields.sessionChange = message.sessionChange;
    }
    this.logger.debug(fields, 'result received');
    this.settle(pending, outcome);
  }

  private onDeadline(pending: PendingRequest): void {
    if (this.buffer.get(pending.request.requestId) !== pending) {
      return;
    }
    this.expire(pending);
  }

  /**
   * Settles `pending` as a timeout: one log line, a best-effort cancel to the
   * Server that holds it, and the terminal outcome. Shared by the deadline
   * timer and delivery, so a deadline discovered to be already past produces
   * exactly the same observable result as one that fired on time.
   */
  private expire(pending: PendingRequest): void {
    // warn, not error: the connection layer records the failed outcome at error
    // level, and one request failure should not produce two error lines.
    this.logger.warn(
      {
        ...requestLogFields(pending.request),
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
    const server = this.currentServer();
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
      this.logger.warn(
        {
          ...requestLogFields(pending.request),
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
   * Fails the requests that never reached a live Server, so they do not burn
   * their whole deadline while the executor closes (SPEC 6.3 step 3).
   * `reason` is log-only and names the call site.
   */
  failUndelivered(reason: string): void {
    const server = this.currentServer();
    this.failWhere(
      (pending) =>
        server === undefined || server.closed || pending.sentOn === undefined,
      FRAMEWORK_SHUTDOWN,
      'ipc executor is closing',
      reason,
    );
  }

  /**
   * Tightens every buffered deadline to `declared` and re-arms its timer. Only
   * ever shortens: a later Server declaring a longer ceiling must not extend a
   * deadline the Portal already promised (SPEC 3.4).
   */
  tightenDeadlines(declared: number): void {
    for (const pending of this.buffer.values()) {
      const tightened = Math.min(pending.effectiveTimeoutMs, declared);
      if (tightened === pending.effectiveTimeoutMs) {
        continue;
      }
      pending.effectiveTimeoutMs = tightened;
      this.armTimer(pending);
    }
  }

  /**
   * Settles every buffered request with one failure. `message` is the outcome
   * text the caller sees; `reason` is log-only and names the call site, because
   * several paths share a code and would otherwise look identical in the logs.
   */
  failAllPending(code: string, message: string, reason: string): void {
    this.failWhere(() => true, code, message, reason);
  }

  /**
   * Fails every buffered request `predicate` selects.
   *
   * Removing the current entry during iteration is safe for a Map, and settling
   * only resolves a promise (the continuation runs as a microtask), so this
   * cannot mutate the map mid-loop. The per-request line is the only trace of
   * why a request that no Server result or deadline reaches ended.
   */
  private failWhere(
    predicate: (pending: PendingRequest) => boolean,
    code: string,
    message: string,
    reason: string,
  ): void {
    for (const pending of this.buffer.values()) {
      if (!predicate(pending)) {
        continue;
      }
      // warn, not error: the connection layer records the failed outcome at
      // error level, and one request failure should not produce two error lines.
      this.logger.warn(
        {
          ...requestLogFields(pending.request),
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
