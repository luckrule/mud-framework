import type {
  CancelMessage,
  IpcMessage,
  RequestMessage,
} from '../ipc/protocol.ts';
import type { LogFields, Logger } from '../logging/types.ts';
import type { ExecutorOutcome } from '../shared/executor.ts';
import { messageOf } from '../shared/guards.ts';
import type { RequestIdentity } from '../shared/identity.ts';
import { settleWithin } from '../shared/timeout.ts';
import {
  FRAMEWORK_SHUTDOWN,
  INTERNAL_ERROR,
  failureLogLevel,
} from '../shared/wire-codes.ts';
import type { Runtime } from './server-startup.ts';

interface PendingRequest {
  readonly message: RequestMessage;
  /** Set once the handler call has been made; a started request cannot be stopped. */
  started: boolean;
  /** Cancel code that arrived before the handler started: the handler never runs. */
  preStartCancel: string | undefined;
  /**
   * Cancel code that invalidated the transaction while the handler ran. The
   * handler keeps running, but its outcome is neither committed nor answered.
   */
  cancelledCode: string | undefined;
}

/**
 * How many duplicate request ids are remembered for log suppression. Bounded
 * because the set outlives individual requests: an unbounded one would grow for
 * the life of the process on a Portal that keeps replaying.
 */
const DUPLICATE_LOG_MEMORY = 100;

export interface RequestRunnerDeps {
  readonly runtime: Runtime;
  readonly logger: Logger;
  /**
   * Sends one message to Portal; `what` names it in the failure line. An
   * implementation must log a send failure (`IPC_SEND_FAILED`) rather than
   * propagate it: the link is gone, and Portal's replay buffer still holds
   * whatever it never acknowledged.
   */
  readonly send: (message: IpcMessage, what: string) => void;
  /**
   * Whether the process still takes new work. A refused request is answered
   * with `FRAMEWORK_SHUTDOWN` rather than dropped (SPEC 3.5).
   */
  readonly accepting: () => boolean;
  /**
   * This Server's opaque code version. Compared against a replay's stamped
   * version only to log a mismatch; it never changes the outcome (SPEC 6.4).
   */
  readonly codeVersion?: string;
}

/**
 * Owns the request half of the Server process: which requests are in flight,
 * which handlers are still running, and every log line and result that follows
 * from one of them (SPEC 3.6, 7.3).
 *
 * `drain` is here because it is the one consumer that needs both collections at
 * once. What the process itself reports about them — a count in a `hello` or
 * `shutdown` line, the ids in a stuck close — goes through the getters, which
 * exist only to fill those fields.
 */
export class RequestRunner {
  private readonly deps: RequestRunnerDeps;
  /** Accepted requests that have not reached a terminal state, by request id. */
  private readonly inFlight = new Map<string, PendingRequest>();
  /** Promises of accepted requests, so shutdown has a fixed set to wait for. */
  private readonly executing = new Set<Promise<void>>();
  /**
   * Request ids whose duplicate delivery has already been logged, most recent
   * last. Bounded by `DUPLICATE_LOG_MEMORY` and pruned on terminal state, so a
   * long-lived process cannot accumulate ids it will never see again.
   */
  private readonly duplicateLogged = new Set<string>();

  constructor(deps: RequestRunnerDeps) {
    this.deps = deps;
  }

  /** Count of requests that have not reached a terminal state. */
  get pending(): number {
    return this.inFlight.size;
  }

  /** Count of handler executions still running. */
  get running(): number {
    return this.executing.size;
  }

  /** Ids of the requests that have not reached a terminal state, oldest first. */
  get pendingRequestIds(): readonly string[] {
    return [...this.inFlight.keys()];
  }

  /** Accepts one request: duplicate check, then execution (SPEC 7.3). */
  accept(message: RequestMessage): void {
    // The duplicate check comes first: an id already in flight is owed its
    // real result even during shutdown, so answering it with a refusal would
    // report a failure for a request that may still succeed.
    if (this.inFlight.has(message.requestId)) {
      // Running the same id twice at once would split one input across two
      // transactions and two log trails (SPEC 7.3). The execution already in
      // flight still owes Portal a result for this id, so the duplicate is
      // dropped rather than queued behind it; Portal keeps it unacknowledged
      // until that result arrives. If that result cannot be sent, the channel
      // is closed and this process receives no further deliveries, so there is
      // no path where Portal waits for an answer that was never attempted.
      if (!this.duplicateLogged.has(message.requestId)) {
        this.rememberDuplicate(message.requestId);
        this.deps.logger.warn(
          {
            ...this.requestFields(message),
            code: 'DUPLICATE_REQUEST',
            replayCount: message.replayCount,
            reason: 'request is already in flight',
          },
          'duplicate delivery ignored',
        );
      }
      return;
    }
    this.warnIfReplayCodeVersionChanged(message);
    if (!this.deps.accepting()) {
      // SPEC 3.5: a refused input gets a stable code rather than silence.
      // Portal may already have the request on the wire and will not settle it
      // until a result or its own deadline arrives, so dropping it here would
      // spend the whole timeout budget to learn what this line already knows.
      this.deps.logger.warn(
        {
          ...this.requestFields(message),
          code: 'SERVER_SHUTTING_DOWN',
          reason: 'request arrived after shutdown started',
        },
        'request refused',
      );
      this.deps.send(
        {
          ...resultEnvelope(message),
          ok: false,
          error: {
            code: FRAMEWORK_SHUTDOWN,
            message: 'server is shutting down',
          },
        },
        'shutdown result',
      );
      return;
    }
    const entry: PendingRequest = {
      message,
      started: false,
      preStartCancel: undefined,
      cancelledCode: undefined,
    };
    this.inFlight.set(message.requestId, entry);
    // Added synchronously so a `shutdown` in the same batch has a fixed set of
    // requests to wait for, including ones still inside the pre-start window.
    const work = this.executeRequest(entry).catch((error: unknown) => {
      // `executeRequest` is written not to throw; this keeps one unexpected
      // throw from becoming an unhandled rejection in the IPC callback.
      this.deps.logger.error(
        {
          ...this.requestFields(message),
          code: INTERNAL_ERROR,
          reason: messageOf(error),
        },
        'request execution failed unexpectedly',
      );
    });
    this.executing.add(work);
    // `work` already maps every failure to a log line, so it never rejects and
    // a single fulfilled handler is enough to retire it from the drain set.
    void work.then(() => {
      this.executing.delete(work);
      return undefined;
    });
  }

  /** Cancels an in-flight request; no-op for unknown or finished ids (SPEC 3.6). */
  cancel(message: CancelMessage): void {
    const entry = this.inFlight.get(message.requestId);
    if (entry === undefined) {
      // Nothing to cancel: the request already reached a terminal state, or it
      // never arrived. Remembering the id would leak memory for the life of the
      // process and could suppress a legitimate replay, so it is only logged.
      this.deps.logger.debug(
        {
          requestId: message.requestId,
          code: message.code,
          reason: 'no request in flight',
        },
        'cancel ignored',
      );
      return;
    }
    if (entry.started) {
      // SPEC 3.3/3.6: a running handler cannot be preempted, but its
      // transaction can be invalidated. Every later write is rejected and the
      // transaction rolls back when the handler returns, so a cancel really
      // cancels instead of merely muting the answer. A `false` return means the
      // request went terminal in between; the outcome is then already owed or
      // already answered, so nothing is recorded.
      if (
        this.deps.runtime.dispatcher.cancel(message.requestId, message.code)
      ) {
        entry.cancelledCode = message.code;
        this.deps.logger.warn(
          {
            ...this.requestFields(entry.message),
            code: message.code,
            reason: 'transaction invalidated while the handler runs',
          },
          'cancel applied to a running handler',
        );
        return;
      }
      this.deps.logger.debug(
        {
          ...this.requestFields(entry.message),
          code: message.code,
          reason: 'request reached a terminal state before the cancel',
        },
        'cancel ignored',
      );
      return;
    }
    entry.preStartCancel = message.code;
    this.deps.logger.info(
      { ...this.requestFields(entry.message), code: message.code },
      'request cancelled before start',
    );
  }

  /**
   * Waits for the accepted requests to settle, but not forever. A backend whose
   * `close` awaits its queue (SQLite does) makes `executor.close()` wait for the
   * handler too, so an unbounded drain would postpone `closed` past any budget
   * Portal is willing to wait for. Unfinished requests are reported rather than
   * silently abandoned; their ids say which handler is holding the process.
   */
  async drain(limitMs: number): Promise<void> {
    const drained = await settleWithin(
      Promise.allSettled(this.executing),
      limitMs,
    );
    if (drained.status === 'timeout') {
      this.deps.logger.error(
        {
          code: 'SHUTDOWN_DRAIN_TIMEOUT',
          timeoutMs: limitMs,
          pendingRequests: this.pending,
          requestIds: this.pendingRequestIds,
        },
        'shutdown drain exceeded its budget; closing anyway',
      );
    }
  }

  /**
   * Records that a duplicate of `requestId` has been reported. Insertion order
   * is the age order, so the oldest entry is the first one evicted.
   */
  private rememberDuplicate(requestId: string): void {
    this.duplicateLogged.add(requestId);
    if (this.duplicateLogged.size > DUPLICATE_LOG_MEMORY) {
      const oldest = this.duplicateLogged.values().next();
      if (!oldest.done) {
        this.duplicateLogged.delete(oldest.value);
      }
    }
  }

  /**
   * A replay keeps the code version it was accepted under, so a difference
   * from this Server's version means the replacement may not behave the same.
   * The framework reports that instead of guessing compatibility; the Request
   * still runs unchanged because at-least-once is the whole promise (SPEC 6.4).
   */
  private warnIfReplayCodeVersionChanged(message: RequestMessage): void {
    const requestCodeVersion = message.codeVersion;
    const serverCodeVersion = this.deps.codeVersion;
    if (
      message.replayCount === 0 ||
      requestCodeVersion === undefined ||
      serverCodeVersion === undefined ||
      requestCodeVersion === serverCodeVersion
    ) {
      return;
    }
    this.deps.logger.warn(
      {
        ...this.requestFields(message),
        requestCodeVersion,
        serverCodeVersion,
        replayCount: message.replayCount,
      },
      'replayed request carries a different code version',
    );
  }

  /**
   * Log fields for one request: its ids, plus the Extension that registered the
   * handler when one did (SPEC 7.1). The registration metadata lives in the
   * dispatcher, so the lookup goes through it; a method no Extension owns gets
   * no `extension` key at all rather than an `undefined` one.
   */
  private requestFields(message: RequestMessage): LogFields {
    const fields: LogFields = { ...requestIds(message) };
    const extension = this.deps.runtime.dispatcher.extensionOf(message.method);
    if (extension !== undefined) {
      fields.extension = extension;
    }
    return fields;
  }

  /**
   * Runs one accepted request to a terminal state and reports it. Never
   * rejects: its only caller is the IPC callback, which has no one to hand a
   * failure to, and `run`'s shutdown wait relies on it settling.
   */
  private async executeRequest(entry: PendingRequest): Promise<void> {
    const { message } = entry;
    try {
      // One microtask of grace: a cancel queued in the same batch as its
      // request (in-memory delivery, or the channel's buffered flush at
      // registration) lands here and saves the handler from running at all.
      // Over real IPC each message is its own event-loop turn, so a cancel
      // usually arrives after `started` — that window is the only cancel that
      // can actually stop work (SPEC 3.4: JS cannot preempt a running handler),
      // and widening it would delay every request on the hot path.
      await Promise.resolve();
      if (entry.preStartCancel !== undefined) {
        this.deps.logger.info(
          {
            ...this.requestFields(message),
            code: entry.preStartCancel,
            reason: 'cancelled before the handler started',
          },
          'cancelled request skipped',
        );
        return;
      }
      entry.started = true;
      const startedAt = performance.now();
      const outcome = await this.executeSafely(message);
      if (
        entry.cancelledCode !== undefined &&
        !outcome.ok &&
        outcome.error.code === entry.cancelledCode
      ) {
        // `dispatcher.cancel` invalidated the transaction and rolled it back,
        // and Portal settled this id when it sent the cancel. A result now
        // would be a late answer nobody can match, so it is dropped; the
        // rollback is the visible outcome (SPEC 3.3, 3.6). The outcome code is
        // checked, not just the flag: a cancel that lost the race to the commit
        // left a real success behind, and suppressing that would hide it.
        this.deps.logger.warn(
          {
            ...this.requestFields(message),
            code: entry.cancelledCode,
            durationMs: elapsedSince(startedAt),
            reason: 'result suppressed after cancel',
          },
          'cancelled request finished; result not sent',
        );
        return;
      }
      this.report(message, outcome, elapsedSince(startedAt));
    } finally {
      this.inFlight.delete(message.requestId);
      // The id is terminal, so a later delivery of it is a fresh request rather
      // than a duplicate of this one and deserves its own log line.
      this.duplicateLogged.delete(message.requestId);
    }
  }

  private async executeSafely(
    message: RequestMessage,
  ): Promise<ExecutorOutcome> {
    try {
      // `RequestMessage` already has every `ExecutorRequest` field with the
      // same meaning, so it is passed as is; the extra `type`/`replayCount`
      // only matter to the wire and the excess-property check does not apply
      // to a non-literal argument. Portal's deadline stays authoritative: it
      // settles this id at that instant, so a Server that ran longer would
      // commit writes nobody is waiting for (SPEC 3.4, INV-007).
      return await this.deps.runtime.executor.execute(message);
    } catch (error) {
      // The executor contract forbids throwing, but an escaping throw still
      // has to produce a result, or Portal waits for one that never comes.
      return {
        ok: false,
        error: { code: INTERNAL_ERROR, message: messageOf(error) },
      };
    }
  }

  private report(
    message: RequestMessage,
    outcome: ExecutorOutcome,
    durationMs: number,
  ): void {
    if (outcome.ok) {
      const fields: LogFields = { ...this.requestFields(message), durationMs };
      const change = outcome.sessionChange;
      if (change !== undefined) {
        // `sessionId` already names the entry Session; the routing change is a
        // separate field so the two cannot be confused.
        fields.sessionChange = change;
      }
      this.deps.logger.debug(fields, 'request completed');
      // The outcome already has the shape of the message's payload half, so it
      // is spread rather than copied field by field. An unchanged success
      // carries `sessionChange: undefined` as an own property, which every
      // channel either drops (JSON) or keeps with the same meaning as absence
      // (SPEC 2).
      this.deps.send({ ...resultEnvelope(message), ...outcome }, 'result');
      return;
    }
    this.deps.logger[failureLogLevel(outcome.error)](
      {
        ...this.requestFields(message),
        code: outcome.error.code,
        reason: outcome.error.message,
        durationMs,
      },
      'request failed',
    );
    this.deps.send({ ...resultEnvelope(message), ...outcome }, 'result');
  }
}

/**
 * The correlation half of every result sent for `message`: its request id plus
 * the Session it entered with, echoed so the Portal can name the Session of a
 * result that arrives after its pending entry is gone. The echo is not an
 * outcome field; a Session change declared by the handler stays in
 * `outcome.sessionChange`.
 */
function resultEnvelope(message: RequestMessage): {
  type: 'result';
  requestId: string;
  requestSessionId?: string;
} {
  if (message.sessionId === undefined) {
    return { type: 'result', requestId: message.requestId };
  }
  return {
    type: 'result',
    requestId: message.requestId,
    requestSessionId: message.sessionId,
  };
}

/**
 * The identity fields of one request, ready to be used as log bindings. Shared
 * with the process-level lines (`REQUEST_BEFORE_READY` and friends) so the two
 * sides cannot drift into logging different ids for the same request.
 */
export function requestIds(message: RequestMessage): RequestIdentity {
  const ids: RequestIdentity = {
    requestId: message.requestId,
    connectionId: message.connectionId,
    clientRequestId: message.clientRequestId,
  };
  if (message.sessionId === undefined) {
    return ids;
  }
  return { ...ids, sessionId: message.sessionId };
}

function elapsedSince(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}
