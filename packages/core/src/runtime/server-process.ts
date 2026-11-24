import { messageOf } from '../internal/guards.ts';
import { createProcessChannel } from '../ipc/bun-child.ts';
import type { IpcChannel } from '../ipc/channel.ts';
import type {
  CancelMessage,
  HelloMessage,
  IpcMessage,
  RequestMessage,
  ShutdownMessage,
} from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import { checkBackendCapabilities } from '../storage/capabilities.ts';
import { degradations } from '../storage/degradations.ts';
import { unmetRequirements } from '../storage/requirements.ts';
import type { StorageBackend, StorageCapabilities } from '../storage/types.ts';
import { Dispatcher } from './dispatcher.ts';
import type { ExecutorOutcome } from './executor.ts';
import { LocalExecutor } from './executor.ts';
import type { Extension } from './extension.ts';
import type { RequestIdentity } from './identity.ts';
import { settleWithin } from './timeout.ts';
import {
  FRAMEWORK_SHUTDOWN,
  INTERNAL_ERROR,
  failureLogLevel,
} from './wire-codes.ts';

export interface ServerProcessOptions {
  /** Builds the storage backend. Called once per process start. */
  createBackend: () => Promise<StorageBackend> | StorageBackend;
  /**
   * Extensions in registration order. Each is checked against the backend
   * before its `register` runs (SPEC 4.3).
   */
  extensions: readonly Extension[];
  /** Framework-side request ceiling; effective = min(this, backend max). */
  timeoutMs: number;
  logger?: Logger;
  /**
   * The IPC channel; defaults to the real process channel.
   *
   * At most one `runServerProcess` per OS process may use the real channel:
   * `createProcessChannel` is a process-level singleton and a second one throws.
   * Two Servers in one process would receive each other's requests and both
   * write to storage (INV-002).
   */
  channel?: IpcChannel;
  /**
   * Overrides the budget for each abort step (see `ABORT_TIMEOUT_MS`). Exists
   * for tests, which would otherwise have to wait the real 5s; a deployment has
   * no reason to set it.
   */
  abortTimeoutMs?: number;
  /**
   * Overrides the parent-liveness poll interval. 0 disables the poll, which is
   * what a test harness with an in-memory channel needs: there is no real
   * parent to watch, and a background timer would outlive the test.
   */
  parentCheckIntervalMs?: number;
  /**
   * Reads the current parent pid. Defaults to `process.ppid`; injectable so a
   * test can simulate a reparented process, which cannot be done for real from
   * inside the test process.
   */
  readParentPid?: () => number;
}

/**
 * The process could not reach the readiness barrier: backend creation, the
 * capability self-check, an Extension's unmet requirements or its `register`
 * failed. `ready` was not sent, so Portal must not have started delivering
 * requests.
 */
export class ServerStartupError extends Error {
  readonly code = 'SERVER_STARTUP_FAILED';
  readonly reason: string;

  constructor(reason: string) {
    super(`server process failed to start: ${reason}`);
    this.name = 'ServerStartupError';
    this.reason = reason;
  }
}

/**
 * The link to Portal went away without `shutdown`. The process has no one left
 * to answer, so it stops; Portal is expected to restart it and replay.
 */
export class ServerChannelClosedError extends Error {
  readonly code = 'IPC_CHANNEL_CLOSED';
  readonly reason: string;

  constructor(reason: string) {
    super(`ipc channel closed before shutdown: ${reason}`);
    this.name = 'ServerChannelClosedError';
    this.reason = reason;
  }
}

interface Runtime {
  readonly dispatcher: Dispatcher;
  readonly executor: LocalExecutor;
  readonly capabilities: StorageCapabilities;
}

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
/**
 * Multiplier applied to `timeoutMs` when bounding the shutdown as a whole, on
 * the fallback path only (a Portal that did not declare a budget). The
 * dispatcher answers every request at `timeoutMs` even if its handler is still
 * running (SPEC 3.4), so a close that has not finished within twice that budget
 * is stuck on something the framework cannot stop waiting for. It is a total,
 * not a per-step value: drain and backend close share one deadline.
 */
const DRAIN_LIMIT_MULTIPLIER = 2;

/**
 * Budget for each step of an abort (Portal died, nobody can read a result).
 * Deliberately a fixed value unrelated to `timeoutMs`: the `4 * timeoutMs`
 * fallback used on the orderly path is 4 minutes at `timeoutMs = 60s`, which is
 * indistinguishable from waiting forever, and every second of it is a second
 * this orphaned process keeps writing to storage beside the next Server
 * (INV-002). Nobody is waiting for this process, so a short cap is the right
 * trade: a handler that cannot finish in 5s is abandoned rather than honoured.
 */
const ABORT_TIMEOUT_MS = 5000;

/**
 * How often the Server checks whether Portal is still alive. `process.ppid`
 * changing means the parent died and this process was reparented; the IPC
 * `disconnect` event does not fire for an abrupt parent death, so without this
 * poll an orphan could keep running (and writing) indefinitely.
 */
const PARENT_CHECK_INTERVAL_MS = 500;

/**
 * The Server side of the two-process split (ADR-0002): it owns Extensions,
 * handler execution and the only storage writer (INV-002).
 *
 * A library function rather than an entry point, because the Extension and the
 * backend belong to the business and the framework ships neither (INV-016,
 * SPEC 4.1): the business entry file calls this with its own backend and its own
 * registrations.
 *
 * The returned promise is the exit signal, since a library must not call
 * `process.exit` and a caller needs something observable to map to an exit code:
 *
 * - resolves after an orderly `shutdown` (the last message is `closed`, the
 *   backend is closed and the link is closed): exit 0;
 * - rejects with `ServerStartupError` when the process never became ready, and
 *   with `ServerChannelClosedError` when the link died without `shutdown`: exit
 *   non-zero so the Portal restart policy sees a failure.
 */
export function runServerProcess(options: ServerProcessOptions): Promise<void> {
  return new ServerProcess(options).run();
}

class ServerProcess {
  private readonly options: ServerProcessOptions;
  private readonly logger: Logger;
  private readonly channel: IpcChannel;
  private readonly completion = Promise.withResolvers<void>();
  /** Accepted requests that have not reached a terminal state, by request id. */
  private readonly inFlight = new Map<string, PendingRequest>();
  /** Promises of accepted requests, so shutdown has a fixed set to wait for. */
  private readonly running = new Set<Promise<void>>();
  private portalGeneration: number | undefined;
  /**
   * `accepting` until shutdown begins, then `shutting-down` for the rest of the
   * process's life. One field rather than two booleans because they only ever
   * changed together, and every read asks the same question: "may this process
   * still take new work?".
   */
  private phase: 'accepting' | 'shutting-down' = 'accepting';
  /**
   * True once the missing handshake has been reported for this Portal. A request
   * before `hello` means the timing is broken, and logging every one of them
   * would scale the log volume with the request rate (SPEC 7.1).
   */
  private missingHelloLogged = false;
  /**
   * Request ids whose duplicate delivery has already been logged, most recent
   * last. Bounded by `DUPLICATE_LOG_MEMORY` and pruned on terminal state, so a
   * long-lived process cannot accumulate ids it will never see again.
   */
  private readonly duplicateLogged = new Set<string>();
  /** Set while the parent-liveness poll is installed; cleared on completion. */
  private parentTimer: ReturnType<typeof setInterval> | undefined;
  /**
   * The running backend and dispatcher. Stored because the parent-liveness poll
   * fires outside the message flow, where the runtime is not a parameter.
   */
  private runtime: Runtime | undefined;

  constructor(options: ServerProcessOptions) {
    this.options = options;
    this.logger =
      options.logger ??
      createJsonLogger({ bindings: { component: 'server-process' } });
    this.channel =
      options.channel ?? createProcessChannel({ logger: this.logger });
    // A startup failure throws out of `run` before it awaits the completion,
    // so a later rejection there must not surface as an unhandled rejection.
    void this.completion.promise.catch(() => undefined);
  }

  async run(): Promise<void> {
    try {
      await this.start();
    } catch (error) {
      this.stopParentWatch();
      this.channel.close('startup failed');
      throw error;
    }
    // Installed after the readiness barrier: before it there is no storage open
    // that an orphan could write to, and a failed start is already reported.
    this.startParentWatch();
    try {
      await this.completion.promise;
    } finally {
      // Also on the failure paths (a closed channel rejects): a live interval
      // would keep the process alive after the caller already got its answer.
      this.stopParentWatch();
    }
  }

  /**
   * Watches for the Portal dying without closing the link. `process.ppid` is
   * read on an interval because an abrupt parent death (SIGKILL) reparents this
   * process without firing `disconnect`; without the poll the orphan would keep
   * running and, worse, keep writing to storage beside the next Server
   * (INV-002). The timer is unref'd so it never keeps the process alive.
   */
  private startParentWatch(): void {
    const intervalMs =
      this.options.parentCheckIntervalMs ?? PARENT_CHECK_INTERVAL_MS;
    if (intervalMs <= 0) {
      return;
    }
    const readParentPid = this.options.readParentPid ?? (() => process.ppid);
    const parentPid = readParentPid();
    this.parentTimer = setInterval(() => {
      if (readParentPid() === parentPid) {
        return;
      }
      this.stopParentWatch();
      this.onParentGone();
    }, intervalMs);
    this.parentTimer.unref();
  }

  private stopParentWatch(): void {
    if (this.parentTimer === undefined) {
      return;
    }
    clearInterval(this.parentTimer);
    this.parentTimer = undefined;
  }

  private onParentGone(): void {
    if (this.phase === 'shutting-down') {
      return;
    }
    this.phase = 'shutting-down';
    this.logger.error(
      { code: 'IPC_PARENT_GONE', reason: 'the parent process is gone' },
      'parent process gone; server stopping',
    );
    const runtime = this.runtime;
    if (runtime === undefined) {
      this.completion.reject(
        new ServerChannelClosedError('the parent process is gone'),
      );
      return;
    }
    void this.abort(runtime, 'the parent process is gone');
  }

  private async start(): Promise<void> {
    const backend = await this.createBackend();
    let dispatcher: Dispatcher | undefined;
    try {
      // SPEC 6.2 step 1: the backend self-checks its declaration before any
      // handler can see it. A declaration the framework cannot reason about is
      // refused here, not degraded into undefined later (INV-009, INV-010).
      const checked = checkBackendCapabilities(backend.capabilities());
      if (!checked.ok) {
        throw new ServerStartupError(
          `storage capability self-check failed: ${checked.reason}`,
        );
      }
      // SPEC 4.3: a backend weaker than the strongest level is a degradation,
      // reported once per dimension here so it stays at startup volume. The
      // `ready` info line below carries the full declaration; these lines carry
      // what each shortfall costs the business. Both are needed: one for the
      // whole picture, one per consequence.
      for (const degradation of degradations(checked.capabilities)) {
        this.logger.warn(
          {
            capability: degradation.capability,
            declared: degradation.declared,
            consequence: degradation.consequence,
          },
          'storage capability degraded',
        );
      }
      dispatcher = new Dispatcher({
        backend,
        timeoutMs: this.options.timeoutMs,
        logger: this.logger,
      });
      const runtime: Runtime = {
        dispatcher,
        executor: new LocalExecutor({ dispatcher }),
        capabilities: checked.capabilities,
      };
      this.runtime = runtime;
      // SPEC 6.2 step 2: every Extension is checked against the backend and
      // registered before Portal can deliver anything (INV-010).
      const skipped = await this.registerExtensions(
        dispatcher,
        checked.capabilities,
      );
      if (skipped > 0 && skipped === this.options.extensions.length) {
        // Still ready: a Server with no handlers answers every request with
        // UNKNOWN_METHOD, which is diagnosable, whereas refusing to start would
        // take down a process the Portal is waiting on. The line is the signal.
        this.logger.warn(
          { total: this.options.extensions.length, skipped },
          'no extension registered: every extension was skipped',
        );
      }
      // SPEC 6.2 step 3: only now is the readiness barrier crossed. Sending it
      // earlier would let the first requests hit a half-initialized Server.
      this.channel.send({
        type: 'ready',
        pid: process.pid,
        capabilities: runtime.capabilities,
      });
      this.logger.info(
        { pid: process.pid, capabilities: runtime.capabilities },
        'server process ready',
      );
      // Inbound plumbing goes last: a message that arrives before these
      // handlers are attached is buffered by the channel and delivered at
      // registration, so none is lost and none is handled before `ready`.
      this.channel.onMessage((raw) => {
        this.onMessage(runtime, raw);
      });
      this.channel.onClose((reason) => {
        this.onChannelClose(runtime, reason);
      });
    } catch (error) {
      const failure =
        error instanceof ServerStartupError
          ? error
          : new ServerStartupError(messageOf(error));
      this.logger.error(
        { code: failure.code, reason: failure.reason },
        'server process failed to start',
      );
      await closeQuietly(dispatcher, backend, this.logger);
      throw failure;
    }
  }

  /**
   * Checks each Extension against the backend declaration and registers the
   * ones it satisfies (SPEC 4.3, INV-010). Returns how many were skipped.
   *
   * A skipped Extension never has its `register` called, so none of its
   * handlers exist; one Extension being skipped does not affect the others. A
   * `required` Extension that fails is different: the process cannot run
   * without it, so startup is refused before `ready` (SPEC 6.2 step 2).
   */
  private async registerExtensions(
    dispatcher: Dispatcher,
    capabilities: StorageCapabilities,
  ): Promise<number> {
    let skipped = 0;
    for (const extension of this.options.extensions) {
      const unmet = unmetRequirements(capabilities, extension.requires ?? {});
      if (unmet.length === 0) {
        // oxlint-disable-next-line eslint/no-await-in-loop -- registration order is the contract: a later Extension may rely on an earlier one's handlers
        await extension.register(dispatcher);
        this.logger.info({ extension: extension.name }, 'extension registered');
        continue;
      }
      if (extension.required === true) {
        this.logger.error(
          { extension: extension.name, unmet },
          'required extension cannot start',
        );
        throw new ServerStartupError(
          `required extension "${extension.name}" cannot start: ${unmet.join('; ')}`,
        );
      }
      skipped += 1;
      this.logger.warn(
        { extension: extension.name, unmet },
        'extension skipped: storage capabilities unmet',
      );
    }
    return skipped;
  }

  private async createBackend(): Promise<StorageBackend> {
    try {
      return await this.options.createBackend();
    } catch (error) {
      const failure = new ServerStartupError(
        `storage backend could not be created: ${messageOf(error)}`,
      );
      this.logger.error(
        { code: failure.code, reason: failure.reason },
        'server process failed to start',
      );
      throw failure;
    }
  }

  private onMessage(runtime: Runtime, raw: unknown): void {
    const parsed = parseIpcMessage(raw);
    if (!parsed.ok) {
      // warn, not error: one unreadable message is a rejected input, and
      // dropping it must not take the process (or the rest of the batch) down.
      this.logger.warn(
        { code: 'INVALID_IPC_MESSAGE', reason: parsed.reason },
        'ipc message rejected',
      );
      return;
    }
    const message = parsed.message;
    switch (message.type) {
      case 'hello':
        this.onHello(runtime, message);
        return;
      case 'request':
        this.onRequest(runtime, message);
        return;
      case 'cancel':
        this.onCancel(runtime, message);
        return;
      case 'shutdown':
        this.onShutdown(runtime, message);
        return;
      case 'ready':
      case 'result':
      case 'closed':
        // Server-to-Portal types: a Portal that sends one is out of protocol.
        this.logger.warn(
          {
            code: 'UNEXPECTED_IPC_MESSAGE',
            reason: `server received "${message.type}"`,
          },
          'ipc message ignored',
        );
    }
  }

  private onHello(runtime: Runtime, message: HelloMessage): void {
    if (this.phase === 'shutting-down') {
      // Answering a handshake with `ready` while shutting down would promise
      // readiness this process no longer offers.
      this.logger.warn(
        {
          code: 'HELLO_DURING_SHUTDOWN',
          portalGeneration: message.portalGeneration,
        },
        'hello ignored',
      );
      return;
    }
    const previous = this.portalGeneration;
    if (previous !== undefined && message.portalGeneration <= previous) {
      this.logger.warn(
        {
          code: 'STALE_HELLO',
          portalGeneration: message.portalGeneration,
          previousPortalGeneration: previous,
        },
        'hello ignored',
      );
      return;
    }
    this.portalGeneration = message.portalGeneration;
    // A later Portal can break the ordering again, and that recurrence should
    // be visible rather than silenced by the first one.
    this.missingHelloLogged = false;
    if (previous === undefined) {
      this.logger.info(
        { portalGeneration: message.portalGeneration },
        'portal generation accepted',
      );
      return;
    }
    // The new Portal was not around for the first `ready`, and the barrier is
    // per Portal: without a second one it would wait for readiness forever.
    // In-flight work is deliberately kept — the handlers are already running,
    // their results are still the only answer for those ids (duplicates are
    // the business's problem, INV-011), and results go out on this same link.
    this.logger.warn(
      {
        code: 'PORTAL_GENERATION_CHANGED',
        portalGeneration: message.portalGeneration,
        previousPortalGeneration: previous,
        pendingRequests: this.inFlight.size,
      },
      'new portal generation; resending ready',
    );
    this.sendQuietly(
      {
        type: 'ready',
        pid: process.pid,
        capabilities: runtime.capabilities,
      },
      'ready',
    );
  }

  private onRequest(runtime: Runtime, message: RequestMessage): void {
    const ids = requestIds(message);
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
        this.logger.warn(
          {
            ...ids,
            code: 'DUPLICATE_REQUEST',
            replayCount: message.replayCount,
            reason: 'request is already in flight',
          },
          'duplicate delivery ignored',
        );
      }
      return;
    }
    if (this.phase === 'shutting-down') {
      // SPEC 3.5: a refused input gets a stable code rather than silence.
      // Portal may already have the request on the wire and will not settle it
      // until a result or its own deadline arrives, so dropping it here would
      // spend the whole timeout budget to learn what this line already knows.
      this.logger.warn(
        {
          ...ids,
          code: 'SERVER_SHUTTING_DOWN',
          reason: 'request arrived after shutdown started',
        },
        'request refused',
      );
      this.sendQuietly(
        {
          type: 'result',
          requestId: message.requestId,
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
    if (this.portalGeneration === undefined) {
      // Executing is the only way to answer: the protocol has no "not ready"
      // reply, and dropping would leave Portal replaying this request forever
      // without ever getting a result. The missing handshake is recorded once so
      // it stays diagnosable without logging once per request (SPEC 7.1).
      if (!this.missingHelloLogged) {
        this.missingHelloLogged = true;
        this.logger.warn(
          {
            ...ids,
            code: 'REQUEST_BEFORE_HELLO',
            reason: 'no hello received from this portal',
          },
          'request received before hello',
        );
      }
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
    const work = this.executeRequest(runtime, entry).catch((error: unknown) => {
      // `executeRequest` is written not to throw; this keeps one unexpected
      // throw from becoming an unhandled rejection in the IPC callback.
      this.logger.error(
        {
          ...requestIds(message),
          code: INTERNAL_ERROR,
          reason: messageOf(error),
        },
        'request execution failed unexpectedly',
      );
    });
    this.running.add(work);
    // `work` already maps every failure to a log line, so it never rejects and
    // a single fulfilled handler is enough to retire it from the drain set.
    void work.then(() => {
      this.running.delete(work);
      return undefined;
    });
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
   * Runs one accepted request to a terminal state and reports it. Never
   * rejects: its only caller is the IPC callback, which has no one to hand a
   * failure to, and `run`'s shutdown wait relies on it settling.
   */
  private async executeRequest(
    runtime: Runtime,
    entry: PendingRequest,
  ): Promise<void> {
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
        this.logger.info(
          {
            ...requestIds(message),
            code: entry.preStartCancel,
            reason: 'cancelled before the handler started',
          },
          'cancelled request skipped',
        );
        return;
      }
      entry.started = true;
      const startedAt = performance.now();
      const outcome = await this.executeSafely(runtime, message);
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
        this.logger.warn(
          {
            ...requestIds(message),
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
    runtime: Runtime,
    message: RequestMessage,
  ): Promise<ExecutorOutcome> {
    try {
      // `RequestMessage` already has every `ExecutorRequest` field with the
      // same meaning, so it is passed as is; the extra `type`/`replayCount`
      // only matter to the wire and the excess-property check does not apply
      // to a non-literal argument. Portal's deadline stays authoritative: it
      // settles this id at that instant, so a Server that ran longer would
      // commit writes nobody is waiting for (SPEC 3.4, INV-007).
      return await runtime.executor.execute(message);
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
    const ids = requestIds(message);
    if (outcome.ok) {
      const sessionId = outcome.sessionId;
      const fields: Record<string, unknown> = { ...ids, durationMs };
      if (sessionId !== undefined) {
        fields.sessionId = sessionId;
      }
      this.logger.debug(fields, 'request completed');
      // The outcome already has the shape of the message's payload half, so it
      // is spread rather than copied field by field. An unbound success carries
      // `sessionId: undefined` as an own property, which every channel either
      // drops (JSON) or keeps with the same meaning as absence (SPEC 2).
      this.sendQuietly(
        { type: 'result', requestId: message.requestId, ...outcome },
        'result',
      );
      return;
    }
    this.logger[failureLogLevel(outcome.error)](
      {
        ...ids,
        code: outcome.error.code,
        reason: outcome.error.message,
        durationMs,
      },
      'request failed',
    );
    this.sendQuietly(
      { type: 'result', requestId: message.requestId, ...outcome },
      'result',
    );
  }

  private onCancel(runtime: Runtime, message: CancelMessage): void {
    const entry = this.inFlight.get(message.requestId);
    if (entry === undefined) {
      // Nothing to cancel: the request already reached a terminal state, or it
      // never arrived. Remembering the id would leak memory for the life of the
      // process and could suppress a legitimate replay, so it is only logged.
      this.logger.debug(
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
      if (runtime.dispatcher.cancel(message.requestId, message.code)) {
        entry.cancelledCode = message.code;
        this.logger.warn(
          {
            ...requestIds(entry.message),
            code: message.code,
            reason: 'transaction invalidated while the handler runs',
          },
          'cancel applied to a running handler',
        );
        return;
      }
      this.logger.debug(
        {
          ...requestIds(entry.message),
          code: message.code,
          reason: 'request reached a terminal state before the cancel',
        },
        'cancel ignored',
      );
      return;
    }
    entry.preStartCancel = message.code;
    this.logger.info(
      { ...requestIds(entry.message), code: message.code },
      'request cancelled before start',
    );
  }

  private onShutdown(runtime: Runtime, message: ShutdownMessage): void {
    if (this.phase === 'shutting-down') {
      this.logger.warn(
        { code: 'SHUTDOWN_IN_PROGRESS', reason: 'shutdown already requested' },
        'duplicate shutdown ignored',
      );
      return;
    }
    this.phase = 'shutting-down';
    // SPEC 6.3 step 1: stop accepting before draining, so the drain below has a
    // closed set of requests and nothing new can enter the backend.
    const budgetMs = this.shutdownBudgetMs(message);
    this.logger.info(
      { pendingRequests: this.running.size, budgetMs },
      'server shutting down',
    );
    void this.shutdown(runtime, budgetMs).catch((error: unknown) => {
      // Nothing below `shutdown` is expected to throw, but a channel that
      // throws on close must still resolve the completion instead of hanging.
      this.logger.error(
        { code: 'SHUTDOWN_FAILED', reason: messageOf(error) },
        'server shutdown failed',
      );
      this.completion.resolve();
    });
  }

  private async shutdown(runtime: Runtime, limitMs: number): Promise<void> {
    const startedAt = performance.now();
    // One deadline for both steps, like `abort`: the Portal declared a single
    // budget for the whole close, so spending it twice would overrun it by
    // exactly the amount that made the Portal kill a Server that was closing
    // correctly.
    const deadlineAt = performance.now() + limitMs;
    // SPEC 6.3 steps 2-3: every accepted request settles to a terminal state,
    // bounded by the per-request timeout the dispatcher already enforces, and
    // its result still goes out before `closed`.
    await this.drain(limitMs);
    // Step 4: close the backend before announcing, so Portal never restarts a
    // Server whose storage is still open.
    await this.closeStorage(
      runtime,
      Math.max(1, Math.round(deadlineAt - performance.now())),
    );
    this.sendQuietly({ type: 'closed', reason: 'shutdown' }, 'closed');
    // Step 5: the link itself goes last, after the final message.
    this.channel.close('shutdown complete');
    this.logger.info(
      { durationMs: elapsedSince(startedAt) },
      'server process stopped',
    );
    this.completion.resolve();
  }

  /**
   * The total budget for the graceful close, from what the Portal declared.
   *
   * A message without a budget cannot come off the wire — the protocol requires
   * it — but a directly constructed one can, and the Server must still close
   * rather than refuse. The fallback is the pre-protocol derivation, and it is
   * logged as a degradation because it may exceed the Portal's real patience.
   */
  private shutdownBudgetMs(message: ShutdownMessage): number {
    return resolveShutdownBudgetMs(
      message.timeoutMs,
      this.options.timeoutMs,
      this.logger,
    );
  }

  /**
   * Waits for the accepted requests to settle, but not forever. A backend whose
   * `close` awaits its queue (SQLite does) makes `executor.close()` wait for the
   * handler too, so an unbounded drain would postpone `closed` past any budget
   * Portal is willing to wait for. Unfinished requests are reported rather than
   * silently abandoned; their ids say which handler is holding the process.
   */
  private async drain(limitMs: number): Promise<void> {
    const drained = await settleWithin(
      Promise.allSettled(this.running),
      limitMs,
    );
    if (drained.status === 'timeout') {
      this.logger.error(
        {
          code: 'SHUTDOWN_DRAIN_TIMEOUT',
          timeoutMs: limitMs,
          pendingRequests: this.inFlight.size,
          requestIds: [...this.inFlight.keys()],
        },
        'shutdown drain exceeded its budget; closing anyway',
      );
    }
  }

  /**
   * Closes the backend, which is mandatory (SPEC 6.3 step 4), but bounded: a
   * backend that waits for its queue would otherwise keep `closed` from ever
   * being sent when a handler never returns. The wait is abandoned, not the
   * close: the call is still made and its own failure is still logged.
   */
  private async closeStorage(runtime: Runtime, limitMs: number): Promise<void> {
    // `settleWithin` consumes a rejection itself, so the failure is read from
    // the result instead of being mapped into a value first.
    const outcome = await settleWithin(runtime.executor.close(), limitMs);
    if (outcome.status === 'timeout') {
      this.logger.error(
        {
          code: 'STORAGE_CLOSE_TIMEOUT',
          timeoutMs: limitMs,
          pendingRequests: this.inFlight.size,
          requestIds: [...this.inFlight.keys()],
        },
        'storage backend close exceeded its budget; continuing',
      );
      return;
    }
    if (outcome.status === 'rejected') {
      this.logger.error(
        {
          code: 'STORAGE_CLOSE_FAILED',
          reason: messageOf(outcome.error),
        },
        'storage backend could not be closed',
      );
    }
  }

  private onChannelClose(runtime: Runtime, reason: string): void {
    if (this.phase === 'shutting-down') {
      // Our own `close` at the end of an orderly shutdown.
      return;
    }
    this.phase = 'shutting-down';
    this.logger.error(
      { code: 'IPC_CHANNEL_CLOSED', reason },
      'ipc channel closed; server stopping',
    );
    void this.abort(runtime, reason);
  }

  private async abort(runtime: Runtime, reason: string): Promise<void> {
    // Nobody can receive results any more. Settle the in-flight work first so
    // the backend is closed on a known state, then hand the reason to the
    // caller, which owns the exit code.
    //
    // The budget is fixed and short (ABORT_TIMEOUT_MS), not derived from
    // `timeoutMs`: Portal is gone, so nobody is waiting for this process, and a
    // long wait only extends the window in which this orphan and the next
    // Server both hold the storage (INV-002). It is one deadline shared by both
    // steps, not one per step, so the total is what the constant promises.
    const limitMs = this.options.abortTimeoutMs ?? ABORT_TIMEOUT_MS;
    const deadlineAt = performance.now() + limitMs;
    await this.drain(limitMs);
    await this.closeStorage(
      runtime,
      Math.max(1, deadlineAt - performance.now()),
    );
    // On the parent-gone path the link is still open, and an open IPC channel
    // keeps the event loop alive: the whole point is that an orphaned process
    // goes away rather than lingering as a second writer (INV-002).
    this.channel.close('aborting');
    this.completion.reject(new ServerChannelClosedError(reason));
  }

  private sendQuietly(message: IpcMessage, what: string): void {
    try {
      this.channel.send(message);
    } catch (error) {
      // The link is gone. Portal's replay buffer still holds whatever it never
      // acknowledged, so there is nothing to retry from here.
      this.logger.error(
        { code: 'IPC_SEND_FAILED', reason: messageOf(error) },
        `${what} could not be sent`,
      );
    }
  }
}

/**
 * Total budget for a graceful close. The Portal's declaration wins; without one
 * the pre-protocol derivation applies (twice `timeoutMs` for the drain plus
 * twice for the backend close) and is logged, because it may outlast the
 * Portal's real patience and get this process killed mid-close.
 *
 * Exported for the test that pins the fallback: the wire parser rejects a
 * `shutdown` without a budget, so the degraded path is unreachable through a
 * message and would otherwise be untested.
 */
export function resolveShutdownBudgetMs(
  declaredMs: number | undefined,
  requestTimeoutMs: number,
  logger: Logger,
): number {
  if (declaredMs !== undefined) {
    return declaredMs;
  }
  const fallback = requestTimeoutMs * DRAIN_LIMIT_MULTIPLIER * 2;
  logger.warn(
    {
      code: 'SHUTDOWN_BUDGET_UNDECLARED',
      timeoutMs: fallback,
      reason: 'shutdown message carried no timeoutMs; using the legacy budget',
    },
    'running shutdown on the fallback budget',
  );
  return fallback;
}

function requestIds(message: RequestMessage): RequestIdentity {
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

async function closeQuietly(
  dispatcher: Dispatcher | undefined,
  backend: StorageBackend,
  logger: Logger,
): Promise<void> {
  try {
    if (dispatcher !== undefined) {
      await dispatcher.close();
    } else {
      await backend.close();
    }
  } catch (error) {
    logger.error(
      { code: 'STORAGE_CLOSE_FAILED', reason: messageOf(error) },
      'storage backend could not be closed',
    );
  }
}

function elapsedSince(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}
