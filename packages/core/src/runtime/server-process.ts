import { messageOf } from '../internal/guards.ts';
import { createProcessChannel } from '../ipc/bun-child.ts';
import type { IpcChannel } from '../ipc/channel.ts';
import type {
  HelloMessage,
  IpcMessage,
  ShutdownMessage,
} from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { StorageBackend } from '../storage/types.ts';
import type { Extension } from './extension.ts';
import type { RequestIdentity } from './identity.ts';
import { RequestRunner } from './server-requests.ts';
import type { Runtime } from './server-startup.ts';
import { startServer } from './server-startup.ts';
import { settleWithin } from './timeout.ts';

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
  /** Set while the parent-liveness poll is installed; cleared on completion. */
  private parentTimer: ReturnType<typeof setInterval> | undefined;
  /**
   * The running backend and dispatcher. Stored because the parent-liveness poll
   * fires outside the message flow, where the runtime is not a parameter.
   */
  private runtime: Runtime | undefined;
  /**
   * The request runner, created together with the runtime. Stored for the same
   * reason: the parent-liveness poll reaches the drain through `abort`.
   */
  private runner: RequestRunner | undefined;

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
    const runner = this.runner;
    if (runtime === undefined || runner === undefined) {
      this.completion.reject(
        new ServerChannelClosedError('the parent process is gone'),
      );
      return;
    }
    void this.abort(runtime, runner, 'the parent process is gone');
  }

  /**
   * Crosses the readiness barrier through `startServer` (SPEC 6.2) and only
   * then attaches the inbound handlers: the channel buffers messages that
   * arrive before registration, so nothing is lost and nothing is handled
   * before `ready`. Attaching them earlier would break that contract.
   */
  private async start(): Promise<void> {
    const { runtime } = await startServer({
      options: this.options,
      logger: this.logger,
      channel: this.channel,
    });
    const runner = new RequestRunner({
      runtime,
      logger: this.logger,
      send: (message, what) => this.sendQuietly(message, what),
      // The phase and the handshake flag stay in this class: `shutdown` and
      // `hello` own them, and the runner only asks.
      accepting: () => this.phase === 'accepting',
      noteRequestBeforeHello: (ids) => this.noteRequestBeforeHello(ids),
    });
    this.runtime = runtime;
    this.runner = runner;
    // Inbound plumbing goes last: a message that arrives before these
    // handlers are attached is buffered by the channel and delivered at
    // registration, so none is lost and none is handled before `ready`.
    this.channel.onMessage((raw) => {
      this.onMessage(runtime, runner, raw);
    });
    this.channel.onClose((reason) => {
      this.onChannelClose(runtime, runner, reason);
    });
  }

  private onMessage(
    runtime: Runtime,
    runner: RequestRunner,
    raw: unknown,
  ): void {
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
        this.onHello(runtime, runner, message);
        return;
      case 'request':
        runner.accept(message);
        return;
      case 'cancel':
        runner.cancel(message);
        return;
      case 'shutdown':
        this.onShutdown(runtime, runner, message);
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

  private onHello(
    runtime: Runtime,
    runner: RequestRunner,
    message: HelloMessage,
  ): void {
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
        pendingRequests: runner.pending,
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

  /**
   * The request arrived before any `hello`. Executing is the only way to answer
   * it: the protocol has no "not ready" reply, and dropping would leave Portal
   * replaying this request forever without ever getting a result. The missing
   * handshake is recorded once so it stays diagnosable without logging once per
   * request (SPEC 7.1).
   */
  private noteRequestBeforeHello(ids: RequestIdentity): void {
    if (this.portalGeneration !== undefined || this.missingHelloLogged) {
      return;
    }
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

  private onShutdown(
    runtime: Runtime,
    runner: RequestRunner,
    message: ShutdownMessage,
  ): void {
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
      { pendingRequests: runner.running, budgetMs },
      'server shutting down',
    );
    void this.shutdown(runtime, runner, budgetMs).catch((error: unknown) => {
      // Nothing below `shutdown` is expected to throw, but a channel that
      // throws on close must still resolve the completion instead of hanging.
      this.logger.error(
        { code: 'SHUTDOWN_FAILED', reason: messageOf(error) },
        'server shutdown failed',
      );
      this.completion.resolve();
    });
  }

  private async shutdown(
    runtime: Runtime,
    runner: RequestRunner,
    limitMs: number,
  ): Promise<void> {
    const startedAt = performance.now();
    // One deadline for both steps, like `abort`: the Portal declared a single
    // budget for the whole close, so spending it twice would overrun it by
    // exactly the amount that made the Portal kill a Server that was closing
    // correctly.
    const deadlineAt = performance.now() + limitMs;
    // SPEC 6.3 steps 2-3: every accepted request settles to a terminal state,
    // bounded by the per-request timeout the dispatcher already enforces, and
    // its result still goes out before `closed`.
    await runner.drain(limitMs);
    // Step 4: close the backend before announcing, so Portal never restarts a
    // Server whose storage is still open.
    await this.closeStorage(
      runtime,
      runner,
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
   * Closes the backend, which is mandatory (SPEC 6.3 step 4), but bounded: a
   * backend that waits for its queue would otherwise keep `closed` from ever
   * being sent when a handler never returns. The wait is abandoned, not the
   * close: the call is still made and its own failure is still logged.
   */
  private async closeStorage(
    runtime: Runtime,
    runner: RequestRunner,
    limitMs: number,
  ): Promise<void> {
    // `settleWithin` consumes a rejection itself, so the failure is read from
    // the result instead of being mapped into a value first.
    const outcome = await settleWithin(runtime.executor.close(), limitMs);
    if (outcome.status === 'timeout') {
      this.logger.error(
        {
          code: 'STORAGE_CLOSE_TIMEOUT',
          timeoutMs: limitMs,
          pendingRequests: runner.pending,
          requestIds: runner.pendingRequestIds,
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

  private onChannelClose(
    runtime: Runtime,
    runner: RequestRunner,
    reason: string,
  ): void {
    if (this.phase === 'shutting-down') {
      // Our own `close` at the end of an orderly shutdown.
      return;
    }
    this.phase = 'shutting-down';
    this.logger.error(
      { code: 'IPC_CHANNEL_CLOSED', reason },
      'ipc channel closed; server stopping',
    );
    void this.abort(runtime, runner, reason);
  }

  private async abort(
    runtime: Runtime,
    runner: RequestRunner,
    reason: string,
  ): Promise<void> {
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
    await runner.drain(limitMs);
    await this.closeStorage(
      runtime,
      runner,
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

function elapsedSince(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}
