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
import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import { preparedMessage, readyMessage } from '../portal/ipc-outbound.ts';
import { messageOf } from '../shared/guards.ts';
import { settleWithin } from '../shared/timeout.ts';
import type { StorageBackend } from '../storage/types.ts';
import type { Extension } from './extension.ts';
import { RequestRunner, requestIds } from './requests.ts';
import type { Runtime } from './server-startup.ts';
import {
  assertCodeVersion,
  ServerStartupError,
  startServer,
} from './server-startup.ts';

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
  /**
   * Opaque application code version; must be a non-empty string when set.
   * Reported in `ready` and compared against a replay's stamped version only to
   * log a mismatch; the framework never uses it to accept, reject or alter a
   * Request (SPEC 6.4).
   */
  codeVersion?: string;
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
 * Budget for each step of an abort (Portal died, nobody can read a result).
 * Deliberately a fixed value unrelated to `timeoutMs`: nobody is waiting for
 * this process, and every second of a long wait is a second this orphaned
 * process keeps writing to storage beside the next Server (INV-002). A short
 * cap is the right trade: a handler that cannot finish in 5s is abandoned
 * rather than honoured.
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
 * Where the two-phase activation has got to (ADR-0007). Only `active` may run
 * requests; `shutting-down` is terminal for the rest of the process's life.
 */
type Phase =
  | 'awaiting-hello'
  | 'preparing'
  | 'starting'
  | 'active'
  | 'shutting-down';

/**
 * The Server side of the two-process split (ADR-0002): it owns Extensions,
 * handler execution and the only storage writer (INV-002).
 *
 * Startup is two-phase (ADR-0007): `hello` is answered with `prepared` while no
 * storage exists, and only `activate` runs `startServer` (backend, capability
 * self-check, Extensions, `ready`). A Portal that never activates never makes
 * this process a writer, which is what lets a replacement Server be spawned
 * before the old one released storage.
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
 *   backend is closed and the link is closed): exit 0. A `shutdown` before
 *   activation resolves without ever opening storage;
 * - rejects with `ServerStartupError` when activation never reached `ready`, and
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
  /**
   * Resolves true once the Server is ready and the request handlers are
   * attached, false when an orderly shutdown ended the process before that
   * possibly happened, and rejects when startup failed.
   */
  private readonly activation = Promise.withResolvers<boolean>();
  private portalGeneration: number | undefined;
  private phase: Phase = 'awaiting-hello';
  /**
   * True once input that arrived before `ready` has been reported for this
   * Portal. A request or a cancel before the barrier means the timing is broken,
   * and logging every one of them would scale the log volume with the request
   * rate (SPEC 7.1). One flag for both: they are the same fault.
   */
  private preReadyInputLogged = false;
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
  /** A `shutdown` that arrived while `startServer` was still running. */
  private pendingShutdown: ShutdownMessage | undefined;
  /**
   * The link died before activation. Recorded because the readiness barrier may
   * only be crossed later, and a startup that finishes after the death must
   * close the storage it just opened instead of announcing readiness.
   */
  private channelClosedReason: string | undefined;
  /**
   * The parent was seen gone while a start was in flight. Same reason as
   * `channelClosedReason`: the start owns the close and must not announce
   * readiness to a Portal that no longer exists.
   */
  private parentGoneReason: string | undefined;

  constructor(options: ServerProcessOptions) {
    this.options = options;
    this.logger = safeLogger(
      options.logger ??
        createJsonLogger({ bindings: { component: 'server-process' } }),
    );
    this.channel =
      options.channel ?? createProcessChannel({ logger: this.logger });
    // A startup failure throws out of `run` before it awaits the completion,
    // so a later rejection there must not surface as an unhandled rejection.
    void this.completion.promise.catch(() => undefined);
    void this.activation.promise.catch(() => undefined);
  }

  async run(): Promise<void> {
    try {
      await this.begin();
    } catch (error) {
      this.stopParentWatch();
      // The failure is already reported. Marking the close as ours keeps the
      // early `onClose` handler from logging a second, misleading
      // `IPC_CHANNEL_CLOSED` for the link this line closes.
      this.phase = 'shutting-down';
      this.channel.close('startup failed');
      throw error;
    }
    try {
      await this.completion.promise;
    } finally {
      // Also on the failure paths (a closed channel rejects): a live interval
      // would keep the process alive after the caller already got its answer.
      this.stopParentWatch();
    }
  }

  /**
   * Waits for the handshake and then for activation (ADR-0007). The channel
   * may buffer what arrived before registration, so the handlers go in first.
   *
   * The parent watch goes in here rather than after `ready`: a prepared Server
   * holds no storage, but it can still be the process a `SIGKILL`ed Portal left
   * behind, and nothing else would ever tell it to stop. It is unref'd, so it
   * does not keep the process alive on its own.
   */
  private async begin(): Promise<void> {
    // Checked before the handshake answers: `prepared` carries the version, and
    // the wire parser accepts only a non-empty string, so an invalid value
    // would produce a message no Portal can read.
    assertCodeVersion(this.options.codeVersion, this.logger);
    this.channel.onMessage((raw) => {
      this.onMessage(raw);
    });
    this.channel.onClose((reason) => {
      this.onChannelClose(reason);
    });
    this.startParentWatch();
    await this.activation.promise;
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
    const previous = this.phase;
    if (previous === 'shutting-down') {
      return;
    }
    this.phase = 'shutting-down';
    this.parentGoneReason = 'the parent process is gone';
    this.logger.error(
      { code: 'IPC_PARENT_GONE', reason: 'the parent process is gone' },
      'parent process gone; server stopping',
    );
    const runtime = this.runtime;
    const runner = this.runner;
    if (runtime !== undefined && runner !== undefined) {
      void this.abort(runtime, runner, 'the parent process is gone');
      return;
    }
    if (previous === 'starting') {
      // `startServer` is mid-flight and owns what it opened: it closes the
      // backend and rejects the activation when it finishes, so rejecting here
      // would let the caller exit the process while that close is in flight.
      return;
    }
    // ADR-0007: before activation there is no storage to drain, but `run()` is
    // parked on `activation.promise`, so rejecting it is what makes the process
    // return instead of outliving the Portal that started it. Closing the link
    // is what lets the event loop empty; without it an orphan lingers.
    this.activation.reject(
      new ServerChannelClosedError('the parent process is gone'),
    );
    this.channel.close('the parent process is gone');
  }

  /**
   * Answers the handshake without touching storage (ADR-0007). Nothing that
   * can write exists at this point, which is what keeps a prepared Server from
   * competing with the one it is meant to replace (INV-002).
   */
  private beginPrepare(): void {
    this.phase = 'preparing';
    this.logger.info(
      { pid: process.pid, codeVersion: this.options.codeVersion },
      'server prepared; waiting for activation',
    );
    this.sendPrepared();
  }

  private sendPrepared(): void {
    try {
      this.sendBarrier(
        preparedMessage(process.pid, this.options.codeVersion),
        'prepared',
      );
    } catch (error) {
      // No storage exists yet, so there is nothing to close: failing the
      // activation is what makes `run()` return instead of parking on a Portal
      // that will never send `activate`.
      this.phase = 'shutting-down';
      this.activation.reject(error);
    }
  }

  /**
   * Whether the process has begun closing. Reads the field through a method so
   * a phase set inside a synchronously delivered handler is not narrowed away
   * by the compiler.
   */
  private isShuttingDown(): boolean {
    return this.phase === 'shutting-down';
  }

  /**
   * Runs once `activate` arrives: opens the runtime through `startServer`
   * (SPEC 6.2), attaches the request handlers, marks the process active and
   * only then sends `ready`. The pre-activation handlers stay installed until
   * the new ones are in place, so a request that arrives during the start is
   * ignored instead of meeting a half-built Server.
   */
  private async startAndServe(): Promise<void> {
    this.phase = 'starting';
    let runtime: Runtime;
    try {
      ({ runtime } = await startServer({
        options: this.options,
        logger: this.logger,
      }));
    } catch (error) {
      // `startServer` closed whatever it opened. A link that died during the
      // start is reported as what it is: the peer is gone, not that the
      // configuration was refused. The same holds for a parent that vanished
      // mid-start.
      const gone = this.channelClosedReason ?? this.parentGoneReason;
      throw gone === undefined ? error : new ServerChannelClosedError(gone);
    }
    const runner = new RequestRunner({
      runtime,
      logger: this.logger,
      codeVersion: this.options.codeVersion,
      send: (message, what) => this.sendQuietly(message, what),
      // The phase stays in this class: `shutdown` owns it, and the runner only
      // asks.
      accepting: () => this.phase === 'active',
    });
    this.runtime = runtime;
    this.runner = runner;
    try {
      this.channel.onMessage((raw) => {
        this.onMessage(raw);
      });
      this.channel.onClose((reason) => {
        this.onChannelClose(reason);
      });
    } catch (error) {
      // The channel is outside code: a registration that throws must not leave
      // the backend open. The original error is the one worth reporting, so a
      // failure to close does not replace it.
      await runtime.executor.close().catch(() => undefined);
      throw error;
    }
    // Registration above can deliver what arrived while this was starting. A
    // dead link wins over a parked shutdown: nobody can receive `closed`, so
    // the close has to be an abort. Both are applied before the phase check,
    // because a shutdown parked during `startServer` already set the phase and
    // would otherwise be left without a drain.
    if (this.channelClosedReason !== undefined) {
      this.phase = 'shutting-down';
      this.activation.resolve(false);
      void this.abort(runtime, runner, this.channelClosedReason);
      return;
    }
    if (this.parentGoneReason !== undefined) {
      // The poll fired during the start. Nobody is left to announce readiness
      // to, and the backend this start just opened must be closed here: the
      // poll could not do it because the runtime did not exist yet.
      this.phase = 'shutting-down';
      this.activation.resolve(false);
      void this.abort(runtime, runner, this.parentGoneReason);
      return;
    }
    const pendingShutdown = this.pendingShutdown;
    if (pendingShutdown !== undefined) {
      this.activation.resolve(false);
      this.beginShutdown(runtime, runner, pendingShutdown);
      return;
    }
    if (this.isShuttingDown()) {
      // A `shutdown` delivered at registration found the runtime and started
      // its own drain; there is nothing left to begin.
      this.activation.resolve(false);
      return;
    }
    // SPEC 6.2 step 3, in this order and no other: the handler is installed, the
    // phase says requests may run, and only then is the barrier crossed. A
    // Portal is entitled to deliver the instant it sees `ready`, so anything
    // sent before this point would arrive with nowhere to go and be dropped.
    this.phase = 'active';
    try {
      this.sendBarrier(
        readyMessage(
          process.pid,
          runtime.capabilities,
          this.options.codeVersion,
        ),
        'ready',
      );
    } catch (error) {
      // The barrier was never crossed, so this process is not a Server anyone
      // can use: leaving the backend open would keep a writer alive beside the
      // replacement (INV-002), and leaving the phase `active` would promise work
      // this process cannot deliver. Rejecting the activation is what makes
      // `run()` end as a startup failure instead of parking forever.
      //
      // The drain is here for the channel that delivers `ready` and only then
      // throws: a request flushed by that delivery may already be running, and
      // the backend must not close under it. One deadline covers both steps,
      // like `abort`.
      this.phase = 'shutting-down';
      const limitMs = this.options.abortTimeoutMs ?? ABORT_TIMEOUT_MS;
      const deadlineAt = performance.now() + limitMs;
      await runner.drain(limitMs);
      await this.closeStorage(
        runtime,
        runner,
        Math.max(1, deadlineAt - performance.now()),
      );
      this.activation.reject(error);
      return;
    }
    this.logger.info(
      { pid: process.pid, capabilities: runtime.capabilities },
      'server process ready',
    );
    this.activation.resolve(true);
  }

  private onMessage(raw: unknown): void {
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
        this.onHello(message);
        return;
      case 'activate':
        this.onActivate();
        return;
      case 'request':
        this.onRequest(message);
        return;
      case 'cancel':
        this.onCancel(message);
        return;
      case 'shutdown':
        this.onShutdown(message);
        return;
      case 'prepared':
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

  private onHello(message: HelloMessage): void {
    if (this.phase === 'shutting-down') {
      // Answering a handshake with a barrier while shutting down would promise
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
    if (previous === undefined) {
      this.logger.info(
        { portalGeneration: message.portalGeneration },
        'portal generation accepted',
      );
      this.beginPrepare();
      return;
    }
    // A later Portal can break the ordering again, and that recurrence should
    // be visible rather than silenced by the first one.
    this.preReadyInputLogged = false;
    if (this.phase === 'preparing') {
      // The new Portal was not around for the first `prepared`, and the barrier
      // is per Portal: without a second one it would wait for an answer it
      // already missed.
      this.logger.warn(
        {
          code: 'PORTAL_GENERATION_CHANGED',
          portalGeneration: message.portalGeneration,
          previousPortalGeneration: previous,
        },
        'new portal generation; resending prepared',
      );
      this.sendPrepared();
      return;
    }
    if (this.phase === 'starting') {
      // The start has not crossed the barrier yet, and it will send `ready`
      // when it does. The new Portal is already reading this link, so there is
      // no barrier to resend.
      this.logger.warn(
        {
          code: 'PORTAL_GENERATION_CHANGED',
          portalGeneration: message.portalGeneration,
          previousPortalGeneration: previous,
        },
        'new portal generation during startup; ready will follow',
      );
      return;
    }
    const runtime = this.runtime;
    const runner = this.runner;
    if (runtime === undefined || runner === undefined) {
      return;
    }
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
      readyMessage(process.pid, runtime.capabilities, this.options.codeVersion),
      'ready',
    );
  }

  private onActivate(): void {
    if (this.phase === 'preparing') {
      this.logger.info(
        { pid: process.pid },
        'activation received; starting server',
      );
      void this.startAndServe().catch((error: unknown) => {
        this.activation.reject(error);
      });
      return;
    }
    if (this.phase === 'starting') {
      // Storage is already opening; a second activate would start it twice.
      this.logger.debug({}, 'duplicate activation ignored');
      return;
    }
    this.logger.warn(
      {
        code: 'ACTIVATE_IGNORED',
        reason: `activate arrived while ${this.phase}`,
      },
      'ipc message ignored',
    );
  }

  /**
   * A request before `ready` cannot run: no backend and no handler exist yet
   * (ADR-0007), and the Portal must not deliver before the barrier (SPEC 6.2).
   * It is ignored, and the fault is recorded once per Portal generation so the
   * log volume does not track the request rate (SPEC 7.1).
   */
  private onRequest(message: RequestMessage): void {
    const runner = this.runner;
    if (runner !== undefined) {
      runner.accept(message);
      return;
    }
    if (this.preReadyInputLogged) {
      return;
    }
    this.preReadyInputLogged = true;
    this.logger.warn(
      {
        ...requestIds(message),
        code: 'REQUEST_BEFORE_READY',
        phase: this.phase,
        reason: 'no handler can run before the server is ready',
      },
      'request ignored before ready',
    );
  }

  private onCancel(message: CancelMessage): void {
    const runner = this.runner;
    if (runner !== undefined) {
      runner.cancel(message);
      return;
    }
    // Same fault as a pre-ready request and the same volume, so it shares the
    // one-per-generation flag rather than adding a second unbounded line.
    if (this.preReadyInputLogged) {
      return;
    }
    this.preReadyInputLogged = true;
    // warn, not error: nothing ran, so nothing needs rolling back.
    this.logger.warn(
      {
        requestId: message.requestId,
        cancelCode: message.code,
        code: 'CANCEL_BEFORE_READY',
        phase: this.phase,
        reason: 'no request can be in flight before the server is ready',
      },
      'cancel ignored before ready',
    );
  }

  private onShutdown(message: ShutdownMessage): void {
    if (this.phase === 'shutting-down') {
      this.logger.warn(
        { code: 'SHUTDOWN_IN_PROGRESS', reason: 'shutdown already requested' },
        'duplicate shutdown ignored',
      );
      return;
    }
    const runtime = this.runtime;
    const runner = this.runner;
    if (runtime !== undefined && runner !== undefined) {
      this.beginShutdown(runtime, runner, message);
      return;
    }
    if (this.phase === 'starting') {
      // Storage is opening right now; closing the link here would leave a
      // backend no one owns. The start finishes and then closes it.
      this.phase = 'shutting-down';
      this.pendingShutdown = message;
      this.logger.info(
        { pid: process.pid },
        'shutdown requested during activation; closing after start',
      );
      return;
    }
    // ADR-0007: before activation there is no storage to drain or close, so an
    // orderly close is just the announcement and the link.
    this.phase = 'shutting-down';
    this.logger.info(
      { pid: process.pid, activated: false },
      'server shut down before activation; no storage was opened',
    );
    this.sendQuietly({ type: 'closed', reason: 'shutdown' }, 'closed');
    this.channel.close('shutdown complete');
    this.completion.resolve();
    this.activation.resolve(false);
  }

  private beginShutdown(
    runtime: Runtime,
    runner: RequestRunner,
    message: ShutdownMessage,
  ): void {
    this.phase = 'shutting-down';
    // SPEC 6.3 step 1: stop accepting before draining, so the drain below has a
    // closed set of requests and nothing new can enter the backend.
    const budgetMs = message.timeoutMs;
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

  private onChannelClose(reason: string): void {
    // Recorded for the activation path: a start that finishes after the death
    // must close the storage it just opened instead of announcing readiness.
    this.channelClosedReason = reason;
    if (this.phase === 'shutting-down') {
      // Our own `close` at the end of an orderly shutdown, or the close that an
      // abort already issued.
      return;
    }
    const runtime = this.runtime;
    const runner = this.runner;
    if (runtime === undefined || runner === undefined) {
      // ADR-0007: no storage is open before `ready`, so failing here cannot
      // leave a second writer behind (INV-002). During `starting` the run
      // itself closes whatever it just opened; rejecting here as well would let
      // the caller exit the process while that close is still in flight.
      this.logger.error(
        { code: 'IPC_CHANNEL_CLOSED', reason },
        'ipc channel closed before ready; server stopping',
      );
      if (this.phase !== 'starting') {
        this.phase = 'shutting-down';
        this.activation.reject(new ServerChannelClosedError(reason));
      }
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

  /**
   * Sends a readiness barrier, where a failed send is a startup failure rather
   * than a logged annoyance. Without the barrier Portal never learns this
   * process is usable: it would wait out its start timeout and kill a Server
   * that is holding storage in the `active` phase. The caller decides what to
   * close and rejects the activation with this error.
   */
  private sendBarrier(message: IpcMessage, what: string): void {
    try {
      this.channel.send(message);
    } catch (error) {
      const failure = new ServerStartupError(
        `${what} could not be sent: ${messageOf(error)}`,
      );
      this.logger.error(
        { code: failure.code, reason: failure.reason },
        'server process failed to start',
      );
      throw failure;
    }
  }
}

function elapsedSince(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}
