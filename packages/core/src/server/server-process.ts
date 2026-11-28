import { createProcessChannel } from '../ipc/bun-child.ts';
import type { IpcChannel } from '../ipc/channel.ts';
import { preparedMessage } from '../ipc/outbound.ts';
import type {
  CancelMessage,
  IpcMessage,
  RequestMessage,
  ShutdownMessage,
} from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import { messageOf } from '../shared/guards.ts';
import { requestLogFields } from '../shared/identity.ts';
import type { RequestRunner } from './request-runner.ts';
import type { ActivationDeps } from './server-activation.ts';
import { startAndServe } from './server-activation.ts';
import { ParentWatch } from './server-liveness.ts';
import type { ServerProcessOptions } from './server-process-options.ts';
import { ABORT_TIMEOUT_MS, ServerShutdown } from './server-shutdown.ts';
import type { Runtime } from './server-startup.ts';
import { ServerStartupError } from './server-startup.ts';

export type { ServerProcessOptions };

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
 * - resolves after an orderly `shutdown`: the bounded close flow has been
 *   attempted (`closed` was sent and the link is closed), reported as exit 0.
 *   Resolution does not guarantee the backend was closed or that the process
 *   has exited; a failure or timeout inside the close flow is only logged. A
 *   `shutdown` before activation resolves without ever opening storage;
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
   * Resolves once the Server is ready, or when an orderly shutdown ended the
   * process before that happened; rejects when startup failed.
   */
  private readonly activation = Promise.withResolvers<void>();
  private readonly shutdown: ServerShutdown;
  private readonly parentWatch: ParentWatch;
  private phase: Phase = 'awaiting-hello';
  /**
   * True once input that arrived before `ready` has been reported. A request or
   * a cancel before the barrier means the timing is broken, and logging every
   * one of them would scale the log volume with the request rate (SPEC 7.1).
   * One flag for both: they are the same fault.
   */
  private preReadyInputLogged = false;
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
    this.shutdown = new ServerShutdown({
      logger: this.logger,
      channel: this.channel,
      readAbortBudgetMs: () => options.abortTimeoutMs ?? ABORT_TIMEOUT_MS,
      sendQuietly: (message, what) => this.sendQuietly(message, what),
      markShuttingDown: () => {
        this.enterShuttingDown();
      },
      complete: () => {
        this.completion.resolve();
      },
      rejectCompletion: (reason) => {
        this.completion.reject(new ServerChannelClosedError(reason));
      },
    });
    this.parentWatch = new ParentWatch(
      {
        parentCheckIntervalMs: options.parentCheckIntervalMs,
        readParentPid: options.readParentPid,
      },
      () => {
        this.onParentGone();
      },
    );
    // A startup failure throws out of `run` before it awaits the completion,
    // so a later rejection there must not surface as an unhandled rejection.
    void this.completion.promise.catch(() => undefined);
    void this.activation.promise.catch(() => undefined);
  }

  async run(): Promise<void> {
    try {
      await this.begin();
    } catch (error) {
      this.parentWatch.stop();
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
      this.parentWatch.stop();
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
    this.channel.onMessage((raw) => {
      this.onMessage(raw);
    });
    this.channel.onClose((reason) => {
      this.onChannelClose(reason);
    });
    this.parentWatch.start();
    await this.activation.promise;
  }

  /**
   * The parent disappeared. Before activation there is no storage to drain, so
   * the activation is failed and the link closed; once a runtime exists the
   * close belongs to `abort`. During `starting` the start owns whatever it
   * opened and closes it when it finishes.
   */
  private onParentGone(): void {
    if (this.phase === 'shutting-down') {
      return;
    }
    const wasStarting = this.phase === 'starting';
    const reason = 'the parent process is gone';
    this.enterShuttingDown();
    this.parentGoneReason = reason;
    this.logger.error(
      { code: 'IPC_PARENT_GONE', reason },
      'parent process gone; server stopping',
    );
    const runtime = this.runtime;
    const runner = this.runner;
    if (runtime !== undefined && runner !== undefined) {
      void this.shutdown.abort(runtime, runner, reason);
      return;
    }
    if (wasStarting) {
      // `startServer` is mid-flight and owns what it opened: it closes the
      // backend and rejects the activation when it finishes, so rejecting here
      // would let the caller exit the process while that close is in flight.
      return;
    }
    // ADR-0007: before activation there is no storage to drain, but `run()` is
    // parked on `activation.promise`, so rejecting it is what makes the process
    // return instead of outliving the Portal that started it. Closing the link
    // is what lets the event loop empty; without it an orphan lingers.
    this.activation.reject(new ServerChannelClosedError(reason));
    this.channel.close(reason);
  }

  /**
   * Answers the handshake without touching storage (ADR-0007). Nothing that
   * can write exists at this point, which is what keeps a prepared Server from
   * competing with the one it is meant to replace (INV-002).
   */
  private beginPrepare(): void {
    this.phase = 'preparing';
    this.logger.info(
      { pid: process.pid },
      'server prepared; waiting for activation',
    );
    this.sendPrepared();
  }

  private sendPrepared(): void {
    try {
      this.sendBarrier(preparedMessage(process.pid), 'prepared');
    } catch (error) {
      // No storage exists yet, so there is nothing to close: failing the
      // activation is what makes `run()` return instead of parking on a Portal
      // that will never send `activate`.
      this.phase = 'shutting-down';
      this.activation.reject(error);
    }
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
        this.onHello();
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
      case 'event':
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

  private onHello(): void {
    if (this.phase !== 'awaiting-hello') {
      // Every launch opens a new channel, so the handshake arrives exactly
      // once per process. A second hello is out of protocol; ignoring it keeps
      // a duplicate from reopening the prepare state or resending a barrier.
      this.logger.warn(
        {
          code: 'UNEXPECTED_IPC_MESSAGE',
          reason: `server received "hello" while ${this.phase}`,
        },
        'ipc message ignored',
      );
      return;
    }
    this.beginPrepare();
  }

  private onActivate(): void {
    if (this.phase === 'preparing') {
      this.logger.info(
        { pid: process.pid },
        'activation received; starting server',
      );
      void startAndServe(this.activationDeps()).catch((error: unknown) => {
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
        ...requestLogFields(message),
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
      this.shutdown.begin(runtime, runner, message);
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
    this.activation.resolve();
  }

  /**
   * The link closed. The reason is always recorded for a start that may still
   * be in flight: it must close the storage it just opened instead of
   * announcing readiness to a Portal that is gone.
   */
  private onChannelClose(reason: string): void {
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
        this.enterShuttingDown();
        this.activation.reject(new ServerChannelClosedError(reason));
      }
      return;
    }
    this.enterShuttingDown();
    this.logger.error(
      { code: 'IPC_CHANNEL_CLOSED', reason },
      'ipc channel closed; server stopping',
    );
    void this.shutdown.abort(runtime, runner, reason);
  }

  private enterShuttingDown(): void {
    this.phase = 'shutting-down';
  }

  private activationDeps(): ActivationDeps {
    return {
      options: this.options,
      logger: this.logger,
      channel: this.channel,
      shutdown: this.shutdown,
      activation: this.activation,
      beginStarting: () => {
        this.phase = 'starting';
      },
      activate: () => {
        this.phase = 'active';
      },
      enterShuttingDown: () => {
        this.enterShuttingDown();
      },
      isShuttingDown: () => this.phase === 'shutting-down',
      isActive: () => this.phase === 'active',
      attach: (runtime, runner) => {
        this.runtime = runtime;
        this.runner = runner;
      },
      pendingShutdown: () => this.pendingShutdown,
      channelClosedReason: () => this.channelClosedReason,
      parentGoneReason: () => this.parentGoneReason,
      sendBarrier: (message, what) => {
        this.sendBarrier(message, what);
      },
      sendQuietly: (message, what) => this.sendQuietly(message, what),
      onMessage: (raw) => {
        this.onMessage(raw);
      },
      onChannelClose: (reason) => {
        this.onChannelClose(reason);
      },
      channelClosedError: (reason) => new ServerChannelClosedError(reason),
    };
  }

  private sendQuietly(message: IpcMessage, what: string): boolean {
    try {
      this.channel.send(message);
      return true;
    } catch (error) {
      // The link is gone. Portal's replay buffer still holds whatever it never
      // acknowledged, so there is nothing to retry from here.
      this.logger.error(
        { code: 'IPC_SEND_FAILED', reason: messageOf(error) },
        `${what} could not be sent`,
      );
      return false;
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
