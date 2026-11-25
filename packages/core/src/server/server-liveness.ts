import type { Logger } from '../logging/types.ts';
import type { RequestRunner } from './request-runner.ts';
import type { Runtime } from './server-startup.ts';

/**
 * How often the Server checks whether Portal is still alive. `process.ppid`
 * changing means the parent died and this process was reparented; the IPC
 * `disconnect` event does not fire for an abrupt parent death, so without this
 * poll an orphan could keep running (and writing) indefinitely.
 */
const PARENT_CHECK_INTERVAL_MS = 500;

export interface ParentWatchOptions {
  readonly parentCheckIntervalMs?: number;
  readonly readParentPid?: () => number;
}

/**
 * Watches for the Portal dying without closing the link. `process.ppid` is
 * read on an interval because an abrupt parent death (SIGKILL) reparents this
 * process without firing `disconnect`; without the poll the orphan would keep
 * running and, worse, keep writing to storage beside the next Server
 * (INV-002). The timer is unref'd so it never keeps the process alive.
 */
export class ParentWatch {
  private timer: ReturnType<typeof setInterval> | undefined;

  constructor(
    private readonly options: ParentWatchOptions,
    private readonly onGone: () => void,
  ) {}

  start(): void {
    const intervalMs =
      this.options.parentCheckIntervalMs ?? PARENT_CHECK_INTERVAL_MS;
    if (intervalMs <= 0) {
      return;
    }
    const readParentPid = this.options.readParentPid ?? (() => process.ppid);
    const parentPid = readParentPid();
    this.timer = setInterval(() => {
      if (readParentPid() === parentPid) {
        return;
      }
      this.stop();
      this.onGone();
    }, intervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer === undefined) {
      return;
    }
    clearInterval(this.timer);
    this.timer = undefined;
  }
}

export interface ParentGoneDeps {
  readonly logger: Logger;
  readonly isShuttingDown: () => boolean;
  readonly isStarting: () => boolean;
  readonly enterShuttingDown: () => void;
  readonly recordParentGone: (reason: string) => void;
  readonly runtime: () => Runtime | undefined;
  readonly runner: () => RequestRunner | undefined;
  readonly abort: (
    runtime: Runtime,
    runner: RequestRunner,
    reason: string,
  ) => Promise<void>;
  readonly channelClosedError: (reason: string) => Error;
  readonly rejectActivation: (error: Error) => void;
  readonly closeChannel: (reason: string) => void;
}

/**
 * The parent disappeared. Before activation there is no storage to drain, so
 * the activation is failed and the link closed; once a runtime exists the
 * close belongs to `abort`. During `starting` the start owns whatever it
 * opened and closes it when it finishes.
 */
export function handleParentGone(deps: ParentGoneDeps): void {
  if (deps.isShuttingDown()) {
    return;
  }
  const wasStarting = deps.isStarting();
  const reason = 'the parent process is gone';
  deps.enterShuttingDown();
  deps.recordParentGone(reason);
  deps.logger.error(
    { code: 'IPC_PARENT_GONE', reason },
    'parent process gone; server stopping',
  );
  const runtime = deps.runtime();
  const runner = deps.runner();
  if (runtime !== undefined && runner !== undefined) {
    void deps.abort(runtime, runner, reason);
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
  deps.rejectActivation(deps.channelClosedError(reason));
  deps.closeChannel(reason);
}

export interface ChannelCloseDeps {
  readonly logger: Logger;
  readonly recordChannelClosed: (reason: string) => void;
  readonly isShuttingDown: () => boolean;
  readonly isStarting: () => boolean;
  readonly enterShuttingDown: () => void;
  readonly runtime: () => Runtime | undefined;
  readonly runner: () => RequestRunner | undefined;
  readonly abort: (
    runtime: Runtime,
    runner: RequestRunner,
    reason: string,
  ) => Promise<void>;
  readonly channelClosedError: (reason: string) => Error;
  readonly rejectActivation: (error: Error) => void;
}

/**
 * The link closed. The reason is always recorded for a start that may still be
 * in flight: it must close the storage it just opened instead of announcing
 * readiness to a Portal that is gone.
 */
export function handleChannelClose(
  reason: string,
  deps: ChannelCloseDeps,
): void {
  deps.recordChannelClosed(reason);
  if (deps.isShuttingDown()) {
    // Our own `close` at the end of an orderly shutdown, or the close that an
    // abort already issued.
    return;
  }
  const runtime = deps.runtime();
  const runner = deps.runner();
  if (runtime === undefined || runner === undefined) {
    // ADR-0007: no storage is open before `ready`, so failing here cannot
    // leave a second writer behind (INV-002). During `starting` the run
    // itself closes whatever it just opened; rejecting here as well would let
    // the caller exit the process while that close is still in flight.
    deps.logger.error(
      { code: 'IPC_CHANNEL_CLOSED', reason },
      'ipc channel closed before ready; server stopping',
    );
    if (!deps.isStarting()) {
      deps.enterShuttingDown();
      deps.rejectActivation(deps.channelClosedError(reason));
    }
    return;
  }
  deps.enterShuttingDown();
  deps.logger.error(
    { code: 'IPC_CHANNEL_CLOSED', reason },
    'ipc channel closed; server stopping',
  );
  void deps.abort(runtime, runner, reason);
}
