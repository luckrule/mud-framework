import type { IpcChannel } from '../ipc/channel.ts';
import type { IpcMessage, ShutdownMessage } from '../ipc/protocol.ts';
import type { Logger } from '../logging/types.ts';
import { messageOf } from '../shared/guards.ts';
import { elapsedMs, settleWithin } from '../shared/timeout.ts';
import type { RequestRunner } from './request-runner.ts';
import type { Runtime } from './server-startup.ts';

/**
 * Budget for each step of an abort (Portal died, nobody can read a result).
 * Deliberately a fixed value unrelated to `timeoutMs`: nobody is waiting for
 * this process, and every second of a long wait is a second this orphaned
 * process keeps writing to storage beside the next Server (INV-002). A short
 * cap is the right trade: a handler that cannot finish in 5s is abandoned
 * rather than honoured.
 */
export const ABORT_TIMEOUT_MS = 5000;

export interface ServerShutdownDeps {
  readonly logger: Logger;
  readonly channel: IpcChannel;
  /** Reads the abort budget: the test override, else `ABORT_TIMEOUT_MS`. */
  readonly readAbortBudgetMs: () => number;
  /** Returns whether the message was sent; callers may ignore it. */
  readonly sendQuietly: (message: IpcMessage, what: string) => boolean;
  /** Enters `shutting-down`; the facade owns the phase field. */
  readonly markShuttingDown: () => void;
  /** Resolves the process completion with no error. */
  readonly complete: () => void;
  /** Rejects the process completion because the link died. */
  readonly rejectCompletion: (reason: string) => void;
}

/**
 * Owns the closing half of a Server process: the orderly drain that ends in
 * `closed`, and the abort used when nobody is left to receive one (SPEC 6.3).
 * The facade owns the phase, the channel lifecycle and the completion promise.
 */
export class ServerShutdown {
  constructor(private readonly deps: ServerShutdownDeps) {}

  /** The fixed budget an abort shares by its two steps. */
  get abortBudgetMs(): number {
    return this.deps.readAbortBudgetMs();
  }

  begin(
    runtime: Runtime,
    runner: RequestRunner,
    message: ShutdownMessage,
  ): void {
    this.deps.markShuttingDown();
    // Stop accepting before draining, so the drain below has a closed set of
    // requests and nothing new can enter the backend (SPEC 6.3 step 1).
    const budgetMs = message.timeoutMs;
    this.deps.logger.info(
      { pendingRequests: runner.running, budgetMs },
      'server shutting down',
    );
    void this.shutdown(runtime, runner, budgetMs).catch((error: unknown) => {
      // Nothing below `shutdown` is expected to throw, but a channel that
      // throws on close must still resolve the completion instead of hanging.
      this.deps.logger.error(
        { code: 'SHUTDOWN_FAILED', reason: messageOf(error) },
        'server shutdown failed',
      );
      this.deps.complete();
    });
  }

  private async shutdown(
    runtime: Runtime,
    runner: RequestRunner,
    limitMs: number,
  ): Promise<void> {
    const startedAt = performance.now();
    // Every accepted request settles to a terminal state, bounded by the
    // per-request timeout the dispatcher already enforces, and its result still
    // goes out before `closed`. Storage closes before `closed` is announced, so
    // Portal never restarts a Server whose backend is still open.
    await this.drainAndClose(runtime, runner, limitMs);
    this.deps.sendQuietly({ type: 'closed', reason: 'shutdown' }, 'closed');
    // The link itself goes last, after the final message.
    this.deps.channel.close('shutdown complete');
    this.deps.logger.info(
      { durationMs: elapsedMs(startedAt) },
      'server process stopped',
    );
    this.deps.complete();
  }

  /**
   * Drains and closes the backend under one deadline: the caller declared a
   * single budget for the whole close, so the second step spends what the first
   * left rather than a second full one.
   */
  async drainAndClose(
    runtime: Runtime,
    runner: RequestRunner,
    limitMs: number,
  ): Promise<void> {
    const deadlineAt = performance.now() + limitMs;
    await runner.drain(limitMs);
    await this.closeStorage(
      runtime,
      runner,
      Math.max(1, Math.round(deadlineAt - performance.now())),
    );
  }

  /**
   * Closes the backend, which is mandatory (SPEC 6.3 step 3), but bounded: a
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
      this.deps.logger.error(
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
      this.deps.logger.error(
        {
          code: 'STORAGE_CLOSE_FAILED',
          reason: messageOf(outcome.error),
        },
        'storage backend could not be closed',
      );
    }
  }

  async abort(
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
    const limitMs = this.deps.readAbortBudgetMs();
    try {
      await this.drainAndClose(runtime, runner, limitMs);
    } catch (error) {
      // Every caller uses `void abort(...)`, so a throw here would be an
      // unhandled rejection while `completion` never settles.
      this.deps.logger.error(
        { code: 'ABORT_DRAIN_FAILED', reason: messageOf(error) },
        'server abort drain failed',
      );
    }
    // On the parent-gone path the link is still open, and an open IPC channel
    // keeps the event loop alive: the whole point is that an orphaned process
    // goes away rather than lingering as a second writer (INV-002). The
    // injected channel is outside code, so a close that throws must not keep
    // `completion` from rejecting: the failure is logged for diagnosis and the
    // process still ends.
    try {
      this.deps.channel.close('aborting');
    } catch (error) {
      this.deps.logger.error(
        { code: 'ABORT_CHANNEL_CLOSE_FAILED', reason: messageOf(error) },
        'ipc channel could not be closed while aborting',
      );
    }
    this.deps.rejectCompletion(reason);
  }
}
