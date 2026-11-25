import type { Logger } from '../../logging/types.ts';
import {
  EXIT_WAIT_TIMEOUT_MS,
  SHUTDOWN_ACK_SLACK_MS,
} from '../../shared/budget.ts';
import { messageOf } from '../../shared/guards.ts';
import { settleWithin } from '../../shared/timeout.ts';
import { FRAMEWORK_SHUTDOWN } from '../../shared/wire-codes.ts';
import { killQuietly, waitForExit } from './server-launcher.ts';
import type { ServerSlot, ServerState } from './server-slot.ts';
import type { SwitchOperation } from './version-switch.ts';

/**
 * What the close sequence needs from the executor. Every member reads live
 * state or calls back into the executor, so a step never holds a stale status,
 * Server, buffer size or start waiter.
 *
 * Narrow on purpose: no delivery, restart or switch policy lives here. The
 * state transitions still go through the slot's own methods, so a close cannot
 * write a status the executor would not have written.
 */
export interface ShutdownHost {
  readonly logger: Logger;
  /** Sole owner of the status and the current Server; writes go through it. */
  readonly slot: ServerSlot<SwitchOperation>;
  readonly shutdownTimeoutMs: number;
  readonly versionSwitch: { abortSwitchForClose(): Promise<void> };
  readonly ledger: {
    failUndelivered(reason: string): void;
    awaitPendingSettled(): Promise<void>;
    failAllPending(code: string, message: string, reason: string): void;
    /** Buffered, unacknowledged requests; reported when the drain times out. */
    pendingCount(): number;
  };
  readonly restart: { clearRestartTimer(): void; clearStableClock(): void };
  /**
   * The recovery barrier's terminal transition. A confirmed close releases it,
   * so health converges to `closed`; a close that could not confirm every
   * exit must keep reporting `blocked`, because it is the only observable
   * signal that a writer may still be alive (INV-002).
   */
  readonly recovery: { clear(): void; block(): void };
  /**
   * The two promises the executor may be parked on. Both must be ended by the
   * close: a start still waiting for `ready` is rejected rather than left
   * hanging, and a `closed` wait is what the shutdown handshake resolves.
   */
  readonly waiters: {
    /** Stops the start timeout so it cannot fire mid-close. */
    clearStartTimer(): void;
    /** Fails a `start()` still waiting for `ready`. */
    rejectStart(error: Error): void;
    waitForClosed(server: ServerState, waitMs: number): Promise<void>;
  };
  /** Snapshots unconfirmed processes without dropping their retained references. */
  listPendingExits(): ServerState[];
}

/**
 * Ends the executor: stop the timers and any switch in flight, fail what cannot
 * complete, shut the Server down and wait for it to be gone.
 *
 * The step order is the contract, not an implementation detail: the status flips
 * first so nothing new is accepted, the switch is interrupted before the drain
 * so it cannot spawn beside the process being closed, undelivered requests fail
 * before the drain (they cannot complete), and the status only becomes `closed`
 * after the process exit was confirmed (INV-002). Callers memoise this, so it
 * runs once per executor.
 */
export async function doClose(host: ShutdownHost): Promise<void> {
  host.slot.markClosing();
  host.restart.clearRestartTimer();
  host.restart.clearStableClock();
  host.waiters.clearStartTimer();
  // A switch in flight must not keep spawning, activating or restarting: it is
  // interrupted here and its staged process abandoned, so `close()` ends with
  // at most one Server to shut down and no orphan writer (INV-002).
  await host.versionSwitch.abortSwitchForClose();
  // A Request that never reached a live Server cannot complete, so it fails
  // now instead of burning its whole deadline. Delivered ones are given
  // until their deadline to settle (SPEC 6.3 step 3).
  host.ledger.failUndelivered('executor closing before delivery');
  // Bounded by the same budget as the rest of the shutdown. An unbounded wait
  // here used to mean "as long as the longest request deadline", so a
  // ConnectionLayer with a shorter shutdownTimeout would return while this
  // executor was still waiting and the Server was still writing (the two
  // timeouts were unrelated). Past the budget the requests are failed and the
  // Server is shut down anyway.
  const drained = await settleWithin(
    host.ledger.awaitPendingSettled(),
    host.shutdownTimeoutMs,
  );
  // A rejection counts as "no longer pending" here, on purpose: the waiter
  // only ever resolves, so the one thing this branch must distinguish is a
  // timeout. Treating a rejection as a timeout would fail every buffered
  // request with a message about a deadline that was never reached.
  if (drained.status === 'timeout') {
    host.logger.warn(
      {
        timeoutMs: host.shutdownTimeoutMs,
        pending: host.ledger.pendingCount(),
      },
      'pending requests did not settle before the shutdown timeout',
    );
    host.ledger.failAllPending(
      FRAMEWORK_SHUTDOWN,
      'ipc executor closed before the request settled',
      'shutdown drain timed out',
    );
  }
  const current = host.slot.server;
  const currentExited = await shutdownServer(host);
  const abandonedExited = await confirmAbandonedExits(host, current);
  host.ledger.failAllPending(
    FRAMEWORK_SHUTDOWN,
    'ipc executor closed',
    'executor closed',
  );
  // A start() still waiting for `ready` must not be told "ready" by a close.
  host.waiters.rejectStart(
    new Error('ipc executor closed before the server became ready'),
  );
  if (!currentExited || !abandonedExited) {
    // An unconfirmed exit leaves a possible writer behind: the executor must
    // keep reporting `blocked` rather than converge to `closed` (INV-002).
    host.recovery.block();
    throw new Error('ipc executor close could not confirm all server exits');
  }
  // The close confirmed every exit, so the recovery barrier has no meaning any
  // more; a stale `pendingRecovery` must not make health keep reporting it.
  host.recovery.clear();
  host.slot.markClosed();
}

/**
 * Confirms every retained non-current process with a bounded wait → kill → wait.
 * A failed final wait leaves the reference retained and makes close reject;
 * neither a sent kill nor a closed channel proves storage was released.
 */
async function confirmAbandonedExits(
  host: ShutdownHost,
  current: ServerState | undefined,
): Promise<boolean> {
  const pending = host
    .listPendingExits()
    .filter((server) => server !== current);
  // Parallel, not sequential: these are independent processes, and the budget
  // counts one bounded wait→kill→wait, not one per process.
  const confirmed = await Promise.all(
    pending.map(async (server) => {
      const exited = await waitForExit(server, host.logger);
      if (exited) {
        host.slot.confirmExit(server);
        return true;
      }
      host.logger.warn(
        { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
        'abandoned replacement did not exit; killing it before close returns',
      );
      killQuietly(server, host.logger);
      const afterKill = await waitForExit(server, host.logger);
      if (!afterKill) {
        host.logger.warn(
          { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
          'abandoned replacement is still running after a kill; close cannot confirm its exit',
        );
      } else {
        host.slot.confirmExit(server);
      }
      return afterKill;
    }),
  );
  return confirmed.every(Boolean);
}

async function shutdownServer(host: ShutdownHost): Promise<boolean> {
  const server = host.slot.server;
  if (server === undefined) {
    return true;
  }
  if (!server.closed) {
    if (server.ready && !server.shutdownSent) {
      try {
        // The Server gets exactly the grace `waitForClosed` will wait for, so
        // its drain and backend close share one deadline that ends with the
        // Portal's patience rather than twice its own request ceiling.
        server.shutdownSent = true;
        server.channel.send({
          type: 'shutdown',
          timeoutMs: host.shutdownTimeoutMs,
        });
      } catch (error) {
        // The process died between the pending drain and this send; the
        // channel close handler is about to report the same thing.
        host.logger.warn(
          { pid: server.pid, reason: messageOf(error) },
          'shutdown message could not be sent',
        );
      }
    }
    if (server.ready) {
      await host.waiters.waitForClosed(
        server,
        host.shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS,
      );
    }
    if (!server.closed && !server.shutdownAcknowledged) {
      // A stalled graceful shutdown, or a Server that never finished
      // starting, must not hold the Portal open. The logged wait is the one
      // `waitForClosed` actually performed, not the declared grace.
      host.logger.warn(
        {
          pid: server.pid,
          timeoutMs: host.shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS,
          ready: server.ready,
        },
        'server did not finish shutting down; killing it',
      );
      killQuietly(server, host.logger);
    }
  }
  // A `closed` message says the Server finished its work, not that the
  // process is gone; the storage must not be reopened while it lingers
  // (INV-002). This also covers a process that was already killed from the
  // channel-loss path. Waiting is bounded, and a lingering process is logged.
  let exited = await waitForExit(server, host.logger);
  if (!exited) {
    // `close()` must not return while the Server may still be writing: a
    // caller that then reopens storage gets two writers, which is exactly the
    // defect this class exists to prevent. The graceful path withheld the kill
    // on purpose, so it is issued here and the exit is awaited once more.
    host.logger.warn(
      { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
      'server process did not exit; killing it before close returns',
    );
    killQuietly(server, host.logger);
    exited = await waitForExit(server, host.logger);
    if (!exited) {
      host.logger.warn(
        { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
        'server is still running after a kill; close cannot confirm its exit',
      );
    }
  }
  server.closed = true;
  server.closedSignal.resolve();
  try {
    server.channel.close('portal is shutting down');
  } catch (error) {
    host.logger.debug({ reason: messageOf(error) }, 'ipc channel close failed');
  }
  host.slot.confirmExit(server);
  host.slot.clearIfCurrent(server);
  return exited;
}
