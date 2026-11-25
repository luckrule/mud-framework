import type { ServerExitInfo, SpawnedServer } from '../../ipc/spawn-port.ts';
import type { Logger } from '../../logging/types.ts';
import { EXIT_WAIT_TIMEOUT_MS } from '../../shared/budget.ts';
import { messageOf } from '../../shared/guards.ts';
import type { ServerState } from './server-slot.ts';

/**
 * The process-lifecycle primitives the Portal side of the split (ADR-0002)
 * spends on one Server: wrapping a spawn in a `ServerState`, wiring its
 * message/close/exit handlers, and the bounded waits and quiet kills the start,
 * switch, crash and close paths share.
 *
 * Kept apart from the executor so the decisions stay there: nothing here reads
 * a status or a "current Server", nothing imports the executor, and every
 * caller-specific fact (the generation to stamp, the wait to spend, the logger)
 * arrives as an argument.
 */

/** What the owner of a process reacts to. */
export interface ServerHandlers {
  /** One raw message from the Server; parsing and dispatch are the caller's. */
  onMessage: (raw: unknown) => void;
  /** Actual exit confirmation is independent of loss-notification deduplication. */
  onExit: () => void;
  /**
   * The process is gone, or its exit could not be observed. Both signals — the
   * channel close and the exit promise — arrive here, because they race and
   * only the owner can decide what the loss means.
   */
  onLost: (code: number | null, signal: string | null, reason: string) => void;
}

/**
 * Wraps a spawn in the per-process bookkeeping the executor reads. `generation`
 * is allocated by the caller: the counter belongs to the slot that also
 * compares generations.
 */
export function buildServer(
  spawned: SpawnedServer,
  generation: number,
): ServerState {
  return {
    generation,
    channel: spawned.channel,
    kill: () => {
      spawned.kill();
    },
    exitPromise: spawned.exited,
    lastExit: () => spawned.lastExit(),
    ready: false,
    closed: false,
    exited: false,
    shutdownAcknowledged: false,
    pid: undefined,
    staged: false,
    retiring: false,
    declaredMaxDurationMs: undefined,
    reportedCodeVersion: undefined,
    shutdownSent: false,
    closedSignal: Promise.withResolvers<void>(),
  };
}

/**
 * Installs the message, close and exit handlers on one process. Shared by the
 * ordinary start/restart path and by `switchTo`, so a staged process is
 * observed exactly like the current one.
 */
export function attachServer(
  server: ServerState,
  handlers: ServerHandlers,
): void {
  // Handlers before the handshake: a `ready` that beats the registration is
  // still delivered (the channel queues it), but registering first keeps the
  // real Bun channel from dropping an early message.
  server.channel.onMessage((raw) => {
    handlers.onMessage(raw);
  });
  server.channel.onClose((reason) => {
    // The exit event may already have happened: `exited` resolves on the same
    // tick, but its continuation is a microtask and this callback is
    // synchronous, so the promise is not readable yet. The synchronous view
    // is what keeps the real exit code in the crash line instead of a null.
    const recorded = server.lastExit();
    if (recorded?.observed === true) {
      server.exited = true;
      handlers.onLost(recorded.code, recorded.signal, reason);
      handlers.onExit();
      return;
    }
    handlers.onLost(null, null, reason);
  });
  // Loss notifications race and are deduplicated by the owner. Real exit
  // confirmation must still arrive independently after an earlier channel loss.
  void (async () => {
    // `exited` resolves with `observed: false` rather than rejecting, so a
    // failed waitpid still reaches the same handling path. The catch is for a
    // spawn implementation that rejects instead.
    const info = await server.exitPromise.catch((): ServerExitInfo => ({
      code: null,
      signal: null,
      observed: false,
    }));
    // `exited` means "known to be gone". An unobserved exit must leave it
    // false so the owner still takes the defensive kill path: claiming it
    // exited would skip the only kill and start a second writer beside a
    // process that may still be running (INV-002).
    if (info.observed) {
      server.exited = true;
    }
    handlers.onLost(
      info.code,
      info.signal,
      info.observed
        ? 'server process exited'
        : 'server exit could not be observed',
    );
    if (info.observed) {
      handlers.onExit();
    }
  })();
}

/**
 * Waits until the child process is known to be gone, bounded so an
 * uninterruptible process cannot stall shutdown or restart forever. Resolves
 * true only when the exit was observed. False retains the process reference:
 * recovery stays blocked, and close rejects if its final retry is unconfirmed.
 *
 * The exit info is read here rather than from `server.exited`, because that
 * flag is set by the launch continuation and the two awaits are not ordered
 * against each other.
 */
export async function waitForExit(
  server: ServerState,
  logger: Logger,
): Promise<boolean> {
  if (server.exited) {
    return true;
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<'timeout'>((resolve) => {
    timer = setTimeout(() => {
      resolve('timeout');
    }, EXIT_WAIT_TIMEOUT_MS);
  });
  const info = await Promise.race([
    server.exitPromise.catch((): ServerExitInfo => ({
      code: null,
      signal: null,
      observed: false,
    })),
    timeout,
  ]);
  if (timer !== undefined) {
    clearTimeout(timer);
  }
  if (info === 'timeout' || !info.observed) {
    logger.warn(
      { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
      'server process did not exit within the wait; it may still be running',
    );
    return false;
  }
  server.exited = true;
  return true;
}

/**
 * The single acknowledgement slot a close path waits on and the message/exit
 * handlers wake. It holds the one pending wait and its timer, because both the
 * `closed` message and a lost process can end that wait.
 */
export class ClosedWaiter {
  private waiter: (() => void) | undefined;
  private timer: ReturnType<typeof setTimeout> | undefined;

  /**
   * Waits up to `waitMs` for the Server's `closed` acknowledgement. Returns at
   * once when the acknowledgement already arrived.
   */
  waitForClosed(server: ServerState, waitMs: number): Promise<void> {
    if (server.closed || server.shutdownAcknowledged) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.waiter = resolve;
      this.timer = setTimeout(() => {
        this.resolve();
      }, waitMs);
      // The exit may have landed between the check above and this assignment,
      // in which case its `resolve` found no waiter. Re-check so the grace
      // period is not paid for a process that is already gone.
      if (server.closed || server.shutdownAcknowledged) {
        this.resolve();
      }
    });
  }

  /** Ends the pending wait, if any; safe to call when nothing is waiting. */
  resolve(): void {
    if (this.timer !== undefined) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    const waiter = this.waiter;
    this.waiter = undefined;
    waiter?.();
  }
}

export function killQuietly(server: ServerState, logger: Logger): void {
  try {
    server.kill();
  } catch (error) {
    logger.warn(
      { pid: server.pid, reason: messageOf(error) },
      'server could not be killed',
    );
  }
}

export function closeChannelQuietly(
  server: ServerState,
  reason: string,
  logger: Logger,
): void {
  try {
    server.channel.close(reason);
  } catch (error) {
    logger.debug({ reason: messageOf(error) }, 'ipc channel close failed');
  }
}
