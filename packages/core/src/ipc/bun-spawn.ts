import { constants as osConstants } from 'node:os';
import type { Subprocess } from 'bun';

import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import { messageOf } from '../shared/guards.ts';
import { IpcChannelClosedError } from './channel.ts';
import type { IpcChannel } from './channel.ts';
import type { IpcMessage } from './protocol.ts';
import type { ServerExitInfo, SpawnedServer } from './spawn-port.ts';

export interface BunServerSpawnOptions {
  /** argv; the first entry is resolved through `PATH`. */
  command: string[];
  cwd?: string;
  /** Only used for the exit-after-disconnect note. */
  logger?: Logger;
}

const LOCAL_CLOSE_REASON = 'closed by portal';
const DISCONNECTED_REASON = 'ipc channel disconnected';

/**
 * `Bun.spawn` as a `SpawnedServer` (ADR-0002). The IPC callbacks are the only
 * bridge between the two processes, so this file exists to keep `Bun.*` out of
 * the executor, which must stay testable against the in-memory pair.
 *
 * Two Bun behaviours shape the implementation:
 *
 * - `onExit` may run before `Bun.spawn` returns, so nothing in the exit path may
 *   assume the `Subprocess` handle is assigned yet.
 * - `subprocess.send()` throws once the child is gone ("cannot be used after the
 *   process has exited"), and a `killed` process may still have exited on its
 *   own. Exit state is therefore taken from `exitCode`/`signalCode` only.
 */
export function spawnBunServer(options: BunServerSpawnOptions): SpawnedServer {
  const inbound: unknown[] = [];
  const exit = Promise.withResolvers<ServerExitInfo>();
  const logger =
    options.logger === undefined ? undefined : safeLogger(options.logger);

  let onMessage: ((message: unknown) => void) | undefined;
  let onClose: ((reason: string) => void) | undefined;
  let closeReason: string | undefined;
  let closedLocally = false;
  let notified = false;
  let subprocess: Subprocess | undefined;
  let recorded: ServerExitInfo | undefined;

  function notifyClose(): void {
    if (closeReason === undefined || notified || closedLocally) {
      return;
    }
    if (onClose === undefined) {
      // Leave `notified` false: a close that beats subscription must still
      // reach a handler registered later (IpcChannel contract).
      return;
    }
    notified = true;
    onClose(closeReason);
  }

  function markClosed(reason: string): void {
    if (closeReason !== undefined) {
      return;
    }
    closeReason = reason;
    inbound.length = 0;
    notifyClose();
  }

  /**
   * Records how the process ended and reports it exactly once. `observed` is
   * false when the exit could not be determined: the executor must then assume
   * the process may still be running rather than report a fake exit with a null
   * code, which would skip its defensive kill (INV-002).
   */
  function recordExit(
    code: number | null,
    signal: string | null,
    observed: boolean,
  ): void {
    if (recorded !== undefined) {
      return;
    }
    recorded = { code, signal, observed };
    const channelWasClosed = closeReason !== undefined;
    if (observed) {
      markClosed(
        signal === null
          ? `server process exited with code ${String(code)}`
          : `server process killed by ${signal}`,
      );
    }
    exit.resolve(recorded);
    // `onDisconnect` can win the race and record its reason first. The exit is
    // the real cause, so it still gets a line of its own: otherwise the most
    // common failure (a crash after the pipe dropped) never logs its code.
    if (observed && channelWasClosed && !closedLocally) {
      logger?.warn(
        {
          code: 'IPC_SERVER_EXITED',
          exitCode: code,
          signal,
          reason: closeReason,
        },
        'server process exited after its channel closed',
      );
    }
  }

  const spawned = Bun.spawn({
    cmd: options.command,
    cwd: options.cwd,
    // Only another Bun process can speak this protocol; a Node child would
    // silently misinterpret the serialized frames.
    ipc: (message: unknown) => {
      if (closeReason !== undefined) {
        return;
      }
      if (onMessage === undefined) {
        inbound.push(message);
        return;
      }
      onMessage(message);
    },
    onExit: (_subprocess, exitCode, signalCode) => {
      recordExit(exitCode, normaliseSignal(signalCode), true);
    },
    onDisconnect: () => {
      markClosed(DISCONNECTED_REASON);
    },
    // The child must not read the terminal: stdin is the Portal's. Its logs go
    // to the same stderr so they stay visible.
    stdio: ['ignore', 'inherit', 'inherit'],
  });
  subprocess = spawned;
  // `onExit` is not guaranteed to be the first observer: the `exited` promise
  // resolves on the same event, and whichever arrives first records the result.
  void (async () => {
    try {
      const code = await spawned.exited;
      recordExit(code, normaliseSignal(spawned.signalCode), true);
    } catch {
      // waitpid failed. The synchronous fields are the only evidence left; if
      // they say nothing, the exit is unobservable rather than clean.
      const code = spawned.exitCode;
      const signal = normaliseSignal(spawned.signalCode);
      if (code === null && signal === null) {
        recordExit(null, null, false);
        return;
      }
      recordExit(code, signal, true);
    }
  })();

  const channel: IpcChannel = {
    send(message: IpcMessage): void {
      if (closeReason !== undefined) {
        throw new IpcChannelClosedError(closeReason);
      }
      if (subprocess === undefined) {
        throw new IpcChannelClosedError('server process is not available');
      }
      try {
        subprocess.send(message);
      } catch (error) {
        // The child exited between the check above and this call. Surfacing it
        // as the channel's own error keeps one failure type for "cannot send".
        throw new IpcChannelClosedError(messageOf(error));
      }
    },
    onMessage(handler: (message: unknown) => void): void {
      onMessage = handler;
      const queued = inbound.splice(0);
      for (const message of queued) {
        handler(message);
      }
    },
    onClose(handler: (reason: string) => void): void {
      onClose = handler;
      notifyClose();
    },
    close(reason?: string): void {
      if (closeReason !== undefined) {
        return;
      }
      closedLocally = true;
      closeReason = reason ?? LOCAL_CLOSE_REASON;
      inbound.length = 0;
      try {
        // Closing the channel closes the pipe, not the process: killing is the
        // executor's decision, and it already has `kill()` for that.
        subprocess?.disconnect();
      } catch {
        // Already disconnected or already exited; the state we wanted holds.
      }
    },
  };

  return {
    channel,
    exited: exit.promise,
    // Read synchronously by the executor's channel-close handler: `recordExit`
    // records before it notifies, so this is already populated by the time
    // `onClose` runs even though `exited` has not settled yet.
    lastExit: () => recorded,
    kill(): void {
      try {
        subprocess?.kill('SIGKILL');
      } catch {
        // Raced with a natural exit; nothing left to kill.
      }
    },
  };
}

/**
 * `onExit` types the signal as `NodeJS.Signals | number | null`, so the same
 * kill could be recorded as "SIGKILL" by one path and "9" by the other
 * depending on which observer ran first. Both are normalised to the name.
 */
function normaliseSignal(
  signal: NodeJS.Signals | number | null | undefined,
): string | null {
  if (signal === null || signal === undefined) {
    return null;
  }
  if (typeof signal === 'string') {
    return signal;
  }
  const name = SIGNAL_NAMES.get(signal);
  return name ?? String(signal);
}

/** Signal number to name, built once: the reverse of `os.constants.signals`. */
const SIGNAL_NAMES: ReadonlyMap<number, string> = new Map(
  Object.entries(osConstants.signals).map(([name, number]) => [number, name]),
);
