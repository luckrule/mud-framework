import { constants as osConstants } from 'node:os';
import type { Subprocess } from 'bun';

import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import { messageOf } from '../shared/guards.ts';
import { settleWithin } from '../shared/timeout.ts';
import {
  STDERR_DRAIN_GRACE_MS,
  pumpStderr,
  stderrCapture,
} from './bun-spawn-stderr.ts';
import { createEndpointChannel } from './channel-endpoint.ts';
import { IpcChannelClosedError } from './channel.ts';
import type { IpcMessage } from './protocol.ts';
import type { ServerExitInfo, SpawnedServer } from './spawn-port.ts';

export interface BunServerSpawnOptions {
  /** argv; the first entry is resolved through `PATH`. */
  command: string[];
  cwd?: string;
  /** Only used for the exit-after-disconnect note. */
  logger?: Logger;
  /**
   * Keep the last `stderrTailLines` lines of the child's stderr and report them
   * on `ServerExitInfo.stderrTail`. Off by default: stderr stays inherited, so
   * enabling this is the only thing that changes what the Portal's console
   * shows. Enabling it pipes stderr and mirrors each chunk back to the Portal's
   * stderr, because the lines are the operator's only view of a child that dies
   * before the handshake.
   */
  stderrTailLines?: number;
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
  const exit = Promise.withResolvers<ServerExitInfo>();
  const logger =
    options.logger === undefined ? undefined : safeLogger(options.logger);
  const capture = stderrCapture(options.stderrTailLines);

  let closedLocally = false;
  let subprocess: Subprocess | undefined;
  let recorded: ServerExitInfo | undefined;
  /**
   * Resolves once the stderr pipe is drained, or at once when capture is off.
   * `exited` waits on it (bounded) so a consumer sees the final lines: Bun
   * reports the exit independently of the pipe, and a snapshot taken at exit
   * time is routinely empty.
   */
  const drained = Promise.withResolvers<void>();

  const channel = createEndpointChannel({
    send: (message: IpcMessage): void => {
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
    // Closing the channel closes the pipe, not the process: killing is the
    // executor's decision, and it already has `kill()` for that.
    disconnect: (): void => {
      subprocess?.disconnect();
    },
    // The Portal closes this channel itself on every shutdown path; a handler
    // registered afterwards must not hear about a close it caused.
    suppressNotifyOnLocalClose: true,
    defaultCloseReason: LOCAL_CLOSE_REASON,
    // Read by `recordExit` so a local close does not log the exit as a
    // peer-side loss.
    onLocalClose: (): void => {
      closedLocally = true;
    },
  });

  /**
   * Records how the process ended and reports it exactly once. `observed` is
   * false when the exit could not be determined: the executor must then assume
   * the process may still be running rather than report a fake exit with a null
   * code, which would skip its defensive kill (INV-002).
   *
   * The record itself is taken immediately — `lastExit()` must stay synchronous
   * for the channel-close path — while `exited` waits (bounded) for the stderr
   * pipe so the tail it carries is the final one. The wait never gates the
   * channel close or the kill: both happen before it starts.
   */
  function recordExit(
    code: number | null,
    signal: string | null,
    observed: boolean,
  ): void {
    if (recorded !== undefined) {
      return;
    }
    recorded = exitInfoOf(code, signal, observed);
    const channelWasClosed = channel.closedReason !== undefined;
    if (observed) {
      channel.markClosed(
        signal === null
          ? `server process exited with code ${String(code)}`
          : `server process killed by ${signal}`,
      );
    }
    // `onDisconnect` can win the race and record its reason first. The exit is
    // the real cause, so it still gets a line of its own: otherwise the most
    // common failure (a crash after the pipe dropped) never logs its code.
    if (observed && channelWasClosed && !closedLocally) {
      logger?.warn(
        {
          code: 'IPC_SERVER_EXITED',
          exitCode: code,
          signal,
          reason: channel.closedReason,
        },
        'server process exited after its channel closed',
      );
    }
    // Default mode resolves `exited` here, exactly as before. Only a capture
    // run defers, and only to let the pipe deliver the lines the tail exists
    // for; `recorded` shares the live array, so the drain fills it in place.
    if (capture.lines === undefined) {
      exit.resolve(recorded);
      return;
    }
    void (async () => {
      await settleWithin(drained.promise, STDERR_DRAIN_GRACE_MS);
      exit.resolve(recorded);
    })();
  }

  /**
   * The tail array is shared with the exit info rather than copied when the
   * exit is recorded: `lastExit()` must answer synchronously for the
   * channel-close path, which can run before the pipe has delivered the last
   * lines. Sharing lets that caller see the same array fill in, while `exited`
   * (which waits for the drain) reports a stable one.
   */
  function exitInfoOf(
    code: number | null,
    signal: string | null,
    observed: boolean,
  ): ServerExitInfo {
    const base = { code, signal, observed };
    return capture.lines === undefined
      ? base
      : { ...base, stderrTail: capture.lines };
  }

  const spawned = Bun.spawn({
    cmd: options.command,
    cwd: options.cwd,
    // Only another Bun process can speak this protocol; a Node child would
    // silently misinterpret the serialized frames.
    ipc: (message: unknown) => {
      channel.receive(message);
    },
    onExit: (_subprocess, exitCode, signalCode) => {
      recordExit(exitCode, normaliseSignal(signalCode), true);
    },
    onDisconnect: () => {
      channel.markClosed(DISCONNECTED_REASON);
    },
    // The child must not read the terminal: stdin is the Portal's. Its logs go
    // to the same stderr so they stay visible; only an explicit capture request
    // pipes stderr, and then the pump mirrors every chunk back to it.
    stdio: capture.stdio,
  });
  subprocess = spawned;
  const stderrStream = spawned.stderr;
  if (capture.lines !== undefined && stderrStream !== undefined) {
    void (async () => {
      await pumpStderr(stderrStream, capture);
      drained.resolve();
    })();
  } else {
    drained.resolve();
  }
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
