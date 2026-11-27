import { constants as osConstants } from 'node:os';
import type { Subprocess } from 'bun';

import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import { messageOf } from '../shared/guards.ts';
import { settleWithin } from '../shared/timeout.ts';
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
/** Caps one retained line so a child cannot make the tail unbounded. */
const STDERR_LINE_LIMIT = 2000;
/**
 * Bounds how long exit reporting waits for the stderr pipe to drain. Bun
 * reports the exit independently of the pipe, so without this the tail a
 * consumer reads can be empty; a grandchild can keep the pipe open forever, so
 * the wait has to be bounded — the tail is diagnostic, not evidence.
 */
const STDERR_DRAIN_GRACE_MS = 250;

type ServerStdio =
  | ['ignore', 'inherit', 'inherit']
  | ['ignore', 'inherit', 'pipe'];

interface StderrCapture {
  readonly stdio: ServerStdio;
  /** The live tail, or undefined when capture is off. */
  readonly lines: string[] | undefined;
  readonly limit: number;
}

/**
 * Resolves the capture option to the stdio tuple and the live tail it fills.
 * A bad count throws instead of silently disabling the diagnostic: a caller who
 * asked for a tail must not get a run that looks configured but reports none.
 */
function stderrCapture(limit: number | undefined): StderrCapture {
  if (limit === undefined) {
    return {
      stdio: ['ignore', 'inherit', 'inherit'],
      lines: undefined,
      limit: 0,
    };
  }
  if (!Number.isInteger(limit) || limit <= 0) {
    throw new TypeError(
      `stderrTailLines must be a positive integer, got ${String(limit)}`,
    );
  }
  return { stdio: ['ignore', 'inherit', 'pipe'], lines: [], limit };
}

/**
 * Keeps the last `limit` lines while mirroring every chunk to the Portal's own
 * stderr, and resolves once the pipe is drained.
 *
 * Mirroring rather than logging keeps the operator's view identical to the
 * default `inherit`: the same fd 2, no structured-log flood from a chatty child,
 * and a custom logger sink is not silently bypassed. The mirror is best-effort;
 * a closed stderr must not break exit observation.
 *
 * The tail array is shared with every `ServerExitInfo` this spawn reports. Bun
 * reports the exit independently of the pipe, so a consumer can observe the
 * exit before the last lines have been read; the returned promise lets
 * `recordExit` wait for them within a bound instead of reporting an empty tail.
 */
function pumpStderr(
  stream: ReadableStream<Uint8Array>,
  capture: StderrCapture,
): Promise<void> {
  const lines = capture.lines;
  if (lines === undefined) {
    return Promise.resolve();
  }
  const decoder = new TextDecoder();
  let pending = '';
  const trim = (): void => {
    const overflow = lines.length - capture.limit;
    if (overflow > 0) {
      lines.splice(0, overflow);
    }
  };
  const record = (text: string): void => {
    const parts = text.split('\n');
    pending = parts.pop() ?? '';
    for (const part of parts) {
      lines.push(part.slice(0, STDERR_LINE_LIMIT));
    }
    // A child that never writes a newline must not grow the tail either.
    if (pending.length > STDERR_LINE_LIMIT) {
      lines.push(pending.slice(0, STDERR_LINE_LIMIT));
      pending = '';
    }
    trim();
  };
  return (async () => {
    try {
      for await (const chunk of stream) {
        const text = decoder.decode(chunk, { stream: true });
        mirrorToPortalStderr(text);
        record(text);
      }
      mirrorToPortalStderr(decoder.decode());
      if (pending.length > 0) {
        lines.push(pending);
        pending = '';
        trim();
      }
    } catch {
      // A read failure only costs diagnostics; exit observation must not
      // depend on the pipe.
    }
  })();
}

function mirrorToPortalStderr(text: string): void {
  if (text.length === 0) {
    return;
  }
  try {
    process.stderr.write(text);
  } catch {
    // The Portal's stderr is gone; the child's output has nowhere to go.
  }
}

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
  const capture = stderrCapture(options.stderrTailLines);

  let onMessage: ((message: unknown) => void) | undefined;
  let onClose: ((reason: string) => void) | undefined;
  let closeReason: string | undefined;
  let closedLocally = false;
  let notified = false;
  let subprocess: Subprocess | undefined;
  let recorded: ServerExitInfo | undefined;
  /**
   * Resolves once the stderr pipe is drained, or at once when capture is off.
   * `exited` waits on it (bounded) so a consumer sees the final lines: Bun
   * reports the exit independently of the pipe, and a snapshot taken at exit
   * time is routinely empty.
   */
  const drained = Promise.withResolvers<void>();

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
    const channelWasClosed = closeReason !== undefined;
    if (observed) {
      markClosed(
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
          reason: closeReason,
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
