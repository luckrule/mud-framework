import type { IpcChannel } from './channel.ts';

/**
 * How a Server process ended. `observed` is false when the exit could not be
 * determined (a failed waitpid): `code` and `signal` are then `null` because
 * nothing was seen, not because the process exited without them. Callers must
 * treat an unobserved exit as "the process may still be alive".
 */
export interface ServerExitInfo {
  readonly code: number | null;
  readonly signal: string | null;
  readonly observed: boolean;
  /**
   * The last stderr lines, when the spawn was asked to capture them. Absent
   * under the default `inherit` mode, which is why this is optional: a startup
   * failure's real cause (a syntax error, a missing module) otherwise exists
   * only on the Portal's console and cannot reach a caller programmatically.
   */
  readonly stderrTail?: readonly string[];
}

/**
 * One Server process plus the channel that talks to it. A channel close does
 * not mean the process is gone: only `exited` resolving with `observed: true`
 * releases the writer barrier, and the two race, so a ServerState is the
 * identity used to make crash handling happen at most once per process.
 *
 * This is the port the executor depends on, not an implementation detail of it:
 * a spawn can come from `Bun.spawn`, `child_process`, a container or a remote
 * link, so the contract lives beside the channel it carries rather than inside
 * the consumer that happens to use it first.
 */
export interface SpawnedServer {
  channel: IpcChannel;
  /** Resolves once the child process is gone, or once that cannot be observed. */
  exited: Promise<ServerExitInfo>;
  /**
   * The exit recorded so far, readable synchronously. `undefined` means "no
   * exit has been recorded yet", not "this spawn cannot report one".
   *
   * `exited` resolves on the same event, but its continuation runs a microtask
   * later and the channel's `onClose` fires synchronously first: without a
   * synchronous read the crash line reports `exitCode: null` and the real code
   * survives only inside the free-text reason. Required because every spawn
   * can answer it — the caller has no asynchronous fallback for the
   * channel-close path.
   */
  lastExit(): ServerExitInfo | undefined;
  /** Forcible termination, used when graceful shutdown stalls. */
  kill(): void;
}
