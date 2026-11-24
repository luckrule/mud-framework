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
}

/**
 * One Server process plus the channel that talks to it. `exited` and the
 * channel's close both mean "this process is gone"; they race, so a ServerState
 * is the identity used to make crash handling happen at most once per process.
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
   * The exit recorded so far, readable synchronously.
   *
   * `exited` resolves on the same event, but its continuation runs a microtask
   * later and the channel's `onClose` fires synchronously first: without a
   * synchronous read the crash line reports `exitCode: null` and the real code
   * survives only inside the free-text reason. Optional so a minimal
   * implementation still satisfies the interface; a caller then falls back to
   * the asynchronous path.
   */
  lastExit?: () => ServerExitInfo | undefined;
  /** Forcible termination, used when graceful shutdown stalls. */
  kill(): void;
}
