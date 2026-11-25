import type { IpcChannel } from '../ipc/channel.ts';
import type { ServerExitInfo } from '../ipc/spawn-port.ts';

/**
 * `failed` is a terminal state that is not `closed`: the executor gave up
 * because the Server never became ready within `startTimeoutMs`. It is distinct
 * from `closed` so `execute` can report a different message and so a failed
 * start cannot be mistaken for an orderly shutdown.
 *
 * `switching` is not terminal: a replacement is being prepared or the previous
 * process is being retired. Requests keep arriving and stay buffered; only the
 * promotion to the new generation delivers them.
 */
type Status =
  | 'idle'
  | 'starting'
  | 'ready'
  | 'switching'
  | 'closing'
  | 'closed'
  | 'failed';

export interface ServerState {
  readonly generation: number;
  readonly channel: IpcChannel;
  readonly kill: () => void;
  /**
   * Resolves when the child process is known to be gone. Awaited before a
   * replacement is spawned, because SIGKILL returning does not mean the process
   * released its storage yet (INV-002).
   */
  readonly exitPromise: Promise<ServerExitInfo>;
  /** Synchronous view of the exit, for the channel-close path. */
  readonly lastExit: () => ServerExitInfo | undefined;
  ready: boolean;
  closed: boolean;
  /** True once the child process itself is known to be gone. */
  exited: boolean;
  /** Set when the Server answered `shutdown` with `closed`. */
  shutdownAcknowledged: boolean;
  pid: number | undefined;
  /**
   * Owned by a `switchTo` in progress: not the current Server, so losing it
   * fails the switch instead of triggering the crash-restart path.
   */
  staged: boolean;
  /**
   * The previous Server a `switchTo` is draining. Its exit is expected, so it
   * must not be reported as a crash or restarted.
   */
  retiring: boolean;
  /** Capabilities the Server declared in `ready`; absent before it. */
  declaredMaxDurationMs: number | undefined;
  /** `codeVersion` the Server reported in `prepared`/`ready`. */
  reportedCodeVersion: string | undefined;
  /** True once a `shutdown` was sent, so a switch and `close()` cannot both send one. */
  shutdownSent: boolean;
  /** Resolves once `closed` arrived or the process is known to be gone. */
  readonly closedSignal: PromiseWithResolvers<void>;
}

/**
 * What the slot needs from an in-flight switch to enforce INV-002: the process
 * it is promoting and the one it is retiring. `SwitchOperation` is the only
 * implementation.
 */
interface SwitchOwner {
  readonly staged: ServerState | undefined;
  readonly previous: ServerState | undefined;
}

/**
 * Single owner of the executor's process identity: which status the Portal is
 * in, which Server is current, which generation number is next, and which
 * switch (if any) is in flight.
 *
 * The fields are only mutated through the transitions below, so a state write
 * cannot skip the meaning attached to it. Reads are direct: a getter that
 * returned a copy would make the executor compare stale values.
 */
export class ServerSlot<Op extends SwitchOwner = SwitchOwner> {
  private currentStatus: Status = 'idle';
  private currentServer: ServerState | undefined;
  private currentSwitch: Op | undefined;
  private generations = 0;

  get status(): Status {
    return this.currentStatus;
  }

  get server(): ServerState | undefined {
    return this.currentServer;
  }

  get switching(): Op | undefined {
    return this.currentSwitch;
  }

  /**
   * True once the executor can neither restart a Server nor accept new work.
   *
   * Kept in one place because the invariant it guards is INV-002: a restart is
   * only safe when no other Server process can still hold storage. The decision
   * "may I restart / deliver" is made at several points, and a new state (say
   * `draining`) added to all but `onServerLost` would let a `close()` restart
   * the Server, leaving two writers on the same storage. That failure is data
   * corruption, not an error, and the log would read as an ordinary crash plus
   * a backoff, so it cannot be diagnosed from the outside.
   */
  get isTerminal(): boolean {
    return (
      this.currentStatus === 'closing' ||
      this.currentStatus === 'closed' ||
      this.currentStatus === 'failed'
    );
  }

  /** `start()` leaves `idle`; a restart after a loss uses `markStartingAfterLoss`. */
  beginStart(): void {
    this.currentStatus = 'starting';
  }

  /** A spawned process becomes current while it is still starting. */
  trackSpawning(server: ServerState): void {
    this.currentServer = server;
  }

  /** The handshake completed: this generation is current and may serve. */
  markReady(server: ServerState): void {
    this.currentServer = server;
    this.currentStatus = 'ready';
  }

  /** A `switchTo` took ownership of the current Server and its replacement. */
  beginSwitch(op: Op): void {
    this.currentSwitch = op;
    this.currentStatus = 'switching';
  }

  /**
   * The replacement was promoted: it becomes the current Server and the
   * recovery barrier opens. The operation itself is released by the switch's
   * own finish step, which still has to report the outcome.
   */
  adopt(server: ServerState): void {
    this.currentServer = server;
    this.currentStatus = 'ready';
  }

  /** The switch failed before disturbing the previous process, which keeps serving. */
  resumeServing(): void {
    this.currentStatus = 'ready';
  }

  /** Ends one switch; a later one may then start. */
  releaseSwitch(op: Op): void {
    if (this.currentSwitch === op) {
      this.currentSwitch = undefined;
    }
  }

  /**
   * Drops `server` as the current one, and only if it still is: an exit that
   * arrives after a replacement was promoted must not clear the replacement.
   */
  clearIfCurrent(server: ServerState): boolean {
    if (this.currentServer !== server) {
      return false;
    }
    this.currentServer = undefined;
    return true;
  }

  markClosing(): void {
    this.currentStatus = 'closing';
  }

  markClosed(): void {
    this.currentStatus = 'closed';
  }

  markFailed(): void {
    this.currentStatus = 'failed';
  }

  /** No Server is live any more and the executor is trying to get one. */
  markStartingAfterLoss(): void {
    this.currentStatus = 'starting';
  }

  /** Allocates the identity stamped on a Server and its deliveries. */
  nextGeneration(): number {
    this.generations += 1;
    return this.generations;
  }

  /**
   * The INV-002 gate as a debug assertion: a process may only open storage when
   * no other one can still be writing. `except` is the process about to be
   * activated.
   *
   * Unreachable under the current switch orchestration — `activate` goes out
   * only after the previous process was confirmed gone, and `currentServer` is
   * that same process — so this throws only if a future orchestration change
   * introduces a second writer. It stays a throw (not a log) because reaching it
   * means data corruption, and a switch that fails is the safe outcome.
   */
  assertNoOtherLiveServer(except?: ServerState): void {
    const candidates: (ServerState | undefined)[] = [
      this.currentServer,
      this.currentSwitch?.staged,
      this.currentSwitch?.previous,
    ];
    for (const candidate of candidates) {
      if (
        candidate === undefined ||
        candidate === except ||
        candidate.closed ||
        candidate.exited
      ) {
        continue;
      }
      throw new Error(
        `ipc executor has another live server (generation ${candidate.generation}, pid ${candidate.pid ?? 'unknown'}) while activating generation ${except?.generation ?? 'unknown'}`,
      );
    }
  }
}
