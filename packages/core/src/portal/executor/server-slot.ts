import type { IpcChannel } from '../../ipc/channel.ts';
import type { ServerExitInfo } from '../../ipc/spawn-port.ts';
import type { IpcHealthStatus } from './executor-health.ts';

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
  private currentStatus: IpcHealthStatus = 'idle';
  private currentServer: ServerState | undefined;
  private currentSwitch: Op | undefined;
  private generations = 0;
  private readonly processes = new Set<ServerState>();

  get pendingExits(): ServerState[] {
    return [...this.processes].filter((server) => !server.exited);
  }

  track(server: ServerState): void {
    this.processes.add(server);
  }

  confirmExit(server: ServerState): void {
    if (server.exited) {
      this.processes.delete(server);
    }
  }

  get status(): IpcHealthStatus {
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

  /** The first `start()` and a restart after a loss both need a Server. */
  beginStart(): void {
    this.currentStatus = 'starting';
  }

  /** A spawned process becomes current while it is still starting. */
  trackSpawning(server: ServerState): void {
    this.track(server);
    this.currentServer = server;
  }

  /**
   * The Server is serving: a completed handshake, or a promoted replacement.
   * A switch's own finish step still releases its operation and reports the
   * outcome.
   */
  markReady(server: ServerState): void {
    this.currentServer = server;
    this.currentStatus = 'ready';
  }

  /** A `switchTo` took ownership of the current Server and its replacement. */
  beginSwitch(op: Op): void {
    this.currentSwitch = op;
    this.currentStatus = 'switching';
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

  /** Allocates the identity stamped on a Server and its deliveries. */
  nextGeneration(): number {
    this.generations += 1;
    return this.generations;
  }

  /**
   * INV-002: no launch or activation may pass an unconfirmed process retained
   * from a loss or failed switch. `except` permits the current process while
   * staging a replacement, or the replacement that is about to be activated.
   * Channel closure and a sent kill never release this gate.
   */
  assertNoOtherLiveServer(except?: ServerState): void {
    // Every process is `track`ed at creation, so `processes` already covers the
    // current Server and both switch participants.
    for (const candidate of this.processes) {
      if (candidate === except || candidate.exited) {
        continue;
      }
      throw new Error(
        `ipc executor has another live server (generation ${candidate.generation}, pid ${candidate.pid ?? 'unknown'}) while activating generation ${except?.generation ?? 'unknown'}`,
      );
    }
  }
}
