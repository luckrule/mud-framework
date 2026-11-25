import type { ResultMessage } from '../../ipc/protocol.ts';
import type { SpawnedServer } from '../../ipc/spawn-port.ts';
import type { Logger } from '../../logging/types.ts';
import { messageOf } from '../../shared/guards.ts';
import { effectiveTimeoutMs } from '../../shared/timeout.ts';
import { FRAMEWORK_SHUTDOWN, INTERNAL_ERROR } from '../../shared/wire-codes.ts';
import type { DeliveryLedger } from './delivery-ledger.ts';
import { onServerMessage } from './ipc-inbound.ts';
import type { RestartPolicy } from './restart-policy.ts';
import type { ServerHandlers } from './server-launcher.ts';
import {
  ClosedWaiter,
  attachServer,
  buildServer,
  closeChannelQuietly,
  killQuietly,
  waitForExit,
} from './server-launcher.ts';
import type { ServerSlot, ServerState } from './server-slot.ts';
import type { SwitchOperation, VersionSwitch } from './version-switch.ts';

export interface ServerSupervisorOptions {
  readonly logger: Logger;
  readonly slot: ServerSlot<SwitchOperation>;
  readonly ledger: DeliveryLedger;
  readonly versionSwitch: VersionSwitch;
  readonly restart: RestartPolicy;
  readonly startTimeoutMs: number;
  readonly portalTimeoutMs: number;
  readonly spawn: () => SpawnedServer;
}

/**
 * The executor's process state machine: launching a Server, the start and
 * generation watchdogs, crash recovery, restart backoff, and the dispatch of
 * Server→Portal messages.
 *
 * Split from `IpcExecutor` so the request-facing API and the process lifecycle
 * change for different reasons. Everything shared with the facade (`slot`,
 * `ledger`, `versionSwitch`) arrives through the constructor; this class never
 * imports `IpcExecutor`.
 */
export class ServerSupervisor {
  private spawn: () => SpawnedServer;
  readonly logger: Logger;
  readonly slot: ServerSlot<SwitchOperation>;
  private readonly ledger: DeliveryLedger;
  private readonly versionSwitch: VersionSwitch;
  private readonly restart: RestartPolicy;
  private readonly startTimeoutMs: number;
  private readonly portalTimeoutMs: number;
  private readonly closedWaiter = new ClosedWaiter();
  /** The Portal-promised request ceiling; only ever tightened. */
  private ceilingMs: number;
  private startTimer: ReturnType<typeof setTimeout> | undefined;
  private generationTimer:
    | { server: ServerState; timer: ReturnType<typeof setTimeout> }
    | undefined;
  private startWaiter:
    | { resolve: () => void; reject: (error: Error) => void }
    | undefined;
  private pendingRecovery: { backoffMs: number; blocked: boolean } | undefined;
  /**
   * Set when a close could not confirm every exit. Unlike `pendingRecovery` it
   * is never lifted by a late exit: the close is already terminal, and the
   * unconfirmed writer is exactly what health must keep reporting (INV-002).
   */
  private closeBlocked = false;
  /** Separates the first start from a restart when deriving health. */
  private handshakeDone = false;

  constructor(options: ServerSupervisorOptions) {
    this.spawn = options.spawn;
    this.logger = options.logger;
    this.slot = options.slot;
    this.ledger = options.ledger;
    this.versionSwitch = options.versionSwitch;
    this.restart = options.restart;
    this.startTimeoutMs = options.startTimeoutMs;
    this.portalTimeoutMs = options.portalTimeoutMs;
    // Until `ready` declares the backend ceiling, the Portal ceiling applies.
    this.ceilingMs = options.portalTimeoutMs;
  }

  get effectiveTimeoutMs(): number {
    return this.ceilingMs;
  }

  get handshakeCompleted(): boolean {
    return this.handshakeDone;
  }

  /** Recovery is stopped by an unconfirmed writer exit (INV-002). */
  get recoveryBlocked(): boolean {
    return this.pendingRecovery?.blocked === true || this.closeBlocked;
  }

  /** Spawns the Server, sends `hello` and resolves once the Server is `ready`. */
  async start(): Promise<void> {
    if (this.slot.status !== 'idle') {
      throw new Error(`ipc executor cannot start while ${this.slot.status}`);
    }
    this.slot.beginStart();
    const ready = new Promise<void>((resolve, reject) => {
      this.startWaiter = { resolve, reject };
    });
    // Bounded from the first launch: without this a Server that never reaches
    // the handshake leaves `start` pending forever while the restart loop keeps
    // spawning processes.
    this.startTimer = setTimeout(() => {
      this.onStartTimeout();
    }, this.startTimeoutMs);
    this.launch();
    await ready;
  }

  /**
   * Gives up on a Server that never became ready. The restart loop is stopped
   * rather than left running: continuing to spawn would keep burning CPU and
   * ports on a configuration error, and the caller already has the failure.
   */
  private onStartTimeout(generation?: ServerState): void {
    if (
      this.slot.isTerminal ||
      (generation === undefined
        ? this.startWaiter === undefined
        : this.slot.server !== generation || generation.ready)
    ) {
      return;
    }
    this.clearStartTimer();
    this.clearGenerationTimer(generation);
    const attempts = this.restart.attempts;
    this.restart.clearRestartTimer();
    this.restart.clearStableClock();
    this.slot.markFailed();
    this.logger.error(
      { timeoutMs: this.startTimeoutMs, attempts },
      'server did not become ready before the start timeout',
    );
    // A Server that is up but never handshook has no owner any more; leaving it
    // running would be an orphan writer (INV-002).
    const server = this.slot.server;
    if (server !== undefined && this.slot.clearIfCurrent(server)) {
      this.abandonServer(server);
    }
    this.ledger.failAllPending(
      INTERNAL_ERROR,
      `ipc executor failed to start: server did not become ready within ${this.startTimeoutMs}ms`,
      'executor failed to start',
    );
    this.rejectStart(
      new Error(
        `ipc executor failed to start: server did not become ready within ${this.startTimeoutMs}ms`,
      ),
    );
  }

  /** Closes the link and kills the process, without the graceful handshake. */
  private abandonServer(server: ServerState): void {
    // Marked closed first so the exit that follows does not run `onServerLost`
    // again: it would log a second "server process lost" error for a crash this
    // timeout already reported.
    server.closed = true;
    if (!server.exited) {
      try {
        server.kill();
      } catch (error) {
        this.logger.warn(
          { pid: server.pid, reason: messageOf(error) },
          'server could not be killed after the start timeout',
        );
      }
    }
    closeChannelQuietly(server, 'start timeout', this.logger);
  }

  onServerLost(
    server: ServerState,
    code: number | null,
    signal: string | null,
    reason: string,
  ): void {
    if (server.closed) {
      return;
    }
    server.closed = true;
    server.closedSignal.resolve();
    this.clearGenerationTimer(server);
    // The switch owns the loss of a process it staged or is retiring; the
    // branches and their order live there. A promoted process that dies is an
    // ordinary crash, so this returns false and the crash path below runs.
    if (this.versionSwitch.onServerLost(server, reason)) {
      return;
    }
    // A stable clock from the dead generation must not end the crash streak.
    this.restart.clearStableClock();
    if (server.retiring) {
      // A switch retired this process but had already settled (promoted or
      // rolled back) before its exit landed. Its exit is expected: no crash
      // line and no restart beside the current Server.
      this.resolveClosed(server);
      return;
    }
    this.slot.clearIfCurrent(server);
    // A close that is waiting for `closed` will never get it now.
    this.resolveClosed(server);
    if (!server.exited) {
      // The link died but the process may still be running: it can no longer
      // be reached or asked to stop, and restarting beside it would give the
      // storage two writers (INV-002). Kill before spawning a replacement, and
      // wait for it to actually go: SIGKILL returning is not the process
      // releasing its storage.
      this.logger.warn(
        { pid: server.pid, reason },
        'server channel lost while the process was alive; killing it',
      );
      killQuietly(server, this.logger);
    }
    if (this.slot.isTerminal) {
      // SPEC 6.3: no restart while shutting down; nothing can complete these.
      this.ledger.failAllPending(
        FRAMEWORK_SHUTDOWN,
        'server process exited during shutdown',
        'server lost during shutdown',
      );
      return;
    }
    const backoffMs = this.restart.nextBackoffMs();
    this.logger.error(
      { pid: server.pid, exitCode: code, signal, backoffMs, reason },
      'server process lost',
    );
    this.recoverAfterExits(backoffMs);
  }

  /**
   * Starts recovery and defers the restart until every process whose exit was
   * not observed is confirmed gone. A rollback passes no backoff and takes the
   * next one from the crash policy, exactly like the crash path.
   */
  recoverAfterExits(backoffMs = this.restart.nextBackoffMs()): void {
    if (this.slot.isTerminal) {
      return;
    }
    this.slot.beginStart();
    const recovery = { backoffMs, blocked: false };
    this.pendingRecovery = recovery;
    const pending = this.slot.pendingExits;
    if (pending.length === 0) {
      this.resumeRecovery();
      return;
    }
    void Promise.all(
      pending.map(async (server) => {
        const exited = await waitForExit(server, this.logger);
        this.slot.confirmExit(server);
        return exited;
      }),
    ).then((exited) => {
      if (this.pendingRecovery !== recovery || this.slot.isTerminal) {
        return undefined;
      }
      if (exited.every(Boolean)) {
        this.resumeRecovery();
      } else {
        recovery.blocked = true;
        this.logger.error(
          { pendingExits: this.slot.pendingExits.length },
          'server recovery blocked: process exit is unconfirmed',
        );
        this.ledger.failAllPending(
          INTERNAL_ERROR,
          'server recovery blocked: process exit is unconfirmed',
          'server exit is unconfirmed',
        );
      }
      return undefined;
    });
  }

  private resumeRecovery(): void {
    if (
      this.slot.isTerminal ||
      this.slot.pendingExits.length > 0 ||
      this.pendingRecovery === undefined
    ) {
      return;
    }
    const { backoffMs } = this.pendingRecovery;
    this.pendingRecovery = undefined;
    this.scheduleRestart(backoffMs);
  }

  /**
   * Arms the restart unless the executor is already shutting down. The terminal
   * check stays here because it is the process state machine's decision, not the
   * policy's: a `RestartPolicy` never reads the slot.
   */
  private scheduleRestart(backoffMs: number): void {
    if (this.slot.isTerminal) {
      return;
    }
    this.restart.scheduleRestart(backoffMs, () => {
      this.launch();
    });
  }

  private launch(): void {
    if (this.slot.isTerminal || this.slot.pendingExits.length > 0) {
      return;
    }
    let spawned: SpawnedServer;
    try {
      spawned = this.spawn();
    } catch (error) {
      // A failed spawn is another crash: keep buffering and back off.
      const backoffMs = this.restart.nextBackoffMs();
      this.logger.error(
        { reason: messageOf(error), backoffMs },
        'server spawn failed',
      );
      this.slot.beginStart();
      this.scheduleRestart(backoffMs);
      return;
    }
    const server = buildServer(spawned, this.slot.nextGeneration());
    this.slot.trackSpawning(server);
    this.clearGenerationTimer();
    this.generationTimer = {
      server,
      timer: setTimeout(() => {
        if (this.generationTimer?.server === server) {
          this.onStartTimeout(server);
        }
      }, this.startTimeoutMs),
    };
    attachServer(server, this.serverHandlers(server));
    try {
      server.channel.send({ type: 'hello' });
    } catch (error) {
      this.onServerLost(
        server,
        null,
        null,
        `hello failed: ${messageOf(error)}`,
      );
    }
  }

  /**
   * Binds the handlers to one process, so a staged process is observed exactly
   * like the current one.
   */
  private serverHandlers(server: ServerState): ServerHandlers {
    return {
      onMessage: (raw) => {
        onServerMessage(server, raw, this);
      },
      onLost: (code, signal, reason) => {
        this.onServerLost(server, code, signal, reason);
      },
      onExit: () => {
        this.slot.confirmExit(server);
        this.resumeRecovery();
      },
    };
  }

  /** Installs the message and loss handlers on a freshly built process. */
  attach(server: ServerState): void {
    this.slot.track(server);
    attachServer(server, this.serverHandlers(server));
  }

  /** A successful switch changes what a crash restarts. */
  adoptSpawn(spawn: () => SpawnedServer): void {
    this.spawn = spawn;
  }

  /**
   * Applies the backend's declared request ceiling to the buffered requests.
   * Only ever tightens: a later Server declaring a longer ceiling must not
   * extend a deadline the Portal already promised, or a crash loop could keep a
   * request alive past its original ceiling (SPEC 3.4).
   */
  adoptCeiling(declaredMaxDurationMs: number): void {
    this.ceilingMs = effectiveTimeoutMs(
      this.portalTimeoutMs,
      declaredMaxDurationMs,
    );
    this.ledger.tightenDeadlines(this.ceilingMs);
  }

  /**
   * Re-delivers the buffered requests when the Server is not ready or a switch
   * is in progress. The hot path sends a single request directly through the
   * ledger; this is the catch-up path after a barrier or a failed switch.
   *
   * The switching gate lives here because it is executor state: while a
   * replacement is being staged or the previous process is being retired,
   * delivering now would send a request to a process the switch is draining.
   */
  pump(): void {
    if (this.slot.status === 'switching') {
      return;
    }
    this.ledger.flush(this.slot.server);
  }

  clearRestartTimer(): void {
    this.restart.clearRestartTimer();
  }

  startStableClock(): void {
    this.restart.startStableClock();
  }

  clearStableClock(): void {
    this.restart.clearStableClock();
  }

  /** Stops both watchdog timers so neither can fire mid-close. */
  clearStartTimers(): void {
    this.clearStartTimer();
    this.clearGenerationTimer();
  }

  rejectStart(error: Error): void {
    const waiter = this.startWaiter;
    this.startWaiter = undefined;
    this.clearStartTimer();
    waiter?.reject(error);
  }

  /** Wakes the `closed` wait a `close()` may be parked on, for that Server only. */
  resolveClosed(server: ServerState): void {
    this.closedWaiter.resolve(server);
  }

  waitForClosed(server: ServerState, waitMs: number): Promise<void> {
    return this.closedWaiter.waitForClosed(server, waitMs);
  }

  /** A confirmed close releases the recovery barrier. */
  clearRecoveryBarrier(): void {
    this.pendingRecovery = undefined;
    this.closeBlocked = false;
  }

  /** An unconfirmed exit must keep health reporting `blocked` (INV-002). */
  blockRecoveryBarrier(): void {
    this.closeBlocked = true;
  }

  adoptReady(server: ServerState, declaredMaxDurationMs: number): number {
    this.clearGenerationTimer(server);
    this.slot.markReady(server);
    // The crash streak ends only after a stable run, not at the handshake: a
    // Server that reaches `ready` and then dies on its first request would
    // otherwise reset the backoff and restart immediately, forever.
    this.restart.startStableClock();
    this.adoptCeiling(declaredMaxDurationMs);
    return this.ceilingMs;
  }

  resolveStart(): void {
    this.handshakeDone = true;
    const waiter = this.startWaiter;
    this.startWaiter = undefined;
    this.clearStartTimer();
    waiter?.resolve();
  }

  private clearStartTimer(): void {
    if (this.startTimer === undefined) {
      return;
    }
    clearTimeout(this.startTimer);
    this.startTimer = undefined;
  }

  private clearGenerationTimer(server?: ServerState): void {
    if (
      this.generationTimer === undefined ||
      (server !== undefined && this.generationTimer.server !== server)
    ) {
      return;
    }
    clearTimeout(this.generationTimer.timer);
    this.generationTimer = undefined;
  }

  onResult(message: ResultMessage): void {
    this.ledger.acceptResult(message);
  }
}
