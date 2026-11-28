import type { SpawnedServer } from '../../ipc/spawn-port.ts';
import type { Logger } from '../../logging/types.ts';
import { SHUTDOWN_ACK_SLACK_MS } from '../../shared/budget.ts';
import { messageOf, toError } from '../../shared/guards.ts';
import { settleWithin } from '../../shared/timeout.ts';
import {
  buildServer,
  closeChannelQuietly,
  ensureExited,
  killQuietly,
} from './server-launcher.ts';
import type { ServerSlot, ServerState } from './server-slot.ts';
import { ServerSwitchError, withStderrTail } from './switch-failure.ts';
import type { SwitchFailureStage } from './switch-failure.ts';

/** A bounded wait for `prepared` or `ready` from the staged process. */
export interface SwitchBarrier {
  readonly promise: Promise<void>;
  resolve: () => void;
  seen: boolean;
}

/**
 * How long a failed switch waits for a replacement that already died to report
 * its exit. Bounded because the exit is evidence for INV-002 but the stderr tail
 * is only a diagnostic: an unobservable exit must not delay the rejection.
 */
const SWITCH_STDERR_GRACE_MS = 250;

/**
 * One `switchTo` in flight. It owns the staged process and the previous one,
 * the barriers it waits for, and the abort signal `close()` uses to interrupt
 * the sequence at any await.
 */
export interface SwitchOperation {
  readonly spawn: () => SpawnedServer;
  resolve: () => void;
  reject: (error: Error) => void;
  /** Resolves once the switch and its cleanup are done, whatever the outcome. */
  readonly completion: Promise<void>;
  readonly markComplete: () => void;
  /** Resolves (never rejects) when the switch is interrupted, so a race cannot leak. */
  readonly abortSignal: Promise<void>;
  /**
   * Interrupts an in-flight switch. `byClose` marks the case where `close()`
   * owns the cleanup: the switch then only abandons what it staged.
   */
  abort: (error: Error, byClose: boolean) => void;
  abortError: Error | undefined;
  /** True when `close()` interrupted the switch and owns the cleanup. */
  closeAborted: boolean;
  phase: SwitchFailureStage;
  /** The process being promoted; not the current Server until `ready`. */
  staged: ServerState | undefined;
  /** The Server being replaced; only retired after its exit is confirmed. */
  previous: ServerState | undefined;
  /** True once the previous process can no longer serve. */
  previousGone: boolean;
  /** True when the previous process died on its own before it was retired. */
  previousLost: boolean;
  /**
   * Processes this switch killed or abandoned without observing their exit.
   * They may still hold storage, so no restart may begin until each is
   * confirmed gone — the same gate the crash path applies (INV-002).
   */
  exitsPending: ServerState[];
  /** True once the replacement was promoted to the current Server. */
  promoted: boolean;
  /** True when the promoted replacement was lost before the switch settled. */
  promotedLost: boolean;
  /** True once `activate` was sent, so the staged process may hold storage. */
  activated: boolean;
  stagedLost: Error | undefined;
  readonly prepared: SwitchBarrier;
  readonly ready: SwitchBarrier;
  /** Why the switch is failing; read by `runSwitch` to pick the failure path. */
  failure: Error | undefined;
}

/**
 * The switch's dependencies: the operations it actually calls. `IpcExecutor`
 * supplies a private adapter over its supervisor, so the switch never holds the
 * executor and none of these operations are part of its public surface.
 *
 * The switch never writes `status` or `server` directly; it only asks the slot
 * for a semantic transition, so a state write cannot skip the meaning attached
 * to it.
 */
export interface SwitchDeps {
  readonly logger: Logger;
  readonly slot: ServerSlot<SwitchOperation>;
  readonly startTimeoutMs: number;
  readonly shutdownTimeoutMs: number;
  /**
   * A successful switch changes what a crash restarts; a rollback never calls
   * this.
   */
  adoptSpawn(spawn: () => SpawnedServer): void;
  /** Installs the owner's message and loss handlers on a freshly built process. */
  attach(server: ServerState): void;
  /** Delivers the buffered requests to the current generation. */
  pump(): void;
  /** Wakes the `closed` wait a `close()` may be parked on, for that Server only. */
  resolveClosed(server: ServerState): void;
  clearRestartTimer(): void;
  startStableTimer(): void;
  adoptCeiling(declaredMaxDurationMs: number): void;
  /** Shares the crash path's bounded exit wait and late-exit recovery gate. */
  recoverAfterExits(): void;
}

/**
 * The `switchTo` sequence (ADR-0007): stage a replacement, retire the previous
 * process, activate the replacement, promote it — in that order, because the
 * order is what keeps two processes from opening the same storage (INV-002).
 *
 * Split from the executor so the four steps and their two failure boundaries
 * live in one place: everything executor-specific (the process factory, the
 * generation counter, the timers, delivery) arrives through `SwitchDeps`. The
 * executor keeps the ordinary start/restart/close paths and the handshake
 * branches that do not belong to a switch.
 */
export class VersionSwitch {
  private readonly deps: SwitchDeps;

  constructor(deps: SwitchDeps) {
    this.deps = deps;
  }

  /**
   * Replaces the Server process with one built from `spawn` without dropping
   * the recovery barrier (ADR-0007, INV-002).
   *
   * The order is the whole point and cannot be rearranged: spawn and wait for
   * `prepared` (no storage is opened yet), retire the current process and wait
   * for it to exit, and only then send `activate` and wait for `ready`. A
   * prepared Server is not a writer, so it may exist beside the old one; an
   * activated one is, so `activate` must never be sent while the old process
   * could still hold storage. Requests keep arriving during the switch and stay
   * in the unacknowledged buffer (SPEC 6.5) — they are delivered to the new
   * generation once it is ready, not failed.
   *
   * Failure before the old process is retired leaves it serving and rejects the
   * switch. Failure after that point has no live Server left, so it falls back
   * to the crash-restart path with the previous spawn and rejects as well.
   */
  switchTo(options: { spawn: () => SpawnedServer }): Promise<void> {
    const slot = this.deps.slot;
    if (slot.switching !== undefined) {
      return Promise.reject(
        new Error('an ipc executor switch is already in progress'),
      );
    }
    if (slot.status !== 'ready') {
      return Promise.reject(
        new Error(`ipc executor cannot switch while ${slot.status}`),
      );
    }
    try {
      slot.assertNoOtherLiveServer(slot.server);
    } catch (error) {
      return Promise.reject(toError(error));
    }
    const deferred = Promise.withResolvers<void>();
    const completion = Promise.withResolvers<void>();
    const abortSignal = Promise.withResolvers<void>();
    const op: SwitchOperation = {
      spawn: options.spawn,
      resolve: deferred.resolve,
      reject: deferred.reject,
      completion: completion.promise,
      markComplete: completion.resolve,
      abortSignal: abortSignal.promise,
      abort: (error, byClose) => {
        if (op.abortError !== undefined) {
          return;
        }
        op.abortError = error;
        op.closeAborted = byClose;
        abortSignal.resolve();
      },
      abortError: undefined,
      closeAborted: false,
      phase: 'preparing',
      staged: undefined,
      previous: slot.server,
      previousGone: false,
      previousLost: false,
      exitsPending: [],
      promoted: false,
      promotedLost: false,
      activated: false,
      stagedLost: undefined,
      prepared: newBarrier(),
      ready: newBarrier(),
      failure: undefined,
    };
    slot.beginSwitch(op);
    this.deps.logger.info({}, 'server switch started');
    void this.runSwitch(op);
    return deferred.promise;
  }

  /**
   * Reports a process loss to the switch. Returns true when the switch owns the
   * loss and the caller must stop; false when the loss is an ordinary crash of
   * the current Server, which the crash path handles.
   *
   * The order of the branches is load-bearing: a staged process that was never
   * promoted is not a crash, a promoted one is, and the previous process's
   * death is expected during its own retirement.
   */
  onServerLost(server: ServerState, reason: string): boolean {
    const op = this.deps.slot.switching;
    if (op?.staged === server && !op.promoted) {
      // The replacement died while the switch was waiting on it. Record the
      // loss; the switch's own await reacts to `closedSignal` and reports it.
      op.stagedLost ??= new Error(`replacement server lost: ${reason}`);
      if (!server.exited) {
        killQuietly(server, this.deps.logger);
      }
      return true;
    }
    if (op?.staged === server) {
      // Promoted, but the switch has not settled yet: this is an ordinary crash
      // of the current Server, handled by the crash path below (clear the
      // current Server, wait for the exit, schedule the restart). The switch
      // must not report success for a process that is already gone.
      op.promotedLost = true;
      op.stagedLost ??= new Error(
        `replacement server lost after promotion: ${reason}`,
      );
      op.failure ??= op.stagedLost;
      return false;
    }
    if (op?.previous === server) {
      // The old process died during its own retirement. That is the expected
      // exit, not a crash: do not restart beside it or log a crash. It is not
      // "lost" either — the switch asked it to stop.
      if (server.retiring || op.previousGone) {
        op.previousGone = true;
        if (!server.exited) {
          killQuietly(server, this.deps.logger);
          this.recordPendingExit(op, server);
        }
        return true;
      }
      // It crashed before the switch retired it. The switch has no previous
      // Server to fall back to, so it fails and the crash path restarts.
      op.previousLost = true;
      op.previousGone = true;
      this.deps.slot.clearIfCurrent(server);
      this.deps.resolveClosed(server);
      if (!server.exited) {
        killQuietly(server, this.deps.logger);
        this.recordPendingExit(op, server);
      }
      op.abort(
        new Error(`previous server lost during the switch: ${reason}`),
        false,
      );
      return true;
    }
    return false;
  }

  /**
   * Interrupts a `switchTo` in flight and waits (bounded) for it to release its
   * staged process. `close()` owns the old Server from here, so the switch only
   * abandons what it spawned and never restarts or promotes anything.
   */
  async abortSwitchForClose(): Promise<void> {
    const op = this.deps.slot.switching;
    if (op === undefined) {
      return;
    }
    op.abort(new Error('ipc executor is closing: server switch aborted'), true);
    const done = await settleWithin(op.completion, this.deps.shutdownTimeoutMs);
    if (done.status === 'timeout') {
      // The switch's own awaits are bounded by `startTimeoutMs`; a completion
      // that outlives the shutdown budget means a process it staged is still
      // around. Kill it directly so `close()` does not leave an orphan, and
      // hand it to `doClose` when it may hold storage.
      const staged = op.staged;
      if (staged !== undefined && !staged.closed) {
        staged.closed = true;
        staged.closedSignal.resolve();
        if (!staged.exited) {
          killQuietly(staged, this.deps.logger);
        }
        closeChannelQuietly(
          staged,
          'switch aborted by close',
          this.deps.logger,
        );
        if (op.activated) {
          // The switch's abort path only kills; `close()` must confirm this
          // exit itself before it returns (INV-002).
          this.recordAbandoned(staged);
        }
      }
      this.deps.logger.warn(
        { timeoutMs: this.deps.shutdownTimeoutMs },
        'server switch did not finish before the shutdown timeout',
      );
    }
  }

  /** The switch sequence; always settles `op` and releases the slot's switch. */
  private async runSwitch(op: SwitchOperation): Promise<void> {
    try {
      if (!(await this.stageReplacement(op))) {
        return;
      }
      if (!(await this.retirePrevious(op))) {
        return;
      }
      await this.activateReplacement(op);
    } catch (error) {
      op.failure ??= toError(error);
    } finally {
      await this.awaitLostExit(op);
      this.finishSwitch(op);
    }
  }

  /**
   * Gives a replacement that died on its own a bounded moment to report its
   * exit, so the rejection can carry its stderr tail.
   *
   * The channel can close before the exit is recorded, and the loss is what
   * wakes the switch: without this the tail is read one turn too early and is
   * empty exactly when the diagnostic matters. Only the self-death case waits —
   * a timed-out barrier leaves a process that is still alive, and a switch must
   * not sit on its rejection for that.
   */
  private async awaitLostExit(op: SwitchOperation): Promise<void> {
    const staged = op.staged;
    if (staged === undefined || op.stagedLost === undefined || staged.exited) {
      return;
    }
    await settleWithin(staged.exitPromise, SWITCH_STDERR_GRACE_MS);
  }

  /**
   * Spawns the replacement and waits for `prepared`, bounded by
   * `startTimeoutMs`. A process that never prepares is abandoned (killed) and
   * the previous Server is untouched, so the switch can fail without an outage.
   */
  private async stageReplacement(op: SwitchOperation): Promise<boolean> {
    let spawned: SpawnedServer;
    try {
      spawned = op.spawn();
    } catch (error) {
      op.failure = new Error(`spawn failed: ${messageOf(error)}`);
      return false;
    }
    const server = buildServer(spawned, this.deps.slot.nextGeneration());
    op.staged = server;
    this.deps.attach(server);
    try {
      server.channel.send({ type: 'hello' });
    } catch (error) {
      op.stagedLost = new Error(`hello failed: ${messageOf(error)}`);
      op.failure = op.stagedLost;
      return false;
    }
    return this.awaitBarrier(
      op,
      op.prepared,
      'prepared',
      this.deps.startTimeoutMs,
    );
  }

  /**
   * Stops the current Server and waits until it is known to be gone. Nothing is
   * activated until this resolves true: `closed` says the Server finished
   * writing, and the exit wait is what says the storage is released (INV-002).
   */
  private async retirePrevious(op: SwitchOperation): Promise<boolean> {
    const previous = op.previous;
    const logger = this.deps.logger;
    op.phase = 'retiring';
    if (previous === undefined) {
      op.previousGone = true;
      return true;
    }
    previous.retiring = true;
    if (!previous.closed && previous.ready) {
      try {
        previous.shutdownSent = true;
        previous.channel.send({
          type: 'shutdown',
          timeoutMs: this.deps.shutdownTimeoutMs,
        });
      } catch (error) {
        // The process died between the switch and this send; the channel close
        // handler reports the same thing.
        logger.warn(
          { pid: previous.pid, reason: messageOf(error) },
          'shutdown message could not be sent',
        );
      }
      const acknowledged = await this.awaitPreviousClosed(op, previous);
      if (op.abortError !== undefined) {
        op.failure = op.abortError;
        return false;
      }
      if (!acknowledged && !previous.closed) {
        logger.warn(
          {
            pid: previous.pid,
            timeoutMs: this.deps.shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS,
            ready: previous.ready,
          },
          'server did not finish shutting down; killing it',
        );
        killQuietly(previous, logger);
      }
    }
    const exited = await ensureExited(previous, logger, {
      kill: 'server process did not exit; killing it before switching',
    });
    closeChannelQuietly(previous, 'portal is switching servers', logger);
    if (op.abortError !== undefined) {
      // `close()` took over while the old process was exiting. Its own path
      // finishes the shutdown, including the exit wait, so nothing is recorded
      // here and nothing may be activated now. Whether the exit was observed is
      // `exitsPending`'s business, not this flag's.
      op.failure = op.abortError;
      op.previousGone = true;
      return false;
    }
    if (!exited) {
      // Killed but its exit could not be observed. It cannot be treated as
      // serving any more, so the switch fails and rolls back — but the restart
      // waits for this exit like the crash path does, because the process may
      // still hold storage (INV-002).
      op.failure = new Error(
        'previous server process did not exit before the switch',
      );
      op.previousGone = true;
      op.exitsPending.push(previous);
      return false;
    }
    op.previousGone = true;
    logger.info({ pid: previous.pid }, 'previous server retired');
    return true;
  }

  /**
   * Opens the replacement's storage and waits for `ready`, bounded by
   * `startTimeoutMs`. This is the first moment the new process can write, which
   * is why it comes last.
   */
  private async activateReplacement(op: SwitchOperation): Promise<void> {
    const staged = op.staged;
    op.phase = 'activating';
    if (staged === undefined) {
      op.failure = new Error('the replacement was never spawned');
      return;
    }
    // INV-002: retained processes from earlier failures also block activation,
    // even when this switch confirmed its own previous process's exit.
    try {
      this.deps.slot.assertNoOtherLiveServer(staged);
    } catch (error) {
      op.failure = toError(error);
      return;
    }
    try {
      // Delivery may take effect before send throws or invokes a sync handler.
      op.activated = true;
      staged.channel.send({ type: 'activate' });
    } catch (error) {
      op.stagedLost = new Error(`activate failed: ${messageOf(error)}`);
      op.failure = op.stagedLost;
      return;
    }
    const ready = await this.awaitBarrier(
      op,
      op.ready,
      'become ready',
      this.deps.startTimeoutMs,
    );
    if (!ready || op.abortError !== undefined) {
      return;
    }
    if (staged.closed || staged.exited) {
      // The barrier resolved, but the process died before this continuation
      // ran. Promoting a dead generation would leave `status = 'ready'` with no
      // Server behind it — a state only a crash could leave, and only after the
      // switch had already reported success. Fail the switch instead, so the
      // ordinary rollback restarts.
      op.stagedLost ??= new Error(
        'replacement server exited before it could be promoted',
      );
      op.failure ??= op.stagedLost;
      return;
    }
    // Promote: the replacement becomes the current Server and the barrier is
    // the only thing that releases the buffered requests to it.
    op.promoted = true;
    this.deps.slot.markReady(staged);
    // A crash after this point restarts the process factory that is now
    // serving. A rollback keeps the old one because this only runs after the
    // promotion succeeded.
    this.deps.adoptSpawn(op.spawn);
    this.deps.clearRestartTimer();
    this.deps.startStableTimer();
    if (staged.declaredMaxDurationMs !== undefined) {
      this.deps.adoptCeiling(staged.declaredMaxDurationMs);
    }
    this.deps.logger.info({ pid: staged.pid }, 'replacement server ready');
    this.deps.pump();
    if (op.promotedLost) {
      // The first flush killed the promoted process; `onServerLost` already
      // took the crash path (restart scheduled), so this is not a completed
      // switch and `finishSwitch` rejects it.
      return;
    }
    this.deps.logger.info({ pid: staged.pid }, 'server switch completed');
  }

  /**
   * Waits for one handshake barrier, racing the process's exit, an abort from
   * `close()` and the timeout. On any non-barrier outcome it records the reason
   * in `op.failure`, which `finishSwitch` turns into the rejection.
   */
  private async awaitBarrier(
    op: SwitchOperation,
    barrier: SwitchBarrier,
    what: string,
    timeoutMs: number,
  ): Promise<boolean> {
    if (barrier.seen) {
      return true;
    }
    const staged = op.staged;
    if (staged === undefined) {
      op.failure ??= new Error('the replacement was never spawned');
      return false;
    }
    const outcome = await settleWithin(
      Promise.race([
        barrier.promise.then(() => 'barrier' as const),
        op.abortSignal.then(() => 'aborted' as const),
        staged.closedSignal.promise.then(() => 'lost' as const),
      ]),
      timeoutMs,
    );
    if (outcome.status === 'timeout') {
      this.deps.logger.warn(
        { pid: staged.pid, timeoutMs, expected: what },
        'replacement server did not reach its barrier before the switch timeout',
      );
      op.failure ??= new Error(
        `replacement did not ${what} within ${timeoutMs}ms`,
      );
      return false;
    }
    if (outcome.status === 'rejected') {
      op.failure ??= toError(outcome.error);
      return false;
    }
    const result = outcome.value;
    if (result === 'barrier') {
      return true;
    }
    if (result === 'aborted') {
      op.failure ??= op.abortError;
      return false;
    }
    op.stagedLost ??= new Error(`replacement server exited before ${what}`);
    op.failure ??= op.stagedLost;
    return false;
  }

  /** Waits for `closed` from the retiring process, bounded and abortable. */
  private async awaitPreviousClosed(
    op: SwitchOperation,
    previous: ServerState,
  ): Promise<boolean> {
    if (previous.closed || previous.shutdownAcknowledged) {
      return true;
    }
    const waitMs = this.deps.shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS;
    const outcome = await settleWithin(
      Promise.race([
        previous.closedSignal.promise.then(() => 'closed' as const),
        op.abortSignal.then(() => 'aborted' as const),
      ]),
      waitMs,
    );
    return outcome.status === 'settled' && outcome.value === 'closed';
  }

  /**
   * Ends a switch: abandons anything staged, settles the caller, then picks the
   * recovery path. Failure before the old process stopped serving leaves it
   * serving; failure after that (or after it was lost) has no live Server, so it
   * restarts the previous version through the crash path. An abort from
   * `close()` does neither: `doClose` owns the cleanup from there.
   */
  private finishSwitch(op: SwitchOperation): void {
    const staged = op.staged;
    let abandoned: ServerState | undefined;
    if (staged !== undefined && !op.promoted) {
      // Never promoted: no storage the Portal owns, so abandoning it cannot
      // leave a writer behind — unless `activate` already went out, in which
      // case the process may have opened storage and its exit must be awaited
      // before any restart (INV-002).
      abandoned = staged;
      this.abandonStaged(staged);
      op.staged = undefined;
      if (op.activated) {
        this.recordPendingExit(op, staged);
      }
    }
    this.deps.slot.releaseSwitch(op);
    if (op.promoted) {
      if (op.promotedLost) {
        // Promotion succeeded, but the process died before the switch could
        // report it. `onServerLost` already owns the recovery (crash path:
        // restart with the promoted spawn), so no rollback runs here — the
        // switch only refuses to call this a success.
        op.reject(
          this.switchFailure(
            op,
            op.abortError ??
              op.stagedLost ??
              op.failure ??
              new Error('the promoted replacement server was lost'),
            abandoned,
          ),
        );
      } else {
        op.resolve();
      }
      op.markComplete();
      return;
    }
    op.reject(
      this.switchFailure(
        op,
        op.abortError ??
          op.failure ??
          new Error('ipc executor switch ended unexpectedly'),
        abandoned,
      ),
    );
    if (op.previous !== undefined && !op.previousGone) {
      // The previous Server can still serve; it is still the current one, so it
      // must be restartable again if it later crashes.
      op.previous.retiring = false;
    }
    if (op.closeAborted) {
      // `doClose` owns the rest: it shuts down whatever Server is left and
      // confirms the exit of an abandoned replacement that received `activate`
      // before returning. The previous Server is `shutdownServer`'s job, so it
      // is not listed here even when its exit was not observed.
      if (abandoned !== undefined && op.activated) {
        this.recordAbandoned(abandoned);
      }
      op.markComplete();
      return;
    }
    const reason = op.failure === undefined ? 'unknown' : messageOf(op.failure);
    if (op.previousGone || op.previousLost) {
      this.deps.logger.warn(
        {
          reason,
          phase: op.phase,
          pendingExits: op.exitsPending.length,
        },
        op.previousLost
          ? 'server switch failed because the previous server was lost; restarting the previous version'
          : 'server switch failed after the previous server retired; restarting the previous version',
      );
      this.rollbackToPrevious(op.exitsPending);
    } else {
      this.deps.logger.warn(
        {
          reason,
          phase: op.phase,
          pid: this.deps.slot.server?.pid,
        },
        'server switch failed; the previous server keeps serving',
      );
      if (!this.deps.slot.isTerminal) {
        this.deps.slot.resumeServing();
        this.deps.pump();
      }
    }
    op.markComplete();
  }

  /**
   * Wraps a switch failure with what it leaves behind, so a caller can tell a
   * safe retry from a rollback already in flight without reading the message.
   *
   * The facts are read from `op` at the moment the switch settles; the recovery
   * branch below acts on the same state, so the two cannot disagree about
   * whether the previous Server is still serving.
   */
  private switchFailure(
    op: SwitchOperation,
    cause: Error,
    abandoned: ServerState | undefined,
  ): ServerSwitchError {
    const previousServing = !(op.previousGone || op.previousLost);
    const failed = abandoned ?? op.staged;
    const tail = failed?.lastExit()?.stderrTail;
    return new ServerSwitchError(withStderrTail(cause.message, tail), {
      stage: op.phase,
      // A close-aborted switch leaves the previous Server as the current one,
      // but the executor is shutting down: reporting it as serving would tell a
      // caller to retry into a closed executor.
      previousServing: !op.closeAborted && previousServing,
      rolledBack: !op.closeAborted && !op.promoted && !previousServing,
      promoted: op.promoted,
      closeAborted: op.closeAborted,
      pendingExits: op.exitsPending.length,
      ...(tail === undefined ? {} : { stderrTail: tail }),
    });
  }

  /**
   * Hands a process to `close()`, which confirms its exit before returning. A
   * replacement that received `activate` may hold storage, and a process a
   * rollback waits on has the same problem: that wait is a floating promise
   * `close()` cannot join.
   */
  private recordAbandoned(server: ServerState): void {
    this.deps.slot.track(server);
  }

  /**
   * Remembers a process whose exit this switch has not observed. It may still
   * hold storage, so the rollback restart must wait for it, exactly like the
   * crash path waits for the process it just killed (INV-002).
   */
  private recordPendingExit(op: SwitchOperation, server: ServerState): void {
    if (!server.exited && !op.exitsPending.includes(server)) {
      op.exitsPending.push(server);
    }
  }

  /** Closes and kills a staged process the switch no longer needs. */
  private abandonStaged(server: ServerState): void {
    if (server.closed) {
      return;
    }
    server.closed = true;
    server.closedSignal.resolve();
    if (!server.exited) {
      killQuietly(server, this.deps.logger);
    }
    closeChannelQuietly(server, 'switch abandoned', this.deps.logger);
  }

  /**
   * Restarts the previous version after the old process was already retired or
   * lost. Reuses the crash path (`starting` → `launch`/`scheduleRestart`) so the
   * buffered requests are not left without a Server.
   *
   * `pendingExits` are processes this switch killed or abandoned without
   * observing their exit: the previous one when it vanished mid-switch, and the
   * replacement if it already received `activate`. The restart is deferred
   * until each is confirmed gone, mirroring the crash path, so no two processes
   * write to the same storage (INV-002).
   */
  private rollbackToPrevious(pendingExits: readonly ServerState[]): void {
    if (this.deps.slot.isTerminal) {
      return;
    }
    for (const server of pendingExits) {
      this.recordAbandoned(server);
    }
    this.deps.recoverAfterExits();
  }
}

/** A one-shot, never-rejecting barrier for one handshake step. */
function newBarrier(): SwitchBarrier {
  const deferred = Promise.withResolvers<void>();
  return {
    promise: deferred.promise,
    resolve: deferred.resolve,
    seen: false,
  };
}
