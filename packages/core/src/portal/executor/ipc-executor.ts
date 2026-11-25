import { failure } from '../../ipc/outbound.ts';
import type { EventMessage } from '../../ipc/protocol.ts';
import type { SpawnedServer } from '../../ipc/spawn-port.ts';
import { createJsonLogger } from '../../logging/json-logger.ts';
import { safeLogger } from '../../logging/safe-logger.ts';
import type { Logger } from '../../logging/types.ts';
import {
  DEFAULT_MAX_PENDING,
  DEFAULT_MAX_REPLAYS,
  DEFAULT_RESTART_BACKOFF_MS,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
  closeBudget,
} from '../../shared/budget.ts';
import type { ServerEvent } from '../../shared/events.ts';
import type {
  ExecutorHealth,
  ExecutorOutcome,
  ExecutorRequest,
  RequestExecutor,
} from '../../shared/executor.ts';
import { messageOf } from '../../shared/guards.ts';
import { requestLogFields } from '../../shared/identity.ts';
import { tightenTimeout } from '../../shared/timeout.ts';
import {
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_SHUTDOWN,
  INTERNAL_ERROR,
} from '../../shared/wire-codes.ts';
import { DeliveryLedger } from './delivery-ledger.ts';
import { ipcExecutorHealth } from './executor-health.ts';
import type { ShutdownHost } from './executor-shutdown.ts';
import { doClose } from './executor-shutdown.ts';
import type { IpcExecutorOptions } from './ipc-executor-options.ts';
import { PendingBuffer } from './ipc-pending.ts';
import { RestartPolicy } from './restart-policy.ts';
import type { ServerState } from './server-slot.ts';
import { ServerSlot } from './server-slot.ts';
import { ServerSupervisor } from './server-supervisor.ts';
import type { SwitchDeps, SwitchOperation } from './version-switch.ts';
import { VersionSwitch } from './version-switch.ts';

export type { IpcExecutorOptions };

/**
 * The Portal side of the process split (ADR-0002): ships Requests to a Server
 * process over IPC and keeps them buffered until the Server acknowledges them.
 *
 * The recovery barrier is the handshake: nothing is delivered before `ready`
 * (SPEC 6.2), so a Request that arrives while the Server is starting or being
 * restarted waits instead of failing. The handshake itself is two-phase
 * (ADR-0007): `hello` is answered with `prepared`, and `activate` is what opens
 * the Server's storage. This class activates immediately after `prepared`; the
 * ordering that retires a previous process first belongs to a switching caller.
 * A crash does not fail in-flight Requests either — they are replayed to the
 * next process, in the order they were received on their connection (SPEC 6.5),
 * until the replay budget runs out.
 *
 * `execute` never throws and always settles by the request deadline, because
 * the connection layer advances a connection's ordered chain on settlement and
 * a hanging promise would stall every later request on that connection.
 *
 * The process lifecycle lives in `ServerSupervisor`; this class owns the
 * admission and settlement surface those processes serve.
 */
export class IpcExecutor implements RequestExecutor, SwitchDeps {
  readonly shutdownTimeoutMs: number;
  readonly startTimeoutMs: number;
  readonly logger: Logger;
  private readonly buffer: PendingBuffer;
  /** Delivery, deadlines and settlement: everything that resolves a pending entry. */
  private readonly ledger: DeliveryLedger;
  /** Sole owner of `status`, `server`, the generation counter and `switching`. */
  readonly slot = new ServerSlot<SwitchOperation>();
  /** The `switchTo` sequence; it writes state only through `slot`. */
  private readonly versionSwitch: VersionSwitch;
  /** Launch, crash recovery, restart backoff and Server→Portal messages. */
  private readonly supervisor: ServerSupervisor;
  /** Event sinks the connection layer registered (ADR-0008). */
  private readonly eventListeners = new Set<(event: ServerEvent) => void>();
  /** Set once `close()` starts: no event is dispatched after that. */
  private eventsClosed = false;
  private closePromise: Promise<void> | undefined;

  constructor(options: IpcExecutorOptions) {
    if (!Number.isFinite(options.timeoutMs) || options.timeoutMs <= 0) {
      throw new Error(
        `timeoutMs must be a positive finite number, got ${options.timeoutMs}`,
      );
    }
    const maxReplays = options.maxReplays ?? DEFAULT_MAX_REPLAYS;
    if (!Number.isInteger(maxReplays) || maxReplays < 0) {
      throw new Error(
        `maxReplays must be a non-negative integer, got ${maxReplays}`,
      );
    }
    const restartBackoffMs =
      options.restartBackoffMs ?? DEFAULT_RESTART_BACKOFF_MS;
    if (!Number.isFinite(restartBackoffMs) || restartBackoffMs <= 0) {
      throw new Error(
        `restartBackoffMs must be a positive finite number, got ${restartBackoffMs}`,
      );
    }
    this.shutdownTimeoutMs =
      options.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;
    const startTimeoutMs =
      options.startTimeoutMs ?? Math.max(10_000, restartBackoffMs * 10);
    if (!Number.isFinite(startTimeoutMs) || startTimeoutMs <= 0) {
      throw new Error(
        `startTimeoutMs must be a positive finite number, got ${startTimeoutMs}`,
      );
    }
    this.startTimeoutMs = startTimeoutMs;
    // The buffer owns the cap and validates it, so the refusal and the limit
    // it reports cannot drift from each other.
    this.buffer = new PendingBuffer({
      maxPending: options.maxPending ?? DEFAULT_MAX_PENDING,
    });
    this.logger = safeLogger(
      options.logger ??
        createJsonLogger({ bindings: { component: 'ipc-executor' } }),
    );
    // Defaults must exceed the backoff cap (5000ms) and the longest request,
    // otherwise a Server that dies on its first request would reset the streak
    // before it crashed and restart with zero backoff every time.
    const restart = new RestartPolicy({
      baseBackoffMs: restartBackoffMs,
      stableResetMs:
        options.stableResetMs ?? Math.max(30_000, options.timeoutMs),
    });
    // The callbacks read live state when the ledger decides, so a value
    // captured here could not go stale across a status transition.
    this.ledger = new DeliveryLedger({
      buffer: this.buffer,
      logger: this.logger,
      maxReplays,
      isTerminal: () => this.slot.isTerminal,
      currentServer: () => this.slot.server,
      onServerLost: (server, code, signal, reason) => {
        this.supervisor.onServerLost(server, code, signal, reason);
      },
    });
    this.versionSwitch = new VersionSwitch(this);
    this.supervisor = new ServerSupervisor({
      logger: this.logger,
      slot: this.slot,
      ledger: this.ledger,
      versionSwitch: this.versionSwitch,
      restart,
      startTimeoutMs,
      portalTimeoutMs: options.timeoutMs,
      spawn: options.spawn,
      onEvent: (message) => {
        this.dispatchEvent(message);
      },
    });
  }

  /**
   * The switch's dependencies. They delegate to the process supervisor, which
   * owns launching, delivery and the restart clock.
   */
  adoptSpawn(spawn: () => SpawnedServer): void {
    this.supervisor.adoptSpawn(spawn);
  }

  attach(server: ServerState): void {
    this.supervisor.attach(server);
  }

  pump(): void {
    this.supervisor.pump();
  }

  resolveClosed(server: ServerState): void {
    this.supervisor.resolveClosed(server);
  }

  clearRestartTimer(): void {
    this.supervisor.clearRestartTimer();
  }

  startStableTimer(): void {
    this.supervisor.startStableClock();
  }

  adoptCeiling(declaredMaxDurationMs: number): void {
    this.supervisor.adoptCeiling(declaredMaxDurationMs);
  }

  recoverAfterExits(): void {
    this.supervisor.recoverAfterExits();
  }

  /** Spawns the Server, sends `hello` and resolves once the Server is `ready`. */
  start(): Promise<void> {
    return this.supervisor.start();
  }

  execute(request: ExecutorRequest): Promise<ExecutorOutcome> {
    if (this.slot.status === 'idle') {
      // Nothing was ever spawned. Without this the request would sit in the
      // buffer until its deadline and report a timeout, which looks like a slow
      // backend instead of a missing `start()` (SPEC 6.2 step 3).
      this.logger.error(
        {
          ...requestLogFields(request),
          code: INTERNAL_ERROR,
          reason: 'ipc executor was never started',
        },
        'request rejected',
      );
      return Promise.resolve(
        failure(INTERNAL_ERROR, 'ipc executor was never started'),
      );
    }
    if (this.slot.status === 'failed') {
      // A failed start is terminal: the restart loop was stopped, so buffering
      // this request would only hold it until its deadline for a Server that is
      // never coming. Failing now is the same answer as the start rejection.
      this.logger.error(
        {
          ...requestLogFields(request),
          code: INTERNAL_ERROR,
          reason: 'ipc executor failed to start',
        },
        'request rejected',
      );
      return Promise.resolve(
        failure(
          INTERNAL_ERROR,
          'ipc executor failed to start: no server is available',
        ),
      );
    }
    if (this.slot.isTerminal) {
      // `failed` is included here but unreachable: the branch above returns for
      // it first, with a message specific to a start that never succeeded.
      // warn, not error: an orderly shutdown refusal is expected, and the
      // connection layer records the failed outcome at error level already.
      this.logger.warn(
        {
          ...requestLogFields(request),
          code: FRAMEWORK_SHUTDOWN,
          reason: `executor ${this.slot.status}`,
        },
        'request rejected',
      );
      return Promise.resolve(
        failure(FRAMEWORK_SHUTDOWN, 'ipc executor is shutting down'),
      );
    }
    if (this.supervisor.recoveryBlocked) {
      return Promise.resolve(
        failure(
          INTERNAL_ERROR,
          'server recovery blocked: process exit is unconfirmed',
        ),
      );
    }
    // Capacity before duplicate id, matching the order the admission checks had
    // before the buffer was extracted: a request refused because the buffer is
    // full reports the cap, even when its id is also in flight. Reversing them
    // would answer "caller bug" to a request the executor refused by policy.
    // `PendingBuffer.add` keeps its own check as a backstop.
    if (this.buffer.size >= this.buffer.maxPending) {
      return this.refuseQueueFull(request);
    }
    if (this.buffer.get(request.requestId) !== undefined) {
      // Request ids are minted by the connection layer and a replay goes
      // through the buffer, so a second execute() for the same id is a caller
      // bug. Failing it beats overwriting a promise nobody would ever settle.
      this.logger.warn(
        {
          ...requestLogFields(request),
          code: INTERNAL_ERROR,
          reason: 'duplicate requestId',
        },
        'request rejected',
      );
      return Promise.resolve(
        failure(
          INTERNAL_ERROR,
          `request ${request.requestId} is already in flight`,
        ),
      );
    }
    const deferred = Promise.withResolvers<ExecutorOutcome>();
    const pending = this.buffer.add(
      request,
      deferred,
      // A caller-supplied deadline may only tighten the ceiling this executor
      // already promised; otherwise the buffer would outlive the caller's own
      // terminal state and replay work for an answer nobody reads (SPEC 3.4).
      tightenTimeout(this.supervisor.effectiveTimeoutMs, request.timeoutMs),
    );
    if (pending === undefined) {
      // Unreachable after the pre-check above, but `add` owns the cap and may
      // still refuse; sharing the refusal keeps the code, message and log in
      // one place.
      return this.refuseQueueFull(request);
    }
    this.ledger.armTimer(pending);
    try {
      const server = this.slot.server;
      if (
        this.slot.status !== 'switching' &&
        server !== undefined &&
        server.ready &&
        !server.closed
      ) {
        // Nothing older can be undelivered while the Server is ready: every
        // flush completes or leaves no Server. Sending only this request keeps
        // the hot path O(1) instead of scanning the buffer per request.
        this.ledger.deliver(server, pending);
      } else {
        // Queued until a Server is ready; a no-op otherwise. During a switch
        // this is the whole policy: the request waits for the new generation
        // rather than reaching a process that is about to be retired.
        this.supervisor.pump();
      }
    } catch (error) {
      // Delivery is defensive against a throwing channel or callback. The
      // promise must settle regardless.
      this.ledger.settle(pending, failure(INTERNAL_ERROR, messageOf(error)));
    }
    return deferred.promise;
  }

  /**
   * Refuses a request because the unacknowledged buffer is at its cap.
   *
   * warn, not error: a full buffer is a refusal by policy, and the caller gets a
   * stable code rather than a request that waits out its deadline.
   */
  private refuseQueueFull(request: ExecutorRequest): Promise<ExecutorOutcome> {
    this.logger.warn(
      {
        ...requestLogFields(request),
        code: FRAMEWORK_QUEUE_FULL,
        pending: this.buffer.size,
        limit: this.buffer.maxPending,
      },
      'request refused: pending buffer is full',
    );
    return Promise.resolve(
      failure(
        FRAMEWORK_QUEUE_FULL,
        `ipc executor pending buffer is full (${this.buffer.maxPending} unacknowledged)`,
      ),
    );
  }

  close(): Promise<void> {
    // Memoised here so the sequence runs once: a second `close()` joins the
    // first rather than shutting the same process down twice.
    if (this.closePromise === undefined) {
      // Events are best-effort (ADR-0008): after shutdown starts there may be
      // no connection layer left to deliver them to, so the sinks are released
      // and later messages are dropped with a warn rather than held.
      this.eventsClosed = true;
      this.eventListeners.clear();
      this.closePromise = doClose(this.shutdownHost());
    }
    return this.closePromise;
  }

  /**
   * Registers one sink for Server-initiated events and returns the
   * unsubscribe function. A subscription after `close()` is a no-op: the
   * executor can no longer receive events worth delivering.
   */
  onEvent(listener: (event: ServerEvent) => void): () => void {
    if (this.eventsClosed) {
      return () => undefined;
    }
    this.eventListeners.add(listener);
    return () => {
      this.eventListeners.delete(listener);
    };
  }

  /**
   * Hands one parsed event to the registered sinks in registration order.
   * Nothing here may throw at the caller: IPC dispatch has no failure channel,
   * and one broken sink must not take the process or the other sinks down.
   */
  private dispatchEvent(message: EventMessage): void {
    const event: ServerEvent = {
      eventId: message.eventId,
      target: message.target,
      event: message.event,
      payload: message.payload,
    };
    if (this.eventsClosed || this.eventListeners.size === 0) {
      this.logger.warn(
        {
          eventId: event.eventId,
          event: event.event,
          code: 'EVENT_DROPPED',
          reason: 'no event listener',
        },
        'server event dropped',
      );
      return;
    }
    // Snapshot: a sink that unsubscribes (or subscribes) during dispatch must
    // not change who receives this event.
    const listeners = new Set(this.eventListeners);
    for (const listener of listeners) {
      try {
        listener(event);
      } catch (error) {
        this.logger.warn(
          {
            eventId: event.eventId,
            event: event.event,
            code: 'EVENT_LISTENER_FAILED',
            reason: messageOf(error),
          },
          'server event listener failed',
        );
      }
    }
  }

  /**
   * Reads the slot, the current Server and the recovery barrier, and lets
   * `ipcExecutorHealth` classify them (SPEC 6.6). No state is written and no
   * log is emitted, so a probe can poll it.
   */
  health(): ExecutorHealth {
    const server = this.slot.server;
    return ipcExecutorHealth({
      status: this.slot.status,
      handshakeCompleted: this.supervisor.handshakeCompleted,
      serverReady: server !== undefined && server.ready && !server.closed,
      recoveryBlocked: this.supervisor.recoveryBlocked,
    });
  }

  /**
   * Replaces the Server process with one built from `spawn` without dropping
   * the recovery barrier (ADR-0007, INV-002). The sequence itself lives in
   * `VersionSwitch`; this class only hands over the process factory.
   */
  switchTo(options: { spawn: () => SpawnedServer }): Promise<void> {
    return this.versionSwitch.switchTo(options);
  }

  /**
   * The worst case `doClose` can take, derived from the steps it actually runs
   * (see `closeBudget` in `budget.ts`).
   */
  get closeBudgetMs(): number {
    return closeBudget(this.shutdownTimeoutMs);
  }

  /**
   * Ends a request Portal is no longer waiting for and stops buffering it.
   *
   * The local settle happens here rather than on a Server result, because a
   * cancelled request is defined to have no result: the confirmation the buffer
   * waits for will never come, and keeping the entry would replay work whose
   * answer is already discarded (SPEC 3.6, 6.4). The Server is told as well so
   * it rolls back instead of committing writes nobody can see.
   */
  cancel(requestId: string, code: string): Promise<void> {
    const pending = this.buffer.get(requestId);
    if (pending === undefined) {
      // Already terminal, or never accepted: nothing to undo and nothing to
      // tell the Server, which either has no such request or has finished it.
      return Promise.resolve();
    }
    // warn, not error: this is a request ending by policy, not a failure.
    this.logger.warn(
      {
        ...requestLogFields(pending.request),
        code,
        durationMs: Date.now() - pending.startedAt,
      },
      'request cancelled',
    );
    this.ledger.cancelOnServer(pending, code);
    this.ledger.settle(
      pending,
      failure(code, `request ${requestId} was cancelled`),
    );
    return Promise.resolve();
  }

  /**
   * The close sequence's view of this executor. Every member reads live state
   * or calls back into the owner, so a step never holds a stale status, Server
   * or start waiter.
   */
  private shutdownHost(): ShutdownHost {
    const supervisor = this.supervisor;
    return {
      logger: this.logger,
      slot: this.slot,
      shutdownTimeoutMs: this.shutdownTimeoutMs,
      versionSwitch: {
        abortSwitchForClose: () => this.versionSwitch.abortSwitchForClose(),
      },
      ledger: {
        failUndelivered: (reason) => {
          this.ledger.failUndelivered(reason);
        },
        awaitPendingSettled: () => this.ledger.awaitPendingSettled(),
        failAllPending: (code, message, reason) => {
          this.ledger.failAllPending(code, message, reason);
        },
        pendingCount: () => this.buffer.size,
      },
      restart: {
        clearRestartTimer: () => {
          supervisor.clearRestartTimer();
        },
        clearStableClock: () => {
          supervisor.clearStableClock();
        },
      },
      recovery: {
        clear: () => {
          supervisor.clearRecoveryBarrier();
        },
        block: () => {
          supervisor.blockRecoveryBarrier();
        },
      },
      waiters: {
        clearStartTimer: () => {
          supervisor.clearStartTimers();
        },
        rejectStart: (error) => {
          supervisor.rejectStart(error);
        },
        waitForClosed: (server, waitMs) =>
          supervisor.waitForClosed(server, waitMs),
      },
      listPendingExits: () => this.slot.pendingExits,
    };
  }
}
