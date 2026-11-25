import type { ResultMessage } from '../../ipc/protocol.ts';
import type { SpawnedServer } from '../../ipc/spawn-port.ts';
import { createJsonLogger } from '../../logging/json-logger.ts';
import { safeLogger } from '../../logging/safe-logger.ts';
import type { LogFields, Logger } from '../../logging/types.ts';
import {
  DEFAULT_MAX_PENDING,
  DEFAULT_MAX_REPLAYS,
  DEFAULT_RESTART_BACKOFF_MS,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
  closeBudget,
} from '../../shared/budget.ts';
import type {
  ExecutorHealth,
  ExecutorOutcome,
  ExecutorRequest,
  RequestExecutor,
} from '../../shared/executor.ts';
import { messageOf } from '../../shared/guards.ts';
import { effectiveTimeoutMs, tightenTimeout } from '../../shared/timeout.ts';
import {
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_SHUTDOWN,
  INTERNAL_ERROR,
} from '../../shared/wire-codes.ts';
import { failure, nextPortalGeneration } from '../ipc-outbound.ts';
import { PendingBuffer } from '../ipc-pending.ts';
import type { DeliveryHost } from './delivery-ledger.ts';
import { DeliveryLedger, requestFields } from './delivery-ledger.ts';
import { ipcExecutorHealth } from './executor-health.ts';
import type { ShutdownHost } from './executor-shutdown.ts';
import { doClose } from './executor-shutdown.ts';
import type { IpcExecutorOptions } from './ipc-executor-options.ts';
import type { MessageHost } from './ipc-inbound.ts';
import { onServerMessage } from './ipc-inbound.ts';
import { RestartPolicy } from './restart-policy.ts';
import type { ServerHandlers } from './server-launcher.ts';
import {
  ClosedWaiter,
  attachServer,
  buildServer,
  killQuietly,
  waitForExit,
} from './server-launcher.ts';
import type { ServerState } from './server-slot.ts';
import { ServerSlot } from './server-slot.ts';
import type { SwitchHost, SwitchOperation } from './version-switch.ts';
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
 */
export class IpcExecutor implements RequestExecutor {
  /**
   * The factory the next launch uses. A successful `switchTo` replaces it, so a
   * crash after the switch restarts the code that was actually running rather
   * than the version the executor was constructed with (SPEC 6.4).
   */
  private spawn: () => SpawnedServer;
  private readonly portalTimeoutMs: number;
  private readonly maxReplays: number;
  private readonly shutdownTimeoutMs: number;
  private readonly startTimeoutMs: number;
  /** Stamped on new requests and compared against `ready`; follows `spawn`. */
  private codeVersion: string | undefined;
  private readonly logger: Logger;
  private readonly portalGeneration: number;
  private readonly buffer: PendingBuffer;
  /** Delivery, deadlines and settlement: everything that resolves a pending entry. */
  private readonly ledger: DeliveryLedger;
  /** Sole owner of `status`, `server`, the generation counter and `switching`. */
  private readonly slot = new ServerSlot<SwitchOperation>();
  /** The `switchTo` sequence; it writes state only through `slot`. */
  private readonly versionSwitch: VersionSwitch;
  /** The Server→Portal handlers' view of this executor. */
  private readonly messageHost: MessageHost;
  private effectiveTimeoutMs: number;
  /** The crash streak, its backoff and the stability window that ends it. */
  private readonly restart: RestartPolicy;
  private startTimer: ReturnType<typeof setTimeout> | undefined;
  private generationTimer:
    | { server: ServerState; timer: ReturnType<typeof setTimeout> }
    | undefined;
  private startWaiter:
    | { resolve: () => void; reject: (error: Error) => void }
    | undefined;
  private readonly closedWaiter = new ClosedWaiter();
  private closePromise: Promise<void> | undefined;
  private pendingRecovery: { backoffMs: number; blocked: boolean } | undefined;
  /** Separates the first start from a restart when deriving health. */
  private handshakeCompleted = false;

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
    this.spawn = options.spawn;
    this.portalTimeoutMs = options.timeoutMs;
    this.maxReplays = maxReplays;
    // Defaults must exceed the backoff cap (5000ms) and the longest request,
    // otherwise a Server that dies on its first request would reset the streak
    // before it crashed and restart with zero backoff every time.
    this.restart = new RestartPolicy({
      baseBackoffMs: restartBackoffMs,
      stableResetMs:
        options.stableResetMs ?? Math.max(30_000, options.timeoutMs),
    });
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
    // Opaque to the framework, but not every value survives the wire: the peer's
    // parser accepts only a non-empty string, so anything else would leave a
    // Server that can never reach `ready`.
    if (
      options.codeVersion !== undefined &&
      (typeof options.codeVersion !== 'string' ||
        options.codeVersion.length === 0)
    ) {
      throw new Error('codeVersion must be a non-empty string');
    }
    this.codeVersion = options.codeVersion;
    // The buffer owns the cap and validates it, so the refusal and the limit
    // it reports cannot drift from each other.
    this.buffer = new PendingBuffer({
      maxPending: options.maxPending ?? DEFAULT_MAX_PENDING,
    });
    this.logger = safeLogger(
      options.logger ??
        createJsonLogger({ bindings: { component: 'ipc-executor' } }),
    );
    this.portalGeneration = nextPortalGeneration();
    // Until `ready` declares the backend ceiling, the Portal ceiling applies.
    this.effectiveTimeoutMs = options.timeoutMs;
    // The host is a live view, not a snapshot: the getter and the method read
    // this executor's state when the ledger decides, so a value captured here
    // could not go stale across a status transition.
    const slot = this.slot;
    const host: DeliveryHost = {
      logger: this.logger,
      maxReplays: this.maxReplays,
      get isTerminal(): boolean {
        return slot.isTerminal;
      },
      currentServer: () => slot.server,
      onServerLost: (server, code, signal, reason) => {
        this.onServerLost(server, code, signal, reason);
      },
    };
    this.ledger = new DeliveryLedger({ buffer: this.buffer, host });
    this.versionSwitch = new VersionSwitch(this.switchHost());
    this.messageHost = this.messageHostOf();
  }

  /**
   * The switch's view of this executor. Every member reads live state or calls
   * back into the executor, so the switch never holds a stale status or Server.
   */
  private switchHost(): SwitchHost {
    return {
      logger: this.logger,
      slot: this.slot,
      startTimeoutMs: this.startTimeoutMs,
      shutdownTimeoutMs: this.shutdownTimeoutMs,
      portalGeneration: this.portalGeneration,
      currentCodeVersion: () => this.codeVersion,
      adoptVersion: (spawn, codeVersion) => {
        this.spawn = spawn;
        this.codeVersion = codeVersion;
      },
      attach: (server) => {
        this.slot.track(server);
        attachServer(server, this.serverHandlers(server));
      },
      pump: () => {
        this.pump();
      },
      currentServer: () => this.slot.server,
      recordAbandoned: (server) => {
        this.slot.track(server);
      },
      resolveClosed: () => {
        this.resolveClosed();
      },
      clearRestartTimer: () => {
        this.restart.clearRestartTimer();
      },
      startStableTimer: () => {
        this.restart.startStableClock();
      },
      adoptCeiling: (declaredMaxDurationMs) => {
        this.adoptCeiling(declaredMaxDurationMs);
      },
      recoverAfterExits: () => {
        this.recoverAfterExits(this.restart.nextBackoffMs());
      },
    };
  }

  /**
   * The message handlers' view of this executor. Every member reads live state
   * or calls back into the executor, so the handlers never hold a stale status
   * or Server.
   */
  private messageHostOf(): MessageHost {
    return {
      logger: this.logger,
      slot: this.slot,
      currentCodeVersion: () => this.codeVersion,
      adoptReady: (server, declaredMaxDurationMs) => {
        this.clearGenerationTimer(server);
        this.slot.markReady(server);
        // The crash streak ends only after a stable run, not at the handshake:
        // a Server that reaches `ready` and then dies on its first request
        // would otherwise reset the backoff and restart immediately, forever.
        this.restart.startStableClock();
        this.adoptCeiling(declaredMaxDurationMs);
        return this.effectiveTimeoutMs;
      },
      pump: () => {
        this.pump();
      },
      resolveStart: () => {
        this.resolveStart();
      },
      onResult: (message) => {
        this.onResult(message);
      },
      onServerLost: (server, code, signal, reason) => {
        this.onServerLost(server, code, signal, reason);
      },
      resolveClosed: () => {
        this.resolveClosed();
      },
    };
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
    // the handshake (a broken entry script, a failing spawn) leaves `start`
    // pending forever while the restart loop keeps spawning processes.
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
    try {
      server.channel.close('start timeout');
    } catch (error) {
      this.logger.debug(
        { reason: messageOf(error) },
        'ipc channel close failed',
      );
    }
  }

  execute(request: ExecutorRequest): Promise<ExecutorOutcome> {
    if (this.slot.status === 'idle') {
      // Nothing was ever spawned. Without this the request would sit in the
      // buffer until its deadline and report a timeout, which looks like a slow
      // backend instead of a missing `start()` (SPEC 6.2 step 3).
      this.logger.error(
        {
          ...requestFields(request),
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
          ...requestFields(request),
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
          ...requestFields(request),
          code: FRAMEWORK_SHUTDOWN,
          reason: `executor ${this.slot.status}`,
        },
        'request rejected',
      );
      return Promise.resolve(
        failure(FRAMEWORK_SHUTDOWN, 'ipc executor is shutting down'),
      );
    }
    if (this.pendingRecovery?.blocked) {
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
          ...requestFields(request),
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
    const stamped = this.stampCodeVersion(request);
    if (!stamped.ok) {
      this.logger.error(
        {
          ...requestFields(request),
          code: INTERNAL_ERROR,
          reason: stamped.reason,
        },
        'request rejected',
      );
      return Promise.resolve(failure(INTERNAL_ERROR, stamped.reason));
    }
    const deferred = Promise.withResolvers<ExecutorOutcome>();
    const pending = this.buffer.add(
      stamped.request,
      deferred,
      // A caller-supplied deadline may only tighten the ceiling this executor
      // already promised; otherwise the buffer would outlive the caller's own
      // terminal state and replay work for an answer nobody reads (SPEC 3.4).
      tightenTimeout(this.effectiveTimeoutMs, request.timeoutMs),
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
        this.pump();
      }
    } catch (error) {
      // Delivery is defensive against a throwing channel or callback. The
      // promise must settle regardless.
      this.ledger.settle(pending, failure(INTERNAL_ERROR, messageOf(error)));
    }
    return deferred.promise;
  }

  /**
   * Stamps this executor's code version on the Request it stores, so a replay
   * carries the version it was accepted under rather than the replacement
   * Server's (SPEC 6.4).
   *
   * A caller-supplied version wins, because the caller may know which code the
   * Request belongs to, but it must be a non-empty string for the same reason
   * the configured value is checked: the peer's parser rejects anything else, so
   * the Request could never be delivered. An invalid value is refused here
   * instead of being put on the wire.
   */
  private stampCodeVersion(
    request: ExecutorRequest,
  ): { ok: true; request: ExecutorRequest } | { ok: false; reason: string } {
    if (request.codeVersion === undefined) {
      return this.codeVersion === undefined
        ? { ok: true, request }
        : { ok: true, request: { ...request, codeVersion: this.codeVersion } };
    }
    if (
      typeof request.codeVersion !== 'string' ||
      request.codeVersion.length === 0
    ) {
      return {
        ok: false,
        reason:
          'invalid caller-supplied codeVersion: expected a non-empty string',
      };
    }
    return { ok: true, request };
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
        ...requestFields(request),
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
    this.closePromise ??= doClose(this.shutdownHost());
    return this.closePromise;
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
      handshakeCompleted: this.handshakeCompleted,
      serverReady: server !== undefined && server.ready && !server.closed,
      recoveryBlocked: this.pendingRecovery?.blocked === true,
    });
  }

  /**
   * Replaces the Server process with one built from `spawn` without dropping
   * the recovery barrier (ADR-0007, INV-002). The sequence itself lives in
   * `VersionSwitch`; this class only hands over the process factory.
   */
  switchTo(options: {
    spawn: () => SpawnedServer;
    codeVersion?: string;
  }): Promise<void> {
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
        ...requestFields(pending.request),
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
   * or calls back into the executor, so a step never holds a stale status,
   * Server or start waiter.
   */
  private shutdownHost(): ShutdownHost {
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
          this.restart.clearRestartTimer();
        },
        clearStableClock: () => {
          this.restart.clearStableClock();
        },
      },
      waiters: {
        clearStartTimer: () => {
          this.clearStartTimer();
          this.clearGenerationTimer();
        },
        rejectStart: (error) => {
          this.rejectStart(error);
        },
        waitForClosed: (server, waitMs) =>
          this.closedWaiter.waitForClosed(server, waitMs),
      },
      listPendingExits: () => this.slot.pendingExits,
    };
  }

  private resolveClosed(): void {
    this.closedWaiter.resolve();
  }

  private resolveStart(): void {
    this.handshakeCompleted = true;
    const waiter = this.startWaiter;
    this.startWaiter = undefined;
    this.clearStartTimer();
    waiter?.resolve();
  }

  private rejectStart(error: Error): void {
    const waiter = this.startWaiter;
    this.startWaiter = undefined;
    this.clearStartTimer();
    waiter?.reject(error);
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
      this.slot.markStartingAfterLoss();
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
      server.channel.send({
        type: 'hello',
        portalGeneration: this.portalGeneration,
      });
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
   * Binds this executor's handlers to one process, so a staged process is
   * observed exactly like the current one.
   */
  private serverHandlers(server: ServerState): ServerHandlers {
    return {
      onMessage: (raw) => {
        onServerMessage(this.messageHost, server, raw);
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

  /**
   * Applies the backend's declared request ceiling to the buffered requests.
   * Only ever tightens: a later Server declaring a longer ceiling must not
   * extend a deadline the Portal already promised, or a crash loop could keep a
   * request alive past its original ceiling (SPEC 3.4).
   */
  private adoptCeiling(declaredMaxDurationMs: number): void {
    this.effectiveTimeoutMs = effectiveTimeoutMs(
      this.portalTimeoutMs,
      declaredMaxDurationMs,
    );
    this.ledger.tightenDeadlines(this.effectiveTimeoutMs);
  }

  private onResult(message: ResultMessage): void {
    // `requestSessionId` is envelope metadata for logging only: it must never
    // reach the settled outcome. Splitting it off here with `type` and
    // `requestId` is what keeps the outcome equal to `ExecutorOutcome`.
    const {
      type: _type,
      requestId: _requestId,
      requestSessionId,
      ...outcome
    } = message;
    const pending = this.buffer.get(message.requestId);
    if (pending === undefined) {
      // A result for a request that already reached a Portal-decided terminal
      // state. Expected, not an error (SPEC 7.3 keeps the id stable, so the
      // correlation still works — there is just nothing left to settle).
      // The pending entry is gone, so the Session comes from the result's echo
      // of the entry Session, which the Server sends on every result including
      // failures.
      const fields: LogFields = {
        requestId: message.requestId,
        reason: 'request is no longer buffered',
      };
      if (requestSessionId !== undefined) {
        fields.sessionId = requestSessionId;
      }
      this.logger.debug(fields, 'late result discarded');
      return;
    }
    const fields: LogFields = {
      ...requestFields(pending.request),
      durationMs: Date.now() - pending.startedAt,
    };
    // `sessionId` stays the entry Session from `requestFields`; a routing
    // change is reported separately, never as a replacement id.
    if (message.ok && message.sessionChange !== undefined) {
      fields.sessionChange = message.sessionChange;
    }
    this.logger.debug(fields, 'result received');
    this.ledger.settle(pending, outcome);
  }

  private onServerLost(
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
      this.resolveClosed();
      return;
    }
    this.slot.clearIfCurrent(server);
    // A close that is waiting for `closed` will never get it now.
    this.resolveClosed();
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

  private recoverAfterExits(backoffMs: number): void {
    if (this.slot.isTerminal) {
      return;
    }
    this.slot.markStartingAfterLoss();
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

  /**
   * Re-delivers the buffered requests when the Server is not ready or a switch
   * is in progress. The hot path sends a single request directly through
   * `ledger.deliver`; this is the catch-up path after a barrier or a failed
   * switch.
   *
   * The switching gate stays here because it is executor state: while a
   * replacement is being staged or the previous process is being retired,
   * delivering now would send a request to a process the switch is draining.
   * It stays buffered until the replacement is promoted and this runs again.
   */
  private pump(): void {
    if (this.slot.status === 'switching') {
      return;
    }
    this.ledger.flush(this.slot.server);
  }
}
