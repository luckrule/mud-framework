import { messageOf } from '../internal/guards.ts';
import type { ResultMessage } from '../ipc/protocol.ts';
import type { SpawnedServer } from '../ipc/spawn-port.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { safeLogger } from '../logging/safe-logger.ts';
import type { LogFields, Logger } from '../logging/types.ts';
import type { DeliveryHost } from './delivery-ledger.ts';
import { DeliveryLedger, requestFields } from './delivery-ledger.ts';
import type {
  ExecutorOutcome,
  ExecutorRequest,
  RequestExecutor,
} from './executor.ts';
import {
  DEFAULT_MAX_PENDING,
  DEFAULT_MAX_REPLAYS,
  DEFAULT_RESTART_BACKOFF_MS,
  DEFAULT_SHUTDOWN_TIMEOUT_MS,
  EXIT_WAIT_TIMEOUT_MS,
  SHUTDOWN_ACK_SLACK_MS,
  closeBudget,
} from './ipc-budget.ts';
import { failure, nextPortalGeneration } from './ipc-messages.ts';
import { PendingBuffer } from './ipc-pending.ts';
import { RestartPolicy } from './restart-policy.ts';
import type { ServerHandlers } from './server-launcher.ts';
import {
  ClosedWaiter,
  attachServer,
  buildServer,
  killQuietly,
  waitForExit,
} from './server-launcher.ts';
import type { MessageHost } from './server-messages.ts';
import { onServerMessage } from './server-messages.ts';
import type { ServerState } from './server-slot.ts';
import { ServerSlot } from './server-slot.ts';
import { effectiveTimeoutMs, settleWithin, tightenTimeout } from './timeout.ts';
import type { SwitchHost, SwitchOperation } from './version-switch.ts';
import { VersionSwitch } from './version-switch.ts';
import {
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_SHUTDOWN,
  INTERNAL_ERROR,
} from './wire-codes.ts';

export interface IpcExecutorOptions {
  spawn: () => SpawnedServer;
  /** Portal-side per-request ceiling; effective = min(this, backend's declared max). */
  timeoutMs: number;
  /** Max replays of one request before it reaches a terminal state. Default 3. */
  maxReplays?: number;
  /** Base for exponential restart backoff in ms. Default 100, cap 5000. */
  restartBackoffMs?: number;
  /**
   * How long a Server must stay up before its crash streak is considered over.
   * Defaults to `Math.max(30_000, timeoutMs)`.
   *
   * The window must exceed both the backoff cap (5000ms) and the longest a
   * single request can live: a Server that crashes while handling its first
   * request must be counted as a crash, not as a healthy run that reset the
   * backoff. Otherwise "ready, then die on the first request" restarts at
   * `attempt === 0` (immediate) forever.
   */
  stableResetMs?: number;
  /** Grace period for a graceful shutdown before killing. Default 5000ms. */
  shutdownTimeoutMs?: number;
  /**
   * How long `start()` waits for the readiness barrier before failing.
   * Defaults to `Math.max(10_000, restartBackoffMs * 10)`.
   *
   * The value has to cover several restart attempts (a first crash may be a
   * one-off and must not fail the start) while still making a genuinely broken
   * Server visible in seconds rather than never: with the default backoff the
   * restarts inside 10s are immediate, 100, 200, 400, 800, 1600, 3200 and 5000
   * ms, so the default covers eight attempts.
   */
  startTimeoutMs?: number;
  /**
   * Requests buffered but not yet acknowledged by a Server. Default 10000.
   *
   * A pending request lives at most one deadline, so this is not a throughput
   * cap; it is the brake for the one case the deadline cannot bound: a Server
   * that stays unavailable while clients keep writing. Without it the buffer
   * grows until the Portal runs out of memory, and the connection layer's own
   * per-connection cap does not apply once a request left its queue.
   */
  maxPending?: number;
  /**
   * Opaque application code version. Stamped on every Request accepted here and
   * kept across replays; compared only to warn when a `ready` reports another
   * version. The framework never parses it or makes a decision from it
   * (SPEC 6.4). Must be a non-empty string when set.
   *
   * A caller-supplied `ExecutorRequest.codeVersion` must also be a non-empty
   * string; an invalid one fails that request with `INTERNAL_ERROR` instead of
   * being sent.
   */
  codeVersion?: string;
  /**
   * Defaults to a JSON logger writing to stdout with
   * `component: 'ipc-executor'` bindings (SPEC §7.1). Supplying one only
   * redirects where the events go; observability does not depend on it.
   */
  logger?: Logger;
}

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
  private startWaiter:
    | { resolve: () => void; reject: (error: Error) => void }
    | undefined;
  private readonly closedWaiter = new ClosedWaiter();
  private closePromise: Promise<void> | undefined;
  /**
   * Processes a `close()`-aborted switch abandoned, kept until their exit is
   * confirmed. `doClose` drains this list before it returns: an abandoned
   * replacement may have received `activate`, so its exit is what says the
   * storage is released (INV-002).
   */
  private readonly abandonedForClose: ServerState[] = [];

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
        attachServer(server, this.serverHandlers(server));
      },
      pump: () => {
        this.pump();
      },
      currentServer: () => this.slot.server,
      recordAbandoned: (server) => {
        if (!this.abandonedForClose.includes(server)) {
          this.abandonedForClose.push(server);
        }
      },
      forgetAbandoned: (server) => {
        const index = this.abandonedForClose.indexOf(server);
        if (index !== -1) {
          this.abandonedForClose.splice(index, 1);
        }
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
      nextBackoffMs: () => this.restart.nextBackoffMs(),
      scheduleRestart: (backoffMs) => {
        this.scheduleRestart(backoffMs);
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
  private onStartTimeout(): void {
    this.startTimer = undefined;
    if (this.startWaiter === undefined) {
      return;
    }
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
    this.closePromise ??= this.doClose();
    return this.closePromise;
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
   * (see `closeBudget` in `ipc-budget.ts`).
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

  private async doClose(): Promise<void> {
    this.slot.markClosing();
    this.restart.clearRestartTimer();
    this.restart.clearStableClock();
    this.clearStartTimer();
    // A switch in flight must not keep spawning, activating or restarting: it is
    // interrupted here and its staged process abandoned, so `close()` ends with
    // at most one Server to shut down and no orphan writer (INV-002).
    await this.versionSwitch.abortSwitchForClose();
    // A Request that never reached a live Server cannot complete, so it fails
    // now instead of burning its whole deadline. Delivered ones are given
    // until their deadline to settle (SPEC 6.3 step 3).
    this.ledger.failUndelivered('executor closing before delivery');
    // Bounded by the same budget as the rest of the shutdown. An unbounded wait
    // here used to mean "as long as the longest request deadline", so a
    // ConnectionLayer with a shorter shutdownTimeout would return while this
    // executor was still waiting and the Server was still writing (the two
    // timeouts were unrelated). Past the budget the requests are failed and the
    // Server is shut down anyway.
    const drained = await settleWithin(
      this.ledger.awaitPendingSettled(),
      this.shutdownTimeoutMs,
    );
    // A rejection counts as "no longer pending" here, on purpose: the waiter
    // only ever resolves, so the one thing this branch must distinguish is a
    // timeout. Treating a rejection as a timeout would fail every buffered
    // request with a message about a deadline that was never reached.
    if (drained.status === 'timeout') {
      this.logger.warn(
        { timeoutMs: this.shutdownTimeoutMs, pending: this.buffer.size },
        'pending requests did not settle before the shutdown timeout',
      );
      this.ledger.failAllPending(
        FRAMEWORK_SHUTDOWN,
        'ipc executor closed before the request settled',
        'shutdown drain timed out',
      );
    }
    await this.shutdownServer();
    await this.confirmAbandonedExits();
    this.ledger.failAllPending(
      FRAMEWORK_SHUTDOWN,
      'ipc executor closed',
      'executor closed',
    );
    this.slot.markClosed();
    // A start() still waiting for `ready` must not be told "ready" by a close.
    this.rejectStart(
      new Error('ipc executor closed before the server became ready'),
    );
  }

  /**
   * Confirms the exit of every process a `close()`-aborted switch abandoned,
   * with the same bounded wait → kill → wait flow `shutdownServer` uses.
   *
   * A replacement that already received `activate` may hold storage, and the
   * switch's own abort path only issues the kill. `close()` must not return
   * while such a process could still be writing (INV-002); a process that
   * survives the second wait is logged rather than waited on forever.
   */
  private async confirmAbandonedExits(): Promise<void> {
    const pending = this.abandonedForClose.splice(0);
    // Parallel, not sequential: these are independent processes, and the budget
    // counts one bounded wait→kill→wait, not one per process.
    await Promise.all(
      pending.map(async (server) => {
        const exited = await waitForExit(server, this.logger);
        if (exited) {
          return;
        }
        this.logger.warn(
          { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
          'abandoned replacement did not exit; killing it before close returns',
        );
        killQuietly(server, this.logger);
        const afterKill = await waitForExit(server, this.logger);
        if (!afterKill) {
          this.logger.warn(
            { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
            'abandoned replacement is still running after a kill; close cannot confirm its exit',
          );
        }
      }),
    );
  }

  private async shutdownServer(): Promise<void> {
    const server = this.slot.server;
    if (server === undefined) {
      return;
    }
    if (!server.closed) {
      if (server.ready && !server.shutdownSent) {
        try {
          // The Server gets exactly the grace `waitForClosed` will wait for, so
          // its drain and backend close share one deadline that ends with the
          // Portal's patience rather than twice its own request ceiling.
          server.shutdownSent = true;
          server.channel.send({
            type: 'shutdown',
            timeoutMs: this.shutdownTimeoutMs,
          });
        } catch (error) {
          // The process died between the pending drain and this send; the
          // channel close handler is about to report the same thing.
          this.logger.warn(
            { pid: server.pid, reason: messageOf(error) },
            'shutdown message could not be sent',
          );
        }
      }
      if (server.ready) {
        await this.closedWaiter.waitForClosed(
          server,
          this.shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS,
        );
      }
      if (!server.closed && !server.shutdownAcknowledged) {
        // A stalled graceful shutdown, or a Server that never finished
        // starting, must not hold the Portal open. The logged wait is the one
        // `waitForClosed` actually performed, not the declared grace.
        this.logger.warn(
          {
            pid: server.pid,
            timeoutMs: this.shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS,
            ready: server.ready,
          },
          'server did not finish shutting down; killing it',
        );
        try {
          server.kill();
        } catch (error) {
          this.logger.warn(
            { pid: server.pid, reason: messageOf(error) },
            'server could not be killed',
          );
        }
      }
    }
    // A `closed` message says the Server finished its work, not that the
    // process is gone; the storage must not be reopened while it lingers
    // (INV-002). This also covers a process that was already killed from the
    // channel-loss path. Waiting is bounded, and a lingering process is logged.
    const exited = await waitForExit(server, this.logger);
    if (!exited) {
      // `close()` must not return while the Server may still be writing: a
      // caller that then reopens storage gets two writers, which is exactly the
      // defect this class exists to prevent. The graceful path withheld the kill
      // on purpose, so it is issued here and the exit is awaited once more.
      this.logger.warn(
        { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
        'server process did not exit; killing it before close returns',
      );
      try {
        server.kill();
      } catch (error) {
        this.logger.warn(
          { pid: server.pid, reason: messageOf(error) },
          'server could not be killed',
        );
      }
      await waitForExit(server, this.logger);
    }
    try {
      server.channel.close('portal is shutting down');
    } catch (error) {
      this.logger.debug(
        { reason: messageOf(error) },
        'ipc channel close failed',
      );
    }
    this.slot.clearIfCurrent(server);
  }

  private resolveClosed(): void {
    this.closedWaiter.resolve();
  }

  private resolveStart(): void {
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

  private launch(): void {
    if (this.slot.isTerminal) {
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
    // A fresh binding wins over the entry Session the connection layer already
    // knew.
    if (message.ok && message.sessionId !== undefined) {
      fields.sessionId = message.sessionId;
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
    this.slot.markStartingAfterLoss();
    // Wait for the exit before restarting even when the kill was issued: the
    // replacement must not open storage while the old process still holds it.
    void waitForExit(server, this.logger).then(() => {
      this.scheduleRestart(backoffMs);
      return undefined;
    });
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
