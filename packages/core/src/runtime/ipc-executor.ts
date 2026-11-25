import { messageOf } from '../internal/guards.ts';
import type { IpcChannel } from '../ipc/channel.ts';
import type { ResultMessage } from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import type { ServerExitInfo, SpawnedServer } from '../ipc/spawn-port.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { safeLogger } from '../logging/safe-logger.ts';
import type { LogFields, Logger } from '../logging/types.ts';
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
  MAX_RESTART_BACKOFF_MS,
  SHUTDOWN_ACK_SLACK_MS,
  closeBudget,
} from './ipc-budget.ts';
import {
  failure,
  nextPortalGeneration,
  requestMessage,
} from './ipc-messages.ts';
import type { PendingRequest } from './ipc-pending.ts';
import { PendingBuffer } from './ipc-pending.ts';
import { effectiveTimeoutMs, settleWithin, tightenTimeout } from './timeout.ts';
import {
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_REPLAY_EXHAUSTED,
  FRAMEWORK_SHUTDOWN,
  FRAMEWORK_TIMEOUT,
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

interface ServerState {
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

/** A bounded wait for `prepared` or `ready` from the staged process. */
interface SwitchBarrier {
  readonly promise: Promise<void>;
  resolve: () => void;
  seen: boolean;
}

/**
 * One `switchTo` in flight. It owns the staged process and the previous one,
 * the barriers it waits for, and the abort signal `close()` uses to interrupt
 * the sequence at any await.
 */
interface SwitchOperation {
  readonly spawn: () => SpawnedServer;
  readonly targetCodeVersion: string | undefined;
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
  settled: boolean;
  phase: 'preparing' | 'retiring' | 'activating';
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
  versionWarned: boolean;
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
  private readonly restartBackoffMs: number;
  private readonly stableResetMs: number;
  private readonly shutdownTimeoutMs: number;
  private readonly startTimeoutMs: number;
  /** Stamped on new requests and compared against `ready`; follows `spawn`. */
  private codeVersion: string | undefined;
  private readonly logger: Logger;
  private readonly portalGeneration: number;
  private readonly buffer: PendingBuffer;
  private status: Status = 'idle';
  private server: ServerState | undefined;
  private effectiveTimeoutMs: number;
  private generation = 0;
  /**
   * Crashes since the last stable run; drives the restart backoff. Reset only
   * by `stableTimer`, never by `ready`: a Server that reaches the handshake and
   * then dies on its first request is still part of the same crash streak.
   */
  private restartAttempts = 0;
  private restartTimer: ReturnType<typeof setTimeout> | undefined;
  private stableTimer: ReturnType<typeof setTimeout> | undefined;
  private startTimer: ReturnType<typeof setTimeout> | undefined;
  private startWaiter:
    | { resolve: () => void; reject: (error: Error) => void }
    | undefined;
  private closedWaiter: (() => void) | undefined;
  private closedTimer: ReturnType<typeof setTimeout> | undefined;
  private drainedWaiter: (() => void) | undefined;
  private closePromise: Promise<void> | undefined;
  /** The `switchTo` in flight, if any; only one at a time. */
  private switching: SwitchOperation | undefined;
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
    this.restartBackoffMs = restartBackoffMs;
    // Defaults must exceed the backoff cap (5000ms) and the longest request,
    // otherwise a Server that dies on its first request would reset the streak
    // before it crashed and restart with zero backoff every time.
    this.stableResetMs =
      options.stableResetMs ?? Math.max(30_000, options.timeoutMs);
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
  }

  /** Spawns the Server, sends `hello` and resolves once the Server is `ready`. */
  async start(): Promise<void> {
    if (this.status !== 'idle') {
      throw new Error(`ipc executor cannot start while ${this.status}`);
    }
    this.status = 'starting';
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
    const attempts = this.restartAttempts;
    this.clearRestartTimer();
    this.clearStableTimer();
    this.status = 'failed';
    this.logger.error(
      { timeoutMs: this.startTimeoutMs, attempts },
      'server did not become ready before the start timeout',
    );
    // A Server that is up but never handshook has no owner any more; leaving it
    // running would be an orphan writer (INV-002).
    const server = this.server;
    if (server !== undefined) {
      this.server = undefined;
      this.abandonServer(server);
    }
    this.failAllPending(
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
  private get isTerminal(): boolean {
    return (
      this.status === 'closing' ||
      this.status === 'closed' ||
      this.status === 'failed'
    );
  }

  execute(request: ExecutorRequest): Promise<ExecutorOutcome> {
    if (this.status === 'idle') {
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
    if (this.status === 'failed') {
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
    if (this.isTerminal) {
      // `failed` is included here but unreachable: the branch above returns for
      // it first, with a message specific to a start that never succeeded.
      // warn, not error: an orderly shutdown refusal is expected, and the
      // connection layer records the failed outcome at error level already.
      this.logger.warn(
        {
          ...requestFields(request),
          code: FRAMEWORK_SHUTDOWN,
          reason: `executor ${this.status}`,
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
    this.armTimer(pending);
    try {
      const server = this.server;
      if (
        this.status !== 'switching' &&
        server !== undefined &&
        server.ready &&
        !server.closed
      ) {
        // Nothing older can be undelivered while the Server is ready: every
        // flush completes or leaves no Server. Sending only this request keeps
        // the hot path O(1) instead of scanning the buffer per request.
        this.deliver(server, pending);
      } else {
        // Queued until a Server is ready; a no-op otherwise. During a switch
        // this is the whole policy: the request waits for the new generation
        // rather than reaching a process that is about to be retired.
        this.flush();
      }
    } catch (error) {
      // Delivery is defensive against a throwing channel or callback. The
      // promise must settle regardless.
      this.settle(pending, failure(INTERNAL_ERROR, messageOf(error)));
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
  switchTo(options: {
    spawn: () => SpawnedServer;
    codeVersion?: string;
  }): Promise<void> {
    if (this.switching !== undefined) {
      return Promise.reject(
        new Error('an ipc executor switch is already in progress'),
      );
    }
    if (this.status !== 'ready') {
      return Promise.reject(
        new Error(`ipc executor cannot switch while ${this.status}`),
      );
    }
    if (
      options.codeVersion !== undefined &&
      (typeof options.codeVersion !== 'string' ||
        options.codeVersion.length === 0)
    ) {
      return Promise.reject(
        new Error('codeVersion must be a non-empty string'),
      );
    }
    const deferred = Promise.withResolvers<void>();
    const completion = Promise.withResolvers<void>();
    const abortSignal = Promise.withResolvers<void>();
    const op: SwitchOperation = {
      spawn: options.spawn,
      targetCodeVersion: options.codeVersion,
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
      settled: false,
      phase: 'preparing',
      staged: undefined,
      previous: this.server,
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
      versionWarned: false,
    };
    this.switching = op;
    this.status = 'switching';
    this.logger.info(
      {
        fromCodeVersion: this.codeVersion,
        toCodeVersion: options.codeVersion,
      },
      'server switch started',
    );
    void this.runSwitch(op);
    return deferred.promise;
  }

  /** The switch sequence; always settles `op` and clears `this.switching`. */
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
      this.finishSwitch(op);
    }
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
    const server = this.buildServer(spawned);
    server.staged = true;
    op.staged = server;
    this.attachServer(server);
    try {
      server.channel.send({
        type: 'hello',
        portalGeneration: this.portalGeneration,
      });
    } catch (error) {
      op.stagedLost = new Error(`hello failed: ${messageOf(error)}`);
      op.failure = op.stagedLost;
      return false;
    }
    return this.awaitBarrier(op, op.prepared, 'prepared', this.startTimeoutMs);
  }

  /**
   * Stops the current Server and waits until it is known to be gone. Nothing is
   * activated until this resolves true: `closed` says the Server finished
   * writing, and the exit wait is what says the storage is released (INV-002).
   */
  private async retirePrevious(op: SwitchOperation): Promise<boolean> {
    const previous = op.previous;
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
          timeoutMs: this.shutdownTimeoutMs,
        });
      } catch (error) {
        // The process died between the switch and this send; the channel close
        // handler reports the same thing.
        this.logger.warn(
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
        this.logger.warn(
          {
            pid: previous.pid,
            timeoutMs: this.shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS,
            ready: previous.ready,
          },
          'server did not finish shutting down; killing it',
        );
        this.killQuietly(previous);
      }
    }
    let exited = await this.waitForExit(previous);
    if (!exited) {
      this.logger.warn(
        { pid: previous.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
        'server process did not exit; killing it before switching',
      );
      this.killQuietly(previous);
      exited = await this.waitForExit(previous);
    }
    this.closeChannelQuietly(previous, 'portal is switching servers');
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
    this.logger.info({ pid: previous.pid }, 'previous server retired');
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
    try {
      staged.channel.send({ type: 'activate' });
      op.activated = true;
    } catch (error) {
      op.stagedLost = new Error(`activate failed: ${messageOf(error)}`);
      op.failure = op.stagedLost;
      return;
    }
    const ready = await this.awaitBarrier(
      op,
      op.ready,
      'become ready',
      this.startTimeoutMs,
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
    staged.staged = false;
    op.promoted = true;
    this.server = staged;
    this.status = 'ready';
    // A crash after this point restarts the code that is now serving, and new
    // requests carry its version. A switch that declares no target keeps the
    // version it had, matching the expected version used for the staged
    // process. A rollback keeps the old values because this only runs after the
    // promotion succeeded (SPEC 6.4).
    this.spawn = op.spawn;
    this.codeVersion = op.targetCodeVersion ?? this.codeVersion;
    this.clearRestartTimer();
    this.startStableTimer();
    if (staged.declaredMaxDurationMs !== undefined) {
      this.adoptCeiling(staged.declaredMaxDurationMs);
    }
    this.logger.info(
      { pid: staged.pid, codeVersion: staged.reportedCodeVersion },
      'replacement server ready',
    );
    this.flush();
    if (op.promotedLost) {
      // The first flush killed the promoted process; `onServerLost` already
      // took the crash path (restart scheduled), so this is not a completed
      // switch and `finishSwitch` rejects it.
      return;
    }
    this.logger.info(
      { pid: staged.pid, codeVersion: staged.reportedCodeVersion },
      'server switch completed',
    );
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
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => {
        resolve('timeout');
      }, timeoutMs);
    });
    const raced = Promise.race([
      barrier.promise.then(() => 'barrier' as const),
      op.abortSignal.then(() => 'aborted' as const),
      staged.closedSignal.promise.then(() => 'lost' as const),
    ]);
    const result = await Promise.race([raced, timeout]);
    if (timer !== undefined) {
      clearTimeout(timer);
    }
    if (result === 'barrier') {
      return true;
    }
    if (result === 'timeout') {
      this.logger.warn(
        { pid: staged.pid, timeoutMs, expected: what },
        'replacement server did not reach its barrier before the switch timeout',
      );
      op.failure ??= new Error(
        `replacement did not ${what} within ${timeoutMs}ms`,
      );
      return false;
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
    const waitMs = this.shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => {
        resolve('timeout');
      }, waitMs);
    });
    const raced = Promise.race([
      previous.closedSignal.promise.then(() => 'closed' as const),
      op.abortSignal.then(() => 'aborted' as const),
    ]);
    const result = await Promise.race([raced, timeout]);
    if (timer !== undefined) {
      clearTimeout(timer);
    }
    return result === 'closed';
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
    if (this.switching === op) {
      this.switching = undefined;
    }
    if (op.promoted) {
      op.settled = true;
      if (op.promotedLost) {
        // Promotion succeeded, but the process died before the switch could
        // report it. `onServerLost` already owns the recovery (crash path:
        // restart with the promoted spawn), so no rollback runs here — the
        // switch only refuses to call this a success.
        op.reject(
          op.abortError ??
            op.stagedLost ??
            op.failure ??
            new Error('the promoted replacement server was lost'),
        );
      } else {
        op.resolve();
      }
      op.markComplete();
      return;
    }
    if (!op.settled) {
      op.settled = true;
      op.reject(
        op.abortError ??
          op.failure ??
          new Error('ipc executor switch ended unexpectedly'),
      );
    }
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
        this.recordAbandonedForClose(abandoned);
      }
      op.markComplete();
      return;
    }
    const reason = op.failure === undefined ? 'unknown' : messageOf(op.failure);
    if (op.previousGone || op.previousLost) {
      this.logger.warn(
        {
          reason,
          phase: op.phase,
          codeVersion: this.codeVersion,
          pendingExits: op.exitsPending.length,
        },
        op.previousLost
          ? 'server switch failed because the previous server was lost; restarting the previous version'
          : 'server switch failed after the previous server retired; restarting the previous version',
      );
      this.rollbackToPrevious(op.exitsPending);
    } else {
      this.logger.warn(
        { reason, phase: op.phase, pid: this.currentPid() },
        'server switch failed; the previous server keeps serving',
      );
      if (!this.isTerminal) {
        this.status = 'ready';
        this.flush();
      }
    }
    op.markComplete();
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

  /**
   * Hands a process to `doClose`, which confirms its exit before returning.
   * A `close()`-abandoned replacement that received `activate` may hold storage;
   * a process a rollback is waiting on has the same problem, because that wait
   * is a floating promise `close()` cannot join.
   */
  private recordAbandonedForClose(server: ServerState): void {
    if (!this.abandonedForClose.includes(server)) {
      this.abandonedForClose.push(server);
    }
  }

  /** Drops a process from the close list once its exit was confirmed. */
  private forgetAbandonedForClose(server: ServerState): void {
    const index = this.abandonedForClose.indexOf(server);
    if (index !== -1) {
      this.abandonedForClose.splice(index, 1);
    }
  }

  /** Closes and kills a staged process the switch no longer needs. */
  private abandonStaged(server: ServerState): void {
    server.staged = false;
    if (server.closed) {
      return;
    }
    server.closed = true;
    server.closedSignal.resolve();
    if (!server.exited) {
      this.killQuietly(server);
    }
    this.closeChannelQuietly(server, 'switch abandoned');
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
    if (this.isTerminal) {
      return;
    }
    this.status = 'starting';
    const backoffMs = this.nextBackoffMs();
    if (pendingExits.length === 0) {
      this.scheduleRestart(backoffMs);
      return;
    }
    // `close()` cannot join the wait below (it is a floating promise), so it
    // must confirm these exits itself. Without this a close landing in the
    // window between the rollback starting and the exit being observed would
    // return while the process could still hold storage (INV-002).
    for (const server of pendingExits) {
      this.recordAbandonedForClose(server);
    }
    void Promise.all(
      pendingExits.map((server) => this.waitForExit(server)),
    ).then(() => {
      for (const server of pendingExits) {
        this.forgetAbandonedForClose(server);
      }
      this.scheduleRestart(backoffMs);
      return undefined;
    });
  }

  private killQuietly(server: ServerState): void {
    try {
      server.kill();
    } catch (error) {
      this.logger.warn(
        { pid: server.pid, reason: messageOf(error) },
        'server could not be killed',
      );
    }
  }

  private closeChannelQuietly(server: ServerState, reason: string): void {
    try {
      server.channel.close(reason);
    } catch (error) {
      this.logger.debug(
        { reason: messageOf(error) },
        'ipc channel close failed',
      );
    }
  }

  private currentPid(): number | undefined {
    return this.server?.pid;
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
    this.cancelOnServer(pending, code);
    this.settle(pending, failure(code, `request ${requestId} was cancelled`));
    return Promise.resolve();
  }

  private async doClose(): Promise<void> {
    this.status = 'closing';
    this.clearRestartTimer();
    this.clearStableTimer();
    this.clearStartTimer();
    // A switch in flight must not keep spawning, activating or restarting: it is
    // interrupted here and its staged process abandoned, so `close()` ends with
    // at most one Server to shut down and no orphan writer (INV-002).
    await this.abortSwitchForClose();
    const server = this.server;
    // A Request that never reached a live Server cannot complete, so it fails
    // now instead of burning its whole deadline. Delivered ones are given
    // until their deadline to settle (SPEC 6.3 step 3).
    // Deleting the current entry during iteration is safe for a Map, and
    // settling only resolves a promise (the continuation runs as a microtask),
    // so this cannot mutate the map mid-loop.
    for (const pending of this.buffer.values()) {
      if (
        server === undefined ||
        server.closed ||
        pending.sentOn === undefined
      ) {
        // Per-request line: this is the only trace of why the request ended,
        // since no Server result or deadline will ever reach it.
        this.logger.warn(
          {
            ...requestFields(pending.request),
            code: FRAMEWORK_SHUTDOWN,
            reason: 'executor closing before delivery',
            durationMs: Date.now() - pending.startedAt,
          },
          'pending request failed',
        );
        this.settle(
          pending,
          failure(FRAMEWORK_SHUTDOWN, 'ipc executor is closing'),
        );
      }
    }
    // Bounded by the same budget as the rest of the shutdown. An unbounded wait
    // here used to mean "as long as the longest request deadline", so a
    // ConnectionLayer with a shorter shutdownTimeout would return while this
    // executor was still waiting and the Server was still writing (the two
    // timeouts were unrelated). Past the budget the requests are failed and the
    // Server is shut down anyway.
    const drained = await settleWithin(
      this.awaitPendingSettled(),
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
      this.failAllPending(
        FRAMEWORK_SHUTDOWN,
        'ipc executor closed before the request settled',
        'shutdown drain timed out',
      );
    }
    await this.shutdownServer();
    await this.confirmAbandonedExits();
    this.failAllPending(
      FRAMEWORK_SHUTDOWN,
      'ipc executor closed',
      'executor closed',
    );
    this.status = 'closed';
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
        const exited = await this.waitForExit(server);
        if (exited) {
          return;
        }
        this.logger.warn(
          { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
          'abandoned replacement did not exit; killing it before close returns',
        );
        this.killQuietly(server);
        const afterKill = await this.waitForExit(server);
        if (!afterKill) {
          this.logger.warn(
            { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
            'abandoned replacement is still running after a kill; close cannot confirm its exit',
          );
        }
      }),
    );
  }

  /**
   * Interrupts a `switchTo` in flight and waits (bounded) for it to release its
   * staged process. `close()` owns the old Server from here, so the switch only
   * abandons what it spawned and never restarts or promotes anything.
   */
  private async abortSwitchForClose(): Promise<void> {
    const op = this.switching;
    if (op === undefined) {
      return;
    }
    op.abort(new Error('ipc executor is closing: server switch aborted'), true);
    const done = await settleWithin(op.completion, this.shutdownTimeoutMs);
    if (done.status === 'timeout') {
      // The switch's own awaits are bounded by `startTimeoutMs`; a completion
      // that outlives the shutdown budget means a process it staged is still
      // around. Kill it directly so `close()` does not leave an orphan, and
      // hand it to `doClose` when it may hold storage.
      const staged = op.staged;
      if (staged !== undefined && !staged.closed) {
        staged.staged = false;
        staged.closed = true;
        staged.closedSignal.resolve();
        if (!staged.exited) {
          this.killQuietly(staged);
        }
        this.closeChannelQuietly(staged, 'switch aborted by close');
        if (op.activated) {
          this.recordAbandonedForClose(staged);
        }
      }
      this.logger.warn(
        { timeoutMs: this.shutdownTimeoutMs },
        'server switch did not finish before the shutdown timeout',
      );
    }
  }

  private async shutdownServer(): Promise<void> {
    const server = this.server;
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
        await this.waitForClosed(server);
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
    const exited = await this.waitForExit(server);
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
      await this.waitForExit(server);
    }
    try {
      server.channel.close('portal is shutting down');
    } catch (error) {
      this.logger.debug(
        { reason: messageOf(error) },
        'ipc channel close failed',
      );
    }
    if (this.server === server) {
      this.server = undefined;
    }
  }

  private waitForClosed(server: ServerState): Promise<void> {
    if (server.closed || server.shutdownAcknowledged) {
      return Promise.resolve();
    }
    // The declared grace plus the slack: the Server spends the grace starting
    // one IPC hop after this timer does, so waiting exactly the grace would
    // always give up first on a Server that used its whole budget.
    const waitMs = this.shutdownTimeoutMs + SHUTDOWN_ACK_SLACK_MS;
    return new Promise<void>((resolve) => {
      this.closedWaiter = resolve;
      this.closedTimer = setTimeout(() => {
        this.resolveClosed();
      }, waitMs);
      // The exit may have landed between the check above and this assignment,
      // in which case its `resolveClosed` found no waiter. Re-check so the
      // grace period is not paid for a process that is already gone.
      if (server.closed || server.shutdownAcknowledged) {
        this.resolveClosed();
      }
    });
  }

  /**
   * Waits until the child process is known to be gone, bounded so an
   * uninterruptible process cannot stall shutdown or restart forever. Resolves
   * true when the exit was observed, false when the wait gave up: the caller
   * decides whether that is fatal (the close path kills and retries, the restart
   * path logs and moves on).
   *
   * The exit info is read here rather than from `server.exited`, because that
   * flag is set by the launch continuation and the two awaits are not ordered
   * against each other.
   */
  private async waitForExit(server: ServerState): Promise<boolean> {
    if (server.exited) {
      return true;
    }
    let timer: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<'timeout'>((resolve) => {
      timer = setTimeout(() => {
        resolve('timeout');
      }, EXIT_WAIT_TIMEOUT_MS);
    });
    const info = await Promise.race([
      server.exitPromise.catch((): ServerExitInfo => ({
        code: null,
        signal: null,
        observed: false,
      })),
      timeout,
    ]);
    if (timer !== undefined) {
      clearTimeout(timer);
    }
    if (info === 'timeout' || !info.observed) {
      this.logger.warn(
        { pid: server.pid, timeoutMs: EXIT_WAIT_TIMEOUT_MS },
        'server process did not exit within the wait; it may still be running',
      );
      return false;
    }
    server.exited = true;
    return true;
  }

  private resolveClosed(): void {
    if (this.closedTimer !== undefined) {
      clearTimeout(this.closedTimer);
      this.closedTimer = undefined;
    }
    const waiter = this.closedWaiter;
    this.closedWaiter = undefined;
    waiter?.();
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

  private awaitPendingSettled(): Promise<void> {
    if (this.buffer.isEmpty) {
      return Promise.resolve();
    }
    return new Promise<void>((resolve) => {
      this.drainedWaiter = resolve;
    });
  }

  private launch(): void {
    if (this.isTerminal) {
      return;
    }
    let spawned: SpawnedServer;
    try {
      spawned = this.spawn();
    } catch (error) {
      // A failed spawn is another crash: keep buffering and back off.
      const backoffMs = this.nextBackoffMs();
      this.logger.error(
        { reason: messageOf(error), backoffMs },
        'server spawn failed',
      );
      this.status = 'starting';
      this.scheduleRestart(backoffMs);
      return;
    }
    const server = this.buildServer(spawned);
    this.server = server;
    this.attachServer(server);
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

  /** Wraps a spawn result in the executor's own per-process bookkeeping. */
  private buildServer(spawned: SpawnedServer): ServerState {
    this.generation += 1;
    return {
      generation: this.generation,
      channel: spawned.channel,
      kill: () => {
        spawned.kill();
      },
      exitPromise: spawned.exited,
      lastExit: () => spawned.lastExit(),
      ready: false,
      closed: false,
      exited: false,
      shutdownAcknowledged: false,
      pid: undefined,
      staged: false,
      retiring: false,
      declaredMaxDurationMs: undefined,
      reportedCodeVersion: undefined,
      shutdownSent: false,
      closedSignal: Promise.withResolvers<void>(),
    };
  }

  /**
   * Installs the message, close and exit handlers on one process. Shared by the
   * ordinary start/restart path and by `switchTo`, so a staged process is
   * observed exactly like the current one.
   */
  private attachServer(server: ServerState): void {
    // Handlers before the handshake: a `ready` that beats the registration is
    // still delivered (the channel queues it), but registering first keeps the
    // real Bun channel from dropping an early message.
    server.channel.onMessage((raw) => {
      this.onServerMessage(server, raw);
    });
    server.channel.onClose((reason) => {
      // The exit event may already have happened: `exited` resolves on the same
      // tick, but its continuation is a microtask and this callback is
      // synchronous, so the promise is not readable yet. The synchronous view
      // is what keeps the real exit code in the crash line instead of a null.
      const recorded = server.lastExit();
      if (recorded?.observed === true) {
        server.exited = true;
        this.onServerLost(server, recorded.code, recorded.signal, reason);
        return;
      }
      this.onServerLost(server, null, null, reason);
    });
    // Both signals mean "this process is gone" and race each other; the
    // `closed` flag inside `onServerLost` makes the handling run once.
    void (async () => {
      // `exited` resolves with `observed: false` rather than rejecting, so a
      // failed waitpid still reaches the same handling path. The catch is for a
      // spawn implementation that rejects instead.
      const info = await server.exitPromise.catch((): ServerExitInfo => ({
        code: null,
        signal: null,
        observed: false,
      }));
      // `exited` means "known to be gone". An unobserved exit must leave it
      // false so `onServerLost` still takes the defensive kill path: claiming
      // it exited would skip the only kill and start a second writer beside a
      // process that may still be running (INV-002).
      if (info.observed) {
        server.exited = true;
      }
      this.onServerLost(
        server,
        info.code,
        info.signal,
        info.observed
          ? 'server process exited'
          : 'server exit could not be observed',
      );
    })();
  }

  private onServerMessage(server: ServerState, raw: unknown): void {
    const parsed = parseIpcMessage(raw);
    if (!parsed.ok) {
      this.logger.warn(
        { pid: server.pid, reason: parsed.reason },
        'ipc message rejected',
      );
      return;
    }
    const message = parsed.message;
    switch (message.type) {
      case 'prepared':
        this.onPrepared(server, message.pid, message.codeVersion);
        return;
      case 'ready':
        this.onReady(
          server,
          message.pid,
          message.capabilities.maxTransactionDurationMs,
          message.codeVersion,
        );
        return;
      case 'result':
        this.onResult(message);
        return;
      case 'closed':
        this.onClosed(server, message.reason);
        return;
      case 'hello':
      case 'request':
      case 'cancel':
      case 'shutdown':
      case 'activate':
        // Valid shape, wrong direction: a Server must not send these.
        this.logger.warn(
          { pid: server.pid, reason: `unexpected "${message.type}" message` },
          'ipc message rejected',
        );
    }
  }

  private onClosed(server: ServerState, reason: string): void {
    server.closedSignal.resolve();
    if (server.retiring) {
      // The previous process finished its graceful close; the switch waits on
      // its own signal for this before it may activate the replacement
      // (INV-002). A `close()` that took over may also be waiting, so wake it.
      server.shutdownAcknowledged = true;
      this.resolveClosed();
      return;
    }
    if (this.status === 'closing') {
      server.shutdownAcknowledged = true;
      this.resolveClosed();
      return;
    }
    if (this.switching?.staged === server) {
      // A prepared process has no storage to close, so this is not a shutdown
      // ack; the exit that follows is what the switch reacts to.
      this.logger.warn(
        { pid: server.pid, reason },
        'replacement server closed before activation',
      );
      return;
    }
    // Unsolicited: the process exit that follows is what actually matters,
    // and it arrives through `exited`/`onClose`. Not a shutdown ack, so
    // close() must still be able to kill a process that stays alive.
    this.logger.warn(
      { pid: server.pid, reason },
      'server closed without being asked',
    );
  }

  /**
   * The Server accepted the handshake and is waiting for activation (ADR-0007):
   * it holds no storage yet, so the only thing that can happen now is opening
   * it. Sending `activate` at once is the ordinary-start path; a `switchTo`
   * owns its staged process instead and activates it only after the previous
   * one has exited.
   */
  private onPrepared(
    server: ServerState,
    pid: number,
    codeVersion: string | undefined,
  ): void {
    const op = this.switching;
    if (op?.staged === server) {
      server.pid = pid;
      server.reportedCodeVersion = codeVersion;
      this.warnOnVersionMismatch(
        pid,
        op.targetCodeVersion ?? this.codeVersion,
        codeVersion,
        op,
      );
      op.prepared.seen = true;
      op.prepared.resolve();
      return;
    }
    if (this.server !== server || server.closed) {
      this.logger.debug(
        { pid },
        'prepared from a server generation already given up',
      );
      return;
    }
    if (server.ready) {
      // Out of order: this generation already opened its storage.
      this.logger.warn({ pid }, 'prepared received after ready; ignored');
      return;
    }
    if (this.isTerminal) {
      // Nothing may open storage while the Portal is closing: there would be
      // nowhere to drain it. The shutdown path kills this process.
      this.logger.debug({ pid }, 'prepared ignored while closing');
      return;
    }
    server.pid = pid;
    server.reportedCodeVersion = codeVersion;
    try {
      server.channel.send({ type: 'activate' });
    } catch (error) {
      this.onServerLost(
        server,
        null,
        null,
        `activate failed: ${messageOf(error)}`,
      );
      return;
    }
    this.logger.debug({ pid }, 'server prepared; activating');
  }

  private onReady(
    server: ServerState,
    pid: number,
    declaredMaxDurationMs: number,
    actualCodeVersion: string | undefined,
  ): void {
    const op = this.switching;
    if (op?.staged === server) {
      server.ready = true;
      server.pid = pid;
      server.declaredMaxDurationMs = declaredMaxDurationMs;
      server.reportedCodeVersion = actualCodeVersion;
      this.warnOnVersionMismatch(
        pid,
        op.targetCodeVersion ?? this.codeVersion,
        actualCodeVersion,
        op,
      );
      op.ready.seen = true;
      op.ready.resolve();
      return;
    }
    if (this.server !== server || server.closed) {
      this.logger.debug(
        { pid },
        'ready from a server generation already given up',
      );
      return;
    }
    if (server.ready) {
      this.logger.debug({ pid }, 'duplicate ready ignored');
      return;
    }
    server.ready = true;
    server.pid = pid;
    server.declaredMaxDurationMs = declaredMaxDurationMs;
    server.reportedCodeVersion = actualCodeVersion;
    // Reported, not enforced: a Server built from different code may still be
    // the only process available, and refusing it would turn a deployment
    // mismatch into an outage (SPEC 6.4). A deployment that declared no version
    // says nothing, so silence is not a mismatch.
    this.warnOnVersionMismatch(pid, this.codeVersion, actualCodeVersion);
    if (this.isTerminal) {
      return;
    }
    this.status = 'ready';
    // The crash streak ends only after a stable run, not at the handshake: a
    // Server that reaches `ready` and then dies on its first request would
    // otherwise reset the backoff and restart immediately, forever.
    this.startStableTimer();
    this.adoptCeiling(declaredMaxDurationMs);
    this.logger.info(
      { pid, effectiveTimeoutMs: this.effectiveTimeoutMs },
      'server ready',
    );
    this.resolveStart();
    this.flush();
  }

  /**
   * Warns when a Server reports a code version other than the one expected
   * (SPEC 6.4). A missing value on either side says nothing, so silence is not
   * a mismatch. `op` records the warning so a switch does not log the same
   * mismatch for both `prepared` and `ready`.
   */
  private warnOnVersionMismatch(
    pid: number,
    expected: string | undefined,
    actual: string | undefined,
    op?: SwitchOperation,
  ): void {
    if (expected === undefined || actual === undefined || actual === expected) {
      return;
    }
    if (op !== undefined) {
      if (op.versionWarned) {
        return;
      }
      op.versionWarned = true;
    }
    this.logger.warn(
      { pid, expectedCodeVersion: expected, actualCodeVersion: actual },
      'server code version differs from the portal version',
    );
  }

  /**
   * Applies the backend's declared request ceiling to the buffered requests.
   * Only ever tightens: a later Server declaring a longer ceiling must not
   * extend a deadline the Portal already promised, or a crash loop could keep a
   * request alive past its original ceiling (SPEC 3.4).
   */
  private adoptCeiling(declaredMaxDurationMs: number): void {
    const declared = effectiveTimeoutMs(
      this.portalTimeoutMs,
      declaredMaxDurationMs,
    );
    this.effectiveTimeoutMs = declared;
    for (const pending of this.buffer.values()) {
      const tightened = Math.min(pending.effectiveTimeoutMs, declared);
      if (tightened === pending.effectiveTimeoutMs) {
        continue;
      }
      pending.effectiveTimeoutMs = tightened;
      pending.deadlineAt = pending.startedAt + tightened;
      this.armTimer(pending);
    }
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
    this.settle(pending, outcome);
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
    const op = this.switching;
    if (op?.staged === server && !op.promoted) {
      // The replacement died while the switch was waiting on it. Record the
      // loss; the switch's own await reacts to `closedSignal` and reports it.
      op.stagedLost ??= new Error(`replacement server lost: ${reason}`);
      if (!server.exited) {
        this.killQuietly(server);
      }
      return;
    }
    if (op?.staged === server) {
      // Promoted, but the switch has not settled yet: this is an ordinary crash
      // of the current Server, handled by the crash path below (clear
      // `this.server`, wait for the exit, schedule the restart). The switch must
      // not report success for a process that is already gone.
      op.promotedLost = true;
      op.stagedLost ??= new Error(
        `replacement server lost after promotion: ${reason}`,
      );
      op.failure ??= op.stagedLost;
    }
    if (op?.previous === server) {
      // The old process died during its own retirement. That is the expected
      // exit, not a crash: do not restart beside it or log a crash. It is not
      // "lost" either — the switch asked it to stop.
      if (server.retiring || op.previousGone) {
        op.previousGone = true;
        if (!server.exited) {
          this.killQuietly(server);
          this.recordPendingExit(op, server);
        }
        return;
      }
      // It crashed before the switch retired it. The switch has no previous
      // Server to fall back to, so it fails and the crash path restarts.
      op.previousLost = true;
      op.previousGone = true;
      if (this.server === server) {
        this.server = undefined;
      }
      this.resolveClosed();
      if (!server.exited) {
        this.killQuietly(server);
        this.recordPendingExit(op, server);
      }
      op.abort(
        new Error(`previous server lost during the switch: ${reason}`),
        false,
      );
      return;
    }
    // A stable clock from the dead generation must not end the crash streak.
    this.clearStableTimer();
    if (server.retiring) {
      // A switch retired this process but had already settled (promoted or
      // rolled back) before its exit landed. Its exit is expected: no crash
      // line and no restart beside the current Server.
      this.resolveClosed();
      return;
    }
    if (this.server === server) {
      this.server = undefined;
    }
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
      this.killQuietly(server);
    }
    if (this.isTerminal) {
      // SPEC 6.3: no restart while shutting down; nothing can complete these.
      this.failAllPending(
        FRAMEWORK_SHUTDOWN,
        'server process exited during shutdown',
        'server lost during shutdown',
      );
      return;
    }
    const backoffMs = this.nextBackoffMs();
    this.logger.error(
      { pid: server.pid, exitCode: code, signal, backoffMs, reason },
      'server process lost',
    );
    this.status = 'starting';
    // Wait for the exit before restarting even when the kill was issued: the
    // replacement must not open storage while the old process still holds it.
    void this.waitForExit(server).then(() => {
      this.scheduleRestart(backoffMs);
      return undefined;
    });
  }

  private scheduleRestart(backoffMs: number): void {
    if (this.isTerminal) {
      return;
    }
    this.restartTimer = setTimeout(() => {
      this.restartTimer = undefined;
      this.launch();
    }, backoffMs);
  }

  private clearRestartTimer(): void {
    if (this.restartTimer === undefined) {
      return;
    }
    clearTimeout(this.restartTimer);
    this.restartTimer = undefined;
  }

  /**
   * Restarts the stability clock for the current Server. Only its expiry ends
   * the crash streak; `ready` alone does not, because a Server can reach the
   * handshake and still die on its first request.
   */
  private startStableTimer(): void {
    this.clearStableTimer();
    this.stableTimer = setTimeout(() => {
      this.stableTimer = undefined;
      this.restartAttempts = 0;
    }, this.stableResetMs);
  }

  private clearStableTimer(): void {
    if (this.stableTimer === undefined) {
      return;
    }
    clearTimeout(this.stableTimer);
    this.stableTimer = undefined;
  }

  /**
   * Exponential backoff, but the first restart of a crash streak is immediate:
   * a one-off crash should not cost the player 100ms of latency. The streak
   * only ends once a Server has been up for `stableResetMs`, so an immediate
   * restart cannot repeat indefinitely.
   */
  private nextBackoffMs(): number {
    const attempt = this.restartAttempts;
    this.restartAttempts += 1;
    if (attempt === 0) {
      return 0;
    }
    return Math.min(
      this.restartBackoffMs * 2 ** (attempt - 1),
      MAX_RESTART_BACKOFF_MS,
    );
  }

  /**
   * Delivers everything `undelivered` reports for the current generation, in
   * the order it reports them (SPEC 6.5).
   *
   * A request that keeps killing the process starves the later requests on its
   * connection: every flush stops at it (its replay budget runs out, and the
   * flush returns), so those requests wait until it reaches a terminal state.
   * That is the necessary cost of the SPEC 6.5 ordering guarantee, not a drop.
   */
  private flush(): void {
    if (this.status === 'switching') {
      // The current Server is about to be retired; delivering now would send a
      // request to a process the switch is draining. It stays buffered until
      // the replacement is promoted and this method runs again.
      return;
    }
    const server = this.server;
    if (server === undefined || !server.ready) {
      return;
    }
    if (this.isTerminal) {
      return;
    }
    for (const pending of this.buffer.undelivered(server.generation)) {
      if (!this.deliver(server, pending)) {
        return;
      }
    }
  }

  /** Returns false when the channel died and delivery must stop. */
  private deliver(server: ServerState, pending: PendingRequest): boolean {
    if (this.buffer.get(pending.request.requestId) !== pending) {
      return true;
    }
    const isReplay = pending.sentOn !== undefined;
    // The count is only committed after a successful send: a send that dies on
    // a closing channel did not reach any Server and must not burn budget.
    const replayCount = isReplay ? pending.replayCount + 1 : 0;
    if (replayCount > this.maxReplays) {
      // SPEC 6.5: a request that keeps killing the process must stop being
      // retried, otherwise one bad input becomes a crash loop.
      this.logger.error(
        {
          ...requestFields(pending.request),
          replayCount: pending.replayCount,
          code: FRAMEWORK_REPLAY_EXHAUSTED,
        },
        'request replay exhausted',
      );
      this.settle(
        pending,
        failure(
          FRAMEWORK_REPLAY_EXHAUSTED,
          `request ${pending.request.requestId} exceeded ${this.maxReplays} replays`,
        ),
      );
      return true;
    }
    try {
      server.channel.send(
        requestMessage(
          pending.request,
          Math.max(1, pending.deadlineAt - Date.now()),
          replayCount,
        ),
      );
    } catch (error) {
      this.onServerLost(server, null, null, `send failed: ${messageOf(error)}`);
      return false;
    }
    pending.replayCount = replayCount;
    pending.sentOn = server.generation;
    if (isReplay) {
      this.logger.warn(
        {
          ...requestFields(pending.request),
          replayCount,
          durationMs: Date.now() - pending.startedAt,
        },
        'request replayed',
      );
    }
    return true;
  }

  private armTimer(pending: PendingRequest): void {
    if (pending.timer !== undefined) {
      clearTimeout(pending.timer);
    }
    const remaining = Math.max(0, pending.deadlineAt - Date.now());
    pending.timer = setTimeout(() => {
      this.onDeadline(pending);
    }, remaining);
  }

  private onDeadline(pending: PendingRequest): void {
    if (this.buffer.get(pending.request.requestId) !== pending) {
      return;
    }
    pending.timer = undefined;
    // warn, not error: the connection layer records the failed outcome at error
    // level, and one request failure should not produce two error lines.
    this.logger.warn(
      {
        ...requestFields(pending.request),
        code: FRAMEWORK_TIMEOUT,
        durationMs: Date.now() - pending.startedAt,
      },
      'request timed out',
    );
    // The Server may still be working on it; tell it to stop. Best effort: the
    // terminal state is decided here, and a result that arrives later is
    // discarded by requestId.
    this.cancelOnServer(pending, FRAMEWORK_TIMEOUT);
    this.settle(
      pending,
      failure(
        FRAMEWORK_TIMEOUT,
        `request ${pending.request.requestId} timed out after ${pending.effectiveTimeoutMs}ms`,
      ),
    );
  }

  private cancelOnServer(pending: PendingRequest, code: string): void {
    const server = this.server;
    if (
      server === undefined ||
      !server.ready ||
      pending.sentOn !== server.generation
    ) {
      return;
    }
    try {
      server.channel.send({
        type: 'cancel',
        requestId: pending.request.requestId,
        code,
      });
    } catch (error) {
      this.logger.warn(
        {
          ...requestFields(pending.request),
          code,
          reason: messageOf(error),
        },
        'cancel could not be sent',
      );
    }
  }

  private settle(pending: PendingRequest, outcome: ExecutorOutcome): void {
    if (this.buffer.get(pending.request.requestId) !== pending) {
      return;
    }
    this.buffer.take(pending.request.requestId);
    if (pending.timer !== undefined) {
      clearTimeout(pending.timer);
      pending.timer = undefined;
    }
    pending.resolve(outcome);
    if (this.buffer.isEmpty) {
      const drained = this.drainedWaiter;
      this.drainedWaiter = undefined;
      drained?.();
    }
  }

  /**
   * Settles every buffered request with one failure. `message` is the outcome
   * text the caller sees; `reason` is log-only and names the call site, because
   * several paths share a code and would otherwise look identical in the logs.
   */
  private failAllPending(code: string, message: string, reason: string): void {
    for (const pending of this.buffer.values()) {
      // warn, not error: the connection layer records the failed outcome at
      // error level, and one request failure should not produce two error lines.
      this.logger.warn(
        {
          ...requestFields(pending.request),
          code,
          reason,
          durationMs: Date.now() - pending.startedAt,
        },
        'pending request failed',
      );
      this.settle(pending, failure(code, message));
    }
  }
}

/**
 * Request-level log bindings (SPEC 7.1): the ids every request carries plus the
 * Session its connection already held on entry.
 *
 * `sessionId` is omitted, not written as `undefined` or an empty string, when
 * the connection holds no Session: absence is the contract for "unbound"
 * (SPEC 2, identity.ts).
 */
function requestFields(request: ExecutorRequest): LogFields {
  const fields: LogFields = {
    requestId: request.requestId,
    connectionId: request.connectionId,
    clientRequestId: request.clientRequestId,
  };
  if (request.sessionId !== undefined) {
    fields.sessionId = request.sessionId;
  }
  return fields;
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

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(messageOf(error));
}
