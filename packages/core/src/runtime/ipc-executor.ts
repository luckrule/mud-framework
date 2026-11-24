import { messageOf } from '../internal/guards.ts';
import type { IpcChannel } from '../ipc/channel.ts';
import type { RequestMessage, ResultMessage } from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import type { ServerExitInfo, SpawnedServer } from '../ipc/spawn-port.ts';
import type { Logger } from '../logging/types.ts';
import type {
  ExecutorOutcome,
  ExecutorRequest,
  RequestExecutor,
} from './executor.ts';
import { effectiveTimeoutMs, tightenTimeout } from './timeout.ts';
import {
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_REPLAY_EXHAUSTED,
  FRAMEWORK_SHUTDOWN,
  FRAMEWORK_TIMEOUT,
  INTERNAL_ERROR,
} from './wire-codes.ts';

const DEFAULT_MAX_REPLAYS = 3;
const DEFAULT_RESTART_BACKOFF_MS = 100;
const MAX_RESTART_BACKOFF_MS = 5000;
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 5000;
/**
 * Upper bound on how long a kill is awaited before the Portal moves on.
 * SIGKILL normally lands in milliseconds; a process stuck in uninterruptible
 * sleep would otherwise stall shutdown or restart forever.
 */
const EXIT_WAIT_TIMEOUT_MS = 2000;
/**
 * Upper bound on requests buffered but not yet acknowledged. Their lifetime is
 * bounded by the request deadline, so this is not a steady-state limit; it is a
 * brake for the case the deadline cannot cover: a Server that stays unavailable
 * while a client keeps writing, which would otherwise grow the buffer until the
 * Portal runs out of memory.
 */
const DEFAULT_MAX_PENDING = 10_000;

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
  logger?: Logger;
}

/**
 * `failed` is a terminal state that is not `closed`: the executor gave up
 * because the Server never became ready within `startTimeoutMs`. It is distinct
 * from `closed` so `execute` can report a different message and so a failed
 * start cannot be mistaken for an orderly shutdown.
 */
type Status = 'idle' | 'starting' | 'ready' | 'closing' | 'closed' | 'failed';

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
  readonly lastExit: (() => ServerExitInfo | undefined) | undefined;
  ready: boolean;
  closed: boolean;
  /** True once the child process itself is known to be gone. */
  exited: boolean;
  /** Set when the Server answered `shutdown` with `closed`. */
  shutdownAcknowledged: boolean;
  pid: number | undefined;
}

/**
 * A Request that has been accepted but not acknowledged (SPEC §2: only a
 * `result` acknowledges). It stays here across crashes and is removed only by a
 * result or by a Portal-decided terminal state.
 */
interface PendingRequest {
  readonly request: ExecutorRequest;
  /**
   * Ordering key for replay. Comes from one global counter, so it is strictly
   * ascending within any connection (all SPEC 6.5 needs) without a
   * per-connection map that would never shrink.
   */
  readonly perConnectionSeq: number;
  /** When the Portal accepted the request; the deadline is measured from here. */
  readonly startedAt: number;
  readonly resolve: (outcome: ExecutorOutcome) => void;
  replayCount: number;
  /** Server generation this request was last sent on; `undefined` = never sent. */
  sentOn: number | undefined;
  deadlineAt: number;
  effectiveTimeoutMs: number;
  timer: ReturnType<typeof setTimeout> | undefined;
}

/**
 * The Portal side of the process split (ADR-0002): ships Requests to a Server
 * process over IPC and keeps them buffered until the Server acknowledges them.
 *
 * The recovery barrier is the handshake: nothing is delivered before `ready`
 * (SPEC 6.2), so a Request that arrives while the Server is starting or being
 * restarted waits instead of failing. A crash does not fail in-flight Requests
 * either — they are replayed to the next process, in the order they were
 * received on their connection (SPEC 6.5), until the replay budget runs out.
 *
 * `execute` never throws and always settles by the request deadline, because
 * the connection layer advances a connection's ordered chain on settlement and
 * a hanging promise would stall every later request on that connection.
 */
export class IpcExecutor implements RequestExecutor {
  private readonly spawn: () => SpawnedServer;
  private readonly portalTimeoutMs: number;
  private readonly maxReplays: number;
  private readonly restartBackoffMs: number;
  private readonly stableResetMs: number;
  private readonly shutdownTimeoutMs: number;
  private readonly startTimeoutMs: number;
  private readonly maxPending: number;
  private readonly logger: Logger;
  private readonly portalGeneration: number;
  private readonly pending = new Map<string, PendingRequest>();
  private status: Status = 'idle';
  private server: ServerState | undefined;
  private effectiveTimeoutMs: number;
  private generation = 0;
  private seq = 0;
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
    const maxPending = options.maxPending ?? DEFAULT_MAX_PENDING;
    if (!Number.isInteger(maxPending) || maxPending < 1) {
      throw new Error(
        `maxPending must be a positive integer, got ${maxPending}`,
      );
    }
    this.maxPending = maxPending;
    // No concrete logger is imported here; a caller that wants observability
    // injects one, and the default keeps the module dependency-free.
    this.logger = options.logger ?? SILENT_LOGGER;
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
          requestId: request.requestId,
          connectionId: request.connectionId,
          clientRequestId: request.clientRequestId,
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
          requestId: request.requestId,
          connectionId: request.connectionId,
          clientRequestId: request.clientRequestId,
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
      return Promise.resolve(
        failure(FRAMEWORK_SHUTDOWN, 'ipc executor is shutting down'),
      );
    }
    if (this.pending.size >= this.maxPending) {
      // warn, not error: a full buffer is a refusal by policy, and the caller
      // gets a stable code rather than a request that waits out its deadline.
      this.logger.warn(
        {
          requestId: request.requestId,
          connectionId: request.connectionId,
          clientRequestId: request.clientRequestId,
          code: FRAMEWORK_QUEUE_FULL,
          pending: this.pending.size,
          limit: this.maxPending,
        },
        'request refused: pending buffer is full',
      );
      return Promise.resolve(
        failure(
          FRAMEWORK_QUEUE_FULL,
          `ipc executor pending buffer is full (${this.maxPending} unacknowledged)`,
        ),
      );
    }
    if (this.pending.has(request.requestId)) {
      // Request ids are minted by the connection layer and a replay goes
      // through the buffer, so a second execute() for the same id is a caller
      // bug. Failing it beats overwriting a promise nobody would ever settle.
      this.logger.warn(
        {
          requestId: request.requestId,
          connectionId: request.connectionId,
          clientRequestId: request.clientRequestId,
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
    const startedAt = Date.now();
    const pending: PendingRequest = {
      request,
      perConnectionSeq: this.nextSeq(),
      startedAt,
      resolve: deferred.resolve,
      replayCount: 0,
      sentOn: undefined,
      // A caller-supplied deadline may only tighten the ceiling this executor
      // already promised; otherwise the buffer would outlive the caller's own
      // terminal state and replay work for an answer nobody reads (SPEC 3.4).
      effectiveTimeoutMs: tightenTimeout(
        this.effectiveTimeoutMs,
        request.timeoutMs,
      ),
      deadlineAt: 0,
      timer: undefined,
    };
    pending.deadlineAt = startedAt + pending.effectiveTimeoutMs;
    this.pending.set(request.requestId, pending);
    this.armTimer(pending);
    try {
      const server = this.server;
      if (server !== undefined && server.ready && !server.closed) {
        // Nothing older can be undelivered while the Server is ready: every
        // flush completes or leaves no Server. Sending only this request keeps
        // the hot path O(1) instead of scanning the buffer per request.
        this.deliver(server, pending);
      } else {
        // Queued until a Server is ready; a no-op otherwise.
        this.flush();
      }
    } catch (error) {
      // Delivery is defensive against a throwing channel, but a logger or a
      // future callback could still throw. The promise must settle regardless.
      this.settle(pending, failure(INTERNAL_ERROR, messageOf(error)));
    }
    return deferred.promise;
  }

  close(): Promise<void> {
    this.closePromise ??= this.doClose();
    return this.closePromise;
  }

  /**
   * The worst case `doClose` can take: the pending-drain budget, the graceful
   * shutdown grace, then two bounded exit waits (the second follows the kill
   * issued when the first one gave up). A caller that gives this less time
   * would return while the Server could still be writing.
   */
  get closeBudgetMs(): number {
    return this.shutdownTimeoutMs * 2 + EXIT_WAIT_TIMEOUT_MS * 2;
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
    const pending = this.pending.get(requestId);
    if (pending === undefined) {
      // Already terminal, or never accepted: nothing to undo and nothing to
      // tell the Server, which either has no such request or has finished it.
      return Promise.resolve();
    }
    // warn, not error: this is a request ending by policy, not a failure.
    this.logger.warn(
      {
        requestId,
        connectionId: pending.request.connectionId,
        clientRequestId: pending.request.clientRequestId,
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
    const server = this.server;
    // A Request that never reached a live Server cannot complete, so it fails
    // now instead of burning its whole deadline. Delivered ones are given
    // until their deadline to settle (SPEC 6.3 step 3).
    // Deleting the current entry during iteration is safe for a Map, and
    // settling only resolves a promise (the continuation runs as a microtask),
    // so this cannot mutate the map mid-loop.
    for (const pending of this.pending.values()) {
      if (
        server === undefined ||
        server.closed ||
        pending.sentOn === undefined
      ) {
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
    const drained = await this.settleWithin(
      this.awaitPendingSettled(),
      this.shutdownTimeoutMs,
    );
    if (!drained) {
      this.logger.warn(
        { timeoutMs: this.shutdownTimeoutMs, pending: this.pending.size },
        'pending requests did not settle before the shutdown timeout',
      );
      this.failAllPending(
        FRAMEWORK_SHUTDOWN,
        'ipc executor closed before the request settled',
      );
    }
    await this.shutdownServer();
    this.failAllPending(FRAMEWORK_SHUTDOWN, 'ipc executor closed');
    this.status = 'closed';
    // A start() still waiting for `ready` must not be told "ready" by a close.
    this.rejectStart(
      new Error('ipc executor closed before the server became ready'),
    );
  }

  /** Resolves true when `work` settled within `limitMs`, false on timeout. */
  private async settleWithin(
    work: Promise<void>,
    limitMs: number,
  ): Promise<boolean> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const limit = new Promise<boolean>((resolve) => {
      timer = setTimeout(() => {
        resolve(false);
      }, limitMs);
    });
    try {
      return await Promise.race([
        work.then(
          () => true,
          () => true,
        ),
        limit,
      ]);
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
    }
  }

  private async shutdownServer(): Promise<void> {
    const server = this.server;
    if (server === undefined) {
      return;
    }
    if (!server.closed) {
      if (server.ready) {
        try {
          server.channel.send({ type: 'shutdown' });
        } catch (error) {
          // The process died between the pending drain and this send; the
          // channel close handler is about to report the same thing.
          this.logger.warn(
            { pid: server.pid, reason: messageOf(error) },
            'shutdown message could not be sent',
          );
        }
        await this.waitForClosed(server);
      }
      if (!server.closed && !server.shutdownAcknowledged) {
        // A stalled graceful shutdown, or a Server that never finished
        // starting, must not hold the Portal open.
        this.logger.warn(
          {
            pid: server.pid,
            timeoutMs: this.shutdownTimeoutMs,
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
    return new Promise<void>((resolve) => {
      this.closedWaiter = resolve;
      this.closedTimer = setTimeout(() => {
        this.resolveClosed();
      }, this.shutdownTimeoutMs);
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
    if (this.pending.size === 0) {
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
    this.generation += 1;
    const server: ServerState = {
      generation: this.generation,
      channel: spawned.channel,
      kill: () => {
        spawned.kill();
      },
      exitPromise: spawned.exited,
      lastExit: spawned.lastExit,
      ready: false,
      closed: false,
      exited: false,
      shutdownAcknowledged: false,
      pid: undefined,
    };
    this.server = server;
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
      const recorded = server.lastExit?.();
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
      case 'ready':
        this.onReady(
          server,
          message.pid,
          message.capabilities.maxTransactionDurationMs,
        );
        return;
      case 'result':
        this.onResult(message);
        return;
      case 'closed':
        if (this.status === 'closing') {
          server.shutdownAcknowledged = true;
          this.resolveClosed();
          return;
        }
        // Unsolicited: the process exit that follows is what actually matters,
        // and it arrives through `exited`/`onClose`. Not a shutdown ack, so
        // close() must still be able to kill a process that stays alive.
        this.logger.warn(
          { pid: server.pid, reason: message.reason },
          'server closed without being asked',
        );
        return;
      case 'hello':
      case 'request':
      case 'cancel':
      case 'shutdown':
        // Valid shape, wrong direction: a Server must not send these.
        this.logger.warn(
          { pid: server.pid, reason: `unexpected "${message.type}" message` },
          'ipc message rejected',
        );
    }
  }

  private onReady(
    server: ServerState,
    pid: number,
    declaredMaxDurationMs: number,
  ): void {
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
    if (this.isTerminal) {
      return;
    }
    this.status = 'ready';
    // The crash streak ends only after a stable run, not at the handshake: a
    // Server that reaches `ready` and then dies on its first request would
    // otherwise reset the backoff and restart immediately, forever.
    this.startStableTimer();
    const declared = effectiveTimeoutMs(
      this.portalTimeoutMs,
      declaredMaxDurationMs,
    );
    this.effectiveTimeoutMs = declared;
    for (const pending of this.pending.values()) {
      // Only ever tighten. A later Server declaring a longer ceiling must not
      // extend a deadline the Portal already promised, or a crash loop could
      // keep a request alive past its original ceiling (SPEC 3.4).
      const tightened = Math.min(pending.effectiveTimeoutMs, declared);
      if (tightened === pending.effectiveTimeoutMs) {
        continue;
      }
      pending.effectiveTimeoutMs = tightened;
      pending.deadlineAt = pending.startedAt + tightened;
      this.armTimer(pending);
    }
    this.logger.info(
      { pid, effectiveTimeoutMs: this.effectiveTimeoutMs },
      'server ready',
    );
    this.resolveStart();
    this.flush();
  }

  private onResult(message: ResultMessage): void {
    const pending = this.pending.get(message.requestId);
    if (pending === undefined) {
      // A result for a request that already reached a Portal-decided terminal
      // state. Expected, not an error (SPEC 7.3 keeps the id stable, so the
      // correlation still works — there is just nothing left to settle).
      this.logger.debug(
        {
          requestId: message.requestId,
          reason: 'request is no longer buffered',
        },
        'late result discarded',
      );
      return;
    }
    const outcome: ExecutorOutcome = message.ok
      ? { ok: true, result: message.result }
      : {
          ok: false,
          error: { code: message.error.code, message: message.error.message },
        };
    this.logger.debug(
      {
        requestId: message.requestId,
        connectionId: pending.request.connectionId,
        clientRequestId: pending.request.clientRequestId,
        durationMs: Date.now() - pending.startedAt,
      },
      'result received',
    );
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
    // A stable clock from the dead generation must not end the crash streak.
    this.clearStableTimer();
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
      try {
        server.kill();
      } catch (error) {
        this.logger.warn(
          { pid: server.pid, reason: messageOf(error) },
          'server could not be killed after losing its channel',
        );
      }
    }
    if (this.isTerminal) {
      // SPEC 6.3: no restart while shutting down; nothing can complete these.
      this.failAllPending(
        FRAMEWORK_SHUTDOWN,
        'server process exited during shutdown',
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
   * Delivers every buffered request that has not been sent on the current
   * server generation, grouped by connection and ordered by `perConnectionSeq`
   * (SPEC 6.5). Replays and never-sent requests share the one ordering: a
   * request enqueued during a restart always has a higher sequence than the
   * replays on its connection, so one sort per connection satisfies both rules
   * and correctness does not depend on Map insertion order.
   *
   * A request that keeps killing the process starves the later requests on its
   * connection: every flush stops at it (its replay budget runs out, and the
   * flush returns), so those requests wait until it reaches a terminal state.
   * That is the necessary cost of the SPEC 6.5 ordering guarantee, not a drop.
   */
  private flush(): void {
    const server = this.server;
    if (server === undefined || !server.ready) {
      return;
    }
    if (this.isTerminal) {
      return;
    }
    const byConnection = new Map<string, PendingRequest[]>();
    for (const pending of this.pending.values()) {
      if (pending.sentOn === server.generation) {
        continue;
      }
      const group = byConnection.get(pending.request.connectionId);
      if (group === undefined) {
        byConnection.set(pending.request.connectionId, [pending]);
      } else {
        group.push(pending);
      }
    }
    for (const group of byConnection.values()) {
      group.sort(
        (left, right) => left.perConnectionSeq - right.perConnectionSeq,
      );
      for (const pending of group) {
        if (!this.deliver(server, pending)) {
          return;
        }
      }
    }
  }

  /** Returns false when the channel died and delivery must stop. */
  private deliver(server: ServerState, pending: PendingRequest): boolean {
    if (!this.pending.has(pending.request.requestId)) {
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
          requestId: pending.request.requestId,
          connectionId: pending.request.connectionId,
          clientRequestId: pending.request.clientRequestId,
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
      server.channel.send(requestMessage(pending, replayCount));
    } catch (error) {
      this.onServerLost(server, null, null, `send failed: ${messageOf(error)}`);
      return false;
    }
    pending.replayCount = replayCount;
    pending.sentOn = server.generation;
    if (isReplay) {
      this.logger.warn(
        {
          requestId: pending.request.requestId,
          connectionId: pending.request.connectionId,
          clientRequestId: pending.request.clientRequestId,
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
    if (this.pending.get(pending.request.requestId) !== pending) {
      return;
    }
    pending.timer = undefined;
    // warn, not error: the connection layer records the failed outcome at error
    // level, and one request failure should not produce two error lines.
    this.logger.warn(
      {
        requestId: pending.request.requestId,
        connectionId: pending.request.connectionId,
        clientRequestId: pending.request.clientRequestId,
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
          requestId: pending.request.requestId,
          code,
          reason: messageOf(error),
        },
        'cancel could not be sent',
      );
    }
  }

  private settle(pending: PendingRequest, outcome: ExecutorOutcome): void {
    if (this.pending.get(pending.request.requestId) !== pending) {
      return;
    }
    this.pending.delete(pending.request.requestId);
    if (pending.timer !== undefined) {
      clearTimeout(pending.timer);
      pending.timer = undefined;
    }
    pending.resolve(outcome);
    if (this.pending.size === 0) {
      const drained = this.drainedWaiter;
      this.drainedWaiter = undefined;
      drained?.();
    }
  }

  private failAllPending(code: string, message: string): void {
    for (const pending of this.pending.values()) {
      this.settle(pending, failure(code, message));
    }
  }

  private nextSeq(): number {
    this.seq += 1;
    return this.seq;
  }
}

function requestMessage(
  pending: PendingRequest,
  replayCount: number,
): RequestMessage {
  const request = pending.request;
  return {
    type: 'request',
    requestId: request.requestId,
    connectionId: request.connectionId,
    clientRequestId: request.clientRequestId,
    sessionId: request.sessionId,
    method: request.method,
    payload: request.payload,
    // The Portal's deadline is authoritative, so the Server is given the time
    // actually left, not the original budget: a replay that arrives late must
    // be able to roll back on its own instead of writing after the Portal has
    // already settled the request (INV-007). The floor is 1 because the
    // protocol requires a positive `timeoutMs`.
    timeoutMs: Math.max(1, pending.deadlineAt - Date.now()),
    replayCount,
  };
}

function failure(code: string, message: string): ExecutorOutcome {
  return { ok: false, error: { code, message } };
}

let portalGenerationCounter = 0;

function nextPortalGeneration(): number {
  portalGenerationCounter += 1;
  return portalGenerationCounter;
}

/**
 * Used when the caller supplies no logger. Deliberately not a concrete logger
 * import: this module must not depend on a logging implementation, and a
 * deployment that wants observability passes its own `Logger`.
 */
const SILENT_LOGGER: Logger = {
  debug: () => undefined,
  info: () => undefined,
  warn: () => undefined,
  error: () => undefined,
  child: () => SILENT_LOGGER,
};
