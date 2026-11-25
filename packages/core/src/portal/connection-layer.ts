import { randomUUID } from 'node:crypto';

import { createJsonLogger } from '../logging/json-logger.ts';
import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import { JsonCodec } from '../shared/codec.ts';
import type { Codec } from '../shared/codec.ts';
import type { RequestExecutor } from '../shared/executor.ts';
import { describeValue, messageOf, toError } from '../shared/guards.ts';
import { settleWithin } from '../shared/timeout.ts';
import type {
  Transport,
  TransportConnection,
  TransportHandlers,
} from '../transport/types.ts';
import type { ConnectionState } from './connection-admission.ts';
import {
  DEFAULT_MAX_CONNECTIONS,
  DEFAULT_MAX_QUEUED_PER_CONNECTION,
  appendToChain,
  onConnect as admitConnection,
  onDisconnect as disconnectConnection,
} from './connection-admission.ts';
import type { RequestHost } from './connection-request.ts';
import {
  cancelQuietly,
  onMessage as receiveMessage,
} from './connection-request.ts';
import type { ConnectionHealth, ConnectionLayerStatus } from './health.ts';
import { connectionHealthSnapshot, readExecutorHealth } from './health.ts';
import { SessionRouter } from './session-router.ts';

/**
 * SPEC 6.3 bounds the wait for in-flight work. 10s covers a healthy commit or
 * rollback with room to spare, and is short enough that a deploy hitting a
 * stuck handler looks slow rather than hung.
 */
const DEFAULT_STOP_TIMEOUT_MS = 10_000;

/**
 * Slack added to the executor's own close budget. The executor's bound is
 * exact, so waiting precisely that long would race its last step (the bounded
 * exit wait) and abandon it on the tick it was about to finish; a second of
 * margin makes "the executor gave up" the observable outcome instead of "the
 * caller gave up first".
 */
const EXECUTOR_CLOSE_SLACK_MS = 1000;

export interface ConnectionLayerOptions {
  transport: Transport;
  executor: RequestExecutor;
  /** Defaults to `JsonCodec`; swapping it swaps the wire format. */
  codec?: Codec;
  /**
   * Defaults to the JSON logger. Failures that must not reach the transport (a
   * write to a closed socket, a result that cannot be encoded) are recorded
   * here rather than dropped.
   */
  logger?: Logger;
  /**
   * Defaults to `randomUUID`. The id is minted here, not in the executor,
   * because a request enters the framework at this boundary and every log line
   * about it — including a failed execution — must carry the same id (SPEC 7.3).
   */
  createRequestId?: () => string;
  /**
   * Requests allowed to wait on one connection, excluding the one executing
   * (SPEC 3.5). Defaults to `DEFAULT_MAX_QUEUED_PER_CONNECTION`; the request
   * that would exceed it is refused with `FRAMEWORK_QUEUE_FULL`. Must be a
   * positive integer, so at least one waiting request is allowed; `0` is
   * invalid.
   */
  maxQueuedPerConnection?: number;
  /**
   * Ceiling on each shutdown wait: draining in-flight requests, closing the
   * executor and stopping the transport (SPEC 6.3). Defaults to
   * `DEFAULT_STOP_TIMEOUT_MS`. Must be a finite number of at least 1.
   *
   * This is a floor, not the executor's ceiling: when the executor can bound
   * its own `close()` (`RequestExecutor.closeBudgetMs`, which `IpcExecutor`
   * does), the executor step is given at least that long. The two used to be
   * independent, and a short value here made `stop()` return while the Server
   * process was still draining and writing.
   */
  shutdownTimeoutMs?: number;
  /**
   * Connections this layer admits at once (SPEC 6.1). Defaults to
   * `DEFAULT_MAX_CONNECTIONS`. This is an admission cap, not a business
   * Session lifetime limit: a connection beyond it is closed (code `1013`)
   * before any request on it is read. Must be a positive integer.
   */
  maxConnections?: number;
}

/**
 * Wires a transport to an executor. It owns only what sits between the two:
 * decode, per-connection ordering, Session routing, error mapping and encode
 * (SPEC 5.2). The routing table and its ordering gate live in `SessionRouter`;
 * this layer applies the gate at the head of the connection's chain and carries
 * no business state, which belongs to the business process (SPEC 6.1). Replay
 * is `IpcExecutor`'s (SPEC 6.4) and input buffering is the `queued` counter
 * itself (SPEC 3.5).
 */
export class ConnectionLayer implements TransportHandlers {
  private readonly transport: Transport;
  private readonly executor: RequestExecutor;
  private readonly codec: Codec;
  private readonly logger: Logger;
  private readonly createRequestId: () => string;
  private readonly maxQueuedPerConnection: number;
  private readonly maxConnections: number;
  private readonly shutdownTimeoutMs: number;
  private readonly connections = new Map<string, ConnectionState>();
  /** Which connection holds which Session, and the reconnect gate (SPEC 3.2). */
  private readonly router: SessionRouter;
  /** The narrow view the admission, request and reply modules work through. */
  private readonly requestHost: RequestHost;
  private status: ConnectionLayerStatus = 'idle';
  private executorStart: Promise<void> | undefined;
  private transportStart: Promise<void> | undefined;
  /** Retained after a bounded stop so a late listener is still released once. */
  private transportStop: Promise<void> | undefined;
  /** Memoised executor close, so a failed start and a later stop cannot double-close. */
  private executorClose: Promise<void> | undefined;
  /** Memoised shutdown, so every caller observes the same single run. */
  private closePromise: Promise<void> | undefined;

  constructor(options: ConnectionLayerOptions) {
    const maxConnections = options.maxConnections ?? DEFAULT_MAX_CONNECTIONS;
    assertPositiveInteger(maxConnections, 'maxConnections');
    const maxQueuedPerConnection =
      options.maxQueuedPerConnection ?? DEFAULT_MAX_QUEUED_PER_CONNECTION;
    assertPositiveInteger(maxQueuedPerConnection, 'maxQueuedPerConnection');
    const shutdownTimeoutMs =
      options.shutdownTimeoutMs ?? DEFAULT_STOP_TIMEOUT_MS;
    assertFiniteAtLeastOne(shutdownTimeoutMs, 'shutdownTimeoutMs');
    this.transport = options.transport;
    this.executor = options.executor;
    this.codec = options.codec ?? new JsonCodec();
    this.logger = safeLogger(
      options.logger ??
        createJsonLogger({ bindings: { component: 'connection-layer' } }),
    );
    this.createRequestId = options.createRequestId ?? randomUUID;
    this.maxQueuedPerConnection = maxQueuedPerConnection;
    this.maxConnections = maxConnections;
    this.shutdownTimeoutMs = shutdownTimeoutMs;
    this.router = new SessionRouter({ logger: this.logger });
    this.requestHost = {
      codec: this.codec,
      logger: this.logger,
      connections: this.connections,
      maxConnections,
      maxQueuedPerConnection,
      createRequestId: this.createRequestId,
      router: this.router,
      executor: this.executor,
      enqueue: (state, work) => this.enqueue(state, work),
    };
  }

  async start(): Promise<void> {
    if (this.status !== 'idle') {
      throw new Error(`connection layer cannot start while ${this.status}`);
    }
    this.status = 'starting';
    this.executorStart = Promise.resolve().then(() => this.executor.start());
    // SPEC 6.2: readiness must precede listener startup.
    try {
      await this.executorStart;
      if (this.status !== 'starting') {
        throw new Error('connection layer startup interrupted by shutdown');
      }
      this.transportStart = Promise.resolve().then(() =>
        this.transport.start(this),
      );
      await this.transportStart;
    } catch (error) {
      if (this.status === 'starting') {
        this.status = 'stopped';
      }
      await this.closeExecutor().catch(() => undefined);
      throw error;
    }
    if (this.status !== 'starting') {
      await this.stopTransport();
      throw new Error('connection layer startup interrupted by shutdown');
    }
    this.status = 'started';
  }

  /** One close per executor, whether it comes from a failed start or `stop()`. */
  private closeExecutor(): Promise<void> {
    // Closing before an asynchronous start settles can leak resources that the
    // late start creates after close. Retain the continuation after stop times out.
    this.executorClose ??= (this.executorStart ?? Promise.resolve())
      .catch(() => undefined)
      .then(() => this.executor.close());
    return this.executorClose;
  }

  private stopTransport(): Promise<void> {
    this.transportStop ??=
      this.transportStart === undefined
        ? Promise.resolve()
        : this.transportStart.then(
            () => this.transport.stop(),
            () => undefined,
          );
    return this.transportStop;
  }

  onConnect(connection: TransportConnection): Promise<void> {
    return admitConnection(this.requestHost, this.status, connection);
  }

  onMessage(
    connection: TransportConnection,
    payload: string | Uint8Array,
  ): Promise<void> {
    return receiveMessage(this.requestHost, this.status, connection, payload);
  }

  onDisconnect(connection: TransportConnection): Promise<void> {
    return disconnectConnection(this.requestHost, connection, {
      releaseRoute: (connectionId) => this.router.release(connectionId),
      cancel: (requestId, connectionId) =>
        cancelQuietly(this.requestHost, requestId, connectionId),
    });
  }

  stop(): Promise<void> {
    // Idempotent through the promise, not the status. `start()` can fail after
    // the executor started, and `transport.stop()` can fail after the status
    // already advanced; both paths must still release the executor, because a
    // SQLite handle is a real resource rather than a state-machine artefact.
    this.closePromise ??= this.doStop();
    return this.closePromise;
  }

  private async doStop(): Promise<void> {
    // SPEC 6.3 step 1: stop accepting input. Synchronous so a message that
    // arrives while the drain is running is refused rather than queued.
    this.status = 'stopping';
    const timeoutMs = this.shutdownTimeoutMs;
    // Waiting requests may fail during shutdown, never bypass their barrier.
    for (const state of this.connections.values()) {
      state.closed.abort();
    }

    // Step 2: drain what was already accepted. New input is refused, so the
    // chains can only shrink. Bounded because a handler that never returns
    // would otherwise keep the process alive forever, which is worse than the
    // leak the wait is meant to avoid.
    const drained = await settleWithin(
      Promise.all([...this.connections.values()].map((state) => state.chain)),
      timeoutMs,
    );
    if (drained.status === 'timeout') {
      const unfinished = [...this.connections.entries()].filter(
        ([, state]) => state.inFlight.size > 0 || state.queued > 0,
      );
      this.logger.error(
        {
          connectionIds: unfinished.map(([id]) => id),
          count: unfinished.length,
          timeoutMs,
        },
        'in-flight requests did not settle before the shutdown timeout',
      );
    }
    this.connections.clear();
    // SPEC 6.1: routes outlive neither their connections nor the layer. Leaving
    // them behind would make a restarted layer route a reconnect to a
    // connection id that no longer exists.
    this.router.clear();

    // Step 4 before step 5 (SPEC 6.3): storage closes before the transport, so
    // no write can land after the connection that would report it is gone.
    // Closing storage while a request still held a transaction is why the
    // drain above comes first.
    //
    // The budget is the larger of this layer's own timeout and the executor's
    // declared worst case plus slack. A remote executor has to drain, ask the
    // Server to stop and wait for the process to go; giving it less would make
    // `stop()` return while the Server was still writing, which is the defect
    // this budget exists to prevent. A local executor declares nothing, so its
    // step keeps the layer's own timeout.
    const executorBudgetMs =
      this.executor.closeBudgetMs === undefined
        ? timeoutMs
        : Math.max(
            timeoutMs,
            this.executor.closeBudgetMs + EXECUTOR_CLOSE_SLACK_MS,
          );
    let executorFailure: Error | undefined;
    const closed = await settleWithin(this.closeExecutor(), executorBudgetMs);
    if (closed.status === 'timeout') {
      executorFailure = new Error(
        'executor close did not settle before the shutdown timeout',
      );
      this.logger.error(
        { timeoutMs: executorBudgetMs },
        'executor close did not settle before the shutdown timeout',
      );
    } else if (closed.status === 'rejected') {
      executorFailure = toError(closed.error);
      this.logger.error(
        { reason: messageOf(closed.error) },
        'executor close failed',
      );
    }

    const stopped = await settleWithin(this.stopTransport(), timeoutMs);
    this.status = 'stopped';
    if (stopped.status === 'timeout') {
      this.logger.error(
        { timeoutMs },
        'transport stop did not settle before the shutdown timeout',
      );
    }
    if (stopped.status === 'rejected') {
      if (executorFailure !== undefined) {
        throw new AggregateError(
          [executorFailure, toError(stopped.error)],
          'executor and transport failed to close',
        );
      }
      // The sequence is complete, but the transport never confirmed it closed:
      // that is the caller's to handle, so it is rethrown rather than logged
      // away.
      throw toError(stopped.error);
    }
    if (executorFailure !== undefined) {
      throw executorFailure;
    }
  }

  /**
   * Synchronous snapshot for deployment probes (SPEC 6.6). It has no side
   * effects: it reads the layer's status and admission count and asks the
   * executor once, turning a failing self-report into `unknown` rather than
   * letting it escape the probe.
   */
  health(): ConnectionHealth {
    return connectionHealthSnapshot(
      this.status,
      this.connections.size,
      readExecutorHealth(this.executor),
    );
  }

  /**
   * Appends work to the connection's ordered chain; the chain itself lives with
   * `ConnectionState` so every module appends through one implementation.
   */
  private enqueue(
    state: ConnectionState,
    work: () => Promise<void>,
  ): Promise<void> {
    return appendToChain(state, work);
  }
}

function assertPositiveInteger(value: unknown, name: string): void {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a positive integer, got ${describeValue(value)}`,
    );
  }
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(
      `${name} must be a positive integer, got ${String(value)}`,
    );
  }
}

function assertFiniteAtLeastOne(value: unknown, name: string): void {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a finite number of at least 1, got ${describeValue(value)}`,
    );
  }
  if (!Number.isFinite(value) || value < 1) {
    throw new RangeError(
      `${name} must be a finite number of at least 1, got ${String(value)}`,
    );
  }
}
