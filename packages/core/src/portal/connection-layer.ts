import { randomUUID } from 'node:crypto';

import { JsonCodec } from '../client/codec.ts';
import type { Codec, Envelope, ResponseEnvelope } from '../client/codec.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { safeLogger } from '../logging/safe-logger.ts';
import type { Logger } from '../logging/types.ts';
import type {
  ExecutorOutcome,
  RequestExecutor,
  SessionChange,
} from '../shared/executor.ts';
import { describeValue, isRecord, messageOf } from '../shared/guards.ts';
import { settleWithin } from '../shared/timeout.ts';
import {
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_SHUTDOWN,
  INVALID_ENVELOPE,
  INTERNAL_ERROR,
  TRANSPORT_CLOSED,
  TRANSPORT_SEND_FAILED,
  failureLogLevel,
} from '../shared/wire-codes.ts';
import type {
  Transport,
  TransportConnection,
  TransportHandlers,
} from '../transport/types.ts';
import type { ConnectionHealth } from './health.ts';
import { connectionHealthSnapshot, readExecutorHealth } from './health.ts';
import { SessionRouter } from './session-router.ts';

/**
 * SPEC 3.5 requires a hard per-connection cap. 1000 absorbs a legitimate burst
 * (a client replaying its backlog after a reconnect) while bounding the memory
 * one connection can pin; a deeper backlog means the client is flooding or the
 * handler is badly behind, and either way a human should hear about it.
 */
const DEFAULT_MAX_QUEUED_PER_CONNECTION = 1000;

/**
 * SPEC 6.3 bounds the wait for in-flight work. 10s covers a healthy commit or
 * rollback with room to spare, and is short enough that a deploy hitting a
 * stuck handler looks slow rather than hung.
 */
const DEFAULT_SHUTDOWN_TIMEOUT_MS = 10_000;

/**
 * Slack added to the executor's own close budget. The executor's bound is
 * exact, so waiting precisely that long would race its last step (the bounded
 * exit wait) and abandon it on the tick it was about to finish; a second of
 * margin makes "the executor gave up" the observable outcome instead of "the
 * caller gave up first".
 */
const EXECUTOR_CLOSE_SLACK_MS = 1000;

/**
 * SPEC 6.1: a cap on live connections, not on business Session lifetime. A
 * route is held by a connection, so bounding admissions also bounds the
 * routing table without refusing a binding after its handler succeeded.
 * 10k leaves room for a large deployment; an operator with more concurrent
 * players raises it explicitly.
 */
const DEFAULT_MAX_CONNECTIONS = 10_000;

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
   * `DEFAULT_SHUTDOWN_TIMEOUT_MS`. Must be a finite number of at least 1.
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

type Status = 'idle' | 'starting' | 'started' | 'stopping' | 'stopped';

interface ConnectionState {
  readonly connection: TransportConnection;
  /** Tail of this connection's work chain; see `enqueue`. */
  chain: Promise<unknown>;
  /**
   * Requests accepted on this connection that are waiting for their turn,
   * excluding the one executing. The SPEC 3.5 cap is about this backlog: a
   * client that keeps writing while the handler lags is the case it exists for.
   */
  queued: number;
  /**
   * Requests accepted on this connection that have not settled yet. A
   * disconnect cancels exactly these, so no work outlives the only client that
   * could read its result (SPEC 3.6).
   */
  readonly inFlight: Set<string>;
  /** Aborts only waiting work; an unfinished Session barrier survives it. */
  readonly closed: AbortController;
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
  private status: Status = 'idle';
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
      options.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;
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
    if (this.status === 'stopping' || this.status === 'stopped') {
      // SPEC 6.3 step 1: no new connection is admitted once shutdown started.
      // Not registering it is enough: `onMessage` refuses on status before it
      // ever looks the connection up, so nothing is silently dropped.
      return Promise.resolve();
    }
    if (this.connections.size >= this.maxConnections) {
      // Admission is the only point before a request is read, so it is the
      // only place a refusal has no side effect. The client is told to retry
      // rather than left waiting on a connection this layer will not serve.
      this.logger.warn(
        { connectionId: connection.id, limit: this.maxConnections },
        'connection refused: connection limit reached',
      );
      return this.closeRefused(connection);
    }
    const state: ConnectionState = {
      connection,
      chain: Promise.resolve(),
      queued: 0,
      inFlight: new Set(),
      closed: new AbortController(),
    };
    this.connections.set(connection.id, state);
    return Promise.resolve();
  }

  /** Best effort: a failed close must not escape into the transport. */
  private async closeRefused(connection: TransportConnection): Promise<void> {
    try {
      await connection.close(1013, 'connection limit reached');
    } catch (error) {
      this.logger.warn(
        { connectionId: connection.id, reason: messageOf(error) },
        'refused connection could not be closed',
      );
    }
  }

  onMessage(
    connection: TransportConnection,
    payload: string | Uint8Array,
  ): Promise<void> {
    if (this.status === 'stopping' || this.status === 'stopped') {
      // SPEC 6.3 step 1: the layer no longer accepts input. A stable code beats
      // silence, because the client is still waiting for an answer.
      return this.refuse(connection, payload);
    }
    const state = this.connections.get(connection.id);
    if (state === undefined) {
      // Not a tracked connection (a race with stop, or a transport that
      // reconnects with a fresh object): there is nobody to answer.
      return Promise.resolve();
    }
    // Decode for correlation, but budget every ordered reply, including bad
    // frames: retaining uncounted callbacks would make this queue unbounded.
    let envelope: Envelope;
    try {
      envelope = this.codec.decode(payload);
    } catch (error) {
      if (state.queued >= this.maxQueuedPerConnection) {
        return this.refuseQueue(state, '');
      }
      const rawLength = rawLengthOf(payload);
      const reason = messageOf(error);
      state.queued += 1;
      return this.enqueue(state, async () => {
        state.queued -= 1;
        await this.rejectEnvelope(connection, rawLength, reason);
      });
    }
    if (state.queued >= this.maxQueuedPerConnection) {
      return this.refuseQueue(state, envelope.id, envelope.method);
    }
    state.queued += 1;
    return this.enqueue(state, () => this.startAfterGate(state, envelope));
  }

  private refuseQueue(
    state: ConnectionState,
    id: string,
    method?: string,
  ): Promise<void> {
    this.logger.warn(
      {
        connectionId: state.connection.id,
        clientRequestId: id,
        method,
        code: FRAMEWORK_QUEUE_FULL,
        queued: state.queued,
        limit: this.maxQueuedPerConnection,
      },
      'request refused: input queue is full',
    );
    return this.reply(state.connection, {
      id,
      ok: false,
      error: {
        code: FRAMEWORK_QUEUE_FULL,
        message: `input queue is full (${this.maxQueuedPerConnection} waiting)`,
      },
    });
  }

  private async startAfterGate(
    state: ConnectionState,
    envelope: Envelope,
  ): Promise<void> {
    const ran = await this.router.run(
      state.connection.id,
      state.closed.signal,
      async (sessionId, terminal) => {
        state.queued -= 1;
        await this.handle(state.connection, envelope, sessionId, terminal);
      },
    );
    if (!ran) {
      state.queued -= 1;
      // Shutdown can stop waiting, but cannot start overlapping Session work.
      if (this.connections.get(state.connection.id) === state) {
        await this.refuseWithId(state.connection, envelope.id);
      }
    }
  }

  async onDisconnect(connection: TransportConnection): Promise<void> {
    const state = this.connections.get(connection.id);
    this.connections.delete(connection.id);
    if (state === undefined) {
      return;
    }
    this.router.release(connection.id);
    // Only this connection's wait is released; other holders still wait for
    // its framework request completion, even when cancellation settles later.
    state.closed.abort();
    // SPEC 3.6 default policy: a connection closing mid-request cancels it. The
    // handler may still be running, but nobody can read its result, and letting
    // it commit would leave a state change the player never saw.
    const cancels: Promise<void>[] = [];
    for (const requestId of state.inFlight) {
      cancels.push(this.cancelQuietly(requestId, connection.id));
    }
    state.inFlight.clear();
    await Promise.all(cancels);
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
   * Appends work to the connection's ordered chain. Per-connection rather than
   * global: requests on one connection must observe their predecessor's writes
   * (SPEC 3.1), while different connections must stay concurrent (SPEC 3.2).
   * Awaiting the previous tail also makes a request that timed out release the
   * next one immediately, because the executor settles at the timeout, not at
   * handler return.
   */
  private enqueue(
    state: ConnectionState,
    work: () => Promise<void>,
  ): Promise<void> {
    const next = state.chain.then(work);
    // The chain must survive a rejected link, otherwise one failure would
    // poison every later request on this connection.
    state.chain = next.catch(() => undefined);
    return next;
  }

  /** SPEC 6.3 step 2: input arriving after shutdown started gets a stable code. */
  private async refuse(
    connection: TransportConnection,
    payload: string | Uint8Array,
  ): Promise<void> {
    let id: string;
    try {
      id = this.codec.decode(payload).id;
    } catch {
      // Unreadable frame during shutdown: still answer, with no correlation.
      id = '';
    }
    await this.refuseWithId(connection, id);
  }

  private async refuseWithId(
    connection: TransportConnection,
    id: string,
  ): Promise<void> {
    this.logger.warn(
      {
        connectionId: connection.id,
        code: FRAMEWORK_SHUTDOWN,
        reason: 'request arrived after shutdown started',
      },
      'request refused',
    );
    await this.reply(connection, {
      id,
      ok: false,
      error: {
        code: FRAMEWORK_SHUTDOWN,
        message: 'connection layer is shutting down',
      },
    });
  }

  private async rejectEnvelope(
    connection: TransportConnection,
    rawLength: number,
    reason: string,
  ): Promise<void> {
    // warn, not error: one unreadable frame is a rejected input, not a failed
    // request (SPEC 7.1).
    this.logger.warn(
      {
        connectionId: connection.id,
        code: INVALID_ENVELOPE,
        rawLength,
        reason,
      },
      'envelope rejected',
    );
    // The id is unreadable, so the reply cannot be correlated. Answering with
    // an empty id beats closing the connection: a client sees a stable error
    // code it can log, and one bad frame does not destroy a session that may
    // otherwise be healthy (SPEC 3.5 forbids silent drops).
    await this.reply(connection, {
      id: '',
      ok: false,
      error: { code: INVALID_ENVELOPE, message: reason },
    });
  }

  private async handle(
    connection: TransportConnection,
    envelope: Envelope,
    sessionId: string | undefined,
    terminal: Promise<void>,
  ): Promise<void> {
    const method = envelope.method;
    const clientRequestId = envelope.id;
    const requestPayload = envelope.payload;

    // Assigned here, at the framework's entry point, before any client data
    // influences it (SPEC 7.3). The executor carries it through rather than
    // minting its own, so both sides log the same id; a replay passes it back in.
    const requestId = this.createRequestId();
    // The Session this connection already holds, read before the request runs:
    // a request that binds a Session still runs as unbound, and the binding it
    // declares is applied only after it succeeds. Every request line carries it
    // (SPEC 7.1), which is why the logger is built after this lookup.
    const state = this.connections.get(connection.id);
    const logger = this.logger.child({
      requestId,
      connectionId: connection.id,
      clientRequestId,
      ...(sessionId === undefined ? {} : { sessionId }),
    });
    const startedAt = performance.now();

    // A request queued behind another one can reach this point after its
    // connection closed. Executing it would commit writes for a player who is
    // gone, which is the same defect as running a cancelled request (SPEC 3.6),
    // so it is ended here instead — before it acquires a transaction.
    if (state === undefined) {
      logger.warn(
        { method, code: TRANSPORT_CLOSED, reason: 'connection closed' },
        'request dropped before start',
      );
      return;
    }
    // Registered before the executor is called, so a disconnect that lands while
    // the request is queued behind another one still cancels it.
    state.inFlight.add(requestId);
    let outcome: ExecutorOutcome;
    try {
      const raw: unknown = await this.executor.execute({
        requestId,
        connectionId: connection.id,
        clientRequestId,
        // The Session this connection already holds: the only source of
        // `RequestContext.sessionId` (SPEC 2) and of the Session ID on every
        // request line (SPEC 7.1). Read before the call, so a request that
        // binds a Session still runs as unbound — the binding it declares is
        // applied only after it succeeds.
        sessionId,
        method,
        payload: requestPayload,
      });
      // The guard exists because the executor is outside code: an outcome
      // without `error` would otherwise produce no frame at all, which is the
      // silent drop SPEC 3.5 forbids. A valid outcome is passed through as is;
      // the guard only checks the shape and rewrites no field.
      outcome = isExecutorOutcome(raw)
        ? raw
        : toFailure(new Error('executor returned a malformed outcome'));
    } catch (error) {
      // The interface forbids throwing, but an executor is outside code. An
      // escaping throw would reject `handle` and leave the client unanswered,
      // which is the silent drop SPEC 3.5 forbids, so it is mapped to the same
      // failure path as a returned `{ok:false}`.
      outcome = toFailure(error);
    } finally {
      state.inFlight.delete(requestId);
    }
    if (outcome.ok) {
      // SPEC 2: a Session change a handler declared takes effect only when the
      // request succeeded, so a failed login leaves no route behind. Applied
      // before the reply so a reconnect sees the new holder as soon as the
      // client can, and so the reply itself is part of the chain a later
      // handoff waits for.
      //
      // Skipped once the connection is gone: `onDisconnect` already removed its
      // route, and re-adding one here would route a future reconnect to a
      // connection id that no longer exists.
      const change = outcome.sessionChange;
      if (
        change !== undefined &&
        this.connections.get(connection.id) === state
      ) {
        if (change.type === 'bind') {
          this.router.bind(connection.id, change.sessionId, terminal, logger);
        } else {
          // Deliberately behind the same success check: a failed unbind keeps
          // the existing route. `unbind` leaves the Session's unfinished
          // barrier in place (INV-004).
          this.router.unbind(connection.id, logger);
        }
      }
      await this.reply(
        connection,
        { id: clientRequestId, ok: true, result: outcome.result },
        logger,
      );
      return;
    }
    // An untracked connection means `onDisconnect` already ran: the request
    // ended because the client left, which is expected, and nobody can read a
    // reply. Writing one would only add a TRANSPORT_SEND_FAILED error on top
    // of the cancel the executor already logged (SPEC 7.1).
    if (!this.connections.has(connection.id)) {
      logger.debug(
        {
          method,
          code: outcome.error.code,
          durationMs: elapsedSince(startedAt),
        },
        'request ended after the connection closed',
      );
      return;
    }
    // warn for a business refusal: it is an expected outcome the Extension
    // decided on, and SPEC 7.1 forbids recording those at error level. The
    // IPC protocol carries no category, so the shared framework-code list is
    // the only judgement both processes can apply (wire-codes.ts).
    logger[failureLogLevel(outcome.error)](
      {
        method,
        code: outcome.error.code,
        durationMs: elapsedSince(startedAt),
        reason: outcome.error.message,
      },
      'request failed',
    );
    await this.reply(
      connection,
      {
        id: clientRequestId,
        ok: false,
        // The wire contract is `{code, message}` (SPEC 7.2); the category is
        // internal and must not become a client-visible field.
        error: { code: outcome.error.code, message: outcome.error.message },
      },
      logger,
    );
  }

  private async cancelQuietly(
    requestId: string,
    connectionId: string,
  ): Promise<void> {
    try {
      await this.executor.cancel(requestId, TRANSPORT_CLOSED);
    } catch (error) {
      // The interface forbids throwing, but an executor is outside code. One
      // failed cancel must not keep the remaining requests on this connection
      // alive, and there is no caller to hand the failure to.
      this.logger.error(
        {
          requestId,
          connectionId,
          code: TRANSPORT_CLOSED,
          reason: messageOf(error),
        },
        'request could not be cancelled',
      );
    }
  }

  private async reply(
    connection: TransportConnection,
    response: ResponseEnvelope,
    logger: Logger = this.logger,
  ): Promise<void> {
    try {
      let frame: string | Uint8Array;
      try {
        frame = this.codec.encode(response);
      } catch (error) {
        // A handler result JSON cannot represent (cycle, BigInt) must still
        // produce an answer rather than a stuck request.
        logger.error(
          { code: INTERNAL_ERROR, reason: messageOf(error) },
          'response could not be encoded',
        );
        frame = this.codec.encode({
          id: response.id,
          ok: false,
          error: {
            code: INTERNAL_ERROR,
            message: 'result could not be encoded',
          },
        });
      }
      await connection.send(frame);
    } catch (error) {
      // The socket may have closed between dispatch and write. Rethrowing
      // would surface as an unhandled rejection in the transport, which has no
      // caller to hand it to and no way to answer a request nobody can read.
      logger.error(
        { code: TRANSPORT_SEND_FAILED, reason: messageOf(error) },
        'response could not be sent',
      );
    }
  }
}

/** Length only: the raw frame may hold client data that must not be logged. */
function rawLengthOf(payload: string | Uint8Array): number {
  return typeof payload === 'string' ? payload.length : payload.byteLength;
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

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

/**
 * A guard, not a normaliser. The executor is outside code, so a returned object
 * is not proof of the field the branches below read: a bare `{ok:false}` would
 * throw on `outcome.error.code` after the `execute()` try/catch and drop the
 * request with no frame at all (SPEC 3.5). Checking `ok` alone is not enough for
 * that reason — the failure branch must be checked too. Repairing a malformed
 * outcome is not this layer's job: anything shaped wrongly is a framework
 * failure, not a client-visible business result.
 */
function isExecutorOutcome(value: unknown): value is ExecutorOutcome {
  if (!isRecord(value)) {
    return false;
  }
  if (value.ok === true) {
    return (
      value.sessionChange === undefined || isSessionChange(value.sessionChange)
    );
  }
  if (value.ok !== false) {
    return false;
  }
  const error = value.error;
  return (
    isRecord(error) &&
    typeof error.code === 'string' &&
    error.code.length > 0 &&
    typeof error.message === 'string'
  );
}

/**
 * The same strictness as the IPC parser: a malformed change must not reach the
 * router, where a bad id or an unknown type would silently misroute a later
 * request (SPEC 3.7).
 */
function isSessionChange(value: unknown): value is SessionChange {
  if (!isRecord(value)) {
    return false;
  }
  if (value.type === 'bind') {
    return typeof value.sessionId === 'string' && value.sessionId.length > 0;
  }
  if (value.type === 'unbind') {
    return value.sessionId === undefined;
  }
  return false;
}

function toFailure(error: unknown): ExecutorOutcome {
  return {
    ok: false,
    error: { code: INTERNAL_ERROR, message: messageOf(error) },
  };
}

function elapsedSince(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}
