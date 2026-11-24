import { randomUUID } from 'node:crypto';

import { isRecord, messageOf } from '../internal/guards.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type {
  Transport,
  TransportConnection,
  TransportHandlers,
} from '../transport/types.ts';
import { JsonCodec } from './codec.ts';
import type { Codec, Envelope, ResponseEnvelope } from './codec.ts';
import type { ExecutorOutcome, RequestExecutor } from './executor.ts';
import {
  DEFAULT_MAX_SESSIONS,
  DEFAULT_SESSION_HANDOFF_TIMEOUT_MS,
  SessionRouter,
} from './session-router.ts';
import type { SessionGate, SessionHolder } from './session-router.ts';
import { settleWithin } from './timeout.ts';
import {
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_SHUTDOWN,
  INVALID_ENVELOPE,
  INTERNAL_ERROR,
  TRANSPORT_CLOSED,
  TRANSPORT_SEND_FAILED,
  failureLogLevel,
} from './wire-codes.ts';

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
   * that would exceed it is refused with `FRAMEWORK_QUEUE_FULL`.
   */
  maxQueuedPerConnection?: number;
  /**
   * Ceiling on each shutdown wait: draining in-flight requests, closing the
   * executor and stopping the transport (SPEC 6.3). Defaults to
   * `DEFAULT_SHUTDOWN_TIMEOUT_MS`.
   *
   * This is a floor, not the executor's ceiling: when the executor can bound
   * its own `close()` (`RequestExecutor.closeBudgetMs`, which `IpcExecutor`
   * does), the executor step is given at least that long. The two used to be
   * independent, and a short value here made `stop()` return while the Server
   * process was still draining and writing.
   */
  shutdownTimeoutMs?: number;
  /**
   * Session routes this layer may hold at once (SPEC 6.1). Defaults to
   * `DEFAULT_MAX_SESSIONS`; a binding beyond the cap is refused and logged, but
   * the request that carried it still succeeds, because a routing failure is
   * not a business failure.
   */
  maxSessions?: number;
  /**
   * How long a reconnecting connection waits for the previous holder's work to
   * settle (SPEC 3.2, INV-004). Defaults to
   * `DEFAULT_SESSION_HANDOFF_TIMEOUT_MS`; on expiry the wait is released and
   * logged, because the previous connection may be stuck on a handler nobody
   * can preempt (SPEC 3.4).
   */
  sessionHandoffTimeoutMs?: number;
}

type Status = 'idle' | 'started' | 'stopping' | 'stopped';

interface ConnectionState {
  readonly connection: TransportConnection;
  /**
   * The router's view of this connection. Read live, not captured: `chain`
   * advances with every request, so a handoff waits on the tail as it stands.
   */
  readonly holder: SessionHolder;
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
  /**
   * The Session this connection currently holds, if any. Mirrors the router,
   * which owns the table; kept here because the request path reads it on every
   * message and it must not be re-derived from the route.
   */
  sessionId?: string;
  /** Pending reconnect wait; consumed by the first request that waits on it. */
  sessionGate?: SessionGate;
  /**
   * Resolves when this connection is gone, so a request waiting on
   * `sessionGate` does not outlive the client it would answer. Otherwise a
   * reconnect-then-drop would keep the chain, and shutdown's drain, waiting for
   * the full handoff timeout.
   */
  readonly closed: Promise<void>;
  readonly markClosed: () => void;
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
  private readonly shutdownTimeoutMs: number;
  private readonly connections = new Map<string, ConnectionState>();
  /** Which connection holds which Session, and the reconnect gate (SPEC 3.2). */
  private readonly router: SessionRouter;
  private status: Status = 'idle';
  /** Whether `transport.start` succeeded, so `stop` is only called on a live one. */
  private transportStarted = false;
  /** Memoised executor close, so a failed start and a later stop cannot double-close. */
  private executorClose: Promise<void> | undefined;
  /** Memoised shutdown, so every caller observes the same single run. */
  private closePromise: Promise<void> | undefined;

  constructor(options: ConnectionLayerOptions) {
    this.transport = options.transport;
    this.executor = options.executor;
    this.codec = options.codec ?? new JsonCodec();
    this.logger =
      options.logger ??
      createJsonLogger({ bindings: { component: 'connection-layer' } });
    this.createRequestId = options.createRequestId ?? randomUUID;
    this.maxQueuedPerConnection =
      options.maxQueuedPerConnection ?? DEFAULT_MAX_QUEUED_PER_CONNECTION;
    this.shutdownTimeoutMs =
      options.shutdownTimeoutMs ?? DEFAULT_SHUTDOWN_TIMEOUT_MS;
    this.router = new SessionRouter({
      maxSessions: options.maxSessions ?? DEFAULT_MAX_SESSIONS,
      handoffTimeoutMs:
        options.sessionHandoffTimeoutMs ?? DEFAULT_SESSION_HANDOFF_TIMEOUT_MS,
      logger: this.logger,
    });
  }

  async start(): Promise<void> {
    if (this.status !== 'idle') {
      throw new Error(`connection layer cannot start while ${this.status}`);
    }
    // SPEC 6.2 step 3: the transport must not accept input before the backend
    // is ready, so the executor starts first and a failed start leaves no
    // listener behind.
    await this.executor.start();
    try {
      await this.transport.start(this);
    } catch (error) {
      // The executor already started; leaving it running would leak a Server
      // process nobody can reach. Memoised, so a caller that also calls stop()
      // does not close it twice (a backend close is not always idempotent).
      await this.closeExecutor().catch(() => undefined);
      throw error;
    }
    this.transportStarted = true;
    this.status = 'started';
  }

  /** One close per executor, whether it comes from a failed start or `stop()`. */
  private closeExecutor(): Promise<void> {
    this.executorClose ??= this.executor.close();
    return this.executorClose;
  }

  onConnect(connection: TransportConnection): Promise<void> {
    if (this.status === 'stopping' || this.status === 'stopped') {
      // SPEC 6.3 step 1: no new connection is admitted once shutdown started.
      // Not registering it is enough: `onMessage` refuses on status before it
      // ever looks the connection up, so nothing is silently dropped.
      return Promise.resolve();
    }
    const closed = Promise.withResolvers<void>();
    // The holder closes over `state`: `chain` advances on every request, and
    // copying it here would freeze the tail a handoff later waits on.
    const state: ConnectionState = {
      connection,
      holder: {
        connectionId: connection.id,
        chain: () => state.chain,
        closed: () => state.closed,
      },
      chain: Promise.resolve(),
      queued: 0,
      inFlight: new Set(),
      closed: closed.promise,
      markClosed: closed.resolve,
    };
    this.connections.set(connection.id, state);
    return Promise.resolve();
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
    // Decoded before the queue check because a refused request still needs its
    // client id to be answerable. A frame that cannot be decoded has no id and
    // is not a request, so it never occupies a queue slot.
    let envelope: Envelope;
    try {
      envelope = this.codec.decode(payload);
    } catch (error) {
      // A frame that cannot be decoded is not a request, so it never occupies
      // a queue slot. It still runs on the chain, so its reply cannot overtake
      // an accepted request's.
      return this.enqueue(state, () =>
        this.rejectEnvelope(connection, payload, error),
      );
    }
    if (state.queued >= this.maxQueuedPerConnection) {
      // SPEC 3.5 refusal policy. Refusing is the queue's only strategy: it is
      // the one the queue itself can enforce, and it fails a request visibly
      // instead of stalling the connection behind a full input buffer.
      this.logger.warn(
        {
          connectionId: connection.id,
          clientRequestId: envelope.id,
          method: envelope.method,
          code: FRAMEWORK_QUEUE_FULL,
          queued: state.queued,
          limit: this.maxQueuedPerConnection,
        },
        'request refused: input queue is full',
      );
      return this.reply(connection, {
        id: envelope.id,
        ok: false,
        error: {
          code: FRAMEWORK_QUEUE_FULL,
          message: `input queue is full (${this.maxQueuedPerConnection} waiting)`,
        },
      });
    }
    state.queued += 1;
    return this.enqueue(state, () => this.startAfterGate(state, envelope));
  }

  /**
   * Waits out a pending reconnect gate, then runs the request. The wait sits at
   * the head of this connection's chain, so it orders every later request on
   * the new connection behind the old connection's work rather than only the
   * first one (SPEC 3.2, INV-004).
   *
   * The wait itself belongs to the router, which owns the handoff bound; the
   * placement at the chain head belongs here, because only this layer knows the
   * chain.
   */
  private async startAfterGate(
    state: ConnectionState,
    envelope: Envelope,
  ): Promise<void> {
    const gate = state.sessionGate;
    if (gate !== undefined) {
      // Consumed, not re-read: this request is the one that crosses the
      // handoff, and the requests behind it are already ordered by the chain.
      state.sessionGate = undefined;
      await this.router.waitForHandoff(gate, state.holder);
    }
    // Counted down when the request actually starts, not when it is refused or
    // admitted: the cap measures work that is waiting, and a request blocked on
    // the gate is still waiting. It also keeps the count aligned with
    // "excluding the executing request".
    state.queued -= 1;
    return this.handle(state.connection, envelope);
  }

  async onDisconnect(connection: TransportConnection): Promise<void> {
    const state = this.connections.get(connection.id);
    this.connections.delete(connection.id);
    if (state === undefined) {
      return;
    }
    // SPEC 6.1: the layer keeps routing information only, so a route whose
    // connection is gone is gone with it. A reconnect after this point binds as
    // a first binding, not as a handoff from a dead connection. A gate orders
    // this connection behind the previous holder of the Session it just left;
    // that ordering no longer applies to whatever it binds next.
    state.sessionId = undefined;
    state.sessionGate = undefined;
    this.router.release(state.holder);
    // Releases a request still waiting on a reconnect gate: nobody can read its
    // result, and shutdown's drain must not wait for the handoff timeout.
    state.markClosed();
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

  /**
   * Applies the router's decision to this connection (SPEC 2, 3.2). A binding
   * that lands on a Session another connection still holds is a reconnect: the
   * gate returned makes this connection's next request wait for the old one's
   * chain, so the two never process at the same time (INV-004). A refused
   * binding changes nothing here — routing is not the business result, so the
   * request that carried it still succeeds.
   */
  private bindSession(
    state: ConnectionState,
    sessionId: string,
    logger: Logger,
  ): void {
    const gate = this.router.bind(state.holder, sessionId, logger);
    // The router reports a refusal by leaving the holder on its previous
    // Session, so a mismatch means this binding did not take effect.
    if (this.router.sessionOf(state.connection.id) !== sessionId) {
      return;
    }
    state.sessionId = sessionId;
    // Always assigned, not merged: a gate is consumed by the request that
    // crosses it, so none can be pending when a binding is applied.
    state.sessionGate = gate;
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
    // A request waiting on a reconnect gate would otherwise hold its chain for
    // the full handoff timeout, which can be longer than this drain. Shutdown
    // outranks the handoff: release every wait now so the drain measures real
    // work rather than a routing delay.
    for (const state of this.connections.values()) {
      state.markClosed();
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
    const closed = await settleWithin(this.closeExecutor(), executorBudgetMs);
    if (closed.status === 'timeout') {
      this.logger.error(
        { timeoutMs: executorBudgetMs },
        'executor close did not settle before the shutdown timeout',
      );
    } else if (closed.status === 'rejected') {
      this.logger.error(
        { reason: messageOf(closed.error) },
        'executor close failed',
      );
    }

    const stopped = await settleWithin(
      // A transport whose `start` failed has nothing to stop and would reject;
      // its own lifecycle error must not mask the executor close above.
      this.transportStarted ? this.transport.stop() : Promise.resolve(),
      timeoutMs,
    );
    this.status = 'stopped';
    if (stopped.status === 'timeout') {
      this.logger.error(
        { timeoutMs },
        'transport stop did not settle before the shutdown timeout',
      );
      return;
    }
    if (stopped.status === 'rejected') {
      // The sequence is complete, but the transport never confirmed it closed:
      // that is the caller's to handle, so it is rethrown rather than logged
      // away (the old contract, preserved).
      throw toError(stopped.error);
    }
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
    payload: string | Uint8Array,
    error: unknown,
  ): Promise<void> {
    // warn, not error: one unreadable frame is a rejected input, not a failed
    // request (SPEC 7.1).
    this.logger.warn(
      {
        connectionId: connection.id,
        code: INVALID_ENVELOPE,
        rawLength: rawLengthOf(payload),
        reason: messageOf(error),
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
      error: { code: INVALID_ENVELOPE, message: messageOf(error) },
    });
  }

  private async handle(
    connection: TransportConnection,
    envelope: Envelope,
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
    const sessionId = state?.sessionId;
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
        sessionId: state.sessionId,
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
      // SPEC 2: the binding a handler declared takes effect only when the
      // request succeeded, so a failed login leaves no route behind. Applied
      // before the reply so a reconnect sees the new holder as soon as the
      // client can, and so the reply itself is part of the chain a later
      // handoff waits for.
      //
      // Skipped once the connection is gone: `onDisconnect` already removed its
      // route, and re-adding one here would route a future reconnect to a
      // connection id that no longer exists.
      if (
        outcome.sessionId !== undefined &&
        this.connections.get(connection.id) === state
      ) {
        this.bindSession(state, outcome.sessionId, logger);
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
    return true;
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

function toFailure(error: unknown): ExecutorOutcome {
  return {
    ok: false,
    error: { code: INTERNAL_ERROR, message: messageOf(error) },
  };
}

function elapsedSince(startedAt: number): number {
  return Math.round(performance.now() - startedAt);
}
