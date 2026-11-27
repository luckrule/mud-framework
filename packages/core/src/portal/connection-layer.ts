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
  DEFAULT_EVENT_BACKLOG_WARN_BYTES,
  DEFAULT_MAX_CONNECTIONS,
  DEFAULT_MAX_QUEUED_EVENTS,
  DEFAULT_MAX_QUEUED_EVENT_BYTES,
  DEFAULT_MAX_QUEUED_PER_CONNECTION,
  closeForBacklog,
  onConnect as admitConnection,
  onDisconnect as disconnectConnection,
} from './connection-admission.ts';
import type {
  EventDeliveryHost,
  SessionEventQueue,
} from './connection-events.ts';
import {
  deliverServerEvent,
  discardSessionQueues,
} from './connection-events.ts';
import type { LayerHost } from './connection-host.ts';
import type { RequestHost } from './connection-request.ts';
import {
  cancelQuietly,
  onMessage as receiveMessage,
} from './connection-request.ts';
import { EventBacklog } from './event-backlog.ts';
import { EventDropLog } from './event-drop-log.ts';
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

/**
 * Slack added to a transport's declared send bound to form this layer's write
 * bound (SPEC 3.8): one downlink write, an event frame and a response frame
 * alike.
 *
 * The transport's bound is the one that can explain a stalled write: its own
 * timeout logs the more specific failure and terminates the connection. Two
 * equal bounds race instead, and when this layer's timer wins the log says only
 * "the transport did not settle the write" — the operator cannot tell a stalled
 * socket from a broken transport timeout. Making this bound strictly wider
 * leaves the transport's verdict observable. Same order as
 * `SHUTDOWN_ACK_SLACK_MS`: enough for timer granularity and event-loop lag
 * between the two registrations, negligible against a send bound.
 */
export const EVENT_SEND_SLACK_MS = 250;

/**
 * Largest delay `setTimeout` accepts. Bun clamps anything above it to 1ms, so a
 * derived bound that overflowed this would release the writer almost at once —
 * the opposite of what the bound is for (SPEC 3.8). Exported so the option
 * tests can name the same ceiling rather than hard-code it.
 */
export const MAX_TIMER_DELAY_MS = 2_147_483_647;

/**
 * The bound on one downlink write when neither the caller nor the transport
 * supplies one (SPEC 3.8): an event frame and a response frame alike.
 * `Transport.sendTimeoutMs` is optional, so this case is reachable: without a
 * bound, a send that never settles would pin the Session's serial slot, and with
 * it the connection's single writer and the Session's terminal barrier, forever.
 * 30s matches the WS transport's own default, so a stack that declares nothing
 * behaves as it did before the declaration existed.
 *
 * Named for the case it covers rather than as a general default: delivery
 * receives the resolved number and must not carry a second fallback that could
 * disagree with this one. No slack is added here: nothing else bounds the
 * write, so there is no competing timer to lose to and the default is the only
 * verdict available.
 */
export const UNDECLARED_EVENT_SEND_TIMEOUT_MS = 30_000;

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
   * Server events allowed to wait in one event buffer (SPEC 3.8). The bound
   * applies to each connection's event queue and to each Session's pending
   * queue separately, not to a per-connection total. Defaults to
   * `DEFAULT_MAX_QUEUED_EVENTS`. Must be a positive integer.
   */
  maxQueuedEvents?: number;
  /**
   * The same bound in encoded frame bytes (SPEC 3.8), with the same per-queue
   * scope. Defaults to `DEFAULT_MAX_QUEUED_EVENT_BYTES`. Must be a positive
   * integer.
   */
  maxQueuedEventBytes?: number;
  /**
   * Aggregate event bytes above which the layer warns once (SPEC 3.8). The
   * per-queue bounds cap one slow reader each; this one observes what all of
   * them together hold, which is what the deployment has to fit in memory.
   * Defaults to `DEFAULT_EVENT_BACKLOG_WARN_BYTES`. Must be a positive integer.
   *
   * It is an observation, not an admission rule: crossing it logs
   * `EVENT_BACKLOG_HIGH` and nothing else. A global cap here would let one loud
   * emitter drop events for every connection, which is worse than the backlog.
   */
  eventBacklogWarnBytes?: number;
  /**
   * Ceiling on each shutdown wait: draining in-flight requests, closing the
   * executor and stopping the transport (SPEC 6.3). Defaults to
   * `DEFAULT_STOP_TIMEOUT_MS`. Must be a finite number of at least 1, no greater
   * than the largest usable timer delay; a longer value would be clamped to 1ms
   * and drop every queued request instead of waiting for it.
   *
   * This is a floor, not the executor's ceiling: when the executor can bound
   * its own `close()` (`RequestExecutor.closeBudgetMs`, which `IpcExecutor`
   * does), the executor step is given at least that long. The two used to be
   * independent, and a short value here made `stop()` return while the Server
   * process was still draining and writing. That derived budget is capped at
   * the largest usable timer delay, because it grows with this value and would
   * otherwise cross the ceiling this option itself is checked against.
   */
  shutdownTimeoutMs?: number;
  /**
   * Connections this layer admits at once (SPEC 6.1). Defaults to
   * `DEFAULT_MAX_CONNECTIONS`. This is an admission cap, not a business
   * Session lifetime limit: a connection beyond it is closed (code `1013`)
   * before any request on it is read. Must be a positive integer.
   */
  maxConnections?: number;
  /**
   * Upper bound on one downlink write: a Server event frame and a response
   * frame alike (SPEC 3.8). Defaults to the transport's declared
   * `Transport.sendTimeoutMs` plus `EVENT_SEND_SLACK_MS` when that declaration
   * is a finite number of at least 1, and to `UNDECLARED_EVENT_SEND_TIMEOUT_MS`
   * otherwise. Deriving the bound from the transport keeps the Session's serial
   * slot held until the transport itself would fail the write, so the slot is
   * never released while a frame is still being written (SPEC 3.8). The slack
   * makes this bound strictly wider than the transport's, so a stalled write is
   * reported by the transport's own timeout rather than by this layer's
   * fallback.
   *
   * An explicit value here is the layer's own bound and wins over the
   * declaration; no slack is added to it, because it replaces the derivation
   * rather than competing with a transport timer it was meant to outlast. Must
   * be a positive finite number no greater than the largest usable timer delay;
   * a longer value would be clamped to 1ms, turning the widest bound into the
   * narrowest.
   *
   * It bounds the write, not the disconnect: once a frame is being written an
   * abort does not release the Session's serial slot, because the bytes may
   * already be on the wire (SPEC 3.8).
   */
  eventSendTimeoutMs?: number;
}

/**
 * Wires a transport to an executor. It owns only what sits between the two:
 * decode, per-connection ordering, Session routing, error mapping and encode
 * (SPEC 5.2). The routing table and its ordering gate live in `SessionRouter`;
 * this layer applies the gate at the head of the connection's request queue and
 * carries no business state, which belongs to the business process (SPEC 6.1).
 * Replay is `IpcExecutor`'s (SPEC 6.4) and input buffering is the `queued`
 * counter itself (SPEC 3.5).
 */
export class ConnectionLayer implements TransportHandlers {
  private readonly transport: Transport;
  private readonly executor: RequestExecutor;
  private readonly codec: Codec;
  private readonly logger: Logger;
  private readonly createRequestId: () => string;
  private readonly shutdownTimeoutMs: number;
  /** Bound on every downlink write: event frames and response frames (SPEC 3.8). */
  private readonly sendTimeoutMs: number;
  private readonly connections = new Map<string, ConnectionState>();
  /** Per-Session serialization and bounds for Server events (ADR-0008). */
  private readonly sessionQueues = new Map<string, SessionEventQueue>();
  /** Every event buffer reports here, so health sees detached queues too. */
  private readonly backlog: EventBacklog;
  /**
   * Aggregates `EVENT_DROPPED` lines (SPEC 7.1). Stateless across shutdown: a
   * restart is a new layer with a fresh log.
   */
  private readonly dropLog: EventDropLog;
  /** Which connection holds which Session, and the reconnect gate (SPEC 3.2). */
  private readonly router: SessionRouter;
  /** One host object; each module takes the narrow `Pick` it declares. */
  private readonly requestHost: RequestHost;
  /** The same host, viewed through delivery's narrower `Pick` (ADR-0008). */
  private readonly eventHost: EventDeliveryHost;
  /** Set while the layer is subscribed to the executor's event stream. */
  private unsubscribeEvents: (() => void) | undefined;
  /** False from the first instant of shutdown; no event is accepted after it. */
  private eventsOpen = true;
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
    const maxQueuedEvents =
      options.maxQueuedEvents ?? DEFAULT_MAX_QUEUED_EVENTS;
    assertPositiveInteger(maxQueuedEvents, 'maxQueuedEvents');
    const maxQueuedEventBytes =
      options.maxQueuedEventBytes ?? DEFAULT_MAX_QUEUED_EVENT_BYTES;
    assertPositiveInteger(maxQueuedEventBytes, 'maxQueuedEventBytes');
    const eventBacklogWarnBytes =
      options.eventBacklogWarnBytes ?? DEFAULT_EVENT_BACKLOG_WARN_BYTES;
    assertPositiveInteger(eventBacklogWarnBytes, 'eventBacklogWarnBytes');
    const shutdownTimeoutMs =
      options.shutdownTimeoutMs ?? DEFAULT_STOP_TIMEOUT_MS;
    assertTimerDelay(shutdownTimeoutMs, 'shutdownTimeoutMs');
    const sendTimeoutMs = resolveSendTimeoutMs(options);
    this.transport = options.transport;
    this.executor = options.executor;
    this.codec = options.codec ?? new JsonCodec();
    this.logger = safeLogger(
      options.logger ??
        createJsonLogger({ bindings: { component: 'connection-layer' } }),
    );
    this.createRequestId = options.createRequestId ?? randomUUID;
    this.shutdownTimeoutMs = shutdownTimeoutMs;
    this.sendTimeoutMs = sendTimeoutMs;
    this.backlog = new EventBacklog({
      logger: this.logger,
      warnBytes: eventBacklogWarnBytes,
      liveConnections: () => this.connections.size,
    });
    this.router = new SessionRouter({ logger: this.logger });
    this.dropLog = new EventDropLog({ logger: this.logger });
    const host: LayerHost = {
      codec: this.codec,
      logger: this.logger,
      connections: this.connections,
      maxConnections,
      maxQueuedPerConnection,
      maxQueuedEvents,
      maxQueuedEventBytes,
      backlog: this.backlog,
      dropLog: this.dropLog,
      createRequestId: this.createRequestId,
      router: this.router,
      executor: this.executor,
      enqueue: (state, work, queued) => state.writer.submit(work, queued),
      queueEvent: (state, write) => state.writer.enqueue(write),
      cancelEvent: (state, write) => state.writer.cancel(write),
      closeForBacklog: (state, overflow) =>
        closeForBacklog(host, state, overflow),
      acceptingEvents: () => this.eventsOpen,
      sessionQueues: this.sessionQueues,
      sendTimeoutMs: this.sendTimeoutMs,
    };
    this.requestHost = host;
    this.eventHost = host;
  }

  async start(): Promise<void> {
    if (this.status !== 'idle') {
      throw new Error(`connection layer cannot start while ${this.status}`);
    }
    this.status = 'starting';
    this.executorStart = Promise.resolve().then(() => this.executor.start());
    // SPEC 6.2: readiness must precede listener startup.
    try {
      // Subscribed before the transport accepts input, so no request can
      // commit an event that arrives before the sink exists. Best-effort:
      // delivery failures are the delivery module's to log (ADR-0008).
      this.unsubscribeEvents = this.executor.onEvent((event) => {
        // Delivery already contains its own failures; this keeps an unexpected
        // rejection out of the process's unhandled-rejection path.
        void deliverServerEvent(this.eventHost, event).catch(() => undefined);
      });
      await this.executorStart;
      if (this.status !== 'starting') {
        throw new Error('connection layer startup interrupted by shutdown');
      }
      this.transportStart = Promise.resolve().then(() =>
        this.transport.start(this),
      );
      await this.transportStart;
    } catch (error) {
      this.unsubscribeEvents?.();
      this.unsubscribeEvents = undefined;
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
    // Events are additional, not owed (ADR-0008): once the layer stops, an
    // event that would be delivered mid-shutdown is dropped by the executor
    // instead of racing the transport close.
    this.eventsOpen = false;
    this.unsubscribeEvents?.();
    this.unsubscribeEvents = undefined;
    // Queued frames cannot be written any more, and shutdown does not wait for
    // the unreliable event path to drain (SPEC 3.8). A frame already handed to
    // the transport is left to its own send bound.
    const discarded = this.discardEventBacklogs();
    if (discarded.count > 0) {
      this.logger.warn(
        {
          code: 'EVENT_BACKLOG_DISCARDED',
          reason: 'shutdown',
          count: discarded.count,
          connections: discarded.connections,
          frames: discarded.frames,
          bytes: discarded.bytes,
        },
        'server event backlog discarded at shutdown',
      );
    }
    const timeoutMs = this.shutdownTimeoutMs;
    // Waiting requests may fail during shutdown, never bypass their barrier.
    for (const state of this.connections.values()) {
      state.closed.abort();
    }

    // SPEC 6.3 step 2: drain what was already accepted. New input is refused,
    // so the accepted request work can only shrink. Bounded because a handler
    // that never returns would otherwise keep the process alive forever, which
    // is worse than the leak the wait is meant to avoid.
    const drained = await settleWithin(
      Promise.all(
        [...this.connections.values()].map(
          (state) => state.writer.requestsSettled,
        ),
      ),
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
    // Requests still waiting for their turn cannot start within what is left of
    // the drain budget: a started event frame can occupy the writer for its own
    // send bound, which exceeds the shutdown budget (SPEC 3.8). Dropping them
    // here, by name, replaces the silent drop that `connections.clear()` used to
    // cause when they woke up on a gone connection. Started work is untouched.
    this.discardQueuedRequests();
    this.connections.clear();
    // SPEC 6.1: routes outlive neither their connections nor the layer. Leaving
    // them behind would make a restarted layer route a reconnect to a
    // connection id that no longer exists. The Session event queues are cleared
    // with them: a delivery still parked on a dropped queue can never write,
    // and keeping the entry would retain one per Session addressed.
    this.router.clear();
    this.sessionQueues.clear();

    // SPEC 6.3 step 3: close the executor (and with it the Server and storage)
    // before the transport, so no write can land after the connection that
    // would report it is gone. Closing storage while a request still held a
    // transaction is why the drain above comes first.
    //
    // The budget is the larger of this layer's own timeout and the executor's
    // declared worst case plus slack. A remote executor has to drain, ask the
    // Server to stop and wait for the process to go; giving it less would make
    // `stop()` return while the Server was still writing, which is the defect
    // this budget exists to prevent. A local executor declares nothing, so its
    // step keeps the layer's own timeout.
    //
    // Capped at the largest timer delay because this value is derived, not
    // declared: `closeBudgetMs` grows with `shutdownTimeoutMs`, which may itself
    // be that ceiling, so the sum would cross it and Bun would clamp the wait to
    // 1ms — abandoning the executor at once and rejecting `stop()` on the tick
    // it started. Truncating a derived bound is the honest repair; an explicit
    // option is rejected instead (see `assertTimerDelay`).
    const executorBudgetMs =
      this.executor.closeBudgetMs === undefined
        ? timeoutMs
        : Math.min(
            Math.max(
              timeoutMs,
              this.executor.closeBudgetMs + EXECUTOR_CLOSE_SLACK_MS,
            ),
            MAX_TIMER_DELAY_MS,
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

    let transportFailure: Error | undefined;
    // SPEC 6.3 step 4: best-effort transport close, propagated even when the
    // executor step already failed.
    const stopped = await settleWithin(this.stopTransport(), timeoutMs);
    this.status = 'stopped';
    if (stopped.status === 'timeout') {
      // SPEC 6.3: an unconfirmed transport close is a failed close, not a
      // logged footnote. Reporting success would tell the caller the listener
      // was released when it may still be running.
      transportFailure = new Error(
        'transport stop did not settle before the shutdown timeout',
      );
      this.logger.error(
        { timeoutMs },
        'transport stop did not settle before the shutdown timeout',
      );
    } else if (stopped.status === 'rejected') {
      transportFailure = toError(stopped.error);
    }
    if (executorFailure !== undefined && transportFailure !== undefined) {
      throw new AggregateError(
        [executorFailure, transportFailure],
        'executor and transport failed to close',
      );
    }
    // The sequence is complete, but the transport never confirmed it closed:
    // that is the caller's to handle, so it is thrown rather than logged away.
    if (transportFailure !== undefined) {
      throw transportFailure;
    }
    if (executorFailure !== undefined) {
      throw executorFailure;
    }
  }

  /**
   * Drops every accepted request that never started, one warn line each
   * (SPEC 3.8, 6.3). Without this the request would wake up on a cleared table
   * and be reported as `TRANSPORT_CLOSED` — the wrong reason, since the client
   * is still connected and it was shutdown that ended it. Work that already
   * started keeps the existing drain and timeout path.
   */
  private discardQueuedRequests(): void {
    for (const [connectionId, state] of this.connections) {
      for (const identity of state.writer.discardQueuedRequests()) {
        this.logger.warn(
          {
            requestId: identity.requestId,
            method: identity.method,
            ...(identity.clientRequestId === undefined
              ? {}
              : { clientRequestId: identity.clientRequestId }),
            connectionId,
            code: 'REQUEST_DROPPED_AT_SHUTDOWN',
            reason:
              'the request did not start within the shutdown drain budget',
          },
          'request dropped at shutdown',
        );
      }
    }
  }

  /**
   * Drops every accepted event that has not started writing (SPEC 3.8). Frames
   * already handed to the transport are left alone; their send bound ends them.
   * The totals describe what was discarded, so shutdown can report it once.
   */
  private discardEventBacklogs(): {
    count: number;
    connections: number;
    frames: number;
    bytes: number;
  } {
    let connections = 0;
    let frames = 0;
    let bytes = 0;
    for (const buffer of this.backlog.connectionBuffers) {
      const discarded = buffer.discardQueued();
      if (discarded.frames === 0) {
        continue;
      }
      connections += 1;
      frames += discarded.frames;
      bytes += discarded.bytes;
    }
    const sessions = discardSessionQueues(this.sessionQueues, this.backlog);
    frames += sessions.frames;
    bytes += sessions.bytes;
    // One event is one frame, so the count and the frame total coincide today;
    // both are reported because the log contract names both.
    return { count: frames, connections, frames, bytes };
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
      this.backlog.snapshot(),
    );
  }
}

/**
 * Resolves the bound on one downlink write — an event frame or a response
 * frame —: the layer's explicit option first, then the transport's declaration
 * plus `EVENT_SEND_SLACK_MS`, then this layer's default.
 *
 * A transport's value is only taken when it is usable (finite and at least 1).
 * `Infinity` is legal for a transport — `WsTransport` accepts it to disable its
 * own send timeout — but it must not become this bound: an unbounded wait would
 * pin the Session's serial slot, or the connection's single writer, forever, the
 * very failure the bound prevents. Such a declaration counts as "no
 * declaration" and falls back to the default. The fallback is a complete bound
 * of its own, so it takes no slack: nothing else bounds the write, and there is
 * no transport timer whose verdict the slack would preserve.
 *
 * An explicit option is used verbatim. It replaces the derivation rather than
 * standing beside it, so it carries no slack; a caller that wants the
 * transport's bound kept observable leaves the option unset.
 *
 * The derived sum is capped at the largest usable timer delay. A transport may
 * declare a value close to that ceiling, and the addition would push it past
 * the point where `setTimeout` clamps the delay to 1ms — turning the widest
 * possible bound into the narrowest one.
 */
function resolveSendTimeoutMs(options: ConnectionLayerOptions): number {
  if (options.eventSendTimeoutMs !== undefined) {
    assertTimerDelay(options.eventSendTimeoutMs, 'eventSendTimeoutMs');
    return options.eventSendTimeoutMs;
  }
  const declared = options.transport.sendTimeoutMs;
  if (declared !== undefined && Number.isFinite(declared) && declared >= 1) {
    return Math.min(declared + EVENT_SEND_SLACK_MS, MAX_TIMER_DELAY_MS);
  }
  return UNDECLARED_EVENT_SEND_TIMEOUT_MS;
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

function assertFiniteAtLeastOne(
  value: unknown,
  name: string,
): asserts value is number {
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

/**
 * A configured delay must also fit `setTimeout`. Above `MAX_TIMER_DELAY_MS` Bun
 * clamps the delay to 1ms, so the longest wait an operator can ask for becomes
 * the shortest: shutdown would drop every queued request, and a send bound would
 * release the writer while the frame may still be on the wire. Rejected rather
 * than capped because the value is the operator's stated intent; silently
 * shortening it is the same inversion, and a delay this far out of range is a
 * configuration error worth reporting rather than quietly repairing.
 *
 * Every caller of this is a timer delay, so the ceiling applies to all of them.
 */
function assertTimerDelay(
  value: unknown,
  name: string,
): asserts value is number {
  assertFiniteAtLeastOne(value, name);
  if (value > MAX_TIMER_DELAY_MS) {
    throw new RangeError(
      `${name} must not exceed ${MAX_TIMER_DELAY_MS}ms (the largest setTimeout delay), got ${String(value)}`,
    );
  }
}
