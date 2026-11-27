import { CONNECTION_LIMIT_CLOSE_CODE } from '../shared/close-codes.ts';
import { messageOf } from '../shared/guards.ts';
import {
  FRAMEWORK_QUEUE_FULL,
  FRAMEWORK_SHUTDOWN,
} from '../shared/wire-codes.ts';
import type { TransportConnection } from '../transport/types.ts';
import type { LayerHost } from './connection-host.ts';
import { reply } from './connection-reply.ts';
import { ConnectionWriter } from './connection-writer.ts';
import type { ConnectionLayerStatus } from './health.ts';

/**
 * SPEC 3.5 requires a hard per-connection cap. 1000 absorbs a legitimate burst
 * (a client replaying its backlog after a reconnect) while bounding the memory
 * one connection can pin; a deeper backlog means the client is flooding or the
 * handler is badly behind, and either way a human should hear about it.
 */
export const DEFAULT_MAX_QUEUED_PER_CONNECTION = 1000;

/**
 * SPEC 6.1: a cap on live connections, not on business Session lifetime. A
 * route is held by a connection, so bounding admissions also bounds the
 * routing table without refusing a binding after its handler succeeded.
 * 10k leaves room for a large deployment; an operator with more concurrent
 * players raises it explicitly.
 */
export const DEFAULT_MAX_CONNECTIONS = 10_000;

/**
 * SPEC 3.8: events allowed to wait in one buffer before new events are dropped.
 * The same bound applies to each connection's event queue and each Session's
 * pending queue. 1024 covers a burst a healthy transport is still draining while
 * capping the memory one slow reader can pin; events are best-effort, so a
 * deeper backlog is better dropped than buffered.
 */
export const DEFAULT_MAX_QUEUED_EVENTS = 1024;

/**
 * SPEC 3.8: byte side of the same bound. 4 MiB matches the frame cap's order of
 * magnitude: a client that cannot drain a few MiB of events is not going to
 * catch up, and unbounded growth is exactly the failure INV-020 forbids.
 */
export const DEFAULT_MAX_QUEUED_EVENT_BYTES = 4 * 1024 * 1024;

/**
 * Aggregate event bytes above which the layer warns once (SPEC 3.8, 6.6). The
 * per-queue bounds only cap one slow reader; many slow readers add up to a
 * number no single bound sees, and the default envelope (connection cap times
 * per-queue bytes) is far larger than any machine. 1 GiB is two orders of
 * magnitude above the few MiB a healthy backlog holds, so crossing it means the
 * deployment is short of capacity rather than applying normal backpressure.
 * Operators size it against container memory; the warning never rejects or
 * drops an event.
 */
export const DEFAULT_EVENT_BACKLOG_WARN_BYTES = 1024 * 1024 * 1024;

export interface ConnectionState {
  readonly connection: TransportConnection;
  /**
   * The connection's single writer (SPEC 3.8): request work runs first, one
   * queued event follows each completed request, and events fill the idle
   * moments between requests. Ordering state is per connection.
   */
  readonly writer: ConnectionWriter;
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
 * What admission and the refusal paths need from the layer: fixed limits, the
 * live connection table, the wire format for answering a refused frame, and the
 * backlog a new connection's queue reports to. The status is passed per call,
 * because a refusal must observe it at the moment the transport delivers the
 * connection or message.
 */
export type AdmissionHost = Pick<
  LayerHost,
  | 'codec'
  | 'logger'
  | 'connections'
  | 'maxConnections'
  | 'maxQueuedPerConnection'
  | 'maxQueuedEvents'
  | 'maxQueuedEventBytes'
  | 'backlog'
>;

export function onConnect(
  host: AdmissionHost,
  status: ConnectionLayerStatus,
  connection: TransportConnection,
): Promise<void> {
  if (status === 'stopping' || status === 'stopped') {
    // SPEC 6.3 step 1: no new connection is admitted once shutdown started.
    // Not registering it is enough: `onMessage` refuses on status before it
    // ever looks the connection up, so nothing is silently dropped.
    return Promise.resolve();
  }
  if (host.connections.size >= host.maxConnections) {
    // Admission is the only point before a request is read, so it is the only
    // place a refusal has no side effect. The client is told to retry rather
    // than left waiting on a connection this layer will not serve.
    host.logger.warn(
      { connectionId: connection.id, limit: host.maxConnections },
      'connection refused: connection limit reached',
    );
    return closeRefused(host, connection);
  }
  const state: ConnectionState = {
    connection,
    writer: new ConnectionWriter({
      maxQueuedEvents: host.maxQueuedEvents,
      maxQueuedEventBytes: host.maxQueuedEventBytes,
      backlog: host.backlog,
      logger: host.logger,
    }),
    queued: 0,
    inFlight: new Set(),
    closed: new AbortController(),
  };
  host.connections.set(connection.id, state);
  return Promise.resolve();
}

/** Best effort: a failed close must not escape into the transport. */
async function closeRefused(
  host: AdmissionHost,
  connection: TransportConnection,
): Promise<void> {
  try {
    await connection.close(
      CONNECTION_LIMIT_CLOSE_CODE,
      'connection limit reached',
    );
  } catch (error) {
    host.logger.warn(
      { connectionId: connection.id, reason: messageOf(error) },
      'refused connection could not be closed',
    );
  }
}

export function refuseQueue(
  host: AdmissionHost,
  state: ConnectionState,
  id: string,
  method?: string,
  requestId?: string,
): Promise<void> {
  host.logger.warn(
    {
      connectionId: state.connection.id,
      ...(requestId === undefined ? {} : { requestId }),
      clientRequestId: id,
      method,
      code: FRAMEWORK_QUEUE_FULL,
      queued: state.queued,
      limit: host.maxQueuedPerConnection,
    },
    'request refused: input queue is full',
  );
  return reply(host, state.connection, {
    id,
    ok: false,
    error: {
      code: FRAMEWORK_QUEUE_FULL,
      message: `input queue is full (${host.maxQueuedPerConnection} waiting)`,
    },
  });
}

/** SPEC 6.3 step 2: input arriving after shutdown started gets a stable code. */
export async function refuseAfterShutdown(
  host: AdmissionHost,
  connection: TransportConnection,
  payload: string | Uint8Array,
): Promise<void> {
  let id: string;
  try {
    id = host.codec.decode(payload).id;
  } catch {
    // Unreadable frame during shutdown: still answer, with no correlation.
    id = '';
  }
  await refuseWithId(host, connection, id);
}

export async function refuseWithId(
  host: AdmissionHost,
  connection: TransportConnection,
  id: string,
  requestId?: string,
): Promise<void> {
  host.logger.warn(
    {
      connectionId: connection.id,
      ...(requestId === undefined ? {} : { requestId }),
      clientRequestId: id,
      code: FRAMEWORK_SHUTDOWN,
      reason: 'request arrived after shutdown started',
    },
    'request refused',
  );
  await reply(host, connection, {
    id,
    ok: false,
    error: {
      code: FRAMEWORK_SHUTDOWN,
      message: 'connection layer is shutting down',
    },
  });
}

export interface DisconnectHooks {
  /** Drops the Session route this connection holds. */
  releaseRoute(connectionId: string): void;
  /** Ends one accepted request whose result nobody can read (SPEC 3.6). */
  cancel(requestId: string, connectionId: string): Promise<void>;
}

export async function onDisconnect(
  host: AdmissionHost,
  connection: TransportConnection,
  hooks: DisconnectHooks,
): Promise<void> {
  const state = host.connections.get(connection.id);
  // The id is the map key, but the connection object is the identity: a stale
  // disconnect that arrives after a replacement reused the id must not delete
  // the new state or cancel its requests. The contract is that transport ids
  // are unique for the life of the layer (SPEC 5.2); this guard keeps a
  // violating transport from corrupting the replacement's routing.
  if (state === undefined || state.connection !== connection) {
    return;
  }
  host.connections.delete(connection.id);
  hooks.releaseRoute(connection.id);
  // Only this connection's wait is released; other holders still wait for its
  // framework request completion, even when cancellation settles later.
  state.closed.abort();
  // SPEC 3.6 default policy: a connection closing mid-request cancels it. The
  // handler may still be running, but nobody can read its result, and letting
  // it commit would leave a state change the player never saw.
  const cancels: Promise<void>[] = [];
  for (const requestId of state.inFlight) {
    cancels.push(hooks.cancel(requestId, connection.id));
  }
  state.inFlight.clear();
  await Promise.all(cancels);
}
