import type { Logger } from '../logging/types.ts';
import type { Codec } from '../shared/codec.ts';
import type { ServerEvent } from '../shared/events.ts';
import { messageOf } from '../shared/guards.ts';
import { TRANSPORT_SEND_FAILED } from '../shared/wire-codes.ts';
import type { ConnectionState } from './connection-admission.ts';
import type { SessionRouter } from './session-router.ts';

/**
 * What delivery needs from the connection layer: the two lookup tables (which
 * connection exists, which Session it holds), the wire format and the
 * per-connection ordered chain the frame must join.
 */
export interface EventDeliveryHost {
  readonly codec: Codec;
  readonly logger: Logger;
  readonly connections: ReadonlyMap<string, ConnectionState>;
  readonly router: SessionRouter;
  readonly enqueue: (
    state: ConnectionState,
    work: () => Promise<void>,
  ) => Promise<void>;
}

/**
 * Delivers one Server event (ADR-0008), best-effort: every failure is a warn
 * line, none is thrown, and nothing is buffered for a retry.
 *
 * The write joins the target connection's ordered chain rather than calling
 * `send` directly. The Server sends a result and then its events on IPC, but
 * the result's reply is written by an async continuation; without the chain a
 * later IPC message could win the race and put an event in front of the
 * response it belongs to. Joining the chain also keeps two events for the same
 * connection in the order the Server emitted them.
 */
export function deliverServerEvent(
  host: EventDeliveryHost,
  event: ServerEvent,
): Promise<void> {
  const state = resolveTarget(host, event);
  if (state === undefined) {
    return Promise.resolve();
  }
  return host.enqueue(state, () => writeEvent(host, state, event));
}

/**
 * `session` is resolved here, at delivery time, so an event follows a
 * reconnect instead of pinning the holder that existed when the handler ran.
 * A missing target is the expected case for a player who left, not an error.
 */
function resolveTarget(
  host: EventDeliveryHost,
  event: ServerEvent,
): ConnectionState | undefined {
  const target = event.target;
  if (target.kind === 'connection') {
    const state = host.connections.get(target.connectionId);
    if (state === undefined) {
      host.logger.warn(
        {
          eventId: event.eventId,
          event: event.event,
          connectionId: target.connectionId,
          code: 'EVENT_DROPPED',
          reason: 'target connection is not available',
        },
        'server event dropped',
      );
      return undefined;
    }
    return state;
  }
  const connectionId = host.router.connectionOf(target.sessionId);
  if (connectionId === undefined) {
    host.logger.warn(
      {
        eventId: event.eventId,
        event: event.event,
        sessionId: target.sessionId,
        code: 'EVENT_DROPPED',
        reason: 'no connection holds the session',
      },
      'server event dropped',
    );
    return undefined;
  }
  const state = host.connections.get(connectionId);
  if (state === undefined) {
    // The router and the connection table are updated together, so this is a
    // defensive branch: treat a stale route like a missing holder.
    host.logger.warn(
      {
        eventId: event.eventId,
        event: event.event,
        sessionId: target.sessionId,
        connectionId,
        code: 'EVENT_DROPPED',
        reason: 'the session holder is not tracked',
      },
      'server event dropped',
    );
    return undefined;
  }
  return state;
}

async function writeEvent(
  host: EventDeliveryHost,
  state: ConnectionState,
  event: ServerEvent,
): Promise<void> {
  const connectionId = state.connection.id;
  let frame: string | Uint8Array;
  try {
    frame = host.codec.encode({
      kind: 'event',
      eventId: event.eventId,
      event: event.event,
      payload: event.payload,
    });
  } catch (error) {
    // Unlike a result there is no fallback frame to send: the event is simply
    // dropped, and the request that emitted it stays successful.
    host.logger.warn(
      {
        eventId: event.eventId,
        event: event.event,
        connectionId,
        code: TRANSPORT_SEND_FAILED,
        stage: 'encode',
        reason: messageOf(error),
      },
      'server event could not be encoded',
    );
    return;
  }
  try {
    await state.connection.send(frame);
  } catch (error) {
    host.logger.warn(
      {
        eventId: event.eventId,
        event: event.event,
        connectionId,
        code: TRANSPORT_SEND_FAILED,
        stage: 'send',
        reason: messageOf(error),
      },
      'server event could not be sent',
    );
  }
}
