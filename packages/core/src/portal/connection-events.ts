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
 * A Session-addressed event may chase one takeover. A second change before the
 * redelivery runs means the Session moved again mid-handoff; chasing it further
 * would let two connections hand the event back and forth, so it is dropped.
 */
const MAX_SESSION_REDELIVERIES = 1;

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
  const target = event.target;
  if (target.kind === 'connection') {
    const state = host.connections.get(target.connectionId);
    if (state === undefined) {
      warnDropped(
        host,
        event,
        { targetKind: 'connection', connectionId: target.connectionId },
        'target connection is not available',
      );
      return Promise.resolve();
    }
    // A connection id is the address itself (SPEC 5.2), so it stays pinned even
    // when the Session that used to live there moves on.
    return host.enqueue(state, () => writeEvent(host, state, event));
  }
  // The Session id is the address, not the holder. Resolving here only picks
  // the chain to join; `deliverToSession` resolves again before the write.
  const state = holderState(host, target.sessionId, event);
  if (state === undefined) {
    return Promise.resolve();
  }
  return host.enqueue(state, () =>
    deliverToSession(host, target.sessionId, state, event, 0),
  );
}

/**
 * Writes a Session-addressed event to the holder current at write time, not the
 * one that held the Session when the event was queued. A frame can wait behind
 * the old connection's chain long enough for a reconnect to move the Session,
 * and writing it to the old connection would address a player who no longer
 * owns it (SPEC 3.8). A missing holder is the expected case for a player who
 * left, not an error.
 */
async function deliverToSession(
  host: EventDeliveryHost,
  sessionId: string,
  queuedFor: ConnectionState,
  event: ServerEvent,
  redeliveries: number,
): Promise<void> {
  const state = holderState(host, sessionId, event);
  if (state === undefined) {
    return;
  }
  // `queuedFor` only identifies the chain this delivery was queued on; it is
  // never the write target. Equality means the holder did not move.
  if (state === queuedFor) {
    await writeEvent(host, state, event);
    return;
  }
  if (redeliveries >= MAX_SESSION_REDELIVERIES) {
    warnDropped(
      host,
      event,
      { targetKind: 'session', sessionId },
      'the session holder changed again while redelivering',
    );
    return;
  }
  // Joining the new holder's chain keeps that connection's own order. The
  // requeue is not awaited: the old connection's chain must not wait on another
  // connection's work, which could close a cycle through the Session barrier.
  void host
    .enqueue(state, () =>
      deliverToSession(host, sessionId, state, event, redeliveries + 1),
    )
    .catch(() => undefined);
}

/** The tracked connection holding `sessionId`, or a logged drop. */
function holderState(
  host: EventDeliveryHost,
  sessionId: string,
  event: ServerEvent,
): ConnectionState | undefined {
  const connectionId = host.router.connectionOf(sessionId);
  if (connectionId === undefined) {
    warnDropped(
      host,
      event,
      { targetKind: 'session', sessionId },
      'no connection holds the session',
    );
    return undefined;
  }
  const state = host.connections.get(connectionId);
  if (state === undefined) {
    // The router and the connection table are updated together, so this is a
    // defensive branch: treat a stale route like a missing holder.
    warnDropped(
      host,
      event,
      { targetKind: 'session', sessionId, connectionId },
      'the session holder is not tracked',
    );
    return undefined;
  }
  return state;
}

type DropTarget =
  | { readonly targetKind: 'connection'; readonly connectionId: string }
  | {
      readonly targetKind: 'session';
      readonly sessionId: string;
      readonly connectionId?: string;
    };

function warnDropped(
  host: EventDeliveryHost,
  event: ServerEvent,
  target: DropTarget,
  reason: string,
): void {
  host.logger.warn(
    {
      eventId: event.eventId,
      event: event.event,
      ...target,
      code: 'EVENT_DROPPED',
      reason,
    },
    'server event dropped',
  );
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
