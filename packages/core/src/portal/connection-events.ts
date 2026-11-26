import type { Logger } from '../logging/types.ts';
import type { Codec } from '../shared/codec.ts';
import type { ServerEvent } from '../shared/events.ts';
import { messageOf } from '../shared/guards.ts';
import { TRANSPORT_SEND_FAILED } from '../shared/wire-codes.ts';
import type { ConnectionState } from './connection-admission.ts';
import type { SessionRouter } from './session-router.ts';

/**
 * What delivery needs from the connection layer: the two lookup tables (which
 * connection exists, which Session it holds), the wire format, the
 * per-connection ordered chain the frame must join, and the per-Session queue
 * prefix that keeps Session-addressed events in arrival order.
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
  /**
   * Tail of each Session's event queue. Owned by the connection layer rather
   * than the module, so ordering state is per layer (and per test) instead of
   * process-global. An entry exists only while that Session has unfinished
   * delivery work.
   */
  readonly sessionTails: Map<string, Promise<void>>;
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
  // The Session id is the address, not the holder: each event resolves the
  // holder when its turn starts and again right before the write. The Session
  // queue serializes events across a takeover: the holder may change, but the
  // emission order must not.
  return enqueueSessionEvent(host, target.sessionId, event);
}

/**
 * Queues a Session-addressed event behind the previous one for the same
 * Session. Two events for one Session are processed one at a time and in
 * arrival order; the later one waits until the earlier finished writing or was
 * dropped. Without this, a takeover would let a later event overtake an
 * earlier one still waiting on the old holder's chain, and the new holder would
 * receive them out of order (SPEC 3.8).
 */
function enqueueSessionEvent(
  host: EventDeliveryHost,
  sessionId: string,
  event: ServerEvent,
): Promise<void> {
  const previous = host.sessionTails.get(sessionId);
  // With no queued event for this Session, start now rather than in a
  // continuation: an event that follows synchronously (for another Session or a
  // pinned connection) must not overtake it on a shared connection chain.
  const delivery =
    previous === undefined
      ? deliverSessionEvent(host, sessionId, event)
      : previous.then(() => deliverSessionEvent(host, sessionId, event));
  // Mirrors the connection chain: one failed delivery must not poison every
  // later event for the Session.
  const tail = delivery.then(
    () => undefined,
    () => undefined,
  );
  host.sessionTails.set(sessionId, tail);
  // Drop the entry once this is the last delivery for the Session; otherwise
  // the map would retain one resolved promise per Session ever addressed.
  void tail.then(() => {
    if (host.sessionTails.get(sessionId) === tail) {
      host.sessionTails.delete(sessionId);
    }
    return undefined;
  });
  return delivery;
}

type DeliveryOutcome = 'written' | 'dropped' | 'moved' | 'aborted';

/**
 * Delivers one Session event while holding that Session's serial slot. The
 * holder is resolved twice: once to pick the chain to join, again inside that
 * chain right before the write, because a takeover may land while the event
 * waits. A moved holder earns one redelivery (the existing bound); a second
 * change is dropped.
 */
async function deliverSessionEvent(
  host: EventDeliveryHost,
  sessionId: string,
  event: ServerEvent,
): Promise<void> {
  let redeliveries = 0;
  while (true) {
    const state = holderState(host, sessionId, event);
    if (state === undefined) {
      return;
    }
    // oxlint-disable-next-line eslint/no-await-in-loop -- a redelivery must observe the previous attempt before re-resolving the holder
    const outcome = await deliverOnChain(host, sessionId, state, event);
    if (outcome === 'written' || outcome === 'dropped') {
      return;
    }
    if (outcome === 'aborted') {
      // The connection whose chain this event joined closed while the event
      // still waited behind earlier work. The chain does not settle on
      // disconnect, so waiting for the queued callback could pin the Session's
      // serial slot forever; drop the event and let the next one proceed.
      warnDropped(
        host,
        event,
        {
          targetKind: 'session',
          sessionId,
          connectionId: state.connection.id,
        },
        'the target connection closed while the event waited',
      );
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
    redeliveries += 1;
  }
}

/**
 * Joins the current holder's ordered chain and reports what its turn observed.
 * The write re-resolves the holder, so a takeover between queueing and
 * execution is handled here rather than by pinning the old connection. The
 * abort race is what keeps a Session queue from waiting forever on a chain that
 * was removed: `appendToChain` still runs queued work after a disconnect, but
 * only once its predecessor settles, which may never happen.
 */
async function deliverOnChain(
  host: EventDeliveryHost,
  sessionId: string,
  queuedFor: ConnectionState,
  event: ServerEvent,
): Promise<DeliveryOutcome> {
  const signal = queuedFor.closed.signal;
  if (signal.aborted) {
    return 'aborted';
  }
  const result = Promise.withResolvers<DeliveryOutcome>();
  let cancelled = false;
  const onAbort = (): void => {
    cancelled = true;
    result.resolve('aborted');
  };
  signal.addEventListener('abort', onAbort, { once: true });
  try {
    const queued = host.enqueue(queuedFor, async () => {
      // The abort race already reported this event; writing now would only
      // target a connection that is gone.
      if (cancelled) {
        return;
      }
      const current = holderState(host, sessionId, event);
      if (current === undefined) {
        result.resolve('dropped');
        return;
      }
      if (current !== queuedFor) {
        result.resolve('moved');
        return;
      }
      await writeEvent(host, current, event);
      result.resolve('written');
    });
    // A rejected chain link must not strand the Session queue. The connection
    // chain itself already catches, so this is defensive.
    void queued.catch(() => result.resolve('dropped'));
    return await result.promise;
  } finally {
    signal.removeEventListener('abort', onAbort);
  }
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
