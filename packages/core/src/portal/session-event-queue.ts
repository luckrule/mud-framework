import type { ServerEvent } from '../shared/events.ts';
import { messageOf } from '../shared/guards.ts';
import type { ConnectionState } from './connection-state.ts';
import { BACKLOG_CLOSED_REASON } from './connection-state.ts';
import type { EventBacklog } from './event-backlog.ts';
import type { EncodedEvent, EventDeliveryHost } from './event-frame.ts';
import {
  warnDropped,
  watchQueuedWrite,
  writeEventFrame,
} from './event-frame.ts';
import type { EventOutcome } from './event-write.ts';
import { EventWrite } from './event-write.ts';
import type {
  InHandFrame,
  SessionEventQueue,
  SessionQueuedEvent,
} from './session-queue-state.ts';
import { createSessionQueue } from './session-queue-state.ts';

/**
 * Serial delivery for one Session: its pending queue, the pump that holds the
 * Session's slot, and the takeover redelivery a moved holder earns. A Session
 * outlives the connection that holds it, so this ordering lives above any one
 * connection queue.
 */

/**
 * A Session-addressed event may chase one takeover. A second change before the
 * redelivery runs means the Session moved again mid-handoff; chasing it further
 * would let two connections hand the event back and forth, so it is dropped.
 */
const MAX_SESSION_REDELIVERIES = 1;

/**
 * True when this in-hand frame was released by a connection queue for a
 * takeover redelivery and no buffer holds it now (SPEC 3.8).
 *
 * This is the only state that needs shutdown's attention, and it is why the
 * predicate is narrow. The pump shifts a frame out of the Session queue before
 * delivering it, so the Session queue already stopped counting it; from
 * `queueEvent` until the write settles, the connection queue counts it. A
 * `'moved'` settle is the connection queue releasing it without writing it and
 * without re-queueing it, leaving a frame that no buffer reports.
 *
 * Every other settle means the frame was written (`'settled'`), already
 * reported as a drop (`'dropped'`, `'aborted'`), or discarded by the connection
 * queue that counted it in its own totals (`'discarded'`). Counting those again
 * would report one frame twice.
 */
function isUnaccountedInHand(frame: InHandFrame): boolean {
  return frame.write?.settledAs === 'moved';
}

/**
 * Discards every frame still waiting in a Session's own queue at shutdown, plus
 * the one frame the pump holds while it is in no buffer at all (SPEC 3.8).
 *
 * The in-hand frame is the gap this closes: the pump shifts a frame out of the
 * Session queue before delivering it, so the Session queue stops counting it,
 * and a takeover that settles the attempt as `'moved'` releases it from the
 * connection queue without queueing it anywhere else. At that instant no queue
 * can report it, yet it was accepted and never written. Counting it here keeps
 * shutdown from dropping it silently, and `isUnaccountedInHand` keeps it out of
 * the totals whenever a connection queue still holds it, so one frame is
 * reported once.
 *
 * Frames already handed to a connection queue that still holds them belong to
 * that queue and are discarded there. Resolving `done` keeps the promise
 * returned when the event was accepted from hanging.
 */
export function discardSessionQueues(
  queues: Map<string, SessionEventQueue>,
  backlog: EventBacklog,
): { readonly frames: number; readonly bytes: number } {
  let frames = 0;
  let bytes = 0;
  for (const queue of queues.values()) {
    for (const entry of queue.entries) {
      frames += 1;
      bytes += entry.bytes;
      backlog.sessionFrameRemoved(entry.bytes);
      entry.done();
    }
    queue.entries.length = 0;
    queue.frames = 0;
    queue.bytes = 0;
    const inHand = queue.inHand;
    if (inHand !== undefined && isUnaccountedInHand(inHand)) {
      frames += 1;
      bytes += inHand.bytes;
    }
    queue.inHand = undefined;
  }
  queues.clear();
  return { frames, bytes };
}

/**
 * Queues a Session-addressed event behind the previous one for the same
 * Session. Two events for one Session are processed one at a time and in
 * arrival order; the later one waits until the earlier finished writing or was
 * dropped. Without this, a takeover would let a later event overtake an
 * earlier one still waiting on the old holder's connection queue, and the new
 * holder would receive them out of order (SPEC 3.8).
 */
export function enqueueSessionEvent(
  host: EventDeliveryHost,
  sessionId: string,
  event: ServerEvent,
  encoded: EncodedEvent,
): Promise<void> {
  const existing = host.sessionQueues.get(sessionId);
  const queuedEvents = existing?.frames ?? 0;
  const queuedEventBytes = existing?.bytes ?? 0;
  if (
    queuedEvents >= host.maxQueuedEvents ||
    queuedEventBytes + encoded.bytes > host.maxQueuedEventBytes
  ) {
    // The Session queue is full, so the Session's current holder is the
    // connection to disconnect (SPEC 3.8). Without a holder there is no
    // connection to close, and the event is a target-missing drop.
    const holder = holderState(host, sessionId, event);
    if (holder !== undefined) {
      const closing = host.closeForBacklog(holder, {
        buffer: 'session',
        queuedEvents,
        queuedEventBytes,
        maxQueuedEvents: host.maxQueuedEvents,
        maxQueuedEventBytes: host.maxQueuedEventBytes,
        eventId: event.eventId,
        event: event.event,
        sessionId,
      });
      if (!closing) {
        warnDropped(
          host,
          event,
          { targetKind: 'session', sessionId },
          BACKLOG_CLOSED_REASON,
        );
      }
    }
    return Promise.resolve();
  }
  const queue = existing ?? createSessionQueue(sessionId);
  if (existing === undefined) {
    host.sessionQueues.set(sessionId, queue);
  }
  const { promise, resolve } = Promise.withResolvers<void>();
  queue.entries.push({
    event,
    frame: encoded.frame,
    bytes: encoded.bytes,
    done: resolve,
  });
  queue.frames += 1;
  queue.bytes += encoded.bytes;
  host.backlog.sessionFrameAdded(encoded.bytes);
  startSessionPump(host, queue);
  return promise;
}

/**
 * Starts the Session's serial queue. Called synchronously on the first hop so
 * an event emitted later in the same turn (for another Session, or a pinned
 * connection) cannot overtake it on a shared connection queue.
 */
function startSessionPump(
  host: EventDeliveryHost,
  queue: SessionEventQueue,
): void {
  if (queue.pumping) {
    return;
  }
  queue.pumping = true;
  // `pumpSession` reports and contains its own failures. This last line of
  // defence only keeps a failure in that handler itself (a logger that throws,
  // say) out of the process's unhandled-rejection path; `pumpSession`'s own
  // `finally` still releases the Session's serial slot and the queue entry.
  void pumpSession(host, queue).catch(() => undefined);
}

async function pumpSession(
  host: EventDeliveryHost,
  queue: SessionEventQueue,
): Promise<void> {
  /** The entry shifted out of `entries` whose delivery has not settled yet. */
  let current: SessionQueuedEvent | undefined;
  try {
    while (queue.entries.length > 0) {
      if (!host.acceptingEvents()) {
        return;
      }
      const entry = queue.entries.shift();
      if (entry === undefined) {
        break;
      }
      current = entry;
      queue.frames -= 1;
      queue.bytes -= entry.bytes;
      host.backlog.sessionFrameRemoved(entry.bytes);
      // The frame now belongs to the holder's connection queue, so the Session
      // queue's bound no longer covers it (SPEC 3.8). `inHand` keeps it visible
      // to shutdown while it is between buffers, which is the only state no
      // queue accounts for: shifted out here, not yet queued there.
      const inHand: InHandFrame = { bytes: entry.bytes, write: undefined };
      queue.inHand = inHand;
      try {
        // oxlint-disable-next-line eslint/no-await-in-loop -- the serial slot must be held until this frame settles
        await deliverSessionEntry(host, queue.sessionId, entry, inHand);
      } finally {
        queue.inHand = undefined;
        // Resolves the promise returned when the event was accepted, whether
        // delivery succeeded, dropped, or threw.
        entry.done();
        current = undefined;
      }
    }
  } catch (error) {
    // Nothing in the delivery path is expected to throw: `deliverSessionEntry`
    // maps every outcome and `EventWrite.run` consumes a write failure. If one
    // escapes, the Session's serial slot is the only thing that must not stay
    // held with promises unresolved, so the queue is discarded rather than
    // retried — events are best-effort (ADR-0008).
    host.logger.error(
      {
        code: 'EVENT_DELIVERY_FAILED',
        sessionId: queue.sessionId,
        reason: messageOf(error),
      },
      'session event pump stopped unexpectedly',
    );
    for (const entry of queue.entries) {
      queue.frames -= 1;
      queue.bytes -= entry.bytes;
      host.backlog.sessionFrameRemoved(entry.bytes);
      entry.done();
    }
    queue.entries.length = 0;
    if (current !== undefined) {
      // The exception landed before the entry's own `finally` (between the
      // shift/counting and the delivery attempt). `done` is a resolve, so the
      // second call from a `finally` that already ran is harmless.
      queue.inHand = undefined;
      current.done();
      current = undefined;
    }
  } finally {
    queue.pumping = false;
    if (
      queue.entries.length === 0 &&
      host.sessionQueues.get(queue.sessionId) === queue
    ) {
      host.sessionQueues.delete(queue.sessionId);
    }
  }
}

/**
 * Delivers one Session event while holding that Session's serial slot, and
 * reports when the slot may be released. The holder is resolved twice: once to
 * pick the connection queue, again inside that queue right before the write,
 * because a takeover may land while the event waits. A moved holder earns one
 * redelivery (the existing bound); a second change is dropped.
 */
async function deliverSessionEntry(
  host: EventDeliveryHost,
  sessionId: string,
  entry: SessionQueuedEvent,
  inHand: InHandFrame,
): Promise<void> {
  const { event, frame, bytes } = entry;
  let redeliveries = 0;
  while (true) {
    if (!host.acceptingEvents()) {
      return;
    }
    const holder = holderState(host, sessionId, event);
    if (holder === undefined) {
      return;
    }
    // oxlint-disable-next-line eslint/no-await-in-loop -- a redelivery must observe the previous attempt before re-resolving the holder
    const outcome = await deliverToHolder(
      host,
      sessionId,
      holder,
      event,
      frame,
      bytes,
      inHand,
    );
    if (
      outcome === 'settled' ||
      outcome === 'dropped' ||
      outcome === 'discarded'
    ) {
      return;
    }
    if (outcome === 'aborted') {
      // Already logged by `watchQueuedWrite`: the connection whose queue this
      // event joined closed while the frame was still queued. Drop it and let
      // the next event proceed rather than pinning the slot forever.
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
 * Joins the current holder's event queue and reports what its turn observed.
 *
 * Delivery has two phases, and only the first is cancellable:
 *
 * - **queued**: the frame has not started writing. A disconnect aborts it,
 *   the event is dropped, and the Session's serial slot is released at once —
 *   waiting for a queue whose connection is gone could block it forever.
 * - **writing**: `run` was entered. An abort no longer releases the slot: the
 *   frame may already be on the wire, so the write's own bound (the host's
 *   `sendTimeoutMs`) decides when it ends, and a failure is a transport
 *   failure, not a drop. Releasing on abort here would let the next event
 *   overtake one that is still being written (SPEC 3.8).
 */
async function deliverToHolder(
  host: EventDeliveryHost,
  sessionId: string,
  queuedFor: ConnectionState,
  event: ServerEvent,
  frame: string | Uint8Array,
  bytes: number,
  inHand: InHandFrame,
): Promise<EventOutcome> {
  const write = new EventWrite(bytes, async () => {
    const current = holderState(host, sessionId, event);
    if (current === undefined) {
      return 'dropped';
    }
    if (current !== queuedFor) {
      return 'moved';
    }
    await writeEventFrame(host, current, event, frame);
    return 'settled';
  });
  // Recorded before queueing: shutdown may run between the queue call and this
  // frame's turn, and it needs to know whether a connection queue holds it.
  inHand.write = write;
  const queued = host.queueEvent(queuedFor, write);
  if (!queued.accepted) {
    const closing = host.closeForBacklog(queuedFor, {
      buffer: 'connection',
      queuedEvents: queued.queuedEvents,
      queuedEventBytes: queued.queuedEventBytes,
      maxQueuedEvents: host.maxQueuedEvents,
      maxQueuedEventBytes: host.maxQueuedEventBytes,
      eventId: event.eventId,
      event: event.event,
      sessionId,
    });
    if (!closing) {
      warnDropped(
        host,
        event,
        {
          targetKind: 'session',
          sessionId,
          connectionId: queuedFor.connection.id,
        },
        BACKLOG_CLOSED_REASON,
      );
    }
    return 'dropped';
  }
  return watchQueuedWrite(host, queuedFor, write, event, {
    targetKind: 'session',
    sessionId,
  });
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
    // No path in this layer reaches this: a route is only created for a
    // connection that is in the table (`connection-request.ts` checks it before
    // binding), and every removal either releases the route in the same
    // synchronous turn (`onDisconnect`) or clears both tables together
    // (`doStop`). Kept as a drop rather than an assertion because the two maps
    // are separate objects, and a future refactor that opens a window should
    // fail one event, not the process.
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
