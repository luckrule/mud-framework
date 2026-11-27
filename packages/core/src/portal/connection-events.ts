import type { ServerEvent } from '../shared/events.ts';
import type { ConnectionState } from './connection-admission.ts';
import { BACKLOG_CLOSED_REASON } from './connection-admission.ts';
import type { EventDeliveryHost } from './event-frame.ts';
import {
  encodeEvent,
  warnDropped,
  watchQueuedWrite,
  writeEventFrame,
} from './event-frame.ts';
import { EventWrite } from './event-write.ts';
import { enqueueSessionEvent } from './session-event-queue.ts';

/**
 * The entry point of event delivery: it dispatches one Server event by target.
 * A connection-addressed event joins that connection's writer directly; a
 * Session-addressed one goes through the Session's serial queue, so it keeps
 * emission order across a takeover (SPEC 3.8).
 */

/**
 * Delivers one Server event (ADR-0008), best-effort: every failure is a warn
 * line, none is thrown, and nothing is buffered for a retry.
 *
 * The write joins the target connection's event queue rather than calling
 * `send` directly. The Server sends a result and then its events on IPC, but
 * the result's reply is written by an async continuation; without a queue a
 * later IPC message could win the race and put an event in front of the
 * response it belongs to.
 *
 * Ordering between events is narrower than "the order the Server emitted them".
 * Events addressed to the same Session are serialized by that Session's queue,
 * so they keep emission order even across a takeover (SPEC 3.8). A
 * connection-target event only shares its connection's event queue, so on one
 * connection a connection-target event and a Session-target event are not
 * guaranteed to appear in emission order.
 */
export function deliverServerEvent(
  host: EventDeliveryHost,
  event: ServerEvent,
): Promise<void> {
  if (!host.acceptingEvents()) {
    return Promise.resolve();
  }
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
    const encoded = encodeEvent(host, event, {
      targetKind: 'connection',
      connectionId: target.connectionId,
    });
    if (encoded === undefined) {
      return Promise.resolve();
    }
    if (state.closed.signal.aborted) {
      // Already closed: queuing it would only fail the write later, and SPEC
      // 3.8 calls a closed target a drop rather than a transport failure.
      warnDropped(
        host,
        event,
        { targetKind: 'connection', connectionId: target.connectionId },
        closedReason(state),
      );
      return Promise.resolve();
    }
    const write = new EventWrite(encoded.bytes, async () => {
      await writeEventFrame(host, state, event, encoded.frame);
      return 'settled' as const;
    });
    const queued = host.queueEvent(state, write);
    if (!queued.accepted) {
      const closing = host.closeForBacklog(state, {
        buffer: 'connection',
        queuedEvents: queued.queuedEvents,
        queuedEventBytes: queued.queuedEventBytes,
        maxQueuedEvents: host.maxQueuedEvents,
        maxQueuedEventBytes: host.maxQueuedEventBytes,
        eventId: event.eventId,
        event: event.event,
      });
      if (!closing) {
        // The connection was already being closed for backlog, so this frame
        // has nowhere to go: a closed target is a drop (SPEC 3.8).
        warnDropped(
          host,
          event,
          { targetKind: 'connection', connectionId: target.connectionId },
          BACKLOG_CLOSED_REASON,
        );
      }
      return Promise.resolve();
    }
    return watchQueuedWrite(host, state, write, event, {
      targetKind: 'connection',
      connectionId: target.connectionId,
    }).then(() => undefined);
  }
  // The Session id is the address, not the holder: each event resolves the
  // holder when its turn starts and again right before the write. The Session
  // queue serializes events across a takeover: the holder may change, but the
  // emission order must not.
  const encoded = encodeEvent(host, event, {
    targetKind: 'session',
    sessionId: target.sessionId,
  });
  if (encoded === undefined) {
    return Promise.resolve();
  }
  return enqueueSessionEvent(host, target.sessionId, event, encoded);
}

/**
 * Why a closed connection can no longer take a frame (SPEC 3.8). A backlog
 * close is this layer's decision, so it is named as such: otherwise its drops
 * would be indistinguishable from a client that disconnected on its own, and
 * the two need different responses from whoever reads the log.
 */
function closedReason(state: ConnectionState): string {
  return state.backlogClosed
    ? BACKLOG_CLOSED_REASON
    : 'the target connection closed before the event was written';
}
