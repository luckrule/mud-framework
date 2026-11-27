import { Buffer } from 'node:buffer';

import type { ServerEvent } from '../shared/events.ts';
import { messageOf } from '../shared/guards.ts';
import { settleWithin } from '../shared/timeout.ts';
import { TRANSPORT_SEND_FAILED } from '../shared/wire-codes.ts';
import type { ConnectionState } from './connection-admission.ts';
import { BACKLOG_CLOSED_REASON } from './connection-admission.ts';
import type { LayerHost } from './connection-host.ts';
import type { EventOutcome, EventWrite } from './event-write.ts';

/**
 * The shared primitives of event delivery: what the layer must provide, how one
 * frame is encoded and written under a bound, and how a drop is recorded. Both
 * the connection-target path and the Session queue build on these; neither
 * knows about the other.
 */

/**
 * What delivery needs from the connection layer: the two lookup tables (which
 * connection exists, which Session it holds), the wire format, the queue of the
 * connection's single writer, and the per-Session pending queues that keep
 * Session-addressed events in arrival order.
 */
export type EventDeliveryHost = Pick<
  LayerHost,
  | 'codec'
  | 'logger'
  | 'router'
  | 'queueEvent'
  | 'cancelEvent'
  | 'acceptingEvents'
  | 'maxQueuedEvents'
  | 'maxQueuedEventBytes'
  | 'backlog'
  | 'sessionQueues'
  | 'sendTimeoutMs'
  | 'dropLog'
  | 'closeForBacklog'
> & {
  /** Read-only: delivery resolves holders, it never admits or removes one. */
  readonly connections: ReadonlyMap<string, ConnectionState>;
};

type DropTarget =
  | { readonly targetKind: 'connection'; readonly connectionId: string }
  | {
      readonly targetKind: 'session';
      readonly sessionId: string;
      readonly connectionId?: string;
    };

export function warnDropped(
  host: EventDeliveryHost,
  event: ServerEvent,
  target: DropTarget,
  reason: string,
): void {
  // Per-drop lines turn a slow reader into a second unbounded stream; the log
  // aggregates them per reason and target instead (SPEC 7.1). A line keeps the
  // batch's first event for identification and adds `count`/`droppedTotal`.
  host.dropLog.record(
    {
      eventId: event.eventId,
      event: event.event,
      ...target,
      code: 'EVENT_DROPPED',
      reason,
    },
    reason,
    dropTargetId(target),
  );
}

/** The target's id, which is what the aggregation bucket is keyed by. */
function dropTargetId(target: DropTarget): string {
  return target.targetKind === 'connection'
    ? target.connectionId
    : target.sessionId;
}

/** One encoded event frame plus the size charged against the byte bound. */
export interface EncodedEvent {
  readonly frame: string | Uint8Array;
  readonly bytes: number;
}

/**
 * Encodes once, when the event is accepted, so the byte bound is charged for
 * the frame that will actually be written and a redelivery reuses it. The cost
 * is that an unencodable event is reported here rather than at write time; the
 * log code, stage and reason are the same, so the diagnosis does not change.
 */
export function encodeEvent(
  host: EventDeliveryHost,
  event: ServerEvent,
  target: DropTarget,
): EncodedEvent | undefined {
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
        ...target,
        code: TRANSPORT_SEND_FAILED,
        stage: 'encode',
        reason: messageOf(error),
      },
      'server event could not be encoded',
    );
    return undefined;
  }
  return { frame, bytes: frameByteLength(frame) };
}

/**
 * The wire size of one encoded frame. A string frame is measured in UTF-8
 * bytes, which is what a transport puts on the wire and what `byteLength`
 * reports for a binary frame; code units would undercount non-ASCII payloads.
 */
function frameByteLength(frame: string | Uint8Array): number {
  return typeof frame === 'string'
    ? Buffer.byteLength(frame, 'utf8')
    : frame.byteLength;
}

export async function writeEventFrame(
  host: EventDeliveryHost,
  state: ConnectionState,
  event: ServerEvent,
  frame: string | Uint8Array,
): Promise<void> {
  const connectionId = state.connection.id;
  // The write is bounded here rather than by the abort signal: once the frame
  // is handed to the transport an abort says nothing about whether it reached
  // the wire, so the bound is the transport's own send budget. `settleWithin`
  // also consumes a rejection, so a late failure cannot escape.
  let pending: Promise<void>;
  try {
    // Called inside `try` because a transport that throws instead of rejecting
    // would otherwise escape and reject the delivery, which nothing awaits.
    pending = state.connection.send(frame);
  } catch (error) {
    logSendFailure(host, event, connectionId, messageOf(error));
    return;
  }
  const written = await settleWithin(pending, host.sendTimeoutMs);
  if (written.status === 'timeout') {
    logSendFailure(
      host,
      event,
      connectionId,
      `the transport did not settle the write within ${host.sendTimeoutMs}ms`,
    );
    return;
  }
  if (written.status === 'rejected') {
    logSendFailure(host, event, connectionId, messageOf(written.error));
  }
}

function logSendFailure(
  host: EventDeliveryHost,
  event: ServerEvent,
  connectionId: string,
  reason: string,
): void {
  host.logger.warn(
    {
      eventId: event.eventId,
      event: event.event,
      connectionId,
      code: TRANSPORT_SEND_FAILED,
      stage: 'send',
      reason,
    },
    'server event could not be sent',
  );
}

/**
 * Cancels a queued frame when its connection closes, and reports how the write
 * ended. Both target kinds share this, so a connection-addressed event and a
 * Session-addressed one obey the same queued/writing rule (SPEC 3.8): a frame
 * that has not started writing is dropped with `EVENT_DROPPED`, while one
 * already handed to the transport waits for its own result and a failure there
 * stays a `TRANSPORT_SEND_FAILED`.
 *
 * The abort has two causes and they get different reasons (see `closedReason`).
 * Both are drops, but an operator sorting through a flood of `EVENT_DROPPED` has
 * to be able to tell "the player went away" from "we disconnected the player",
 * and the frames still queued behind an overflow are the second case.
 */
export async function watchQueuedWrite(
  host: EventDeliveryHost,
  queuedFor: ConnectionState,
  write: EventWrite,
  event: ServerEvent,
  target: DropTarget,
): Promise<EventOutcome> {
  const signal = queuedFor.closed.signal;
  const onAbort = (): void => {
    // False once the write started: its bound, not the abort, ends it.
    host.cancelEvent(queuedFor, write);
  };
  signal.addEventListener('abort', onAbort, { once: true });
  if (signal.aborted) {
    onAbort();
  }
  try {
    const outcome = await write.outcome;
    if (outcome === 'aborted') {
      // The connection closed while the frame waited behind earlier work, so
      // nothing was written: the target is gone, which SPEC 3.8 says is a drop.
      warnDropped(
        host,
        event,
        { ...target, connectionId: queuedFor.connection.id },
        queuedFor.backlogClosed
          ? BACKLOG_CLOSED_REASON
          : 'the target connection closed before the event was written',
      );
    }
    return outcome;
  } finally {
    signal.removeEventListener('abort', onAbort);
  }
}
