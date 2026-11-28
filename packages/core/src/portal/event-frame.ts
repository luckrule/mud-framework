import { Buffer } from 'node:buffer';

import type { Logger } from '../logging/types.ts';
import type { Codec } from '../shared/codec.ts';
import type { ServerEvent } from '../shared/events.ts';
import { settleWithin } from '../shared/timeout.ts';
import { messageOf } from '../shared/validation.ts';
import { TRANSPORT_SEND_FAILED } from '../shared/wire-codes.ts';
import type { ConnectionState } from './connection-state.ts';
import { BACKLOG_CLOSED_REASON } from './connection-state.ts';
import type { EventBacklog } from './event-backlog.ts';
import type { EventDropLog } from './event-drop-log.ts';
import type {
  BacklogOverflow,
  EventOutcome,
  EventWrite,
  QueueEventResult,
} from './event-write.ts';
import type { SessionEventQueue } from './session-queue-state.ts';
import type { SessionRouter } from './session-router.ts';

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
 * Session-addressed events in arrival order. Declared here rather than picked
 * from the layer's assembled host so delivery does not depend on it.
 */
export interface EventDeliveryHost {
  /** The wire format, and the sink for failures that must not reach the transport. */
  readonly codec: Codec;
  readonly logger: Logger;
  /** Read-only: delivery resolves holders, it never admits or removes one. */
  readonly connections: ReadonlyMap<string, ConnectionState>;
  /** Frame bound of each event buffer (SPEC 3.8). */
  readonly maxQueuedEvents: number;
  /** Byte bound of each event buffer (SPEC 3.8). */
  readonly maxQueuedEventBytes: number;
  /** Layer-wide event totals every buffer reports to (SPEC 6.6). */
  readonly backlog: EventBacklog;
  /**
   * Aggregates repeated `EVENT_DROPPED` lines per reason and target (SPEC 7.1).
   * A missing target or a closed connection can recur at emission rate — a room
   * broadcasting to Sessions nobody holds — so one line per drop would make the
   * diagnostic channel a second unbounded stream.
   */
  readonly dropLog: EventDropLog;
  readonly router: SessionRouter;
  /**
   * Queues one encoded frame on the target connection's event queue. The queue
   * belongs to the connection's single writer, so the frame is written after
   * any request work already waiting (SPEC 3.8).
   */
  readonly queueEvent: (
    state: ConnectionState,
    write: EventWrite,
  ) => QueueEventResult;
  /**
   * Cancels a frame that has not started writing. A no-op once it did: the
   * write's own bound decides when the serial slot is released.
   */
  readonly cancelEvent: (state: ConnectionState, write: EventWrite) => void;
  /**
   * Closes the connection owning an overflowing event buffer with `1013`
   * (SPEC 3.8). Passed in rather than implemented here because the close has to
   * mark the state before the transport reports the disconnect, so `onDisconnect`
   * can tell a backlog close from a client leaving. Returns false when this
   * connection is already being closed for backlog.
   */
  readonly closeForBacklog: (
    state: ConnectionState,
    overflow: BacklogOverflow,
  ) => boolean;
  /**
   * False once the layer started stopping. Events are best-effort and the
   * buffers are discarded at that point, so a late event is not accepted
   * (SPEC 3.8).
   */
  readonly acceptingEvents: () => boolean;
  /**
   * Pending queues of the Sessions the layer addresses, owned by the layer so
   * ordering and bound state is per layer (and per test) rather than
   * process-global. An entry exists while that Session has unfinished delivery
   * work, which is also when its serial slot is held.
   */
  readonly sessionQueues: Map<string, SessionEventQueue>;
  /**
   * Upper bound on one downlink write: a Server event frame and a response
   * frame alike (SPEC 3.8). A transport bounds its own sends, but the layer
   * cannot assume every implementation does: without this bound a send that
   * never settles would pin the Session's serial slot — or the connection's
   * single writer, and with it the Session's terminal barrier — forever.
   */
  readonly sendTimeoutMs: number;
}

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
 * code is the same and `stage` distinguishes `encode` from `send`, so the
 * diagnosis does not change.
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
  // the wire, so the bound is `host.sendTimeoutMs` — the explicit option, the
  // transport's declaration plus slack, or the undeclared default, resolved by
  // the layer. `settleWithin` also consumes a rejection, so a late failure
  // cannot escape.
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
 * Why a closed connection can no longer take a frame (SPEC 3.8). A backlog
 * close is this layer's decision, so it is named as such: otherwise its drops
 * would be indistinguishable from a client that disconnected on its own, and
 * the two need different responses from whoever reads the log.
 */
export function closedReason(state: ConnectionState): string {
  return state.backlogClosed
    ? BACKLOG_CLOSED_REASON
    : 'the target connection closed before the event was written';
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
        closedReason(queuedFor),
      );
    }
    return outcome;
  } finally {
    signal.removeEventListener('abort', onAbort);
  }
}
