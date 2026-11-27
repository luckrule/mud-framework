import { Buffer } from 'node:buffer';

import type { ServerEvent } from '../shared/events.ts';
import { messageOf } from '../shared/guards.ts';
import { settleWithin } from '../shared/timeout.ts';
import { TRANSPORT_SEND_FAILED } from '../shared/wire-codes.ts';
import type { ConnectionState } from './connection-admission.ts';
import type { LayerHost } from './connection-host.ts';
import type { EventBacklog } from './event-backlog.ts';

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
> & {
  /** Read-only: delivery resolves holders, it never admits or removes one. */
  readonly connections: ReadonlyMap<string, ConnectionState>;
};

/** How one queued event frame's write turn ended. */
export type EventOutcome =
  /** The write attempt finished: the transport accepted or failed the frame. */
  | 'settled'
  /** The holder was gone when the write's turn came. */
  | 'dropped'
  /** The holder changed while this frame waited; the caller may redeliver. */
  | 'moved'
  /** The connection closed before the write started. */
  | 'aborted'
  /** The layer started stopping while the frame was still queued. */
  | 'discarded';

/** What a buffer did with one offered frame (SPEC 3.8). */
export type QueueEventResult =
  | { readonly accepted: true }
  | {
      readonly accepted: false;
      /** Frames already waiting, including one being written. */
      readonly queuedEvents: number;
      /** Their encoded size, matching the bound's unit. */
      readonly queuedEventBytes: number;
    };

/**
 * One encoded event frame waiting for its connection's writer (SPEC 3.8).
 *
 * The writer marks the frame started synchronously before the first await, so
 * an abort either cancels a queued frame or observes that writing began; there
 * is no window in between. The outcome promise is how a Session-addressed
 * event's serial queue learns whether to redeliver.
 */
export class EventWrite {
  /** Encoded frame size, charged against the queue's byte bound. */
  readonly bytes: number;
  /** Set by `run` before it yields; a started frame can no longer be cancelled. */
  started = false;
  /** Set with the outcome; a settled frame never writes. */
  settled = false;
  /**
   * The outcome once settled. A Session queue reads it at shutdown to tell a
   * frame handed back for redelivery (still unwritten) from one that already
   * ended in a connection queue, which that queue has counted.
   */
  settledAs: EventOutcome | undefined;
  readonly outcome: Promise<EventOutcome>;
  private readonly write: () => EventOutcome | Promise<EventOutcome>;
  private readonly settleOutcome: (outcome: EventOutcome) => void;

  constructor(
    bytes: number,
    write: () => EventOutcome | Promise<EventOutcome>,
  ) {
    this.bytes = bytes;
    this.write = write;
    const { promise, resolve } = Promise.withResolvers<EventOutcome>();
    this.outcome = promise;
    this.settleOutcome = resolve;
  }

  settle(outcome: EventOutcome): void {
    if (this.settled) {
      return;
    }
    this.settled = true;
    this.settledAs = outcome;
    this.settleOutcome(outcome);
  }

  async run(): Promise<void> {
    this.started = true;
    let outcome: EventOutcome;
    try {
      outcome = await this.write();
    } catch {
      // The frame may already be on the wire; failure logging belongs to the
      // write itself, and the serial slot must still be released.
      outcome = 'settled';
    }
    this.settle(outcome);
  }
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
        'the target connection closed before the event was written',
      );
      return Promise.resolve();
    }
    const write = new EventWrite(encoded.bytes, async () => {
      await writeEventFrame(host, state, event, encoded.frame);
      return 'settled' as const;
    });
    const queued = host.queueEvent(state, write);
    if (!queued.accepted) {
      logBacklogDrop(
        host,
        event,
        { targetKind: 'connection', connectionId: target.connectionId },
        queued,
      );
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
 * One Session-addressed frame waiting for that Session's serial slot.
 * `bytes` is charged against the Session queue's own bound from acceptance
 * until the frame is handed to a connection queue (SPEC 3.8).
 */
export interface SessionQueuedEvent {
  readonly event: ServerEvent;
  readonly frame: string | Uint8Array;
  readonly bytes: number;
  /** Resolves the promise `deliverServerEvent` returned for this event. */
  readonly done: () => void;
}

/**
 * The pending queue of one Session. Bounded per instance: a Session whose
 * holder cannot write accumulates frames here, not in process memory.
 * `pumping` marks that the Session's serial slot is held — one frame is being
 * delivered, and later ones wait in `entries`.
 */
export interface SessionEventQueue {
  readonly sessionId: string;
  readonly entries: SessionQueuedEvent[];
  frames: number;
  bytes: number;
  pumping: boolean;
  /**
   * The frame the pump holds right now: shifted out of `entries`, so no longer
   * counted by `frames`/`bytes`, and either waiting in a connection queue or
   * between buffers during a takeover redelivery (SPEC 3.8).
   */
  inHand: InHandFrame | undefined;
}

export function createSessionQueue(sessionId: string): SessionEventQueue {
  return {
    sessionId,
    entries: [],
    frames: 0,
    bytes: 0,
    pumping: false,
    inHand: undefined,
  };
}

/**
 * The frame the pump currently holds. `write` is the latest delivery attempt,
 * replaced on each redelivery.
 */
export interface InHandFrame {
  readonly bytes: number;
  write: EventWrite | undefined;
}

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
function enqueueSessionEvent(
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
    logBacklogDrop(
      host,
      event,
      { targetKind: 'session', sessionId },
      { queuedEvents, queuedEventBytes },
    );
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
  void pumpSession(host, queue).catch(() => undefined);
}

async function pumpSession(
  host: EventDeliveryHost,
  queue: SessionEventQueue,
): Promise<void> {
  try {
    while (queue.entries.length > 0) {
      if (!host.acceptingEvents()) {
        return;
      }
      const entry = queue.entries.shift();
      if (entry === undefined) {
        break;
      }
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
      }
      entry.done();
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
    logBacklogDrop(
      host,
      event,
      {
        targetKind: 'session',
        sessionId,
        connectionId: queuedFor.connection.id,
      },
      queued,
    );
    return 'dropped';
  }
  return watchQueuedWrite(host, queuedFor, write, event, {
    targetKind: 'session',
    sessionId,
  });
}

/**
 * Cancels a queued frame when its connection closes, and reports how the write
 * ended. Both target kinds share this, so a connection-addressed event and a
 * Session-addressed one obey the same queued/writing rule (SPEC 3.8): a frame
 * that has not started writing is dropped with `EVENT_DROPPED`, while one
 * already handed to the transport waits for its own result and a failure there
 * stays a `TRANSPORT_SEND_FAILED`.
 */
async function watchQueuedWrite(
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
        'the target connection closed before the event was written',
      );
    }
    return outcome;
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
interface EncodedEvent {
  readonly frame: string | Uint8Array;
  readonly bytes: number;
}

/**
 * Encodes once, when the event is accepted, so the byte bound is charged for
 * the frame that will actually be written and a redelivery reuses it. The cost
 * is that an unencodable event is reported here rather than at write time; the
 * log code, stage and reason are the same, so the diagnosis does not change.
 */
function encodeEvent(
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

async function writeEventFrame(
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

/**
 * An event buffer reached one of its bounds, so this event was not accepted
 * (SPEC 3.8). The request that emitted it is unaffected: events are
 * best-effort, and the line is the only record of the drop.
 */
function logBacklogDrop(
  host: EventDeliveryHost,
  event: ServerEvent,
  target: DropTarget,
  queued: { readonly queuedEvents: number; readonly queuedEventBytes: number },
): void {
  host.backlog.recordDrop();
  // This is the path a slow reader drives at event rate, so it goes through the
  // same aggregation as any other drop (SPEC 7.1). `droppedTotal` on the line is
  // the log's running count for this target; `eventBacklog.dropped` above is the
  // contract's exact total and is never aggregated.
  host.dropLog.record(
    {
      eventId: event.eventId,
      event: event.event,
      ...target,
      code: 'EVENT_DROPPED',
      reason: 'backlog',
      queuedEvents: queued.queuedEvents,
      queuedEventBytes: queued.queuedEventBytes,
      maxQueuedEvents: host.maxQueuedEvents,
      maxQueuedEventBytes: host.maxQueuedEventBytes,
    },
    'backlog',
    dropTargetId(target),
  );
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
