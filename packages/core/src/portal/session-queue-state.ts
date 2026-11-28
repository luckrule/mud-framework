import type { ServerEvent } from '../shared/events.ts';
import type { EventWrite } from './event-write.ts';

/**
 * The state of one Session's pending delivery, kept in a leaf module so the
 * delivery primitives can name the queue type without importing the pump.
 */

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
 * holder cannot write accumulates frames here rather than in the connection's
 * queue. `pumping` marks that the Session's serial slot is held — one frame is
 * being delivered, and later ones wait in `entries`.
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
