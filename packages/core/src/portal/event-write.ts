/**
 * The writer side of one queued event frame and what its write turn reported.
 * Kept apart from delivery: `connection-writer` drains frames through these
 * types and never looks at a target.
 */

/**
 * One buffer that reached its bound and refused a frame (SPEC 3.8). The
 * connection owning the buffer is closed with `1013`, so the observation is
 * logged with the close rather than as a per-event drop.
 */
export interface BacklogOverflow {
  /** Which buffer refused the frame; the other one is the bound's other scope. */
  readonly buffer: 'connection' | 'session';
  /** Frames already waiting in that buffer, including one being written. */
  readonly queuedEvents: number;
  /** Their encoded size, matching the bound's unit. */
  readonly queuedEventBytes: number;
  readonly maxQueuedEvents: number;
  readonly maxQueuedEventBytes: number;
  /** The refused event, kept so the line answers "which event caused this". */
  readonly eventId: string;
  readonly event: string;
  /** The Session whose queue overflowed; absent for a connection queue. */
  readonly sessionId?: string;
}

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

/**
 * What a buffer did with one offered frame (SPEC 3.8). A refusal is not a
 * dropped event: the buffer is full, so the caller disconnects the connection
 * that owns it instead of silently discarding the frame.
 */
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
