import type { Logger } from '../logging/types.ts';
import type { ConnectionWriter } from './connection-writer.ts';

/** Point-in-time totals reported by `ConnectionLayer.health()` (SPEC 6.6). */
export interface EventBacklogSnapshot {
  /** Connection event queues that currently hold frames. */
  readonly connections: number;
  /** Frames waiting across every connection queue and Session queue. */
  readonly frames: number;
  /** Their encoded size, in the same unit as `maxQueuedEventBytes`. */
  readonly bytes: number;
  /** Overflow drops since the layer started; shutdown discards do not count. */
  readonly dropped: number;
}

export interface EventBacklogOptions {
  /** Where the aggregate warning goes; it never affects a delivery. */
  readonly logger: Logger;
  /** Aggregate bytes that make the deployment look short of capacity. */
  readonly warnBytes: number;
  /** Live admitted connections, reported with the warning for context. */
  readonly liveConnections: () => number;
}

/**
 * Aggregate of every event buffer the layer holds (SPEC 6.6). Buffers report
 * their own additions and removals instead of the layer walking the admission
 * table: a connection removed on disconnect keeps draining a queue it already
 * accepted frames into, and those frames still occupy memory.
 *
 * The totals also drive one observation: a warning when the sum crosses
 * `warnBytes`. It is deliberately not an admission rule. A global cap would let
 * one loud emitter drop events for every connection, turning "a few slow
 * readers" into "everyone loses events", which is worse than the backlog.
 */
export class EventBacklog {
  private frameTotal = 0;
  private byteTotal = 0;
  private droppedEvents = 0;
  private readonly buffers = new Set<ConnectionWriter>();
  private readonly logger: Logger;
  private readonly warnBytes: number;
  private readonly liveConnections: () => number;
  /** Set while the warning is out; cleared only when every buffer is empty. */
  private warned = false;
  /** A re-arm check is queued; at most one exists at a time. */
  private rearmPending = false;

  constructor(options: EventBacklogOptions) {
    this.logger = options.logger;
    this.warnBytes = options.warnBytes;
    this.liveConnections = options.liveConnections;
  }

  /** Connection queues with frames, for the shutdown discard. */
  get connectionBuffers(): ReadonlySet<ConnectionWriter> {
    return this.buffers;
  }

  connectionFrameAdded(buffer: ConnectionWriter, bytes: number): void {
    this.buffers.add(buffer);
    this.frameTotal += 1;
    this.byteTotal += bytes;
    this.checkWarning();
  }

  connectionFrameRemoved(
    buffer: ConnectionWriter,
    bytes: number,
    empty: boolean,
  ): void {
    if (empty) {
      this.buffers.delete(buffer);
    }
    this.frameTotal -= 1;
    this.byteTotal -= bytes;
    this.checkWarning();
  }

  sessionFrameAdded(bytes: number): void {
    this.frameTotal += 1;
    this.byteTotal += bytes;
    this.checkWarning();
  }

  sessionFrameRemoved(bytes: number): void {
    this.frameTotal -= 1;
    this.byteTotal -= bytes;
    this.checkWarning();
  }

  recordDrop(): void {
    this.droppedEvents += 1;
  }

  snapshot(): EventBacklogSnapshot {
    return {
      connections: this.buffers.size,
      frames: this.frameTotal,
      bytes: this.byteTotal,
      dropped: this.droppedEvents,
    };
  }

  /**
   * Warns once per episode, not once per frame (SPEC 3.8). Rearming the moment
   * the total looks empty is not enough: handing a frame from a Session queue
   * to a connection queue subtracts it and adds it back in two separate calls,
   * so a single in-flight frame makes the total dip to zero in between. A
   * backlog that never truly empties would then rearm on every handoff and warn
   * again on every add — a per-frame flood, which is the opposite of what an
   * alarm is for. The re-arm is therefore deferred one microtask and re-checked:
   * the handoff's add lands in the same synchronous run, so by then the total is
   * back above zero and the episode continues.
   */
  private checkWarning(): void {
    if (this.byteTotal >= this.warnBytes) {
      if (this.warned) {
        return;
      }
      this.warned = true;
      this.logger.warn(
        {
          code: 'EVENT_BACKLOG_HIGH',
          bytes: this.byteTotal,
          frames: this.frameTotal,
          // Named apart from the snapshot's `connections`, which counts buffers
          // holding frames: here it is the admitted connection count.
          activeConnections: this.liveConnections(),
          warnBytes: this.warnBytes,
        },
        'aggregate server event backlog is above the warning threshold',
      );
      return;
    }
    if (this.frameTotal > 0 || !this.warned || this.rearmPending) {
      return;
    }
    this.rearmPending = true;
    queueMicrotask(() => {
      this.rearmPending = false;
      if (this.frameTotal === 0) {
        this.warned = false;
      }
    });
  }
}
