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

/**
 * Aggregate of every event buffer the layer holds (SPEC 6.6). Buffers report
 * their own additions and removals instead of the layer walking the admission
 * table: a connection removed on disconnect keeps draining a queue it already
 * accepted frames into, and those frames still occupy memory.
 */
export class EventBacklog {
  private frameTotal = 0;
  private byteTotal = 0;
  private droppedEvents = 0;
  private readonly buffers = new Set<ConnectionWriter>();

  /** Connection queues with frames, for the shutdown discard. */
  get connectionBuffers(): ReadonlySet<ConnectionWriter> {
    return this.buffers;
  }

  connectionFrameAdded(buffer: ConnectionWriter, bytes: number): void {
    this.buffers.add(buffer);
    this.frameTotal += 1;
    this.byteTotal += bytes;
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
  }

  sessionFrameAdded(bytes: number): void {
    this.frameTotal += 1;
    this.byteTotal += bytes;
  }

  sessionFrameRemoved(bytes: number): void {
    this.frameTotal -= 1;
    this.byteTotal -= bytes;
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
}
