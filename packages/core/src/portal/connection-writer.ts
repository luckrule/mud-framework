import type { Logger } from '../logging/types.ts';
import { messageOf } from '../shared/guards.ts';
import type { EventBacklog } from './event-backlog.ts';
import type { EventWrite, QueueEventResult } from './event-write.ts';

interface RequestWork {
  readonly run: () => Promise<void>;
  readonly done: () => void;
  readonly fail: (error: unknown) => void;
  readonly queued: QueuedRequest | undefined;
}

/**
 * What a request work tells shutdown about itself. A dropped request must undo
 * the accounting its submitter did for the seat it held, and a decoded request
 * must be named in the log — a frame that could not be decoded is not a
 * Request and has no identity to report.
 */
export interface QueuedRequest {
  readonly identity?: QueuedRequestIdentity;
  /** Undoes the caller's accounting for the queued seat this work held. */
  readonly onDropped: () => void;
}

export interface QueuedRequestIdentity {
  readonly requestId: string;
  readonly method: string;
  /** The client's correlation id (SPEC 7.3), omitted when there is none. */
  readonly clientRequestId?: string;
}

export interface ConnectionWriterOptions {
  /** Frames allowed to wait on this connection before it is closed (SPEC 3.8). */
  readonly maxQueuedEvents: number;
  /** The same bound in encoded frame bytes. */
  readonly maxQueuedEventBytes: number;
  /** Layer-wide totals this queue reports its frames to. */
  readonly backlog: EventBacklog;
  /** Records a failed drain turn, which must not go unreported. */
  readonly logger: Logger;
}

/**
 * The single writer of one connection (SPEC 3.8). Request work — input
 * admission through the result write — is one scheduling unit, and a queue of
 * waiting request work runs before queued events. After each completed request
 * one queued event is released, so a busy connection cannot starve its events.
 * That is an implementation choice, not part of the contract (SPEC 3.8 only
 * promises that event waiting is bounded).
 *
 * Separating the queues is what lets a result overtake an event frame that has
 * not started writing. A frame already handed to the transport stays in front;
 * it cannot be preempted, which is the accepted cost of one writer per
 * connection. Different connections have their own writer and stay concurrent.
 */
export class ConnectionWriter {
  private readonly requests: RequestWork[] = [];
  private readonly events: EventWrite[] = [];
  private readonly maxQueuedEvents: number;
  private readonly maxQueuedEventBytes: number;
  private readonly backlog: EventBacklog;
  private readonly logger: Logger;
  private running = false;
  /** Frames accepted and not yet written, including one being written. */
  private eventFrames = 0;
  private eventBytes = 0;
  /**
   * Settles when the most recently accepted request work does. Work runs FIFO,
   * so the newest completion implies every earlier request settled; shutdown
   * waits on this to drain requests without waiting for events.
   */
  private requestTail: Promise<void> = Promise.resolve();

  constructor(options: ConnectionWriterOptions) {
    this.maxQueuedEvents = options.maxQueuedEvents;
    this.maxQueuedEventBytes = options.maxQueuedEventBytes;
    this.backlog = options.backlog;
    this.logger = options.logger;
  }

  get requestsSettled(): Promise<void> {
    return this.requestTail;
  }

  submit(work: () => Promise<void>, queued?: QueuedRequest): Promise<void> {
    const { promise, resolve, reject } = Promise.withResolvers<void>();
    this.requests.push({
      run: work,
      done: () => resolve(),
      fail: (error: unknown) => reject(error),
      queued,
    });
    this.requestTail = promise.then(
      () => undefined,
      () => undefined,
    );
    this.start();
    return promise;
  }

  /**
   * Drops every request that has not started, for shutdown (SPEC 6.3). Work
   * already shifted out of the queue keeps running: it may have committed, and
   * interrupting it is not what a bounded drain is allowed to do.
   *
   * Returning the identities rather than logging here keeps the shutdown
   * policy in the layer. `onDropped` releases the caller's accounting for the
   * seat and `done` settles `requestsSettled`, so a dropped request leaves no
   * count and no hanging barrier behind.
   */
  discardQueuedRequests(): QueuedRequestIdentity[] {
    const identities: QueuedRequestIdentity[] = [];
    for (const work of this.requests) {
      work.queued?.onDropped();
      const identity = work.queued?.identity;
      if (identity !== undefined) {
        identities.push(identity);
      }
      work.done();
    }
    this.requests.length = 0;
    return identities;
  }

  /**
   * Queues one encoded event frame behind the request work. Refused at either
   * bound instead of growing: the caller closes the connection, because only it
   * knows which buffer overflowed and which connection owns it (SPEC 3.8).
   */
  enqueue(event: EventWrite): QueueEventResult {
    if (
      this.eventFrames >= this.maxQueuedEvents ||
      this.eventBytes + event.bytes > this.maxQueuedEventBytes
    ) {
      return {
        accepted: false,
        queuedEvents: this.eventFrames,
        queuedEventBytes: this.eventBytes,
      };
    }
    this.eventFrames += 1;
    this.eventBytes += event.bytes;
    this.events.push(event);
    this.backlog.connectionFrameAdded(this, event.bytes);
    this.start();
    return { accepted: true };
  }

  /**
   * Marks a still-queued frame cancelled. False once the write started: an
   * abort must not release the Session's serial slot while a frame that may
   * already be on the wire is being written (SPEC 3.8).
   */
  cancel(event: EventWrite): boolean {
    if (event.started || event.settled) {
      return false;
    }
    event.settle('aborted');
    this.release(event.bytes);
    return true;
  }

  /**
   * Drops every frame that has not started writing. A frame already handed to
   * the transport is not interrupted: its outcome is bounded by the write's own
   * send timeout, and shutdown does not spend its budget on events (SPEC 3.8).
   */
  discardQueued(): { frames: number; bytes: number } {
    let frames = 0;
    let bytes = 0;
    for (const event of this.events) {
      if (event.settled) {
        continue;
      }
      event.settle('discarded');
      frames += 1;
      bytes += event.bytes;
      this.release(event.bytes);
    }
    this.events.length = 0;
    return { frames, bytes };
  }

  private start(): void {
    if (this.running) {
      return;
    }
    this.running = true;
    // Deferred: no write starts inside the executor's event-listener stack.
    // `drain` resets `running` in its own `finally`, so an escaping rejection
    // cannot leave every later request and event on this connection stuck.
    void Promise.resolve()
      .then(() => this.drain())
      .catch((error: unknown) => {
        this.logger.error(
          { reason: messageOf(error) },
          'connection writer stopped unexpectedly',
        );
      });
  }

  private async drain(): Promise<void> {
    try {
      while (true) {
        const request = this.requests.shift();
        if (request !== undefined) {
          // oxlint-disable-next-line eslint/no-await-in-loop -- this writer is the serialization point; running requests concurrently would defeat it
          await this.runRequest(request);
          const fairnessTurn = this.nextEvent();
          if (fairnessTurn !== undefined) {
            // oxlint-disable-next-line eslint/no-await-in-loop -- the turn settles before the next choice, which is what keeps the writer serial
            await this.runEvent(fairnessTurn);
          }
          continue;
        }
        const next = this.nextEvent();
        if (next !== undefined) {
          // oxlint-disable-next-line eslint/no-await-in-loop -- one frame at a time is the single-writer contract
          await this.runEvent(next);
          continue;
        }
        return;
      }
    } finally {
      this.running = false;
    }
  }

  private async runRequest(request: RequestWork): Promise<void> {
    try {
      await request.run();
      request.done();
    } catch (error) {
      request.fail(error);
    }
  }

  private async runEvent(event: EventWrite): Promise<void> {
    try {
      await event.run();
    } finally {
      // Counted from acceptance until the frame settles, so a write in flight
      // stays visible to the bounds and to health (SPEC 3.8, 6.6).
      this.release(event.bytes);
    }
  }

  private release(bytes: number): void {
    this.eventFrames -= 1;
    this.eventBytes -= bytes;
    this.backlog.connectionFrameRemoved(this, bytes, this.eventFrames === 0);
  }

  /** The next frame that was not cancelled while it waited. */
  private nextEvent(): EventWrite | undefined {
    while (this.events.length > 0) {
      const event = this.events.shift();
      if (event !== undefined && !event.settled) {
        return event;
      }
    }
    return undefined;
  }
}
