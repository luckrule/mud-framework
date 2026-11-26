import { Buffer } from 'node:buffer';

/** Bun's `send` result: `> 0` accepted, `0` dropped, `< 0` accepted but full. */
export type WriteStatus = number;

/** Hard capacity of one connection's outbound queue; both must be positive. */
export interface SendQueueLimits {
  readonly maxQueuedFrames: number;
  readonly maxQueuedBytes: number;
}

/** Which capacity the frame that failed the queue would have exceeded. */
export type SendQueueLimit = 'maxQueuedFrames' | 'maxQueuedBytes';

/** Snapshot of the queue at the moment a frame no longer fits. */
export interface SendQueueOverflow {
  readonly error: Error;
  readonly limit: SendQueueLimit;
  /** Bytes of the frame that would have exceeded the capacity. */
  readonly incomingBytes: number;
  readonly queuedFrames: number;
  readonly queuedBytes: number;
  readonly maxQueuedFrames: number;
  readonly maxQueuedBytes: number;
}

export interface SendQueueSink {
  write(payload: string | Uint8Array): WriteStatus;
  /** Drops the connection once a payload sat unsent past the timeout. */
  abandon(error: Error): void;
  /** Drops the connection once the queue can hold no more payloads. */
  overflow(info: SendQueueOverflow): void;
}

interface Entry {
  readonly payload: string | Uint8Array;
  readonly bytes: number;
  readonly resolve: () => void;
  readonly reject: (error: Error) => void;
  /** Set once `write` accepted the frame; from then on only `drain` settles it. */
  written: boolean;
  timer: ReturnType<typeof setTimeout> | undefined;
}

/**
 * Per-connection outbound queue. Bun reports `-1` when the socket buffer is
 * full, but it has already accepted that frame, so the entry is held until
 * `drain` proves the buffer emptied: writing it again would duplicate it, and
 * resolving it at once would let an upstream writer outrun a client that never
 * reads (SPEC 3.5 bounded queue).
 *
 * Payloads behind a blocked head are not written at all, so this queue is the
 * buffer and its depth is the backpressure the caller feels. Capacity is a
 * hard bound: once a frame no longer fits, the whole queue fails and the
 * connection is dropped instead of letting refusals pile up in memory.
 */
export class SendQueue {
  private readonly id: string;
  private readonly timeoutMs: number;
  private readonly maxQueuedFrames: number;
  private readonly maxQueuedBytes: number;
  private readonly sink: SendQueueSink;
  private readonly entries: Entry[] = [];
  private unsettledBytes = 0;
  private closed: Error | undefined;
  private blocked = false;
  private flushing = false;
  /** Set when `drain` lands while `flush` is already running. */
  private rerun = false;

  constructor(
    id: string,
    timeoutMs: number,
    limits: SendQueueLimits,
    sink: SendQueueSink,
  ) {
    assertPositiveInteger(limits.maxQueuedFrames, 'maxQueuedFrames');
    assertPositiveInteger(limits.maxQueuedBytes, 'maxQueuedBytes');
    this.id = id;
    this.timeoutMs = timeoutMs;
    this.maxQueuedFrames = limits.maxQueuedFrames;
    this.maxQueuedBytes = limits.maxQueuedBytes;
    this.sink = sink;
  }

  /** Payloads still unsettled; `0` means nothing is waiting on the socket. */
  get depth(): number {
    return this.entries.length;
  }

  /** Bytes of the unsettled payloads; released as frames settle. */
  get queuedBytes(): number {
    return this.unsettledBytes;
  }

  /**
   * Resolves once the frame is on a socket with room to spare. Under
   * backpressure it stays pending — neither resolved nor rejected — until
   * `drain` or the timeout, which is what slows the writer down. A frame that
   * would exceed either capacity fails the queue and rejects at once.
   */
  send(payload: string | Uint8Array): Promise<void> {
    const closed = this.closed;
    if (closed !== undefined) {
      return Promise.reject(closed);
    }
    const bytes = payloadBytes(payload);
    if (this.entries.length + 1 > this.maxQueuedFrames) {
      return this.overflow('maxQueuedFrames', bytes);
    }
    if (this.unsettledBytes + bytes > this.maxQueuedBytes) {
      return this.overflow('maxQueuedBytes', bytes);
    }
    return new Promise<void>((resolve, reject) => {
      const entry: Entry = {
        payload,
        bytes,
        resolve,
        reject,
        written: false,
        timer: undefined,
      };
      if (Number.isFinite(this.timeoutMs)) {
        entry.timer = setTimeout(() => {
          this.expire(entry);
        }, this.timeoutMs);
      }
      this.entries.push(entry);
      this.unsettledBytes += bytes;
      this.pump();
    });
  }

  /** Bun's `drain` callback: the socket buffer has room again. */
  drain(): void {
    this.blocked = false;
    this.pump();
  }

  /** Rejects everything outstanding; later sends reject with the same error. */
  fail(error: Error): void {
    this.closeWith(error);
  }

  /**
   * Fails the queue once when the next frame no longer fits. The caller is
   * told synchronously, exactly once, after every pending promise has been
   * rejected, so the transport can close the connection without racing a
   * second overflow.
   */
  private overflow(
    limit: SendQueueLimit,
    incomingBytes: number,
  ): Promise<never> {
    const queuedFrames = this.entries.length;
    const queuedBytes = this.unsettledBytes;
    const error = new Error(
      `connection "${this.id}" send queue overflow (${limit}): ${queuedFrames}/${this.maxQueuedFrames} frames and ${queuedBytes}/${this.maxQueuedBytes} bytes queued; next frame is ${incomingBytes} bytes`,
    );
    this.closeWith(error);
    this.sink.overflow({
      error,
      limit,
      incomingBytes,
      queuedFrames,
      queuedBytes,
      maxQueuedFrames: this.maxQueuedFrames,
      maxQueuedBytes: this.maxQueuedBytes,
    });
    return Promise.reject(error);
  }

  private closeWith(error: Error): void {
    if (this.closed !== undefined) {
      return;
    }
    this.closed = error;
    this.blocked = false;
    const outstanding = this.entries.splice(0);
    this.unsettledBytes = 0;
    for (const entry of outstanding) {
      this.clearTimer(entry);
      entry.reject(error);
    }
  }

  private expire(entry: Entry): void {
    entry.timer = undefined;
    if (!this.detach(entry)) {
      return;
    }
    const error = new Error(
      `connection "${this.id}" send timed out after ${this.timeoutMs}ms`,
    );
    entry.reject(error);
    // A client that stopped reading cannot be waited out: without this the
    // promise would hang forever and the queue would grow without bound.
    this.sink.abandon(error);
  }

  /**
   * The only place `flush` runs from, so a `drain` that arrives mid-write can
   * never start a second pass and reorder the queue.
   */
  private pump(): void {
    if (this.flushing) {
      this.rerun = true;
      return;
    }
    this.flushing = true;
    try {
      do {
        this.rerun = false;
        this.flush();
      } while (this.rerun && this.closed === undefined);
    } finally {
      this.flushing = false;
    }
  }

  private flush(): void {
    while (this.entries.length > 0 && this.closed === undefined) {
      const head = this.entries[0];
      if (head === undefined) {
        return;
      }
      if (head.written) {
        // Already in Bun's buffer; only `drain` may release it.
        if (this.blocked) {
          return;
        }
        this.settle(head, undefined);
        continue;
      }
      let status: WriteStatus;
      try {
        status = this.sink.write(head.payload);
      } catch (error) {
        this.fail(toError(error));
        return;
      }
      if (status === 0) {
        // Bun rejected the frame outright, including a zero-byte one: bytes
        // is not evidence of a write, `status` is.
        this.settle(
          head,
          new Error(
            `connection "${this.id}" dropped a ${head.bytes} byte payload`,
          ),
        );
        continue;
      }
      head.written = true;
      if (status < 0) {
        // Accepted by Bun, but the buffer is now full; hold this entry so the
        // next payload waits for room instead of piling onto an unread socket.
        this.blocked = true;
        return;
      }
      this.settle(head, undefined);
    }
  }

  private settle(entry: Entry, error: Error | undefined): void {
    if (!this.detach(entry)) {
      return;
    }
    if (error === undefined) {
      entry.resolve();
      return;
    }
    entry.reject(error);
  }

  /** Removes a settled entry; false when it had already settled. */
  private detach(entry: Entry): boolean {
    const index = this.entries.indexOf(entry);
    if (index === -1) {
      return false;
    }
    this.entries.splice(index, 1);
    this.unsettledBytes -= entry.bytes;
    this.clearTimer(entry);
    return true;
  }

  private clearTimer(entry: Entry): void {
    if (entry.timer !== undefined) {
      clearTimeout(entry.timer);
      entry.timer = undefined;
    }
  }
}

export function payloadBytes(payload: string | Uint8Array): number {
  // UTF-8 size for strings: WebSocket text frames are UTF-8 on the wire, so a
  // byte cap must not count UTF-16 code units.
  return typeof payload === 'string'
    ? Buffer.byteLength(payload)
    : payload.byteLength;
}

export function assertPositiveInteger(value: unknown, name: string): void {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a positive integer, got ${String(value)}`,
    );
  }
  if (!Number.isInteger(value) || value < 1) {
    throw new RangeError(
      `${name} must be a positive integer, got ${String(value)}`,
    );
  }
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
