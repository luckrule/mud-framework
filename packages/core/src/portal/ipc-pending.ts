import type { ExecutorOutcome, ExecutorRequest } from '../shared/executor.ts';

/**
 * A Request that has been accepted but not acknowledged (SPEC §2: only a
 * `result` acknowledges). It stays here across crashes and is removed only by a
 * result or by a Portal-decided terminal state.
 */
export interface PendingRequest {
  readonly request: ExecutorRequest;
  /**
   * Ordering key for replay. Comes from one global counter, so it is strictly
   * ascending within any connection (all SPEC 6.5 needs) without a
   * per-connection map that would never shrink.
   */
  readonly perConnectionSeq: number;
  /** When the Portal accepted the request; the deadline is measured from here. */
  readonly startedAt: number;
  readonly resolve: (outcome: ExecutorOutcome) => void;
  replayCount: number;
  /** Server generation this request was last sent on; `undefined` = never sent. */
  sentOn: number | undefined;
  deadlineAt: number;
  effectiveTimeoutMs: number;
  timer: ReturnType<typeof setTimeout> | undefined;
}

export interface PendingBufferOptions {
  /**
   * Requests buffered but not yet acknowledged. Their lifetime is bounded by
   * the request deadline, so this is the brake for the one case the deadline
   * cannot bound: a Server that stays unavailable while clients keep writing.
   */
  maxPending: number;
}

/**
 * The unacknowledged-request buffer: registration, lookup, the capacity limit
 * and the SPEC 6.5 order, and nothing else.
 *
 * Delivery, deadlines and settlement are not here because each of them reads
 * or drives the executor's state machine: `flush` needs Server readiness and
 * the channel, a timer needs the entry to still be buffered, and settling
 * resolves a promise and wakes the drain waiter. What is left is pure
 * bookkeeping, which is why the ordering rule is testable without a Server.
 */
export class PendingBuffer {
  private readonly pending = new Map<string, PendingRequest>();
  private readonly capacity: number;
  /** Global and monotonic: see `perConnectionSeq`. */
  private seq = 0;

  constructor(options: PendingBufferOptions) {
    if (!Number.isInteger(options.maxPending) || options.maxPending < 1) {
      throw new Error(
        `maxPending must be a positive integer, got ${options.maxPending}`,
      );
    }
    this.capacity = options.maxPending;
  }

  get size(): number {
    return this.pending.size;
  }

  get isEmpty(): boolean {
    return this.pending.size === 0;
  }

  /** The cap `add` enforces, so a refusal can report it. */
  get maxPending(): number {
    return this.capacity;
  }

  /**
   * Registers a new request and stamps its identity: sequence, start and
   * deadline. Returns undefined when the buffer is full; the caller picks the
   * refusal code.
   *
   * The timeout is passed in rather than chosen here because deciding it (a
   * caller's tighter deadline versus the backend's declared ceiling) is
   * executor policy, not buffering.
   *
   * Id uniqueness is the caller's as well: a second `add` for the same id would
   * replace the entry and orphan the promise the first one held.
   */
  add(
    request: ExecutorRequest,
    outcome: PromiseWithResolvers<ExecutorOutcome>,
    effectiveTimeoutMs: number,
  ): PendingRequest | undefined {
    if (this.pending.size >= this.capacity) {
      return undefined;
    }
    const startedAt = Date.now();
    const pending: PendingRequest = {
      request,
      perConnectionSeq: this.nextSeq(),
      startedAt,
      resolve: outcome.resolve,
      replayCount: 0,
      sentOn: undefined,
      deadlineAt: startedAt + effectiveTimeoutMs,
      effectiveTimeoutMs,
      timer: undefined,
    };
    this.pending.set(request.requestId, pending);
    return pending;
  }

  get(requestId: string): PendingRequest | undefined {
    return this.pending.get(requestId);
  }

  /** Removes and returns the entry; the caller settles the promise. */
  take(requestId: string): PendingRequest | undefined {
    const pending = this.pending.get(requestId);
    if (pending !== undefined) {
      this.pending.delete(requestId);
    }
    return pending;
  }

  /**
   * The requests not yet sent on `generation`, in delivery order (SPEC 6.5).
   *
   * Ordered per connection by `perConnectionSeq`. Replays and never-sent
   * requests share that one ordering: a request enqueued during a restart
   * always has a higher sequence than the replays on its connection, so one
   * sort per connection satisfies both rules and correctness does not depend on
   * insertion order.
   *
   * Groups follow the order their connection first appears. Nothing contracts
   * that order — sends on different connections are independent — but keeping
   * it stable makes a flush reproducible.
   */
  undelivered(generation: number): PendingRequest[] {
    const byConnection = new Map<string, PendingRequest[]>();
    for (const pending of this.pending.values()) {
      if (pending.sentOn === generation) {
        continue;
      }
      const group = byConnection.get(pending.request.connectionId);
      if (group === undefined) {
        byConnection.set(pending.request.connectionId, [pending]);
      } else {
        group.push(pending);
      }
    }
    const ordered: PendingRequest[] = [];
    for (const group of byConnection.values()) {
      group.sort(
        (left, right) => left.perConnectionSeq - right.perConnectionSeq,
      );
      for (const pending of group) {
        ordered.push(pending);
      }
    }
    return ordered;
  }

  /**
   * Live view in insertion order. Removing the entry a loop is on is safe (a
   * Map tolerates it), which is what lets the close path settle while walking.
   */
  values(): IterableIterator<PendingRequest> {
    return this.pending.values();
  }

  /** Empties the buffer and returns everything it held, in insertion order. */
  clear(): PendingRequest[] {
    const all = [...this.pending.values()];
    this.pending.clear();
    return all;
  }

  private nextSeq(): number {
    this.seq += 1;
    return this.seq;
  }
}
