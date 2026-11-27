import type { LogFields, Logger } from '../logging/types.ts';

/**
 * A bucket silent for this long reports its next drop at once. Without it a
 * low-rate stream — a room broadcasting to players who are offline — would stay
 * quiet as its cumulative count grows, because the doubling rule only asks for
 * rarer lines the longer it runs.
 */
const QUIET_RESET_MS = 60_000;

/**
 * Buckets held at once. One bucket per (reason, target) is what keeps a line's
 * `connectionId`/`sessionId` truthful (SPEC 7.1), so the table is bounded by
 * distinct targets rather than by drop count — and `emitToSession` accepts any
 * id, so it needs a cap of its own. 4096 covers a room-sized fan-out with room
 * to spare while staying a few hundred KiB of state.
 *
 * Evicting a bucket flushes whatever it still holds and never silences its
 * target: the next drop starts a new bucket and is reported at once, exactly as
 * the target's first drop was. The cap bounds memory, not line count; a
 * deployment whose distinct dropping targets exceed it can still see a line per
 * drop for the churning targets. That residual is deliberate — merging targets
 * to save lines would mean writing a `connectionId` that is not the drop's own.
 */
const MAX_BUCKETS = 4096;

/**
 * One target's drop accounting. `dropped` is cumulative for the bucket's whole
 * life, so a line's `count` is the difference from the previous line;
 * `logged` remembers where the last line stopped and `loggedAt` when it was
 * written. `first` is the batch's first drop, kept so a folded line names the
 * event that started the batch instead of whichever one happened to be last.
 */
interface DropBucket {
  dropped: number;
  logged: number;
  loggedAt: number;
  first: LogFields;
  /** Last drop seen, for eviction when the table is full. */
  seenAt: number;
}

/**
 * Aggregates repeated `EVENT_DROPPED` lines per reason and target (SPEC 7.1).
 *
 * A missing target or a closed connection can recur at emission rate when a
 * room keeps broadcasting to Sessions nobody holds, so one line per drop turns
 * the diagnostic channel into a second unbounded stream — the exact pressure
 * the event bounds exist to relieve. With a synchronous sink it is worse than
 * unbounded output: it blocks the event loop. The first drop of a bucket is
 * therefore written at once, and later ones fold into a line each time the
 * cumulative count doubles: the line count follows log2(drops) while `count`
 * and `droppedTotal` keep every drop visible, so nothing is silent.
 *
 * A folded line keeps the batch's first `eventId` and `event` (the "which event
 * was dropped" answer) and adds `count` — how many drops this line stands for —
 * and `droppedTotal` — the bucket's running total. Without the first-event
 * fields a folded line would name no event at all; with the last one it would
 * attribute the whole batch to a single event id.
 *
 * Aggregation is logging only. A backlog overflow is not a drop stream: it
 * closes one connection and writes one `EVENT_BACKLOG_DISCONNECTED` line
 * (SPEC 3.8), so it does not go through this log.
 */
export class EventDropLog {
  private readonly logger: Logger;
  private readonly now: () => number;
  private readonly buckets = new Map<string, DropBucket>();

  constructor(options: { logger: Logger; now?: () => number }) {
    this.logger = options.logger;
    this.now = options.now ?? Date.now;
  }

  /**
   * Records one drop and writes a line when this bucket's batch is due.
   * `fields` describes this drop; `reason` and `target` key the bucket, so a
   * flood from one target is never reported as another's.
   */
  record(fields: LogFields, reason: string, target: string): void {
    const now = this.now();
    const bucket = this.bucketFor(reason, target, now);
    if (bucket.dropped === bucket.logged) {
      bucket.first = fields;
    }
    bucket.dropped += 1;
    bucket.seenAt = now;
    const quiet = now - bucket.loggedAt >= QUIET_RESET_MS;
    // First line, then each doubling, then the first drop after a quiet
    // interval. Everything else folds into the next line's `count`.
    if (bucket.logged > 0 && bucket.dropped < bucket.logged * 2 && !quiet) {
      return;
    }
    this.flush(bucket, now);
  }

  private bucketFor(reason: string, target: string, now: number): DropBucket {
    const key = bucketKey(reason, target);
    const existing = this.buckets.get(key);
    if (existing !== undefined) {
      return existing;
    }
    if (this.buckets.size >= MAX_BUCKETS) {
      this.evict(now);
    }
    const bucket: DropBucket = {
      dropped: 0,
      logged: 0,
      loggedAt: now,
      first: {},
      seenAt: now,
    };
    this.buckets.set(key, bucket);
    return bucket;
  }

  /** Writes one line covering every drop since this bucket's last line. */
  private flush(bucket: DropBucket, now: number): void {
    const count = bucket.dropped - bucket.logged;
    if (count === 0) {
      return;
    }
    this.logger.warn(
      { ...bucket.first, count, droppedTotal: bucket.dropped },
      'server event dropped',
    );
    bucket.logged = bucket.dropped;
    bucket.loggedAt = now;
  }

  /**
   * Makes room for one new bucket: stale ones go first, and if the table is
   * still full the least recently seen does. Each removed bucket is flushed, so
   * eviction loses no drop from the log. This only runs when a new target
   * arrives at a full table, so the scan is not on the drop path.
   */
  private evict(now: number): void {
    for (const [key, bucket] of this.buckets) {
      if (now - bucket.seenAt >= QUIET_RESET_MS) {
        this.flush(bucket, now);
        this.buckets.delete(key);
      }
    }
    if (this.buckets.size < MAX_BUCKETS) {
      return;
    }
    let oldestKey: string | undefined;
    let oldestSeen = Number.POSITIVE_INFINITY;
    for (const [key, bucket] of this.buckets) {
      if (bucket.seenAt < oldestSeen) {
        oldestSeen = bucket.seenAt;
        oldestKey = key;
      }
    }
    if (oldestKey !== undefined) {
      const oldest = this.buckets.get(oldestKey);
      if (oldest !== undefined) {
        this.flush(oldest, now);
      }
      this.buckets.delete(oldestKey);
    }
  }
}

/** NUL cannot appear in either part, so the two cannot run together. */
function bucketKey(reason: string, target: string): string {
  return `${reason}\u0000${target}`;
}
