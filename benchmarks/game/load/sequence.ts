/**
 * Say sequence accounting shared by the generator's sender and receiver paths.
 *
 * A sequence number is reserved for an attempt but only committed when the
 * Server accepted the `say`. Failed attempts (queue full, timeout, closed
 * client) therefore leave no hole: the retry reuses the same seq, and a
 * receiver that got every accepted say sees a contiguous run. A timed-out
 * attempt may still have been broadcast, so its retry can arrive twice under
 * one seq; that is counted as a duplicate, never as a gap (see README).
 */

export interface SaySequence {
  /** Next seq an attempt may use; only an accepted say advances it. */
  nextSeq: number;
}

export function beginSaySequence(sequence: SaySequence): number {
  return sequence.nextSeq;
}

export function commitSaySequence(sequence: SaySequence, seq: number): void {
  // The client has one say in flight, so only the current reservation can
  // commit. Guarding on equality keeps a stale result from skipping a seq.
  if (sequence.nextSeq === seq) {
    sequence.nextSeq += 1;
  }
}

export interface SenderArrivals {
  epoch: number;
  lastSeq: number;
}

/** Gaps are only meaningful inside one (sender, epoch) successful sequence. */
export interface ArrivalDelta {
  readonly gap: number;
  readonly duplicate: boolean;
  readonly lateEpoch: boolean;
}

export function observeArrival(
  seen: Map<string, SenderArrivals>,
  sender: string,
  epoch: number,
  seq: number,
): ArrivalDelta {
  const known = seen.get(sender);
  if (known === undefined || epoch > known.epoch) {
    seen.set(sender, { epoch, lastSeq: seq });
    return { gap: 0, duplicate: false, lateEpoch: false };
  }
  if (epoch < known.epoch) {
    return { gap: 0, duplicate: false, lateEpoch: true };
  }
  if (seq > known.lastSeq) {
    const gap = seq - known.lastSeq - 1;
    known.lastSeq = seq;
    return { gap, duplicate: false, lateEpoch: false };
  }
  return { gap: 0, duplicate: true, lateEpoch: false };
}
