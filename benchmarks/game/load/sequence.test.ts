import { describe, expect, test } from 'bun:test';

import {
  beginSaySequence,
  commitSaySequence,
  observeArrival,
} from './sequence.ts';
import type { SaySequence, SenderArrivals } from './sequence.ts';

interface Run {
  received: number;
  gaps: number;
  duplicates: number;
}

/** Sends accepted/rejected attempts and feeds accepted seqs to one receiver. */
function runSays(accepted: readonly boolean[]): Run {
  const sequence: SaySequence = { nextSeq: 0 };
  const seen = new Map<string, SenderArrivals>();
  const run: Run = { received: 0, gaps: 0, duplicates: 0 };
  for (const ok of accepted) {
    const seq = beginSaySequence(sequence);
    if (!ok) {
      continue;
    }
    commitSaySequence(sequence, seq);
    const arrival = observeArrival(seen, 'sender', 0, seq);
    run.received += 1;
    run.gaps += arrival.gap;
    run.duplicates += arrival.duplicate ? 1 : 0;
  }
  return run;
}

describe('say sequence accounting', () => {
  test('rejected attempts do not advance seq or create receiver gaps', () => {
    const run = runSays([false, true, false, true, true, false, true]);

    // Four accepted says: 0,1,2,3. Rejections never consume a number, so the
    // receiver sees a contiguous run and reports no loss.
    expect(run.received).toBe(4);
    expect(run.gaps).toBe(0);
    expect(run.duplicates).toBe(0);

    const sequence: SaySequence = { nextSeq: 0 };
    expect(beginSaySequence(sequence)).toBe(0);
    commitSaySequence(sequence, 0);
    expect(beginSaySequence(sequence)).toBe(1);
  });

  test('every accepted say arriving once leaves no gaps', () => {
    const run = runSays([true, true, true]);

    expect(run).toEqual({ received: 3, gaps: 0, duplicates: 0 });
  });

  test('a retried timed-out say counts as a duplicate, not a gap', () => {
    const sequence: SaySequence = { nextSeq: 0 };
    const seen = new Map<string, SenderArrivals>();

    const seq = beginSaySequence(sequence);
    // The call timed out but the Server had already broadcast the token.
    const first = observeArrival(seen, 'sender', 0, seq);
    // The retry succeeds under the same seq and is broadcast again.
    commitSaySequence(sequence, seq);
    const second = observeArrival(seen, 'sender', 0, seq);

    expect(first).toEqual({ gap: 0, duplicate: false, lateEpoch: false });
    expect(second).toEqual({ gap: 0, duplicate: true, lateEpoch: false });
    expect(beginSaySequence(sequence)).toBe(seq + 1);
  });

  test('a real skipped successful seq is reported as a gap', () => {
    const seen = new Map<string, SenderArrivals>();
    observeArrival(seen, 'sender', 0, 0);
    const arrival = observeArrival(seen, 'sender', 0, 3);

    expect(arrival).toEqual({ gap: 2, duplicate: false, lateEpoch: false });
  });

  test('a sender epoch change re-bases tracking instead of reporting a gap', () => {
    const seen = new Map<string, SenderArrivals>();
    observeArrival(seen, 'sender', 0, 7);
    const restart = observeArrival(seen, 'sender', 1, 0);
    const late = observeArrival(seen, 'sender', 0, 8);

    expect(restart).toEqual({ gap: 0, duplicate: false, lateEpoch: false });
    expect(late).toEqual({ gap: 0, duplicate: false, lateEpoch: true });
  });
});
