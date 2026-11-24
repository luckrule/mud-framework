import { describe, expect, test } from 'bun:test';

import { request } from '../test-support/ipc-executor.ts';
import type { ExecutorOutcome, ExecutorRequest } from './executor.ts';
import type { PendingRequest } from './ipc-pending.ts';
import { PendingBuffer } from './ipc-pending.ts';

function add(
  buffer: PendingBuffer,
  overrides: Partial<ExecutorRequest>,
  effectiveTimeoutMs = 1000,
): PendingRequest {
  const pending = buffer.add(
    request(overrides),
    Promise.withResolvers<ExecutorOutcome>(),
    effectiveTimeoutMs,
  );
  if (pending === undefined) {
    throw new Error('buffer refused an entry the test expected to fit');
  }
  return pending;
}

function ids(entries: PendingRequest[]): string[] {
  return entries.map((entry) => entry.request.requestId);
}

describe('PendingBuffer registration', () => {
  test('add registers the request and stamps its bookkeeping', () => {
    const buffer = new PendingBuffer({ maxPending: 2 });
    const first = add(buffer, { requestId: 'req-1' }, 250);

    expect(buffer.get('req-1')).toBe(first);
    expect(buffer.size).toBe(1);
    expect(buffer.isEmpty).toBe(false);
    expect(first.request.requestId).toBe('req-1');
    expect(first.perConnectionSeq).toBe(1);
    expect(first.replayCount).toBe(0);
    expect(first.sentOn).toBeUndefined();
    expect(first.timer).toBeUndefined();
    expect(first.effectiveTimeoutMs).toBe(250);
    // The deadline is measured from acceptance, not from a caller's clock.
    expect(first.deadlineAt - first.startedAt).toBe(250);

    const second = add(buffer, { requestId: 'req-2' });
    expect(second.perConnectionSeq).toBe(2);
    expect(buffer.size).toBe(2);
  });

  test('add refuses once maxPending is reached', () => {
    const buffer = new PendingBuffer({ maxPending: 2 });
    add(buffer, { requestId: 'req-1' });
    add(buffer, { requestId: 'req-2' });

    const refused = buffer.add(
      request({ requestId: 'req-3' }),
      Promise.withResolvers<ExecutorOutcome>(),
      1000,
    );

    expect(refused).toBeUndefined();
    expect(buffer.size).toBe(2);
    expect(buffer.get('req-3')).toBeUndefined();
    expect(buffer.maxPending).toBe(2);
  });

  test('rejects a cap that cannot hold one request', () => {
    // The executor reports this at the same point it used to: construction.
    expect(() => new PendingBuffer({ maxPending: 0 })).toThrow(
      /maxPending must be a positive integer/,
    );
    expect(() => new PendingBuffer({ maxPending: 1.5 })).toThrow(
      /maxPending must be a positive integer/,
    );
  });
});

describe('PendingBuffer removal', () => {
  test('take returns the entry and removes it; a second take is empty', () => {
    const buffer = new PendingBuffer({ maxPending: 2 });
    const pending = add(buffer, { requestId: 'req-1' });

    expect(buffer.take('req-1')).toBe(pending);
    expect(buffer.size).toBe(0);
    expect(buffer.isEmpty).toBe(true);
    expect(buffer.take('req-1')).toBeUndefined();
  });

  test('clear returns everything in insertion order and empties the buffer', () => {
    const buffer = new PendingBuffer({ maxPending: 3 });
    add(buffer, { requestId: 'req-3', connectionId: 'c2' });
    add(buffer, { requestId: 'req-1', connectionId: 'c1' });
    add(buffer, { requestId: 'req-2', connectionId: 'c1' });

    expect(ids(buffer.clear())).toEqual(['req-3', 'req-1', 'req-2']);
    expect(buffer.size).toBe(0);
    expect(buffer.isEmpty).toBe(true);
    expect(buffer.get('req-1')).toBeUndefined();
    // Still usable afterwards.
    expect(add(buffer, { requestId: 'req-4' }).perConnectionSeq).toBe(4);
  });
});

describe('PendingBuffer.undelivered', () => {
  test('returns only what was not sent on the requested generation', () => {
    const buffer = new PendingBuffer({ maxPending: 3 });
    const first = add(buffer, { requestId: 'req-1' });
    const second = add(buffer, { requestId: 'req-2' });
    const third = add(buffer, { requestId: 'req-3' });
    first.sentOn = 1;
    third.sentOn = 2;

    expect(ids(buffer.undelivered(1))).toEqual(['req-2', 'req-3']);
    expect(ids(buffer.undelivered(2))).toEqual(['req-1', 'req-2']);
    expect(ids(buffer.undelivered(3))).toEqual(['req-1', 'req-2', 'req-3']);
    expect(second.sentOn).toBeUndefined();
  });

  test('orders one connection by perConnectionSeq, whatever the interleaving', () => {
    const buffer = new PendingBuffer({ maxPending: 4 });
    add(buffer, { requestId: 'a1', connectionId: 'c1' });
    add(buffer, { requestId: 'b1', connectionId: 'c2' });
    add(buffer, { requestId: 'a2', connectionId: 'c1' });
    add(buffer, { requestId: 'a3', connectionId: 'c1' });

    const ordered = ids(buffer.undelivered(1));
    // What the executor sends, in order (SPEC 6.5). The flush test in
    // `ipc-executor.replay.test.ts` pins the same shape end to end.
    expect(ordered).toEqual(['a1', 'a2', 'a3', 'b1']);
    expect(ordered.filter((requestId) => requestId.startsWith('a'))).toEqual([
      'a1',
      'a2',
      'a3',
    ]);
  });

  test('groups multiple connections, each contiguous and ascending', () => {
    const buffer = new PendingBuffer({ maxPending: 5 });
    add(buffer, { requestId: 'a1', connectionId: 'c1' });
    add(buffer, { requestId: 'b1', connectionId: 'c2' });
    add(buffer, { requestId: 'a2', connectionId: 'c1' });
    add(buffer, { requestId: 'b2', connectionId: 'c2' });
    add(buffer, { requestId: 'd1', connectionId: 'c3' });

    const ordered = ids(buffer.undelivered(1));
    // Group order follows first appearance; only the in-group order is
    // contractual, but a stable flattening keeps a flush reproducible.
    expect(ordered).toEqual(['a1', 'a2', 'b1', 'b2', 'd1']);
    for (const connectionId of ['c1', 'c2', 'c3']) {
      const ofConnection = buffer
        .undelivered(1)
        .filter((entry) => entry.request.connectionId === connectionId)
        .map((entry) => entry.perConnectionSeq);
      expect(ofConnection).toEqual(ofConnection.toSorted((a, b) => a - b));
    }
  });

  test('orders by perConnectionSeq alone: a never-sent request keeps its place', () => {
    const buffer = new PendingBuffer({ maxPending: 2 });
    const neverSent = add(buffer, {
      requestId: 'never-sent',
      connectionId: 'c1',
    });
    const replay = add(buffer, {
      requestId: 'needs-replay',
      connectionId: 'c1',
    });
    // Delivered on a previous generation. A never-sent entry with a lower
    // sequence stays in front of it: the rule is one ascending sort per
    // connection, not "replays first". The executor cannot produce this exact
    // state (it flushes in sequence order), so the buffer pins the rule.
    replay.sentOn = 1;

    expect(neverSent.perConnectionSeq).toBeLessThan(replay.perConnectionSeq);
    expect(ids(buffer.undelivered(2))).toEqual(['never-sent', 'needs-replay']);
  });
});
