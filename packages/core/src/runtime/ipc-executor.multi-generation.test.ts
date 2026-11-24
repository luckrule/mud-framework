import { describe, expect, jest, test } from 'bun:test';

import {
  failureOf,
  flushMicrotasks,
  harness,
  readyMessage,
  request,
  requestIds,
  requestMessages,
  serverAt,
  successMessage,
} from '../test-support/ipc-executor.ts';
import type {
  FakeServerProcess,
  Harness,
} from '../test-support/ipc-executor.ts';
import type { ExecutorOutcome } from './executor.ts';

interface DeliveredRequest {
  requestId: string;
  replayCount: number;
}

/**
 * Deliveries grouped by connection. SPEC 6.5 orders a connection's
 * unacknowledged requests; it says nothing about the order between
 * connections, so only the per-connection sequence is asserted here.
 */
function deliveredByConnection(
  server: FakeServerProcess,
): Record<string, DeliveredRequest[]> {
  const groups: Record<string, DeliveredRequest[]> = {};
  for (const message of requestMessages(server)) {
    const group = groups[message.connectionId] ?? [];
    group.push({
      requestId: message.requestId,
      replayCount: message.replayCount,
    });
    groups[message.connectionId] = group;
  }
  return groups;
}

function logCount(h: Harness, message: string): number {
  return h.logs().filter((line) => line.msg === message).length;
}

function submit(
  h: Harness,
  requestId: string,
  connectionId: string,
): Promise<ExecutorOutcome> {
  return h.executor.execute(request({ requestId, connectionId }));
}

/** Advances the fake clock and lets the timers' continuations run. */
async function advance(ms: number): Promise<void> {
  jest.advanceTimersByTime(ms);
  await flushMicrotasks();
}

describe('IpcExecutor multi-generation recovery', () => {
  test('replays interleaved requests across four generations and three crashes, then stops after settlement', async () => {
    jest.useFakeTimers();
    try {
      const maxReplays = 3;
      const h = harness({
        timeoutMs: 5000,
        maxReplays,
        restartBackoffMs: 100,
        // The real spawn records the exit before it notifies the channel close,
        // so this is the production crash ordering.
        closeChannelOnExit: true,
      });
      const startPromise = h.executor.start();
      const gen1 = serverAt(h, 0);
      gen1.send(readyMessage());
      await startPromise;

      // Two connections interleaved on gen1.
      const a1 = submit(h, 'a1', 'c1');
      const a2 = submit(h, 'a2', 'c1');
      const a3 = submit(h, 'a3', 'c1');
      const b1 = submit(h, 'b1', 'c2');
      const b2 = submit(h, 'b2', 'c2');
      expect(deliveredByConnection(gen1)).toEqual({
        c1: [
          { requestId: 'a1', replayCount: 0 },
          { requestId: 'a2', replayCount: 0 },
          { requestId: 'a3', replayCount: 0 },
        ],
        c2: [
          { requestId: 'b1', replayCount: 0 },
          { requestId: 'b2', replayCount: 0 },
        ],
      });

      // Acknowledge the first request of each connection before the crash.
      gen1.send(successMessage('a1', 'gen1-a1'));
      gen1.send(successMessage('b1', 'gen1-b1'));
      expect(await a1).toEqual({ ok: true, result: 'gen1-a1' });
      expect(await b1).toEqual({ ok: true, result: 'gen1-b1' });

      // A duplicate result is late: it is discarded, not settled again. The log
      // is the observable, because a promise's `.then` cannot see a second
      // resolve.
      const lateOnGen1 = logCount(h, 'late result discarded');
      gen1.send(successMessage('a1', 'again'));
      await flushMicrotasks();
      expect(logCount(h, 'late result discarded')).toBe(lateOnGen1 + 1);

      // Crash 1 of 3.
      gen1.crash();
      await flushMicrotasks();
      // Only the 0ms restart timer has been armed: this shows the restart is not
      // a synchronous spawn, not that the exit was awaited. The single-writer
      // gate (INV-002) is covered by the test below.
      expect(h.servers).toHaveLength(1);

      // Arrive while no Server is live: buffered, not failed.
      const d = submit(h, 'd', 'c1');
      const e = submit(h, 'e', 'c2');

      // First restart of the streak is immediate but still a timer.
      await advance(1);
      expect(h.servers).toHaveLength(2);

      const gen2 = serverAt(h, 1);
      gen2.send(readyMessage());
      await flushMicrotasks();

      // Replays first, then the requests queued during the restart window; one
      // group per connection, ascending per-connection sequence inside a group.
      expect(deliveredByConnection(gen2)).toEqual({
        c1: [
          { requestId: 'a2', replayCount: 1 },
          { requestId: 'a3', replayCount: 1 },
          { requestId: 'd', replayCount: 0 },
        ],
        c2: [
          { requestId: 'b2', replayCount: 1 },
          { requestId: 'e', replayCount: 0 },
        ],
      });

      gen2.send(successMessage('a2', 'gen2-a2'));
      expect(await a2).toEqual({ ok: true, result: 'gen2-a2' });

      // Crash 2 of 3, with a3, d, b2 and e unacknowledged.
      gen2.crash();
      await flushMicrotasks();
      expect(h.servers).toHaveLength(2);

      // `ready` does not end the crash streak, so the next restart pays the
      // base backoff instead of being immediate.
      await advance(99);
      expect(h.servers).toHaveLength(2);
      await advance(1);
      expect(h.servers).toHaveLength(3);

      const gen3 = serverAt(h, 2);
      gen3.send(readyMessage());
      await flushMicrotasks();

      // a3 and b2 were replayed twice; d and e were first delivered on gen2 and
      // are replayed for the first time here.
      expect(deliveredByConnection(gen3)).toEqual({
        c1: [
          { requestId: 'a3', replayCount: 2 },
          { requestId: 'd', replayCount: 1 },
        ],
        c2: [
          { requestId: 'b2', replayCount: 2 },
          { requestId: 'e', replayCount: 1 },
        ],
      });

      // Crash 3 of 3 before anything on gen3 is settled: the same four requests
      // cross a third crash, now at the doubled backoff.
      gen3.crash();
      await flushMicrotasks();
      expect(h.servers).toHaveLength(3);
      await advance(199);
      expect(h.servers).toHaveLength(3);
      await advance(1);
      expect(h.servers).toHaveLength(4);

      const gen4 = serverAt(h, 3);
      gen4.send(readyMessage());
      await flushMicrotasks();

      // Fourth generation: the replay counts have reached the maxReplays edge.
      // One more crash before a result would exhaust a3 and b2 instead of
      // delivering them.
      expect(deliveredByConnection(gen4)).toEqual({
        c1: [
          { requestId: 'a3', replayCount: maxReplays },
          { requestId: 'd', replayCount: 2 },
        ],
        c2: [
          { requestId: 'b2', replayCount: maxReplays },
          { requestId: 'e', replayCount: 2 },
        ],
      });

      gen4.send(successMessage('a3', 'gen4-a3'));
      gen4.send(successMessage('d', 'gen4-d'));
      gen4.send(successMessage('b2', 'gen4-b2'));
      gen4.send(successMessage('e', 'gen4-e'));
      expect(await a3).toEqual({ ok: true, result: 'gen4-a3' });
      expect(await d).toEqual({ ok: true, result: 'gen4-d' });
      expect(await b2).toEqual({ ok: true, result: 'gen4-b2' });
      expect(await e).toEqual({ ok: true, result: 'gen4-e' });

      // Settled ids stay settled: duplicate results are late, and the executor
      // logs them as discarded rather than accepting a second settlement.
      const lateSettled = logCount(h, 'late result discarded');
      const receivedSettled = logCount(h, 'result received');
      gen4.send(successMessage('a3', 'again'));
      gen4.send(successMessage('e', 'again'));
      await flushMicrotasks();
      expect(logCount(h, 'late result discarded')).toBe(lateSettled + 2);
      expect(logCount(h, 'result received')).toBe(receivedSettled);

      // And they are not put back in the buffer: after one more crash the next
      // generation receives nothing. This is the at-most-once half of the
      // recovery contract for acknowledged work.
      const replayedBefore = logCount(h, 'request replayed');
      gen4.crash();
      await flushMicrotasks();
      expect(h.servers).toHaveLength(4);
      await advance(399);
      expect(h.servers).toHaveLength(4);
      await advance(1);
      expect(h.servers).toHaveLength(5);

      const gen5 = serverAt(h, 4);
      const readyBefore = logCount(h, 'server ready');
      gen5.send(readyMessage());
      await flushMicrotasks();
      expect(logCount(h, 'server ready')).toBe(readyBefore + 1);
      expect(deliveredByConnection(gen5)).toEqual({});
      expect(logCount(h, 'request replayed')).toBe(replayedBefore);

      await h.executor.close();
    } finally {
      jest.useRealTimers();
    }
  });

  test('spawns no successor while the previous process may still be writing', async () => {
    jest.useFakeTimers();
    try {
      const h = harness({
        timeoutMs: 5000,
        killExits: false,
        restartBackoffMs: 100,
      });
      const startPromise = h.executor.start();
      const gen1 = serverAt(h, 0);
      gen1.send(readyMessage());
      await startPromise;

      // The link dies but the process survives SIGKILL: restarting beside it
      // would give the storage two writers (INV-002).
      gen1.closeChannel();
      await flushMicrotasks();
      expect(gen1.killCount).toBe(1);
      expect(h.servers).toHaveLength(1);

      // A request arriving in this window waits; it must not reach a process
      // whose link is already dead.
      let queuedSettled = false;
      const queued = h.executor
        .execute(request({ requestId: 'q1', connectionId: 'c1' }))
        .then((outcome) => {
          queuedSettled = true;
          return outcome;
        });

      // Not a one-tick illusion: the backoff would have expired, yet no
      // successor may appear while the exit is still unobserved, and q1 has not
      // reached any Server (dead or replacement) before its handshake.
      await advance(1000);
      expect(h.servers).toHaveLength(1);
      expect(queuedSettled).toBe(false);
      expect(h.servers.flatMap((server) => requestIds(server))).not.toContain(
        'q1',
      );

      gen1.crash();
      await flushMicrotasks();
      await advance(1);
      expect(h.servers).toHaveLength(2);

      const gen2 = serverAt(h, 1);
      // Still buffered before `ready`: the handshake is the delivery barrier.
      expect(requestIds(gen2)).toEqual([]);
      gen2.send(readyMessage());
      await flushMicrotasks();
      expect(deliveredByConnection(gen2)).toEqual({
        c1: [{ requestId: 'q1', replayCount: 0 }],
      });
      gen2.send(successMessage('q1', 'gen2'));
      expect(await queued).toEqual({ ok: true, result: 'gen2' });

      // Same gate on the second handoff.
      gen2.closeChannel();
      await flushMicrotasks();
      expect(gen2.killCount).toBe(1);
      expect(h.servers).toHaveLength(2);

      await advance(1000);
      expect(h.servers).toHaveLength(2);

      gen2.crash();
      await flushMicrotasks();
      // Second crash of the streak: the base backoff is paid, not bypassed.
      await advance(99);
      expect(h.servers).toHaveLength(2);
      await advance(1);
      expect(h.servers).toHaveLength(3);

      // Third handoff: the backoff doubles again, so the stable window never
      // reset the streak.
      const gen3 = serverAt(h, 2);
      gen3.send(readyMessage());
      await flushMicrotasks();
      gen3.closeChannel();
      await flushMicrotasks();
      expect(gen3.killCount).toBe(1);
      expect(h.servers).toHaveLength(3);

      await advance(1000);
      expect(h.servers).toHaveLength(3);

      gen3.crash();
      await flushMicrotasks();
      await advance(199);
      expect(h.servers).toHaveLength(3);
      await advance(1);
      expect(h.servers).toHaveLength(4);

      const gen4 = serverAt(h, 3);
      gen4.send(readyMessage());
      await flushMicrotasks();
      await h.executor.close();
    } finally {
      jest.useRealTimers();
    }
  });

  test('exhausts one request across generations without stalling the next generation', async () => {
    jest.useFakeTimers();
    try {
      const h = harness({
        timeoutMs: 5000,
        maxReplays: 1,
        restartBackoffMs: 100,
      });
      const startPromise = h.executor.start();
      const gen1 = serverAt(h, 0);
      gen1.send(readyMessage());
      await startPromise;

      const p = submit(h, 'P', 'c1');
      const q = submit(h, 'Q', 'c1');
      expect(deliveredByConnection(gen1)).toEqual({
        c1: [
          { requestId: 'P', replayCount: 0 },
          { requestId: 'Q', replayCount: 0 },
        ],
      });
      gen1.send(successMessage('Q', 'gen1-q'));
      expect(await q).toEqual({ ok: true, result: 'gen1-q' });

      gen1.crash();
      await flushMicrotasks();
      await advance(1);

      const gen2 = serverAt(h, 1);
      gen2.send(readyMessage());
      await flushMicrotasks();
      expect(deliveredByConnection(gen2)).toEqual({
        c1: [{ requestId: 'P', replayCount: 1 }],
      });

      gen2.crash();
      await flushMicrotasks();
      await advance(100);

      const gen3 = serverAt(h, 2);
      gen3.send(readyMessage());
      await flushMicrotasks();

      // Budget spent: the request reaches a terminal state instead of another
      // delivery (SPEC 6.5).
      expect(deliveredByConnection(gen3)).toEqual({});
      expect(failureOf(await p).code).toBe('FRAMEWORK_REPLAY_EXHAUSTED');
      const exhausted = h
        .logs()
        .find((line) => line.msg === 'request replay exhausted');
      expect(exhausted).toMatchObject({
        level: 'error',
        code: 'FRAMEWORK_REPLAY_EXHAUSTED',
      });
      expect(exhausted?.replayCount).toBe(1);

      // A late result for the exhausted request is discarded, not accepted:
      // `result received` is the observable of acceptance and must not move.
      const lateBefore = logCount(h, 'late result discarded');
      const receivedBefore = logCount(h, 'result received');
      gen3.send(successMessage('P', 'late'));
      await flushMicrotasks();
      expect(logCount(h, 'late result discarded')).toBe(lateBefore + 1);
      expect(logCount(h, 'result received')).toBe(receivedBefore);

      // The generation itself is usable: exhaustion is per-request.
      const r = submit(h, 'R', 'c1');
      expect(deliveredByConnection(gen3)).toEqual({
        c1: [{ requestId: 'R', replayCount: 0 }],
      });
      gen3.send(successMessage('R', 'gen3-r'));
      expect(await r).toEqual({ ok: true, result: 'gen3-r' });

      await h.executor.close();
    } finally {
      jest.useRealTimers();
    }
  });
});
