import { describe, expect, test } from 'bun:test';

import {
  expectHandshake,
  failureOf,
  flushTicks,
  harness,
  readyMessage,
  request,
  requestIds,
  requestMessages,
  serverAt,
  started,
  successMessage,
  waitFor,
} from '../test-support/ipc-executor.ts';

describe('IpcExecutor replay', () => {
  test('restarts after a crash and replays the unacknowledged request', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    const outcomePromise = h.executor.execute(
      request({ requestId: 'req-1', sessionId: 's1' }),
    );
    expect(requestIds(first)).toEqual(['req-1']);

    first.crash();
    await flushTicks();

    expect(h.servers).toHaveLength(2);
    const second = serverAt(h, 1);
    expectHandshake(second);

    second.send(readyMessage());
    await flushTicks();

    // Same id, incremented replay count (SPEC 7.3): the replay is the same
    // Request, not a new one.
    const [replay] = requestMessages(second);
    expect(replay?.requestId).toBe('req-1');
    expect(replay?.replayCount).toBe(1);
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg === 'request replayed' &&
            line.replayCount === 1 &&
            line.sessionId === 's1',
        ),
    ).toBe(true);

    second.send(successMessage('req-1', 'after restart'));
    expect(await outcomePromise).toEqual({
      ok: true,
      result: 'after restart',
    });
    await h.executor.close();
  });

  test('gives up with FRAMEWORK_REPLAY_EXHAUSTED after maxReplays', async () => {
    const h = harness({ timeoutMs: 5000, maxReplays: 1 });
    const first = await started(h);

    const outcomePromise = h.executor.execute(
      request({ requestId: 'req-1', sessionId: 's1' }),
    );

    first.crash();
    // The second restart already pays a real backoff: `ready` no longer resets
    // the crash streak, so the first restart is immediate and later ones are not.
    await waitFor(() => h.servers.length === 2);
    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();
    expect(requestMessages(second)).toHaveLength(1);
    expect(requestMessages(second)[0]?.replayCount).toBe(1);

    second.crash();
    await waitFor(() => h.servers.length === 3);
    const third = serverAt(h, 2);
    third.send(readyMessage());
    await flushTicks();

    // Budget spent: no third delivery, a terminal state instead (SPEC 6.5).
    expect(requestMessages(third)).toHaveLength(0);
    const outcome = await outcomePromise;
    expect(failureOf(outcome).code).toBe('FRAMEWORK_REPLAY_EXHAUSTED');
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg === 'request replay exhausted' &&
            line.level === 'error' &&
            line.sessionId === 's1',
        ),
    ).toBe(true);
    await h.executor.close();
  });

  test('replays one connection in its original order', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    // Two requests on c1 with a c2 request between them: the interleaving is
    // what a timed-out chain produces (the chain releases at the timeout, so
    // several unacknowledged requests can coexist on one connection).
    void h.executor.execute(request({ requestId: 'a1', connectionId: 'c1' }));
    void h.executor.execute(request({ requestId: 'b1', connectionId: 'c2' }));
    void h.executor.execute(request({ requestId: 'a2', connectionId: 'c1' }));
    expect(requestIds(first)).toEqual(['a1', 'b1', 'a2']);

    first.crash();
    await flushTicks();
    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();

    // Grouped by connection, ascending per-connection sequence inside a group.
    expect(requestIds(second)).toEqual(['a1', 'a2', 'b1']);
    for (const message of requestMessages(second)) {
      expect(message.replayCount).toBe(1);
    }

    for (const requestId of ['a1', 'a2', 'b1']) {
      second.send(successMessage(requestId, null));
    }
    await h.executor.close();
  });

  test('orders a replay before a request queued during the restart', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    const firstOutcome = h.executor.execute(
      request({ requestId: 'A', connectionId: 'c1' }),
    );
    expect(requestIds(first)).toEqual(['A']);

    first.crash();
    await flushTicks();
    const second = serverAt(h, 1);
    // Queued while the replacement is still starting: a fresh request with a
    // higher per-connection sequence than the replay.
    const secondOutcome = h.executor.execute(
      request({ requestId: 'B', connectionId: 'c1' }),
    );

    second.send(readyMessage());
    await flushTicks();

    const messages = requestMessages(second);
    expect(messages.map((message) => message.requestId)).toEqual(['A', 'B']);
    expect(messages.map((message) => message.replayCount)).toEqual([1, 0]);

    second.send(successMessage('A', null));
    second.send(successMessage('B', null));
    await Promise.all([firstOutcome, secondOutcome]);
    await h.executor.close();
  });

  test('resumes the flush after a mid-flush delivery failure', async () => {
    const h = harness({ timeoutMs: 5000 });
    const startPromise = h.executor.start();
    const first = serverAt(h, 0);
    // The second delivery kills the link: the flush must stop, then replay
    // everything unsent on the replacement without dropping or duplicating.
    first.closeOnRequest(2);

    const outcomes = ['A', 'B', 'C'].map((requestId) =>
      h.executor.execute(request({ requestId, connectionId: 'c1' })),
    );

    first.send(readyMessage());
    await startPromise;
    await flushTicks();

    expect(requestIds(first)).toEqual(['A', 'B']);
    expect(h.servers).toHaveLength(2);

    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();

    const replayed = requestMessages(second);
    expect(replayed.map((message) => message.requestId)).toEqual([
      'A',
      'B',
      'C',
    ]);
    // A and B reached the first Server (its close ran inside B's send), so both
    // are replays; C never left the Portal.
    expect(replayed.map((message) => message.replayCount)).toEqual([1, 1, 0]);

    for (const requestId of ['A', 'B', 'C']) {
      second.send(successMessage(requestId, null));
    }
    const settled = await Promise.all(outcomes);
    expect(settled.every((outcome) => outcome.ok)).toBe(true);
    await h.executor.close();
  });
});
