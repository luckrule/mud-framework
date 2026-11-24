import { describe, expect, test } from 'bun:test';

import {
  failureOf,
  flushTicks,
  harness,
  messagesOf,
  readyMessage,
  request,
  requestMessages,
  serverAt,
  started,
  successMessage,
} from '../test-support/ipc-executor.ts';

describe('IpcExecutor timeout', () => {
  test('times out, cancels, and discards the late result', async () => {
    const h = harness({ timeoutMs: 30 });
    const server = await started(h);

    const outcome = await h.executor.execute(
      request({ requestId: 'req-1', sessionId: 's1' }),
    );

    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    expect(failureOf(outcome).message).toContain('timed out');
    const timedOut = h.logs().find((line) => line.msg === 'request timed out');
    expect(timedOut).toMatchObject({
      level: 'warn',
      requestId: 'req-1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
      code: 'FRAMEWORK_TIMEOUT',
    });
    // The Server is told to stop working on a request Portal already settled.
    expect(messagesOf(server)).toContainEqual({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });

    // A result that arrives after the terminal state must be dropped quietly.
    // The Session in the log comes from the result's echo of the entry Session:
    // the pending entry that held it is gone.
    server.send({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 's1',
      ok: true,
      result: 'too late',
      sessionId: 's-late',
    });
    await flushTicks();
    expect(h.logs().some((line) => line.msg === 'late result discarded')).toBe(
      true,
    );
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg === 'late result discarded' &&
            line.level === 'debug' &&
            line.sessionId === 's1',
        ),
    ).toBe(true);
    await h.executor.close();
  });

  test('a late result does not settle the request a second time', async () => {
    const h = harness({ timeoutMs: 30 });
    const server = await started(h);
    let settles = 0;

    const outcome = await h.executor
      .execute(request({ requestId: 'req-1' }))
      .then((value) => {
        settles += 1;
        return value;
      });

    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    server.send(successMessage('req-1', 'too late'));
    await flushTicks();

    expect(settles).toBe(1);
    // Still the timeout: the late result must not replace the settled outcome.
    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    // An unbound result has no session to report: absence must stay absence,
    // not become an empty string.
    const late = h.logs().find((line) => line.msg === 'late result discarded');
    expect(late).toBeDefined();
    expect(late).not.toHaveProperty('sessionId');
    await h.executor.close();
  });

  test('takes the backend ceiling when it is below the portal ceiling', async () => {
    const h = harness({ timeoutMs: 1000 });
    const server = await started(h, 30);

    const outcome = await h.executor.execute(request({ requestId: 'req-1' }));

    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    const timeoutMs = requestMessages(server)[0]?.timeoutMs;
    expect(timeoutMs).toBeGreaterThan(0);
    expect(timeoutMs).toBeLessThanOrEqual(30);
    await h.executor.close();
  });

  test('keeps the portal ceiling when it is below the backend ceiling', async () => {
    const h = harness({ timeoutMs: 25 });
    const server = await started(h, 60_000);

    const outcome = await h.executor.execute(request({ requestId: 'req-1' }));

    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    const timeoutMs = requestMessages(server)[0]?.timeoutMs;
    expect(timeoutMs).toBeGreaterThan(0);
    expect(timeoutMs).toBeLessThanOrEqual(25);
    await h.executor.close();
  });

  test('a later server cannot extend an existing deadline', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h, 100);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    const firstTimeout = requestMessages(first)[0]?.timeoutMs;
    expect(firstTimeout).toBeGreaterThan(0);
    expect(firstTimeout).toBeLessThanOrEqual(100);

    first.crash();
    await flushTicks();
    const second = serverAt(h, 1);
    // A longer ceiling must not reopen the budget the Portal already promised
    // (SPEC 3.4): the replay still gets what is left of the original deadline.
    second.send(readyMessage(60_000));
    await flushTicks();

    const replay = requestMessages(second)[0];
    expect(replay?.replayCount).toBe(1);
    expect(replay?.timeoutMs).toBeGreaterThan(0);
    expect(replay?.timeoutMs).toBeLessThanOrEqual(100);

    second.send(successMessage('req-1', null));
    expect((await outcomePromise).ok).toBe(true);
    await h.executor.close();
  });

  test('a request timeoutMs tightens the deadline and cannot extend it', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h, 60_000);

    // Smaller than the ceiling: the caller's deadline wins, and the Server is
    // given the same budget so it rolls back at the same instant.
    const outcome = await h.executor.execute(
      request({ requestId: 'req-1', timeoutMs: 20 }),
    );
    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    const sent = requestMessages(server)[0]?.timeoutMs;
    expect(sent).toBeGreaterThan(0);
    expect(sent).toBeLessThanOrEqual(20);

    // Larger than the ceiling: the executor's own promise is unchanged.
    const second = h.executor.execute(
      request({ requestId: 'req-2', timeoutMs: 60_000 }),
    );
    const secondTimeout = requestMessages(server)[1]?.timeoutMs;
    expect(secondTimeout).toBeGreaterThan(20);
    expect(secondTimeout).toBeLessThanOrEqual(5000);
    await h.executor.cancel('req-2', 'TRANSPORT_CLOSED');
    await second;
    await h.executor.close();
  });
});
