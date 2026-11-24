import { describe, expect, test } from 'bun:test';

import {
  expectOnlyHello,
  failureOf,
  flushTicks,
  harness,
  messagesOf,
  readyMessage,
  request,
  requestIds,
  serverAt,
  started,
  successMessage,
} from '../test-support/ipc-executor.ts';

describe('IpcExecutor cancel', () => {
  test('cancel sends a cancel message to the server', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h);

    void h.executor.execute(request({ requestId: 'req-1', sessionId: 's1' }));
    expect(requestIds(server)).toEqual(['req-1']);

    await h.executor.cancel('req-1', 'TRANSPORT_CLOSED');

    expect(messagesOf(server)).toContainEqual({
      type: 'cancel',
      requestId: 'req-1',
      code: 'TRANSPORT_CLOSED',
    });
    const cancelled = h.logs().find((line) => line.msg === 'request cancelled');
    expect(cancelled).toMatchObject({
      level: 'warn',
      requestId: 'req-1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
      code: 'TRANSPORT_CLOSED',
    });
    expect(typeof cancelled?.durationMs).toBe('number');
    await h.executor.close();
  });

  test('cancel settles locally and stops replaying the request', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    expect(requestIds(first)).toEqual(['req-1']);

    await h.executor.cancel('req-1', 'TRANSPORT_CLOSED');
    const outcome = await outcomePromise;

    expect(outcome.ok).toBe(false);
    expect(failureOf(outcome)).toEqual({
      code: 'TRANSPORT_CLOSED',
      message: 'request req-1 was cancelled',
    });

    // The buffer no longer holds the id, so a crash cannot replay it: its
    // answer was already discarded, and a replay would be work nobody reads.
    first.crash();
    await flushTicks();
    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();
    expect(requestIds(second)).toEqual([]);

    // A result for the cancelled id is a late result, not a second settlement.
    second.send(successMessage('req-1', 'too late'));
    await flushTicks();
    expect(h.logs().some((line) => line.msg === 'late result discarded')).toBe(
      true,
    );
    await h.executor.close();
  });

  test('cancel for an unknown requestId is a no-op', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h);

    await h.executor.cancel('never-seen', 'TRANSPORT_CLOSED');

    // Nothing was accepted under that id, so there is nothing to tell the
    // Server: only the handshake ever went out.
    expectOnlyHello(server);
    expect(h.logs().some((line) => line.msg === 'request cancelled')).toBe(
      false,
    );
    await h.executor.close();
  });

  test('logs a cancel the channel refuses with the request session', async () => {
    const h = harness({ timeoutMs: 5000, shutdownTimeoutMs: 20 });
    const server = await started(h);

    const outcomePromise = h.executor.execute(
      request({ requestId: 'req-1', sessionId: 's1' }),
    );
    expect(requestIds(server)).toEqual(['req-1']);

    // Closing the executor-side endpoint directly makes `channel.send` throw.
    // The side that closes locally is not notified, so on the in-memory
    // channel this is a synthetic way into that catch; it does not cover the
    // peer-died path.
    server.handle.channel.close('test channel closed');
    await h.executor.cancel('req-1', 'TRANSPORT_CLOSED');

    const failedCancel = h
      .logs()
      .find((line) => line.msg === 'cancel could not be sent');
    expect(failedCancel).toMatchObject({
      level: 'warn',
      requestId: 'req-1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
      code: 'TRANSPORT_CLOSED',
    });
    expect(failureOf(await outcomePromise).code).toBe('TRANSPORT_CLOSED');
    await h.executor.close();
  });
});
