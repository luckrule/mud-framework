import { describe, expect, test } from 'bun:test';

import { rejectionOf } from '../../testing/common.ts';
import {
  expectHandshake,
  expectOnlyHello,
  failureOf,
  flushTicks,
  harness,
  messagesOf,
  preparedMessage,
  readyMessage,
  request,
  requestIds,
  serverAt,
  started,
  successMessage,
} from '../../testing/ipc-executor.ts';

describe('IpcExecutor handshake', () => {
  test('sends hello, then activate once the server is prepared, and holds requests until ready', async () => {
    const h = harness({ timeoutMs: 500 });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);

    // ADR-0007: the fake answers `hello` with `prepared`, so the two-phase
    // handshake is already complete before the first await.
    expectHandshake(server);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    await flushTicks();
    // The recovery barrier (SPEC 6.2): queued, not failed, not dropped.
    expect(server.received).toHaveLength(2);

    server.send(readyMessage());
    await startPromise;
    await flushTicks();
    expect(requestIds(server)).toEqual(['req-1']);

    server.send(successMessage('req-1', 'ok'));
    expect(await outcomePromise).toEqual({ ok: true, result: 'ok' });
    await h.executor.close();
  });

  test('sends nothing but hello until the server is prepared', async () => {
    const h = harness({ timeoutMs: 500, autoPrepare: false });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);
    expectOnlyHello(server);

    // Activation is the Server's move to make; the Portal must not open its
    // storage on its own.
    await flushTicks();
    expectOnlyHello(server);

    server.send(preparedMessage());
    await flushTicks();
    expectHandshake(server);

    server.send(readyMessage());
    await startPromise;
    await h.executor.close();
  });

  test('does not activate again when prepared arrives after ready', async () => {
    const h = harness({ timeoutMs: 500 });
    const server = await started(h);
    expectHandshake(server);

    // A Server sends `prepared` once, so a second one is out of protocol. What
    // matters is that it cannot reopen storage: this generation is already
    // ready, and a second `activate` would make the Server start twice.
    server.send(preparedMessage());
    await flushTicks();

    expectHandshake(server);
    await h.executor.close();
  });

  test('does not activate a server that prepared while the executor is closing', async () => {
    const h = harness({ timeoutMs: 500, autoPrepare: false });
    void h.executor.start().catch(() => undefined);
    const server = serverAt(h, 0);
    expectOnlyHello(server);

    const closing = h.executor.close();
    // `close()` sets `closing` before its first await, so this lands inside the
    // window the branch guards: nothing may open storage while the Portal is
    // closing, because there would be nowhere to drain it (INV-002).
    server.send(preparedMessage());
    await flushTicks();
    await closing;

    expect(messagesOf(server).map((message) => message.type)).toEqual([
      'hello',
    ]);
  });

  test('fails immediately when execute is called before start', async () => {
    const h = harness({ timeoutMs: 5000 });

    const outcome = await h.executor.execute(
      request({ requestId: 'early', sessionId: 's-early' }),
    );

    // Not a silent wait for the deadline: nothing was ever spawned, and that
    // must be visible in the outcome and the logs (SPEC 6.2 step 3).
    expect(failureOf(outcome).code).toBe('INTERNAL_ERROR');
    expect(failureOf(outcome).message).toContain('never started');
    expect(h.servers).toHaveLength(0);
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg === 'request rejected' &&
            line.code === 'INTERNAL_ERROR' &&
            line.level === 'error' &&
            line.sessionId === 's-early',
        ),
    ).toBe(true);
  });

  test('rejects start when close wins the race to readiness', async () => {
    const h = harness({ timeoutMs: 5000 });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);
    expectHandshake(server);

    await h.executor.close();

    // "Ready" and "closed" must not look the same to a caller.
    const error = await rejectionOf(startPromise);
    expect(error.message).toMatch(/closed before the server became ready/);
    expect(server.killCount).toBe(1);
  });

  test('rejects a duplicate requestId with a logged code', async () => {
    const h = harness();
    const server = await started(h);

    void h.executor.execute(request({ requestId: 'dup', sessionId: 's-dup' }));
    const duplicate = await h.executor.execute(
      request({ requestId: 'dup', sessionId: 's-dup' }),
    );

    expect(failureOf(duplicate).code).toBe('INTERNAL_ERROR');
    const rejected = h.logs().find((line) => line.msg === 'request rejected');
    expect(rejected).toMatchObject({
      level: 'warn',
      code: 'INTERNAL_ERROR',
      sessionId: 's-dup',
    });

    server.send(successMessage('dup', null));
    await h.executor.close();
  });

  test('ignores malformed messages and keeps running', async () => {
    const h = harness();
    const server = await started(h);

    server.sendRaw({ type: 'not-a-message' });
    server.sendRaw({ type: 'result' });
    await flushTicks();

    const warnings = h
      .logs()
      .filter(
        (line) => line.msg === 'ipc message rejected' && line.level === 'warn',
      );
    expect(warnings).toHaveLength(2);
    expect(warnings[0]?.reason).toEqual(expect.any(String));

    // Still functional after the bad input.
    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    server.send(successMessage('req-1', 'alive'));
    expect(await outcomePromise).toEqual({ ok: true, result: 'alive' });
    await h.executor.close();
  });

  test('refuses a request once the pending buffer is full', async () => {
    const h = harness({ timeoutMs: 5000, maxPending: 2 });
    // Started but never ready, so nothing is delivered and the buffer fills.
    void h.executor.start().catch(() => undefined);
    await flushTicks();

    void h.executor.execute(request({ requestId: 'req-1' }));
    void h.executor.execute(request({ requestId: 'req-2' }));
    const refused = await h.executor.execute(
      request({ requestId: 'req-3', sessionId: 's-3' }),
    );

    expect(failureOf(refused).code).toBe('FRAMEWORK_QUEUE_FULL');
    const logged = h
      .logs()
      .find((line) => line.code === 'FRAMEWORK_QUEUE_FULL');
    expect(logged).toMatchObject({
      level: 'warn',
      pending: 2,
      limit: 2,
      sessionId: 's-3',
    });
    await h.executor.close();
  });

  test('reports a full buffer ahead of a duplicate requestId', async () => {
    const h = harness({ timeoutMs: 5000, maxPending: 2 });
    // Started but never ready, so nothing is delivered and the buffer fills.
    void h.executor.start().catch(() => undefined);
    await flushTicks();

    void h.executor.execute(request({ requestId: 'req-1' }));
    void h.executor.execute(request({ requestId: 'req-2' }));
    // Both conditions hold at once: the buffer is full and `req-1` is already
    // in flight. The cap is what refused this request, so the code must be the
    // cap's, not the duplicate's (the pre-extraction order).
    const refused = await h.executor.execute(request({ requestId: 'req-1' }));

    expect(failureOf(refused).code).toBe('FRAMEWORK_QUEUE_FULL');
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg === 'request refused: pending buffer is full' &&
            line.code === 'FRAMEWORK_QUEUE_FULL' &&
            line.pending === 2 &&
            line.limit === 2,
        ),
    ).toBe(true);
    await h.executor.close();
  });
});
