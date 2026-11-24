import { describe, expect, test } from 'bun:test';

import { rejectionOf } from '../test-support/common.ts';
import {
  expectOnlyHello,
  failureOf,
  flushTicks,
  harness,
  readyMessage,
  request,
  requestIds,
  serverAt,
  started,
  successMessage,
} from '../test-support/ipc-executor.ts';

describe('IpcExecutor handshake', () => {
  test('sends hello and holds requests until the server is ready', async () => {
    const h = harness({ timeoutMs: 500 });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);

    expectOnlyHello(server);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    await flushTicks();
    // The recovery barrier (SPEC 6.2): queued, not failed, not dropped.
    expect(server.received).toHaveLength(1);

    server.send(readyMessage());
    await startPromise;
    await flushTicks();
    expect(requestIds(server)).toEqual(['req-1']);

    server.send(successMessage('req-1', 'ok'));
    expect(await outcomePromise).toEqual({ ok: true, result: 'ok' });
    await h.executor.close();
  });

  test('fails immediately when execute is called before start', async () => {
    const h = harness({ timeoutMs: 5000 });

    const outcome = await h.executor.execute(request({ requestId: 'early' }));

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
            line.level === 'error',
        ),
    ).toBe(true);
  });

  test('rejects start when close wins the race to readiness', async () => {
    const h = harness({ timeoutMs: 5000 });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);
    expectOnlyHello(server);

    await h.executor.close();

    // "Ready" and "closed" must not look the same to a caller.
    const error = await rejectionOf(startPromise);
    expect(error.message).toMatch(/closed before the server became ready/);
    expect(server.killCount).toBe(1);
  });

  test('rejects a duplicate requestId with a logged code', async () => {
    const h = harness();
    const server = await started(h);

    void h.executor.execute(request({ requestId: 'dup' }));
    const duplicate = await h.executor.execute(request({ requestId: 'dup' }));

    expect(failureOf(duplicate).code).toBe('INTERNAL_ERROR');
    const rejected = h.logs().find((line) => line.msg === 'request rejected');
    expect(rejected).toMatchObject({ level: 'warn', code: 'INTERNAL_ERROR' });

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
    const refused = await h.executor.execute(request({ requestId: 'req-3' }));

    expect(failureOf(refused).code).toBe('FRAMEWORK_QUEUE_FULL');
    const logged = h
      .logs()
      .find((line) => line.code === 'FRAMEWORK_QUEUE_FULL');
    expect(logged).toMatchObject({
      level: 'warn',
      pending: 2,
      limit: 2,
    });
    await h.executor.close();
  });
});
