import { describe, expect, test } from 'bun:test';

import { rejectionOf, throwingLogger } from '../../testing/common.ts';
import {
  flushTicks,
  harness,
  readyMessage,
  request,
  serverAt,
  successMessage,
} from '../../testing/ipc-executor.ts';

describe('IpcExecutor logger safety', () => {
  test('starts, executes and cancels with a throwing logger', async () => {
    const h = harness({ timeoutMs: 5000, logger: throwingLogger() });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);
    server.send(readyMessage());
    // `onReady` logs before it resolves the start waiter: a throwing logger
    // here used to leave `start()` pending and `execute()` buffered forever.
    await startPromise;

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    await flushTicks();
    server.send(successMessage('req-1', 'ok'));
    expect(await outcomePromise).toEqual({ ok: true, result: 'ok' });

    // The cancel line sits before the Server notification and the local
    // settlement; a throw there would strand the request.
    const pendingPromise = h.executor.execute(request({ requestId: 'req-2' }));
    await flushTicks();
    await h.executor.cancel('req-2', 'FRAMEWORK_TIMEOUT');
    const cancelled = await pendingPromise;
    if (cancelled.ok) {
      throw new Error('expected the cancelled request to fail');
    }
    expect(cancelled.error.code).toBe('FRAMEWORK_TIMEOUT');

    await h.executor.close();
  }, 2000);

  test('fails a start attempt within its bound when the logger throws', async () => {
    const h = harness({
      timeoutMs: 5000,
      startTimeoutMs: 10,
      logger: throwingLogger(),
    });
    const started = h.executor.start();

    const error = await rejectionOf(started);

    expect(error.message).toMatch(/failed to start/);
    // The Server that never handshook must still be abandoned, not left as an
    // orphan writer (INV-002).
    expect(serverAt(h, 0).killCount).toBeGreaterThanOrEqual(1);
    await h.executor.close();
  }, 2000);
});
