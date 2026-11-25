import { describe, expect, test } from 'bun:test';

import { sleep, tick } from '../testing/common.ts';
import { requestMessage } from '../testing/server-process.ts';
import { startRunner } from './request-runner.support.ts';

describe('RequestRunner', () => {
  test('drain waits for a running handler', async () => {
    const harness = startRunner();
    const gate = Promise.withResolvers<void>();
    harness.register(
      'slow',
      async () => {
        await gate.promise;
        return 'done';
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'slow' }));
    await tick();
    let drained = false;
    const drain = harness.runner.drain(500).then(() => {
      drained = true;
      return undefined;
    });

    await sleep(10);
    expect(drained).toBe(false);
    gate.resolve();
    await drain;
    expect(drained).toBe(true);
    expect(harness.sent).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'done' },
    ]);
  });

  test('drain returns when the budget runs out and names what is left', async () => {
    const harness = startRunner();
    const gate = Promise.withResolvers<void>();
    harness.register(
      'stuck',
      async () => {
        await gate.promise;
        return 'late';
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'stuck' }));
    await tick();
    await harness.runner.drain(10);

    const timeouts = harness
      .logs()
      .filter((line) => line.code === 'SHUTDOWN_DRAIN_TIMEOUT');
    expect(timeouts).toHaveLength(1);
    expect(timeouts[0]).toMatchObject({
      level: 'error',
      timeoutMs: 10,
      pendingRequests: 1,
      requestIds: ['req-1'],
    });
    gate.resolve();
    await tick();
  });

  test('counts pending and running requests over a request lifecycle', async () => {
    const harness = startRunner();
    const gate = Promise.withResolvers<void>();
    harness.register(
      'slow',
      async () => {
        await gate.promise;
        return 'done';
      },
      { access: 'public' },
    );

    expect(harness.runner.pending).toBe(0);
    expect(harness.runner.running).toBe(0);

    harness.runner.accept(requestMessage({ method: 'slow' }));
    // Counted synchronously: a `shutdown` in the same batch must see the
    // request even before its handler started.
    expect(harness.runner.pending).toBe(1);
    expect(harness.runner.running).toBe(1);
    expect(harness.runner.pendingRequestIds).toEqual(['req-1']);

    await tick();
    expect(harness.runner.pending).toBe(1);
    expect(harness.runner.running).toBe(1);

    gate.resolve();
    await tick();
    expect(harness.runner.pending).toBe(0);
    expect(harness.runner.running).toBe(0);
    expect(harness.runner.pendingRequestIds).toEqual([]);
  });
});
