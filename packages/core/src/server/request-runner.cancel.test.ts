import { describe, expect, test } from 'bun:test';

import { tick } from '../testing/common.ts';
import { requestMessage } from '../testing/server-process.ts';
import { startRunner } from './request-runner.support.ts';

describe('RequestRunner', () => {
  test('cancel of a running request invalidates its transaction', async () => {
    const harness = startRunner();
    const entered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    let lateWriteError = '';
    harness.register(
      'slow',
      async (ctx) => {
        await ctx.storage.execute('INSERT 1');
        entered.resolve();
        await gate.promise;
        try {
          await ctx.storage.execute('INSERT 2');
          lateWriteError = 'write was accepted';
        } catch (error) {
          lateWriteError =
            error instanceof Error ? error.message : String(error);
        }
        return 'done';
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'slow' }));
    await entered.promise;
    harness.runner.cancel({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });
    gate.resolve();
    await tick();

    // The write after the cancel is rejected, which only happens when
    // `dispatcher.cancel` invalidated the transaction; the handler still
    // returns, and its outcome is neither answered nor committed.
    expect(lateWriteError).toMatch(/expired/);
    expect(harness.sent).toEqual([]);
    const applied = harness
      .logs()
      .filter((line) => line.msg === 'cancel applied to a running handler');
    expect(applied).toHaveLength(1);
    expect(applied[0]).toMatchObject({
      level: 'warn',
      code: 'FRAMEWORK_TIMEOUT',
      requestId: 'req-1',
    });
  });

  test('cancel applied to a running handler names the Extension', async () => {
    const harness = startRunner();
    const entered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    harness.register(
      'slow',
      async () => {
        entered.resolve();
        await gate.promise;
        return 'done';
      },
      { extension: 'banking', access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'slow' }));
    await entered.promise;
    harness.runner.cancel({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });
    gate.resolve();
    await tick();

    const applied = harness
      .logs()
      .filter((line) => line.msg === 'cancel applied to a running handler');
    expect(applied).toHaveLength(1);
    expect(applied[0]).toMatchObject({
      requestId: 'req-1',
      extension: 'banking',
    });
  });

  test('a cancel before start names the Extension in both of its lines', async () => {
    const harness = startRunner();
    harness.register('slow', () => Promise.resolve('done'), {
      extension: 'banking',
      access: 'public',
    });

    harness.runner.accept(requestMessage({ method: 'slow' }));
    // No await in between: the cancel lands inside the pre-start grace period.
    harness.runner.cancel({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });
    await tick();

    const logs = harness.logs();
    const beforeStart = logs.filter(
      (line) => line.msg === 'request cancelled before start',
    );
    const skipped = logs.filter(
      (line) => line.msg === 'cancelled request skipped',
    );
    expect(beforeStart).toHaveLength(1);
    expect(beforeStart[0]).toMatchObject({
      requestId: 'req-1',
      extension: 'banking',
    });
    expect(skipped).toHaveLength(1);
    expect(skipped[0]).toMatchObject({
      requestId: 'req-1',
      extension: 'banking',
    });
  });

  test('cancel of an unknown request id is a no-op', () => {
    const harness = startRunner();

    harness.runner.cancel({
      type: 'cancel',
      requestId: 'req-missing',
      code: 'FRAMEWORK_TIMEOUT',
    });

    expect(harness.sent).toEqual([]);
    const ignored = harness
      .logs()
      .filter((line) => line.msg === 'cancel ignored');
    expect(ignored).toHaveLength(1);
    expect(ignored[0]).toMatchObject({
      reason: 'no request in flight',
      requestId: 'req-missing',
    });
    // No id is remembered: the cancel neither leaked a set entry nor claimed
    // the id for a later delivery.
    expect(harness.runner.pending).toBe(0);
  });
});
