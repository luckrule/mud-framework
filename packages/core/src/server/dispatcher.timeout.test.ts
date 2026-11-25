import { describe, expect, test } from 'bun:test';

import { FrameworkTimeoutError } from '../shared/wire-codes.ts';
import { rejectionOf, sleep } from '../testing/common.ts';
import {
  deferred,
  parseLines,
  quietDispatcher,
} from '../testing/dispatcher.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { Dispatcher } from './dispatcher.ts';

describe('Dispatcher.timeout', () => {
  test('fails the request at the timeout and invalidates later writes', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    const handlerDone = deferred<void>();
    let lateWriteError = '';

    dispatcher.register(
      'slow',
      async (ctx) => {
        await sleep(40);
        try {
          await ctx.storage.execute('INSERT 1');
          lateWriteError = 'write was accepted';
        } catch (error) {
          lateWriteError =
            error instanceof Error ? error.message : String(error);
        }
        handlerDone.resolve();
        return 'late result';
      },
      { access: 'public' },
    );

    const error = await rejectionOf(dispatcher.dispatch('slow', null));
    await handlerDone.promise;
    // The backend rolls back only after the abandoned handler returns.
    await sleep(5);

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    expect(lateWriteError).toMatch(/expired/);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('uses the smaller of the configured timeout and the backend ceiling', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: 5 },
    });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 10_000 });
    dispatcher.register(
      'slow',
      async () => {
        await sleep(50);
        return 'late';
      },
      { access: 'public' },
    );

    const error = await rejectionOf(dispatcher.dispatch('slow', null));

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.timeoutMs).toBe(5);
      expect(error.code).toBe('FRAMEWORK_TIMEOUT');
    }
  });

  test('rejects a non-finite or non-positive timeout', () => {
    const backend = new MemoryStorageBackend();

    expect(
      () => new Dispatcher({ backend, timeoutMs: Number.POSITIVE_INFINITY }),
    ).toThrow(/positive finite/);
    expect(() => new Dispatcher({ backend, timeoutMs: 0 })).toThrow(
      /positive finite/,
    );
  });

  test('logs a timeout at warn with the effective timeout', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: 5 },
    });
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher(
      { backend, timeoutMs: 10_000 },
      lines,
    );
    dispatcher.register(
      'slow',
      async () => {
        await sleep(40);
        return 'late';
      },
      { access: 'public' },
    );

    await rejectionOf(dispatcher.dispatch('slow', null, { requestId: 'r1' }));

    const timedOut = parseLines(lines).filter(
      (entry) => entry.msg === 'request timed out',
    );
    expect(timedOut).toHaveLength(1);
    expect(timedOut[0]).toMatchObject({
      level: 'warn',
      requestId: 'r1',
      method: 'slow',
      timeoutMs: 5,
    });
  });

  test('FrameworkTimeoutError carries the id that was actually used', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    dispatcher.register(
      'slow',
      async () => {
        await sleep(40);
        return 'late';
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('slow', null, { requestId: 'given-id' }),
    );

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.requestId).toBe('given-id');
    }
  });

  test('meta.timeoutMs tightens the effective timeout', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10_000 });
    dispatcher.register(
      'slow',
      async () => {
        await sleep(60);
        return 'late';
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('slow', null, { timeoutMs: 10 }),
    );

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.timeoutMs).toBe(10);
    }
    await sleep(60);
  });

  test('meta.timeoutMs can never relax the configured ceiling', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: 10_000 },
    });
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 15 });
    dispatcher.register(
      'slow',
      async () => {
        await sleep(60);
        return 'late';
      },
      { access: 'public' },
    );

    // The override is far above the configured ceiling; the smaller value wins.
    const error = await rejectionOf(
      dispatcher.dispatch('slow', null, { timeoutMs: 10_000 }),
    );

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.timeoutMs).toBe(15);
    }
    await sleep(60);
  });

  test('meta.timeoutMs is also bounded by the backend ceiling', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: 5 },
    });
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10_000 });
    dispatcher.register(
      'slow',
      async () => {
        await sleep(40);
        return 'late';
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('slow', null, { timeoutMs: 500 }),
    );

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.timeoutMs).toBe(5);
    }
    await sleep(40);
  });
});
