import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { Dispatcher, FrameworkTimeoutError } from './dispatcher.ts';

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

const sleep = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

async function rejectionOf(promise: Promise<unknown>): Promise<Error> {
  try {
    await promise;
  } catch (error) {
    if (error instanceof Error) {
      return error;
    }
    throw new Error(`non-Error rejection: ${String(error)}`, { cause: error });
  }
  throw new Error('expected the promise to reject');
}

describe('Dispatcher', () => {
  test('runs a handler inside a transaction and returns its result', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('echo', async (ctx, payload) => {
      await ctx.storage.execute('INSERT 1', [payload]);
      return payload;
    });

    const result = await dispatcher.dispatch('echo', 'hello');

    expect(result).toBe('hello');
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
  });

  test('rejects and rolls back when the handler throws', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('fail', async () => {
      throw new Error('business failure');
    });

    const error = await rejectionOf(dispatcher.dispatch('fail', null));

    expect(error.message).toBe('business failure');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects unknown handler names instead of returning undefined', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });

    const error = await rejectionOf(dispatcher.dispatch('missing', null));

    expect(error.message).toMatch(/no handler registered/);
  });

  test('fails the request at the timeout and invalidates later writes', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 10 });
    const handlerDone = deferred<void>();
    let lateWriteError = '';

    dispatcher.register('slow', async (ctx) => {
      await sleep(40);
      try {
        await ctx.storage.execute('INSERT 1');
        lateWriteError = 'write was accepted';
      } catch (error) {
        lateWriteError = error instanceof Error ? error.message : String(error);
      }
      handlerDone.resolve();
      return 'late result';
    });

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
    dispatcher.register('slow', async () => {
      await sleep(50);
      return 'late';
    });

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

  test('close shuts down the backend', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });

    await dispatcher.close();

    expect(() => backend.capabilities()).toThrow(/closed/);
    const error = await rejectionOf(dispatcher.dispatch('anything', null));
    expect(error.message).toMatch(/closed/);
  });
});
