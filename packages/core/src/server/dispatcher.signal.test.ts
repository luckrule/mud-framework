import { describe, expect, test } from 'bun:test';

import { FrameworkTimeoutError } from '../shared/wire-codes.ts';
import { rejectionOf, sleep } from '../testing/common.ts';
import { deferred, quietDispatcher } from '../testing/dispatcher.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';

describe('Dispatcher.abortSignal', () => {
  test('hands the handler an unaborted AbortSignal', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let seen: unknown;
    let aborted: boolean | undefined;
    dispatcher.register(
      'inspect',
      async (ctx) => {
        seen = ctx.signal;
        aborted = ctx.signal.aborted;
        return null;
      },
      { access: 'public' },
    );

    await dispatcher.dispatch('inspect', null);

    expect(seen).toBeInstanceOf(AbortSignal);
    expect(aborted).toBe(false);
  });

  test('aborts with a timeout reason when the request times out', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    let aborted = false;
    let reason: unknown;
    const handlerDone = deferred<void>();

    dispatcher.register(
      'slow',
      async (ctx) => {
        await sleep(40);
        aborted = ctx.signal.aborted;
        reason = ctx.signal.reason;
        handlerDone.resolve();
        return 'late';
      },
      { access: 'public' },
    );

    const error = await rejectionOf(dispatcher.dispatch('slow', null));
    await handlerDone.promise;

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    expect(aborted).toBe(true);
    expect(reason).toBe('timeout');
  });

  test('aborts with the caller code as the reason on cancel', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = deferred<void>();
    const release = deferred<void>();
    let aborted = false;
    let reason: unknown;

    dispatcher.register(
      'slow',
      async (ctx) => {
        entered.resolve();
        await release.promise;
        aborted = ctx.signal.aborted;
        reason = ctx.signal.reason;
        return 'late';
      },
      { access: 'public' },
    );

    const pending = dispatcher.dispatch('slow', null, { requestId: 'r1' });
    await entered.promise;
    expect(dispatcher.cancel('r1', 'TRANSPORT_CLOSED')).toBe(true);
    await rejectionOf(pending);
    release.resolve();
    await sleep(5);

    expect(aborted).toBe(true);
    expect(reason).toBe('TRANSPORT_CLOSED');
  });

  test('wakes a handler that awaits the abort instead of its own sleep', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    const woke = deferred<string>();
    const entered = deferred<void>();

    dispatcher.register(
      'wait-for-abort',
      async (ctx) => {
        entered.resolve();
        // The point of the signal: a cooperative handler stops as soon as the
        // request goes terminal. Without the abort this promise never settles,
        // because nothing else in the handler would resolve it.
        const reason = await new Promise<unknown>((resolve) => {
          ctx.signal.addEventListener('abort', () => {
            resolve(ctx.signal.reason);
          });
        });
        woke.resolve(String(reason));
        return 'woke';
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('wait-for-abort', null),
    );
    await entered.promise;

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    expect(await woke.promise).toBe('timeout');
  });

  test('leaves the signal unaborted when the request completes', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let signal: AbortSignal | undefined;
    let abortedInside: boolean | undefined;

    dispatcher.register(
      'inspect',
      async (ctx) => {
        signal = ctx.signal;
        abortedInside = ctx.signal.aborted;
        return null;
      },
      { access: 'public' },
    );

    await dispatcher.dispatch('inspect', null);

    expect(abortedInside).toBe(false);
    expect(signal?.aborted).toBe(false);
  });
});
