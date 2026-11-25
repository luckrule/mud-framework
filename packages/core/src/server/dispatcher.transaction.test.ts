import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf } from '../testing/common.ts';
import {
  SlowCommitBackend,
  deferred,
  parseLines,
  quietDispatcher,
} from '../testing/dispatcher.ts';
import { Dispatcher } from './dispatcher.ts';
import type { RequestContext } from './dispatcher.ts';

describe('Dispatcher.transaction', () => {
  test('runs a handler inside a transaction and returns its result', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'echo',
      async (ctx, payload) => {
        await ctx.storage.execute('INSERT 1', [payload]);
        return payload;
      },
      { access: 'public' },
    );

    const { result } = await dispatcher.dispatch('echo', 'hello');

    expect(result).toBe('hello');
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
  });

  test('rejects and rolls back when the handler throws', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'fail',
      async () => {
        throw new Error('business failure');
      },
      { access: 'public' },
    );

    const error = await rejectionOf(dispatcher.dispatch('fail', null));

    expect(error.message).toBe('business failure');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test.each([
    { first: 'success', second: 'success', committed: 2, rolledBack: 0 },
    { first: 'success', second: 'failure', committed: 1, rolledBack: 1 },
    { first: 'failure', second: 'success', committed: 1, rolledBack: 1 },
    { first: 'failure', second: 'failure', committed: 0, rolledBack: 2 },
  ])(
    'storage from a $first request cannot access a later $second request',
    async ({ first, second, committed, rolledBack }) => {
      const forwarded: string[] = [];
      const backend = new MemoryStorageBackend({
        capabilities: { concurrency: 'single' },
        handler: (sql) => {
          forwarded.push(sql);
          return [];
        },
      });
      const { dispatcher, lines } = quietDispatcher({
        backend,
        timeoutMs: 1000,
      });
      const saved = deferred<RequestContext>();
      const attempts = deferred<PromiseSettledResult<unknown>[]>();
      dispatcher.register(
        'first',
        async (ctx) => {
          saved.resolve(ctx);
          await ctx.storage.execute('first write');
          if (first === 'failure') {
            throw new Error('first failed');
          }
          return 'first result';
        },
        { access: 'public', mayChangeSession: true },
      );
      const firstDispatch = dispatcher.dispatch('first', null);
      if (first === 'failure') {
        expect((await rejectionOf(firstDispatch)).message).toBe('first failed');
      } else {
        expect(await firstDispatch).toEqual({ result: 'first result' });
      }
      const stale = await saved.promise;
      dispatcher.register(
        'second',
        async (ctx) => {
          await ctx.storage.execute('second write');
          expect(() => stale.bindSession('stale-session')).toThrow(/expired/);
          attempts.resolve(
            await Promise.allSettled([
              stale.storage.execute('stale write'),
              stale.storage.query('stale query'),
            ]),
          );
          if (second === 'failure') {
            throw new Error('second failed');
          }
          return 'second result';
        },
        { access: 'public' },
      );

      const secondDispatch = dispatcher.dispatch('second', null);
      if (second === 'failure') {
        expect((await rejectionOf(secondDispatch)).message).toBe(
          'second failed',
        );
      } else {
        expect(await secondDispatch).toEqual({ result: 'second result' });
      }

      expect((await attempts.promise).map((attempt) => attempt.status)).toEqual(
        ['rejected', 'rejected'],
      );
      expect(forwarded).toEqual(['first write', 'second write']);
      expect(backend.stats()).toEqual({ committed, rolledBack });
      expect(stale.signal.aborted).toBe(false);
      expect(
        parseLines(lines).filter(
          (entry) => entry.code === 'CANCEL_LOST_TO_COMMIT',
        ),
      ).toEqual([]);
      await dispatcher.close();
    },
  );

  test('closes storage and session binding before the backend commits', async () => {
    const backend = new SlowCommitBackend();
    const { dispatcher, lines } = quietDispatcher({ backend, timeoutMs: 1000 });
    const saved = deferred<RequestContext>();
    const attempts = deferred<PromiseSettledResult<unknown>[]>();
    let bindingError: unknown;
    dispatcher.register(
      'quick',
      async (ctx) => {
        saved.resolve(ctx);
        setTimeout(() => {
          void Promise.allSettled([
            ctx.storage.execute('late write'),
            ctx.storage.query('late query'),
          ]).then(attempts.resolve);
          try {
            ctx.bindSession('late-session');
          } catch (error) {
            bindingError = error;
          }
        }, 0);
        return 'done';
      },
      { access: 'public', mayChangeSession: true },
    );

    const result = await dispatcher.dispatch('quick', null);

    expect((await attempts.promise).map((attempt) => attempt.status)).toEqual([
      'rejected',
      'rejected',
    ]);
    expect(bindingError).toBeInstanceOf(Error);
    expect(result).toEqual({ result: 'done' });
    expect((await saved.promise).signal.aborted).toBe(false);
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
    expect(
      parseLines(lines).filter(
        (entry) => entry.code === 'CANCEL_LOST_TO_COMMIT',
      ),
    ).toEqual([]);
    await dispatcher.close();
  });

  test('rejects unknown handler names instead of returning undefined', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher, lines } = quietDispatcher({ backend, timeoutMs: 1000 });

    const error = await rejectionOf(dispatcher.dispatch('missing', null));

    expect(error.message).toMatch(/no handler registered/);
    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0] ?? '')).toMatchObject({
      level: 'warn',
      method: 'missing',
    });
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
