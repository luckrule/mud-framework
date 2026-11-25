import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf } from '../testing/common.ts';
import {
  CountingBackend,
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

  test('rejects later writes after the first write fails on a transactions:none backend', async () => {
    const attempted: string[] = [];
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'none' },
      handler: (sql) => {
        attempted.push(sql);
        if (sql === 'first write') {
          throw new Error('disk full');
        }
        return [];
      },
    });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'write-twice',
      async (ctx) => {
        expect(
          (await rejectionOf(ctx.storage.execute('first write'))).message,
        ).toBe('disk full');
        const blocked = await rejectionOf(ctx.storage.execute('second write'));
        expect(blocked.message).toMatch(/earlier write in this request failed/);
        expect(blocked.message).toMatch(/transactions "none"/);
        return 'recovered';
      },
      { access: 'public' },
    );

    expect(await dispatcher.dispatch('write-twice', null)).toEqual({
      result: 'recovered',
    });
    // The second write never reached the backend.
    expect(attempted).toEqual(['first write']);
  });

  test('fails the request when the first write failure is not caught', async () => {
    const attempted: string[] = [];
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'none' },
      handler: (sql) => {
        attempted.push(sql);
        if (sql === 'first write') {
          throw new Error('disk full');
        }
        return [];
      },
    });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'write-twice',
      async (ctx) => {
        await ctx.storage.execute('first write');
        await ctx.storage.execute('second write');
        return 'unreachable';
      },
      { access: 'public' },
    );

    const error = await rejectionOf(dispatcher.dispatch('write-twice', null));

    expect(error.message).toBe('disk full');
    expect(attempted).toEqual(['first write']);
  });

  test('still allows reads after a write failed on a transactions:none backend', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'none' },
      handler: (sql) => {
        if (sql === 'first write') {
          throw new Error('disk full');
        }
        return [{ ok: true }];
      },
    });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'write-then-read',
      async (ctx) => {
        await rejectionOf(ctx.storage.execute('first write'));
        return ctx.storage.query<{ ok: boolean }>('read something');
      },
      { access: 'public' },
    );

    expect(await dispatcher.dispatch('write-then-read', null)).toEqual({
      result: [{ ok: true }],
    });
  });

  test('does not block later writes on a transactions:full backend', async () => {
    const attempted: string[] = [];
    const backend = new MemoryStorageBackend({
      handler: (sql) => {
        attempted.push(sql);
        if (sql === 'first write') {
          throw new Error('disk full');
        }
        return [];
      },
    });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'write-twice',
      async (ctx) => {
        await rejectionOf(ctx.storage.execute('first write'));
        await ctx.storage.execute('second write');
        return 'recovered';
      },
      { access: 'public' },
    );

    expect(await dispatcher.dispatch('write-twice', null)).toEqual({
      result: 'recovered',
    });
    expect(attempted).toEqual(['first write', 'second write']);
  });

  test('keeps a failed write scoped to its own request', async () => {
    const attempted: string[] = [];
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'none' },
      handler: (sql) => {
        attempted.push(sql);
        if (sql === 'first write') {
          throw new Error('disk full');
        }
        return [];
      },
    });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'write',
      async (ctx, payload) => {
        await ctx.storage.execute(String(payload));
        return payload;
      },
      { access: 'public' },
    );

    expect(
      (await rejectionOf(dispatcher.dispatch('write', 'first write'))).message,
    ).toBe('disk full');
    expect(await dispatcher.dispatch('write', 'second write')).toEqual({
      result: 'second write',
    });
    expect(attempted).toEqual(['first write', 'second write']);
  });

  test('reports expiry before the failed-write marker once the request ends', async () => {
    const attempted: string[] = [];
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'none' },
      handler: (sql) => {
        attempted.push(sql);
        if (sql === 'first write') {
          throw new Error('disk full');
        }
        return [];
      },
    });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    const saved = deferred<RequestContext>();
    dispatcher.register(
      'write-then-return',
      async (ctx) => {
        saved.resolve(ctx);
        await rejectionOf(ctx.storage.execute('first write'));
        return 'done';
      },
      { access: 'public' },
    );

    expect(await dispatcher.dispatch('write-then-return', null)).toEqual({
      result: 'done',
    });
    const late = await rejectionOf(
      (await saved.promise).storage.execute('late write'),
    );

    expect(late.message).toMatch(/transaction expired/);
    expect(attempted).toEqual(['first write']);
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

  test('a failed backend close stays observable on every later close', async () => {
    const backend = new CountingBackend();
    const failure = new Error('db close failed');
    let closes = 0;
    backend.close = () => {
      closes += 1;
      return Promise.reject(failure);
    };
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });

    const first = dispatcher.close();
    const second = dispatcher.close();

    // A second close joins the first attempt: returning a fresh resolved
    // promise would report a success that never happened.
    expect(second).toBe(first);
    expect(await rejectionOf(first)).toBe(failure);
    expect(await rejectionOf(second)).toBe(failure);
    expect(closes).toBe(1);
    expect(dispatcher.isClosed()).toBe(true);
  });
});
