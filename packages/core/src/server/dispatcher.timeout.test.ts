import { describe, expect, spyOn, test } from 'bun:test';

import {
  FrameworkTimeoutError,
  RequestCancelledError,
} from '../shared/wire-codes.ts';
import { rejectionOf, sleep } from '../testing/common.ts';
import {
  BusyCommitBackend,
  HookedCommitBackend,
  deferred,
  parseLines,
  quietDispatcher,
} from '../testing/dispatcher.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { Dispatcher } from './dispatcher.ts';
import type { DispatchResult } from './dispatcher.ts';

const DEADLINE_START_MS = 1_000;
const DEADLINE_TIMEOUT_MS = 1_000;

/**
 * Dispatches a committing handler whose transaction settles `offsetMs` after
 * the dispatcher recorded `startedAt`.
 *
 * The dispatcher reads `performance.now()` for `startedAt`, `deadlineAt` and
 * `committedAt`, so a stubbed clock pins all three and makes the boundary
 * reachable exactly. Real time never advances, so the deadline timer cannot
 * fire: only the post-race `committedAt >= deadlineAt` check can time the
 * request out.
 */
async function dispatchAt(committedAtOffsetMs: number): Promise<{
  result: DispatchResult | undefined;
  error: Error | undefined;
  stats: { committed: number; rolledBack: number };
  lostCommits: number;
}> {
  const clock = { now: DEADLINE_START_MS };
  const backend = new HookedCommitBackend(() => {
    clock.now = DEADLINE_START_MS + committedAtOffsetMs;
  });
  const lines: string[] = [];
  const spy = spyOn(performance, 'now').mockImplementation(() => clock.now);
  try {
    const { dispatcher } = quietDispatcher(
      { backend, timeoutMs: DEADLINE_TIMEOUT_MS },
      lines,
    );
    dispatcher.register(
      'commit-at',
      async (ctx) => {
        ctx.bindSession('session-1');
        ctx.emit('committed', { ok: true });
        return 'done';
      },
      { access: 'public', mayChangeSession: true },
    );
    let result: DispatchResult | undefined;
    let error: Error | undefined;
    try {
      result = await dispatcher.dispatch('commit-at', null);
    } catch (caught) {
      error = caught instanceof Error ? caught : new Error(String(caught));
    }
    return {
      result,
      error,
      stats: backend.stats(),
      lostCommits: parseLines(lines).filter(
        (entry) => entry.code === 'CANCEL_LOST_TO_COMMIT',
      ).length,
    };
  } finally {
    spy.mockRestore();
  }
}

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

  test('does not start a queued handler after the request already timed out', async () => {
    const writes: string[] = [];
    const backend = new MemoryStorageBackend({
      capabilities: { concurrency: 'single' },
      handler: (sql) => {
        writes.push(sql);
        return [];
      },
    });
    const { dispatcher, lines } = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = deferred<void>();
    const release = deferred<void>();
    const calls: string[] = [];

    dispatcher.register(
      'first',
      async (ctx) => {
        calls.push('first');
        await ctx.storage.execute('first write');
        entered.resolve();
        await release.promise;
        return 'first result';
      },
      { access: 'public' },
    );
    dispatcher.register(
      'second',
      async (ctx) => {
        calls.push('second');
        await ctx.storage.execute('second write');
        return 'second result';
      },
      { access: 'public' },
    );

    const first = dispatcher.dispatch('first', null);
    await entered.promise;
    // Accepted while the backend slot is held; its deadline runs regardless.
    const second = rejectionOf(
      dispatcher.dispatch('second', null, { timeoutMs: 10 }),
    );
    await sleep(25);
    release.resolve();
    expect(await first).toEqual({ result: 'first result' });
    const error = await second;

    // The handler must not start after its terminal state; the queued
    // transaction still rolls back instead of committing.
    expect(calls).toEqual(['first']);
    expect(writes).toEqual(['first write']);
    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 1 });
    expect(
      parseLines(lines).filter((entry) => entry.msg === 'request timed out'),
    ).toHaveLength(1);

    // The slot is free again and the dispatcher keeps working.
    dispatcher.register('third', async () => 'third result', {
      access: 'public',
    });
    expect(await dispatcher.dispatch('third', null)).toEqual({
      result: 'third result',
    });
    expect(backend.stats()).toEqual({ committed: 2, rolledBack: 1 });
  });

  test('does not start a queued handler after the request was cancelled', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { concurrency: 'single' },
    });
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = deferred<void>();
    const release = deferred<void>();
    let secondStarted = false;

    dispatcher.register(
      'first',
      async () => {
        entered.resolve();
        await release.promise;
        return 'first result';
      },
      { access: 'public' },
    );
    dispatcher.register(
      'second',
      async () => {
        secondStarted = true;
        return 'second result';
      },
      { access: 'public' },
    );

    const first = dispatcher.dispatch('first', null);
    await entered.promise;
    const second = rejectionOf(
      dispatcher.dispatch('second', null, { requestId: 'queued' }),
    );
    // A disconnect for the queued request: its transaction callback has not
    // run yet, so it must not start the handler when the slot frees up.
    expect(dispatcher.cancel('queued', 'TRANSPORT_CLOSED')).toBe(true);
    release.resolve();
    await first;
    const error = await second;
    await sleep(0);

    expect(error).toBeInstanceOf(RequestCancelledError);
    expect(secondStarted).toBe(false);
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 1 });
  });

  test('rolls back when synchronous handler work outlives the timeout', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher, lines } = quietDispatcher({ backend, timeoutMs: 5 });
    const busyUntil = performance.now() + 30;
    let returned = false;
    dispatcher.register(
      'block',
      async () => {
        // Synchronous work: the timer cannot fire until this returns.
        while (performance.now() < busyUntil) {
          // busy wait
        }
        returned = true;
        return 'late';
      },
      { access: 'public' },
    );

    const error = await rejectionOf(dispatcher.dispatch('block', null));

    // The handler did run, but the deadline check before returning from the
    // transaction callback must roll its outcome back, not commit it.
    expect(returned).toBe(true);
    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.timeoutMs).toBe(5);
    }
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
    const timedOut = parseLines(lines).filter(
      (entry) => entry.msg === 'request timed out',
    );
    expect(timedOut).toHaveLength(1);
    expect(timedOut[0]).toMatchObject({
      level: 'warn',
      method: 'block',
      timeoutMs: 5,
    });
  });

  test('times out when a synchronous COMMIT outlives the deadline', async () => {
    const backend = new BusyCommitBackend(35);
    const { dispatcher, lines } = quietDispatcher({ backend, timeoutMs: 5 });
    dispatcher.register(
      'commit-block',
      async (ctx) => {
        ctx.bindSession('session-1');
        ctx.emit('committed', { ok: true });
        return 'done';
      },
      { access: 'public', mayChangeSession: true },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('commit-block', null, { requestId: 'r-busy-commit' }),
    );

    // The COMMIT cannot be aborted, but the request must still end as a
    // timeout: the deadline passed before the transaction settled.
    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.requestId).toBe('r-busy-commit');
      expect(error.code).toBe('FRAMEWORK_TIMEOUT');
      expect(error.timeoutMs).toBe(5);
    }
    // The commit is durable, so the log must say so instead of implying a
    // rollback.
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
    const entries = parseLines(lines);
    expect(
      entries.filter((entry) => entry.msg === 'request timed out'),
    ).toHaveLength(1);
    const lost = entries.filter(
      (entry) => entry.code === 'CANCEL_LOST_TO_COMMIT',
    );
    expect(lost).toHaveLength(1);
    expect(lost[0]).toMatchObject({
      level: 'warn',
      requestId: 'r-busy-commit',
      msg: 'transaction committed after the request reached a terminal state',
    });
    // The buffered session change and event belong to the success path; the
    // timeout outcome must not hand either back.
    expect(
      entries.filter((entry) => entry.msg === 'session change recorded'),
    ).toHaveLength(0);
    // The request is already terminal, so a later cancel has nothing to accept.
    expect(dispatcher.cancel('r-busy-commit', 'TRANSPORT_CLOSED')).toBe(false);
  });

  test('a commit that finishes before the deadline still succeeds', async () => {
    const backend = new BusyCommitBackend(10);
    const { dispatcher, lines } = quietDispatcher({ backend, timeoutMs: 250 });
    dispatcher.register(
      'commit-ok',
      async (ctx) => {
        ctx.bindSession('session-1');
        ctx.emit('committed', { ok: true });
        return 'done';
      },
      { access: 'public', mayChangeSession: true },
    );

    expect(
      await dispatcher.dispatch('commit-ok', null, { connectionId: 'conn-1' }),
    ).toEqual({
      result: 'done',
      sessionChange: { type: 'bind', sessionId: 'session-1' },
      events: [
        {
          target: { kind: 'connection', connectionId: 'conn-1' },
          event: 'committed',
          payload: { ok: true },
        },
      ],
    });
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
    expect(
      parseLines(lines).filter(
        (entry) => entry.code === 'CANCEL_LOST_TO_COMMIT',
      ),
    ).toHaveLength(0);
  });

  describe('commit settlement against the deadline', () => {
    test('settling exactly at the deadline is a timeout', async () => {
      const { result, error, stats, lostCommits } = await dispatchAt(1_000);

      // `>=`, not `>`: a settlement the dispatcher only sees at the deadline is
      // treated as late. The commit is durable, so it must be logged as lost,
      // and no result, session change or event may reach the caller.
      expect(result).toBeUndefined();
      expect(error).toBeInstanceOf(FrameworkTimeoutError);
      expect(stats).toEqual({ committed: 1, rolledBack: 0 });
      expect(lostCommits).toBe(1);
    });

    test('settling one millisecond before the deadline succeeds', async () => {
      const { result, error, stats, lostCommits } = await dispatchAt(999);

      expect(error).toBeUndefined();
      expect(result).toEqual({
        result: 'done',
        sessionChange: { type: 'bind', sessionId: 'session-1' },
        events: [
          {
            target: { kind: 'connection', connectionId: '' },
            event: 'committed',
            payload: { ok: true },
          },
        ],
      });
      expect(stats).toEqual({ committed: 1, rolledBack: 0 });
      expect(lostCommits).toBe(0);
    });
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
