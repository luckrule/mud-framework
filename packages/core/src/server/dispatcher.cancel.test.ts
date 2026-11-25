import { describe, expect, test } from 'bun:test';

import {
  FrameworkTimeoutError,
  RequestCancelledError,
} from '../shared/wire-codes.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf, sleep } from '../testing/common.ts';
import {
  SlowCommitBackend,
  deferred,
  parseLines,
  quietDispatcher,
} from '../testing/dispatcher.ts';

describe('Dispatcher.cancel', () => {
  test('cancel rejects later writes and rolls the transaction back', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = deferred<void>();
    const release = deferred<void>();
    let writeError = '';

    dispatcher.register('slow', async (ctx) => {
      entered.resolve();
      await release.promise;
      try {
        await ctx.storage.execute('INSERT 1');
        writeError = 'write was accepted';
      } catch (error) {
        writeError = error instanceof Error ? error.message : String(error);
      }
      return 'late result';
    });

    const pending = dispatcher.dispatch('slow', null, { requestId: 'r1' });
    await entered.promise;
    expect(dispatcher.cancel('r1', 'TRANSPORT_CLOSED')).toBe(true);
    const error = await rejectionOf(pending);
    release.resolve();
    // The rollback happens only when the abandoned handler returns.
    await sleep(5);

    expect(error).toBeInstanceOf(RequestCancelledError);
    if (error instanceof RequestCancelledError) {
      expect(error.code).toBe('TRANSPORT_CLOSED');
      expect(error.requestId).toBe('r1');
    }
    expect(writeError).toMatch(/expired/);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('cancel settles the dispatch promise and logs at warn', async () => {
    const backend = new MemoryStorageBackend();
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 }, lines);
    const entered = deferred<void>();
    const release = deferred<void>();

    dispatcher.register('slow', async () => {
      entered.resolve();
      await release.promise;
      return 'late';
    });

    const pending = dispatcher.dispatch('slow', null, { requestId: 'r2' });
    await entered.promise;
    dispatcher.cancel('r2', 'FRAMEWORK_TIMEOUT');

    const error = await rejectionOf(pending);
    release.resolve();
    await sleep(5);

    expect(error).toBeInstanceOf(RequestCancelledError);
    const cancelled = parseLines(lines).filter(
      (entry) => entry.msg === 'request cancelled',
    );
    expect(cancelled).toHaveLength(1);
    expect(cancelled[0]).toMatchObject({
      level: 'warn',
      requestId: 'r2',
      code: 'FRAMEWORK_TIMEOUT',
    });
    expect(typeof cancelled[0]?.durationMs).toBe('number');
  });

  test('cancel returns false for a request that already finished', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('echo', async (_ctx, payload) => payload);

    await dispatcher.dispatch('echo', 'hi', { requestId: 'done' });

    expect(dispatcher.cancel('done', 'TRANSPORT_CLOSED')).toBe(false);
    // An id that never arrived is the same case: nothing to undo.
    expect(dispatcher.cancel('never-seen', 'TRANSPORT_CLOSED')).toBe(false);
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
  });

  test('a handler that returns normally after a cancel still rolls back', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = deferred<void>();
    const release = deferred<void>();

    dispatcher.register('write-then-return', async (ctx) => {
      await ctx.storage.execute('INSERT 1');
      entered.resolve();
      await release.promise;
      return 'looks successful';
    });

    const pending = dispatcher.dispatch('write-then-return', null, {
      requestId: 'r3',
    });
    await entered.promise;
    dispatcher.cancel('r3', 'TRANSPORT_CLOSED');
    release.resolve();

    const error = await rejectionOf(pending);
    await sleep(5);

    expect(error).toBeInstanceOf(RequestCancelledError);
    // The write happened before the cancel, but the commit must not.
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('logs CANCEL_LOST_TO_COMMIT when the cancel lands after the handler returned', async () => {
    const backend = new SlowCommitBackend();
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 }, lines);
    let accepted: boolean | undefined;

    dispatcher.register('quick', async () => {
      // Runs before the backend's COMMIT timer, so the cancel lands in the
      // window where the handler is done but the transaction is not.
      setTimeout(() => {
        accepted = dispatcher.cancel('r-commit', 'FRAMEWORK_TIMEOUT');
      }, 0);
      return 'ok';
    });

    const error = await rejectionOf(
      dispatcher.dispatch('quick', null, { requestId: 'r-commit' }),
    );
    await sleep(10);

    expect(error).toBeInstanceOf(RequestCancelledError);
    expect(accepted).toBe(true);
    // The commit is real, and the log must say so: a rollback and a commit used
    // to be indistinguishable from the log alone.
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
    const lost = parseLines(lines).filter(
      (entry) => entry.code === 'CANCEL_LOST_TO_COMMIT',
    );
    expect(lost).toHaveLength(1);
    expect(lost[0]).toMatchObject({
      level: 'warn',
      msg: 'transaction committed after the request reached a terminal state',
      requestId: 'r-commit',
    });
    expect(typeof lost[0]?.durationMs).toBe('number');
  });

  test('logs nothing when the transaction commits before any cancel', async () => {
    const backend = new SlowCommitBackend();
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 }, lines);

    dispatcher.register('quick', () => Promise.resolve('ok'));

    await dispatcher.dispatch('quick', null, { requestId: 'r-plain' });
    await sleep(10);

    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
    expect(
      parseLines(lines).filter(
        (entry) => entry.code === 'CANCEL_LOST_TO_COMMIT',
      ),
    ).toHaveLength(0);
  });

  test('a timeout from an earlier dispatch of the same id cannot evict the later one', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const firstEntered = deferred<void>();
    const secondEntered = deferred<void>();
    const release = deferred<void>();
    let calls = 0;

    dispatcher.register('slow', async () => {
      calls += 1;
      (calls === 1 ? firstEntered : secondEntered).resolve();
      await release.promise;
      return calls;
    });

    // Same id twice: the second `set` overwrites the first's map entry.
    const first = dispatcher.dispatch('slow', null, {
      requestId: 'X',
      timeoutMs: 15,
    });
    await firstEntered.promise;
    const second = dispatcher.dispatch('slow', null, { requestId: 'X' });
    await secondEntered.promise;

    // The first's timer fires while the second is still running. Deleting the
    // entry unconditionally would leave the second uncancellable.
    const firstError = await rejectionOf(first);
    expect(firstError).toBeInstanceOf(FrameworkTimeoutError);

    expect(dispatcher.cancel('X', 'TRANSPORT_CLOSED')).toBe(true);
    release.resolve();
    const secondError = await rejectionOf(second);

    expect(secondError).toBeInstanceOf(RequestCancelledError);
  });

  test('a second cancel is a no-op and does not throw', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = deferred<void>();
    const release = deferred<void>();

    dispatcher.register('slow', async () => {
      entered.resolve();
      await release.promise;
      return 'late';
    });

    const pending = dispatcher.dispatch('slow', null, { requestId: 'r2' });
    await entered.promise;
    expect(dispatcher.cancel('r2', 'TRANSPORT_CLOSED')).toBe(true);
    expect(() => dispatcher.cancel('r2', 'TRANSPORT_CLOSED')).not.toThrow();
    expect(dispatcher.cancel('r2', 'TRANSPORT_CLOSED')).toBe(false);

    await rejectionOf(pending);
    release.resolve();
    await sleep(5);
  });
});
