import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf, sleep } from '../test-support/common.ts';
import {
  SlowCommitBackend,
  deferred,
  parseLines,
  quietDispatcher,
} from '../test-support/dispatcher.ts';
import {
  type CancelOutcome,
  FrameworkTimeoutError,
  RequestCancelledError,
} from './dispatcher.ts';

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
    expect(dispatcher.cancel('r1', 'TRANSPORT_CLOSED')).toBe('cancelled');
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

  test('cancel returns not-in-flight for a request that already finished', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('echo', async (_ctx, payload) => payload);

    await dispatcher.dispatch('echo', 'hi', { requestId: 'done' });

    expect(dispatcher.cancel('done', 'TRANSPORT_CLOSED')).toBe('not-in-flight');
    // An id that never arrived is the same case: nothing to undo.
    expect(dispatcher.cancel('never-seen', 'TRANSPORT_CLOSED')).toBe(
      'not-in-flight',
    );
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
    let accepted: CancelOutcome | undefined;

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
    expect(accepted).toBe('cancelled');
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

    expect(dispatcher.cancel('X', 'TRANSPORT_CLOSED')).toBe('cancelled');
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
    expect(dispatcher.cancel('r2', 'TRANSPORT_CLOSED')).toBe('cancelled');
    expect(() => dispatcher.cancel('r2', 'TRANSPORT_CLOSED')).not.toThrow();
    expect(dispatcher.cancel('r2', 'TRANSPORT_CLOSED')).toBe('not-in-flight');

    await rejectionOf(pending);
    release.resolve();
    await sleep(5);
  });

  test('a handler is cancellable unless it declares otherwise', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = deferred<void>();
    const release = deferred<void>();

    dispatcher.register('slow', async () => {
      entered.resolve();
      await release.promise;
      return 'late';
    });

    const pending = dispatcher.dispatch('slow', null, {
      requestId: 'r-default',
    });
    await entered.promise;

    expect(dispatcher.cancel('r-default', 'TRANSPORT_CLOSED')).toBe(
      'cancelled',
    );
    await rejectionOf(pending);
    release.resolve();
    await sleep(5);
  });

  test('cancel is refused while a non-cancellable handler keeps its transaction', async () => {
    const backend = new MemoryStorageBackend();
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 }, lines);
    const entered = deferred<void>();
    const release = deferred<void>();
    let aborted: boolean | undefined;
    let lateWriteError = '';

    dispatcher.register(
      'charge',
      async (ctx) => {
        await ctx.storage.execute('INSERT 1');
        entered.resolve();
        await release.promise;
        aborted = ctx.signal.aborted;
        try {
          await ctx.storage.execute('INSERT 2');
        } catch (error) {
          lateWriteError =
            error instanceof Error ? error.message : String(error);
        }
        return 'charged';
      },
      { cancellable: false },
    );

    const pending = dispatcher.dispatch('charge', null, { requestId: 'r-nc' });
    await entered.promise;

    expect(dispatcher.cancel('r-nc', 'TRANSPORT_CLOSED')).toBe(
      'not-cancellable',
    );
    // Refused, not deferred: the request is still running and has not been
    // settled as failed.
    expect(
      await Promise.race([
        pending.then(() => 'settled' as const),
        sleep(5).then(() => 'pending' as const),
      ]),
    ).toBe('pending');

    release.resolve();
    const outcome = await pending;

    expect(outcome).toEqual({ result: 'charged' });
    expect(aborted).toBe(false);
    expect(lateWriteError).toBe('');
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
    const refused = parseLines(lines).filter(
      (entry) =>
        entry.msg === 'cancel refused: handler declared non-cancellable',
    );
    expect(refused).toHaveLength(1);
    expect(refused[0]).toMatchObject({
      level: 'warn',
      requestId: 'r-nc',
      code: 'TRANSPORT_CLOSED',
      method: 'charge',
      reason: 'declared non-cancellable',
    });
  });

  test('a non-cancellable request is still bounded by its timeout', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    const handlerDone = deferred<void>();
    let aborted: boolean | undefined;

    dispatcher.register(
      'charge',
      async (ctx) => {
        await sleep(40);
        aborted = ctx.signal.aborted;
        handlerDone.resolve();
        return 'late';
      },
      { cancellable: false },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('charge', null, { requestId: 'r-nc' }),
    );
    await handlerDone.promise;
    await sleep(5);

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    expect(aborted).toBe(true);
    // Only the cancel path is refused; the deadline still invalidates the
    // transaction (SPEC 3.6).
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
    expect(dispatcher.cancel('r-nc', 'TRANSPORT_CLOSED')).toBe('not-in-flight');
  });
});
