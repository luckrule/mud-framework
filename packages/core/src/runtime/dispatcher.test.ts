import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import type {
  StorageBackend,
  StorageCapabilities,
  StorageTransaction,
} from '../storage/types.ts';
import {
  Dispatcher,
  FrameworkTimeoutError,
  RequestCancelledError,
} from './dispatcher.ts';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Parsed log lines, in emission order. */
function parseLines(lines: readonly string[]): Record<string, unknown>[] {
  return lines.map((line) => {
    const parsed: unknown = JSON.parse(line);
    if (!isRecord(parsed)) {
      throw new Error(`log line is not a JSON object: ${line}`);
    }
    return parsed;
  });
}

/** Dispatcher whose logs go to `lines` instead of stdout. */
function quietDispatcher(
  options: ConstructorParameters<typeof Dispatcher>[0],
  lines: string[] = [],
): { dispatcher: Dispatcher; lines: string[] } {
  const logger: Logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  return {
    dispatcher: new Dispatcher({ ...options, logger }),
    lines,
  };
}

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

/**
 * A backend whose COMMIT lands a macrotask after the handler returns, like a
 * real database flushing its log. `MemoryStorageBackend` commits in the same
 * turn, which hides the window where a cancel arrives too late to roll back.
 */
class SlowCommitBackend implements StorageBackend {
  private committed = 0;
  private rolledBack = 0;

  capabilities(): StorageCapabilities {
    return {
      transactions: 'full',
      concurrency: 'pooled',
      snapshotRead: true,
      uniqueConstraint: false,
      maxTransactionDurationMs: Number.POSITIVE_INFINITY,
    };
  }

  async transaction<T>(fn: (tx: StorageTransaction) => Promise<T>): Promise<T> {
    const tx: StorageTransaction = {
      query: <R>(): Promise<R[]> => Promise.resolve([]),
      execute: (): Promise<void> => Promise.resolve(),
    };
    let result: T;
    try {
      result = await fn(tx);
    } catch (error) {
      this.rolledBack += 1;
      throw error;
    }
    await sleep(0);
    this.committed += 1;
    return result;
  }

  close(): Promise<void> {
    return Promise.resolve();
  }

  stats(): { committed: number; rolledBack: number } {
    return { committed: this.committed, rolledBack: this.rolledBack };
  }
}

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

    const { result } = await dispatcher.dispatch('echo', 'hello');

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
    const { dispatcher, lines } = quietDispatcher({ backend, timeoutMs: 1000 });

    const error = await rejectionOf(dispatcher.dispatch('missing', null));

    expect(error.message).toMatch(/no handler registered/);
    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0] ?? '')).toMatchObject({
      level: 'warn',
      method: 'missing',
    });
  });

  test('fails the request at the timeout and invalidates later writes', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
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

  test('generates a request id with the default factory', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    let seen = '';
    dispatcher.register('inspect', async (ctx) => {
      seen = ctx.requestId;
      return null;
    });

    await dispatcher.dispatch('inspect', null);

    expect(seen).toMatch(/^[0-9a-f-]{36}$/);
  });

  test('uses an injected request id factory', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({
      backend,
      timeoutMs: 1000,
      createRequestId: () => 'fixed-id',
    });
    let seen = '';
    dispatcher.register('inspect', async (ctx) => {
      seen = ctx.requestId;
      return null;
    });

    await dispatcher.dispatch('inspect', null);

    expect(seen).toBe('fixed-id');
  });

  test('prefers meta.requestId so a replay keeps the original id', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({
      backend,
      timeoutMs: 1000,
      createRequestId: () => 'fresh-id',
    });
    const seen: string[] = [];
    dispatcher.register('inspect', async (ctx) => {
      seen.push(ctx.requestId);
      return null;
    });

    await dispatcher.dispatch('inspect', null, { requestId: 'original-id' });
    await dispatcher.dispatch('inspect', null);

    expect(seen).toEqual(['original-id', 'fresh-id']);
  });

  test('carries connection, client request and session ids into the context', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let seen: Record<string, unknown> = {};
    dispatcher.register('inspect', async (ctx) => {
      seen = {
        connectionId: ctx.connectionId,
        clientRequestId: ctx.clientRequestId,
        sessionId: ctx.sessionId,
      };
      return null;
    });

    await dispatcher.dispatch('inspect', null, {
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
    });

    expect(seen).toEqual({
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
    });
  });

  test('defaults connection and client ids to empty strings when absent', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let seen: Record<string, unknown> = {};
    dispatcher.register('inspect', async (ctx) => {
      seen = {
        connectionId: ctx.connectionId,
        clientRequestId: ctx.clientRequestId,
        sessionId: ctx.sessionId,
      };
      return null;
    });

    await dispatcher.dispatch('inspect', null);

    expect(seen).toEqual({
      connectionId: '',
      clientRequestId: '',
      sessionId: undefined,
    });
  });

  test('binds the request ids onto ctx.logger', async () => {
    const backend = new MemoryStorageBackend();
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 }, lines);
    dispatcher.register('inspect', async (ctx) => {
      ctx.logger.info({ detail: 'inside handler' }, 'handler ran');
      return null;
    });

    await dispatcher.dispatch('inspect', null, {
      requestId: 'req-9',
      connectionId: 'c9',
      clientRequestId: 'client-9',
    });

    expect(JSON.parse(lines[0] ?? '')).toMatchObject({
      level: 'info',
      msg: 'handler ran',
      requestId: 'req-9',
      connectionId: 'c9',
      clientRequestId: 'client-9',
      detail: 'inside handler',
    });
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
    dispatcher.register('slow', async () => {
      await sleep(40);
      return 'late';
    });

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
    dispatcher.register('slow', async () => {
      await sleep(40);
      return 'late';
    });

    const error = await rejectionOf(
      dispatcher.dispatch('slow', null, { requestId: 'given-id' }),
    );

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.requestId).toBe('given-id');
    }
  });

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

  test('meta.timeoutMs tightens the effective timeout', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10_000 });
    dispatcher.register('slow', async () => {
      await sleep(60);
      return 'late';
    });

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
    dispatcher.register('slow', async () => {
      await sleep(60);
      return 'late';
    });

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
    dispatcher.register('slow', async () => {
      await sleep(40);
      return 'late';
    });

    const error = await rejectionOf(
      dispatcher.dispatch('slow', null, { timeoutMs: 500 }),
    );

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.timeoutMs).toBe(5);
    }
    await sleep(40);
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
});

describe('Dispatcher.bindSession', () => {
  test('reports the session id the handler bound', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('session-1');
      return 'welcome';
    });

    const outcome = await dispatcher.dispatch('login', null);

    expect(outcome).toEqual({ result: 'welcome', boundSessionId: 'session-1' });
  });

  test('leaves the session id undefined when the handler binds nothing', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('look', async () => 'a rock');

    const outcome = await dispatcher.dispatch('look', null);

    expect(outcome.result).toBe('a rock');
    expect(outcome.boundSessionId).toBeUndefined();
  });

  test('does not commit the binding when the handler throws', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('half-made');
      throw new Error('login refused');
    });

    const error = await rejectionOf(dispatcher.dispatch('login', null));

    expect(error.message).toBe('login refused');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('does not commit the binding when the request times out', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    const handlerDone = deferred<void>();
    dispatcher.register('slow-login', async (ctx) => {
      ctx.bindSession('too-late');
      await sleep(40);
      handlerDone.resolve();
      return 'late';
    });

    const error = await rejectionOf(dispatcher.dispatch('slow-login', null));
    // The backend rolls back only after the abandoned handler returns.
    await handlerDone.promise;
    await sleep(5);

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects an empty session id', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('');
      return null;
    });

    const error = await rejectionOf(dispatcher.dispatch('login', null));

    expect(error.message).toMatch(/non-empty session id/);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects a second bind in the same request and keeps the first', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('first');
      ctx.bindSession('second');
      return null;
    });

    const error = await rejectionOf(dispatcher.dispatch('login', null));

    expect(error.message).toMatch(/bindSession called twice/);
    expect(error.message).toContain('first');
    expect(error.message).toContain('second');
    // A rejected request commits nothing, so neither id escapes.
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects a bind after the request already timed out', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    let bindError = '';
    dispatcher.register('slow', async (ctx) => {
      await sleep(40);
      try {
        ctx.bindSession('late');
        bindError = 'bind was accepted';
      } catch (error) {
        bindError = error instanceof Error ? error.message : String(error);
      }
      return 'late';
    });

    await rejectionOf(dispatcher.dispatch('slow', null));
    await sleep(60);

    // The transaction is expired, so a binding made after the deadline is
    // rejected like any other late write (SPEC 3.3, 3.4).
    expect(bindError).toMatch(/expired/);
  });

  test('logs the binding at debug with the session id', async () => {
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      level: 'debug',
      sink: (line) => {
        lines.push(line);
      },
    });
    const dispatcher = new Dispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
      logger,
    });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('session-9');
      return null;
    });

    await dispatcher.dispatch('login', null, { requestId: 'req-9' });

    const bound = parseLines(lines).filter(
      (entry) => entry.msg === 'session bound',
    );
    expect(bound).toHaveLength(1);
    expect(bound[0]).toMatchObject({
      level: 'debug',
      requestId: 'req-9',
      sessionId: 'session-9',
    });
  });
});

describe('Dispatcher.abortSignal', () => {
  test('hands the handler an unaborted AbortSignal', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let seen: unknown;
    let aborted: boolean | undefined;
    dispatcher.register('inspect', async (ctx) => {
      seen = ctx.signal;
      aborted = ctx.signal.aborted;
      return null;
    });

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

    dispatcher.register('slow', async (ctx) => {
      await sleep(40);
      aborted = ctx.signal.aborted;
      reason = ctx.signal.reason;
      handlerDone.resolve();
      return 'late';
    });

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

    dispatcher.register('slow', async (ctx) => {
      entered.resolve();
      await release.promise;
      aborted = ctx.signal.aborted;
      reason = ctx.signal.reason;
      return 'late';
    });

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

    dispatcher.register('wait-for-abort', async (ctx) => {
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
    });

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

    dispatcher.register('inspect', async (ctx) => {
      signal = ctx.signal;
      abortedInside = ctx.signal.aborted;
      return null;
    });

    await dispatcher.dispatch('inspect', null);

    expect(abortedInside).toBe(false);
    expect(signal?.aborted).toBe(false);
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
