import { describe, expect, test } from 'bun:test';

import { SqliteBackend } from './sqlite-backend.ts';

interface Deferred {
  promise: Promise<void>;
  resolve: () => void;
}

function deferred(): Deferred {
  return Promise.withResolvers<void>();
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

function errorCode(error: Error): string | undefined {
  if ('code' in error && typeof error.code === 'string') {
    return error.code;
  }
  return undefined;
}

describe('SqliteBackend', () => {
  test('declares full transactions on a single connection', () => {
    const backend = SqliteBackend.open();

    expect(backend.capabilities()).toEqual({
      transactions: 'full',
      concurrency: 'single',
      snapshotRead: true,
      uniqueConstraint: true,
      maxTransactionDurationMs: Number.POSITIVE_INFINITY,
    });
  });

  test('reports the configured transaction ceiling', () => {
    const backend = SqliteBackend.open({ maxTransactionDurationMs: 250 });

    expect(backend.capabilities().maxTransactionDurationMs).toBe(250);
  });

  test('creates, inserts and reads rows', async () => {
    const backend = SqliteBackend.open();

    const rows = await backend.transaction(async (tx) => {
      await tx.execute(
        'CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)',
      );
      await tx.execute('INSERT INTO items (id, name) VALUES (?, ?)', [1, 'a']);
      await tx.execute('INSERT INTO items (id, name) VALUES (?, ?)', [2, 'b']);
      return tx.query<{ id: number; name: string }>(
        'SELECT id, name FROM items ORDER BY id',
      );
    });

    expect(rows).toEqual([
      { id: 1, name: 'a' },
      { id: 2, name: 'b' },
    ]);
  });

  test('commits when the handler returns', async () => {
    const backend = SqliteBackend.open();
    await backend.transaction(async (tx) => {
      await tx.execute('CREATE TABLE t (v TEXT)');
    });

    await backend.transaction(async (tx) => {
      await tx.execute("INSERT INTO t (v) VALUES ('kept')");
    });

    const rows = await backend.transaction((tx) =>
      tx.query<{ v: string }>('SELECT v FROM t'),
    );

    expect(rows).toEqual([{ v: 'kept' }]);
  });

  test('rolls back when the handler throws', async () => {
    const backend = SqliteBackend.open();
    await backend.transaction(async (tx) => {
      await tx.execute('CREATE TABLE t (v TEXT)');
    });

    const error = await rejectionOf(
      backend.transaction(async (tx) => {
        await tx.execute("INSERT INTO t (v) VALUES ('dropped')");
        throw new Error('boom');
      }),
    );

    const rows = await backend.transaction((tx) =>
      tx.query<{ v: string }>('SELECT v FROM t'),
    );

    expect(error.message).toBe('boom');
    expect(rows).toEqual([]);
  });

  test('serializes transactions instead of interleaving them', async () => {
    const backend = SqliteBackend.open();
    const events: string[] = [];
    const gate = deferred();
    const firstStarted = deferred();

    const first = backend.transaction(async (tx) => {
      events.push('first:start');
      firstStarted.resolve();
      await gate.promise;
      await tx.execute('CREATE TABLE t (v TEXT)');
      events.push('first:end');
    });
    const second = backend.transaction(async (tx) => {
      events.push('second:start');
      await tx.execute("INSERT INTO t (v) VALUES ('second')");
      events.push('second:end');
    });

    await firstStarted.promise;
    // The second handler is queued behind the first, so it must not have run
    // even though the first is parked on an await.
    expect(events).toEqual(['first:start']);

    gate.resolve();
    await Promise.all([first, second]);

    expect(events).toEqual([
      'first:start',
      'first:end',
      'second:start',
      'second:end',
    ]);
  });

  test('a rollback does not erase a concurrent transaction', async () => {
    const backend = SqliteBackend.open();
    await backend.transaction(async (tx) => {
      await tx.execute('CREATE TABLE t (v TEXT)');
    });

    const failing = backend.transaction(async (tx) => {
      await tx.execute("INSERT INTO t (v) VALUES ('rolled-back')");
      throw new Error('boom');
    });
    const succeeding = backend.transaction(async (tx) => {
      await tx.execute("INSERT INTO t (v) VALUES ('committed')");
    });

    const outcomes = await Promise.allSettled([failing, succeeding]);
    const rows = await backend.transaction((tx) =>
      tx.query<{ v: string }>('SELECT v FROM t'),
    );

    expect(outcomes.map((outcome) => outcome.status)).toEqual([
      'rejected',
      'fulfilled',
    ]);
    expect(rows).toEqual([{ v: 'committed' }]);
  });

  test('rejects every call after close', async () => {
    const backend = SqliteBackend.open();
    await backend.close();

    expect(() => backend.capabilities()).toThrow(/closed/);
    expect(() => backend.transaction(async () => undefined)).toThrow(/closed/);
  });

  test('applies the pragmas it was opened with', async () => {
    const backend = SqliteBackend.open({ busyTimeoutMs: 1234 });

    const journal = await backend.transaction((tx) =>
      tx.query<{ journal_mode: string }>('PRAGMA journal_mode'),
    );
    const timeout = await backend.transaction((tx) =>
      tx.query<{ timeout: number }>('PRAGMA busy_timeout'),
    );
    const foreignKeys = await backend.transaction((tx) =>
      tx.query<{ foreign_keys: number }>('PRAGMA foreign_keys'),
    );

    // An in-memory database cannot use WAL; SQLite reports `memory`.
    expect(journal).toEqual([{ journal_mode: 'memory' }]);
    expect(timeout).toEqual([{ timeout: 1234 }]);
    expect(foreignKeys).toEqual([{ foreign_keys: 1 }]);
  });

  test('surfaces unique constraint violations with a code', async () => {
    const backend = SqliteBackend.open();
    await backend.transaction(async (tx) => {
      await tx.execute('CREATE TABLE u (id INTEGER PRIMARY KEY, name TEXT)');
      await tx.execute('INSERT INTO u (id, name) VALUES (1, ?)', ['first']);
    });

    const error = await rejectionOf(
      backend.transaction(async (tx) => {
        await tx.execute('INSERT INTO u (id, name) VALUES (1, ?)', ['second']);
      }),
    );

    expect(error.message).toMatch(/UNIQUE/i);
    expect(errorCode(error)?.startsWith('SQLITE_CONSTRAINT')).toBe(true);
  });
});
