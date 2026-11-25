import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from 'mud-framework-core';
import type { StorageTransaction } from 'mud-framework-core';
import {
  deferred,
  parseJsonLine,
  rejectionOf,
  throwingLogger,
} from 'mud-framework-test-support';

import { SqliteBackend } from './sqlite-backend.ts';

/** JSON logger writing into an array, so emitted events can be asserted. */
function captureLogger(): {
  logger: ReturnType<typeof createJsonLogger>;
  entries: () => Record<string, unknown>[];
} {
  const lines: string[] = [];
  const logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  return {
    logger,
    entries: () => lines.map((line) => parseJsonLine(line, 'log line')),
  };
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

  test.each(['commit', 'rollback'])(
    'a facade from a %s cannot access a later transaction',
    async (outcome) => {
      const backend = SqliteBackend.open();
      const saved = deferred<StorageTransaction>();
      try {
        await backend.transaction((tx) =>
          tx.execute('CREATE TABLE t (v TEXT)'),
        );
        const first = backend.transaction(async (tx) => {
          saved.resolve(tx);
          if (outcome === 'rollback') {
            throw new Error('first transaction failed');
          }
        });
        if (outcome === 'rollback') {
          expect((await rejectionOf(first)).message).toBe(
            'first transaction failed',
          );
        } else {
          await first;
        }
        const stale = await saved.promise;

        const { accesses: attempts, rows: actualRows } =
          await backend.transaction(async (tx) => {
            await tx.execute("INSERT INTO t (v) VALUES ('second')");
            const accesses = await Promise.allSettled([
              Promise.resolve().then(() =>
                stale.execute("INSERT INTO t (v) VALUES ('stale')"),
              ),
              Promise.resolve().then(() => stale.query('SELECT v FROM t')),
            ]);
            const rows = await tx.query<{ v: string }>('SELECT v FROM t');
            return { accesses, rows };
          });

        expect(attempts.map((access) => access.status)).toEqual([
          'rejected',
          'rejected',
        ]);
        expect(actualRows).toEqual([{ v: 'second' }]);
      } finally {
        await backend.close();
      }
    },
  );

  test('serializes transactions instead of interleaving them', async () => {
    const backend = SqliteBackend.open();
    const events: string[] = [];
    const gate = deferred<void>();
    const firstStarted = deferred<void>();

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

  test('close is idempotent and every caller observes the same result', async () => {
    const backend = SqliteBackend.open();

    const first = backend.close();
    const second = backend.close();

    expect(second).toBe(first);
    await first;
    await second;
    expect(() => backend.capabilities()).toThrow(/closed/);
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

  test('logs a PRAGMA verification failure with expected and actual values', () => {
    const { logger, entries } = captureLogger();

    // An in-memory database cannot use WAL, so the readback disagrees.
    expect(() =>
      SqliteBackend.open({ filename: ':memory:', journalMode: 'wal', logger }),
    ).toThrow(/journal_mode/);

    const failures = entries().filter(
      (entry) => entry.msg === 'PRAGMA verification failed',
    );
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      level: 'error',
      backend: 'sqlite',
      pragma: 'journal_mode',
      expected: 'wal',
      actual: 'memory',
    });
  });

  test('logs a failed ROLLBACK and the reason the backend went unusable', async () => {
    const { logger, entries } = captureLogger();
    const backend = SqliteBackend.open({ logger });

    await rejectionOf(
      backend.transaction(async (tx) => {
        await tx.execute('CREATE TABLE t (v TEXT)');
        // Committing behind the backend's back makes its ROLLBACK fail.
        await tx.execute('COMMIT');
        throw new Error('handler failed after the commit');
      }),
    );

    const failures = entries().filter(
      (entry) => entry.msg === 'ROLLBACK failed; backend marked unusable',
    );
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({ level: 'error', backend: 'sqlite' });
    expect(String(failures[0]?.unusableReason)).toMatch(/rollback failed/);
    expect(() => backend.transaction(async () => undefined)).toThrow(
      /unusable/,
    );
  });

  test('keeps the PRAGMA failure when the injected logger throws', () => {
    // The log sits before the throw that reports the same failure. A logger
    // throw must not replace the startup error with the logger's own error.
    expect(() =>
      SqliteBackend.open({
        filename: ':memory:',
        journalMode: 'wal',
        logger: throwingLogger(),
      }),
    ).toThrow(/journal_mode/);
  });
});
