import { describe, expect, test } from 'bun:test';

import { rejectionOf } from 'mud-framework-test-support';

import { MemoryStorageBackend } from './memory-backend.ts';

describe('MemoryStorageBackend', () => {
  test('declares the weakest honest capabilities by default', () => {
    const backend = new MemoryStorageBackend();

    expect(backend.capabilities()).toEqual({
      transactions: 'full',
      concurrency: 'pooled',
      snapshotRead: true,
      uniqueConstraint: false,
      maxTransactionDurationMs: Number.POSITIVE_INFINITY,
    });
  });

  test('lets tests inject capabilities', () => {
    const backend = new MemoryStorageBackend({
      capabilities: {
        concurrency: 'single',
        maxTransactionDurationMs: 25,
      },
    });

    const declared = backend.capabilities();

    expect(declared.concurrency).toBe('single');
    expect(declared.maxTransactionDurationMs).toBe(25);
    expect(declared.transactions).toBe('full');
  });

  test('rejects calls after close', async () => {
    const backend = new MemoryStorageBackend();

    await backend.close();

    expect(() => backend.capabilities()).toThrow(/closed/);
    expect(() => backend.transaction(async () => undefined)).toThrow(/closed/);
  });

  test('counts a committed transaction', async () => {
    const backend = new MemoryStorageBackend();

    const result = await backend.transaction(async () => 'ok');

    expect(result).toBe('ok');
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
  });

  test('rolls back when the transaction body throws', async () => {
    const backend = new MemoryStorageBackend();

    const error = await rejectionOf(
      backend.transaction(async () => {
        throw new Error('boom');
      }),
    );

    expect(error.message).toBe('boom');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('routes query and execute through the injected handler', async () => {
    const calls: { sql: string; params: readonly unknown[] }[] = [];
    const backend = new MemoryStorageBackend({
      handler: (sql, params) => {
        calls.push({ sql, params });
        return [{ value: 42 }];
      },
    });

    const rows = await backend.transaction(async (tx) => {
      const found = await tx.query<{ value: number }>('SELECT 42');
      await tx.execute('INSERT 7', [7]);
      return found;
    });

    expect(rows).toEqual([{ value: 42 }]);
    expect(calls).toEqual([
      { sql: 'SELECT 42', params: [] },
      { sql: 'INSERT 7', params: [7] },
    ]);
  });

  test('serializes transactions when concurrency is single', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { concurrency: 'single' },
    });
    let active = 0;
    let peak = 0;

    const body = async (): Promise<void> => {
      active += 1;
      peak = Math.max(peak, active);
      await new Promise<void>((resolve) => {
        setTimeout(resolve, 5);
      });
      active -= 1;
    };

    await Promise.all([
      backend.transaction(body),
      backend.transaction(body),
      backend.transaction(body),
    ]);

    expect(peak).toBe(1);
    expect(backend.stats().committed).toBe(3);
  });
});
