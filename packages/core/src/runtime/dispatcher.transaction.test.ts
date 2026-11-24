import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf } from '../test-support/common.ts';
import { quietDispatcher } from '../test-support/dispatcher.ts';
import { Dispatcher } from './dispatcher.ts';

describe('Dispatcher.transaction', () => {
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

  test('close shuts down the backend', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });

    await dispatcher.close();

    expect(() => backend.capabilities()).toThrow(/closed/);
    const error = await rejectionOf(dispatcher.dispatch('anything', null));
    expect(error.message).toMatch(/closed/);
  });
});
