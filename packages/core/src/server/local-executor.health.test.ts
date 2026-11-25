import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { Dispatcher } from './dispatcher.ts';
import { LocalExecutor } from './local-executor.ts';

function local(): LocalExecutor {
  return new LocalExecutor({
    dispatcher: new Dispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    }),
  });
}

describe('LocalExecutor health', () => {
  test('is ready while the dispatcher is open', () => {
    expect(local().health()).toEqual({ state: 'ready', ready: true });
  });

  test('is closed once the dispatcher is closed', async () => {
    const executor = local();

    await executor.close();

    expect(executor.health()).toEqual({ state: 'closed', ready: false });
  });
});
