import { describe, expect, test } from 'bun:test';

import { UnknownHandlerError } from '../shared/wire-codes.ts';
import { rejectionOf } from '../testing/common.ts';
import { quietDispatcher } from '../testing/dispatcher.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import type { Handler, HandlerAccess } from './dispatcher.ts';

const handler: Handler = async () => null;

function createDispatcher(): ReturnType<typeof quietDispatcher>['dispatcher'] {
  return quietDispatcher({
    backend: new MemoryStorageBackend(),
    timeoutMs: 1000,
  }).dispatcher;
}

describe('Dispatcher handler registration validation', () => {
  test('rejects invalid HandlerOptions before registering anything', async () => {
    const dispatcher = createDispatcher();

    const badAccess = () =>
      dispatcher.register('bad-access', handler, {
        access: 'admin' as HandlerAccess,
      });
    expect(badAccess).toThrow(TypeError);
    expect(badAccess).toThrow(/access/);
    expect(badAccess).toThrow(/admin/);

    const badChange = () =>
      dispatcher.register('bad-change', handler, {
        mayChangeSession: 'yes' as unknown as boolean,
      });
    expect(badChange).toThrow(TypeError);
    expect(badChange).toThrow(/mayChangeSession/);
    expect(badChange).toThrow(/yes/);

    // A refused registration must leave no handler behind.
    const error = await rejectionOf(dispatcher.dispatch('bad-access', null));
    expect(error).toBeInstanceOf(UnknownHandlerError);
  });
});
