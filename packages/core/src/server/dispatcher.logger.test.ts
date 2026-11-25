import { describe, expect, test } from 'bun:test';

import {
  FrameworkTimeoutError,
  UnknownHandlerError,
} from '../shared/wire-codes.ts';
import { rejectionOf, throwingLogger } from '../testing/common.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { Dispatcher } from './dispatcher.ts';

describe('Dispatcher logger safety', () => {
  test('returns the result and Session binding when the logger throws', async () => {
    const dispatcher = new Dispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
      logger: throwingLogger(),
    });
    dispatcher.register(
      'login',
      (ctx) => {
        ctx.bindSession('s1');
        return Promise.resolve('ok');
      },
      { access: 'public', mayChangeSession: true },
    );

    const result = await dispatcher.dispatch('login', {});

    expect(result).toEqual({
      result: 'ok',
      sessionChange: { type: 'bind', sessionId: 's1' },
    });
    await dispatcher.close();
  }, 2000);

  test('keeps the unknown-method rejection type when the logger throws', async () => {
    const dispatcher = new Dispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
      logger: throwingLogger(),
    });

    const error = await rejectionOf(dispatcher.dispatch('missing', {}));

    expect(error).toBeInstanceOf(UnknownHandlerError);
    await dispatcher.close();
  }, 2000);

  test('still times out a hanging handler when the logger throws', async () => {
    const dispatcher = new Dispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 10,
      logger: throwingLogger(),
    });
    dispatcher.register('hang', () => new Promise<never>(() => undefined), {
      access: 'public',
    });

    const error = await rejectionOf(dispatcher.dispatch('hang', {}));

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    await dispatcher.close();
  }, 2000);
});
