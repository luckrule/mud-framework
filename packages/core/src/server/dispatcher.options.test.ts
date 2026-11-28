import { describe, expect, test } from 'bun:test';

import { rejectionOf } from '../testing/common.ts';
import { quietDispatcher } from '../testing/dispatcher.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { Dispatcher } from './dispatcher.ts';

function dispatcherWith(maxEventsPerRequest: unknown): Dispatcher {
  return new Dispatcher({
    backend: new MemoryStorageBackend(),
    timeoutMs: 1000,
    maxEventsPerRequest: maxEventsPerRequest as number,
  });
}

describe('Dispatcher maxEventsPerRequest validation', () => {
  test.each([
    ['string', '2'],
    ['null', null],
    ['object', { limit: 2 }],
  ] as const)('rejects a non-number %s with TypeError', (_name, value) => {
    expect(() => dispatcherWith(value)).toThrow(TypeError);
    expect(() => dispatcherWith(value)).toThrow(
      /maxEventsPerRequest must be a safe integer of at least 1/,
    );
  });

  test.each([
    ['zero', 0],
    ['negative', -1],
    ['fractional', 1.5],
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['unsafe integer', Number.MAX_SAFE_INTEGER + 1],
    ['1e21', 1e21],
  ] as const)('rejects %s with RangeError', (_name, value) => {
    expect(() => dispatcherWith(value)).toThrow(RangeError);
    expect(() => dispatcherWith(value)).toThrow(
      /maxEventsPerRequest must be a safe integer of at least 1/,
    );
  });

  // Numbers are shown bare, non-numbers keep their type and scalar text.
  test('names the value and the safe-integer contract in the message', () => {
    expect(() => dispatcherWith(1.5)).toThrow(
      'maxEventsPerRequest must be a safe integer of at least 1, got 1.5',
    );
    expect(() => dispatcherWith('2')).toThrow(
      'maxEventsPerRequest must be a safe integer of at least 1, got string "2"',
    );
    expect(() => dispatcherWith(Number.MAX_SAFE_INTEGER + 1)).toThrow(
      'maxEventsPerRequest must be a safe integer of at least 1, got 9007199254740992',
    );
  });

  test('accepts 1 and rejects the second event', async () => {
    const { dispatcher } = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
      maxEventsPerRequest: 1,
    });
    dispatcher.register(
      'one',
      (ctx) => {
        ctx.emit('first', 1);
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );
    dispatcher.register(
      'two',
      (ctx) => {
        ctx.emit('first', 1);
        ctx.emit('second', 2);
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    const allowed = await dispatcher.dispatch('one', null, {
      connectionId: 'conn-1',
    });
    expect(allowed.events ?? []).toHaveLength(1);

    const error = await rejectionOf(
      dispatcher.dispatch('two', null, { connectionId: 'conn-1' }),
    );
    expect(error.message).toBe('a request may emit at most 1 event');
  });
});
