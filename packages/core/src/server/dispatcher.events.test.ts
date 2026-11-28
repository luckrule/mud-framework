import { describe, expect, test } from 'bun:test';

import type { BufferedServerEvent } from '../shared/events.ts';
import type { StorageBackend } from '../storage/types.ts';
import { rejectionOf, sleep } from '../testing/common.ts';
import { quietDispatcher } from '../testing/dispatcher.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { EXPIRED_MESSAGE } from './dispatcher-scope.ts';
import type { DispatchResult } from './dispatcher.ts';

function eventsOf(result: DispatchResult): readonly BufferedServerEvent[] {
  return result.events ?? [];
}

describe('Dispatcher server events', () => {
  test('buffers ctx.emit for the request connection and returns it on success', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'look',
      (ctx) => {
        ctx.emit('room.description', { text: 'a cave' });
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    const outcome = await dispatcher.dispatch('look', null, {
      connectionId: 'conn-1',
    });

    expect(outcome.result).toBe('ok');
    expect(eventsOf(outcome)).toEqual([
      {
        target: { kind: 'connection', connectionId: 'conn-1' },
        event: 'room.description',
        payload: { text: 'a cave' },
      },
    ]);
  });

  test('buffers ctx.emitToSession for a named session', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'tell',
      (ctx) => {
        ctx.emitToSession('session-2', 'chat.message', { text: 'hi' });
        return Promise.resolve(null);
      },
      { access: 'public' },
    );

    const outcome = await dispatcher.dispatch('tell', null, {
      sessionId: 'session-1',
    });

    expect(eventsOf(outcome)).toEqual([
      {
        target: { kind: 'session', sessionId: 'session-2' },
        event: 'chat.message',
        payload: { text: 'hi' },
      },
    ]);
  });

  test('preserves emission order across both targets', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'both',
      (ctx) => {
        ctx.emit('first', 1);
        ctx.emitToSession('session-2', 'second', 2);
        ctx.emit('third', 3);
        return Promise.resolve(null);
      },
      { access: 'public' },
    );

    const outcome = await dispatcher.dispatch('both', null, {
      connectionId: 'conn-1',
    });

    expect(eventsOf(outcome).map((event) => event.event)).toEqual([
      'first',
      'second',
      'third',
    ]);
  });

  test('returns no events field when the handler emitted nothing', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('quiet', () => Promise.resolve('ok'), {
      access: 'public',
    });

    const outcome = await dispatcher.dispatch('quiet', null);

    expect(outcome).not.toHaveProperty('events');
  });

  test('discards buffered events when the handler throws', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'fail',
      (ctx) => {
        ctx.emit('never.sent', null);
        throw new Error('business failure');
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('fail', null, { connectionId: 'conn-1' }),
    );

    expect(error.message).toBe('business failure');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('discards buffered events when the commit fails', async () => {
    const backend: StorageBackend = {
      capabilities: () => ({
        transactions: 'full',
        concurrency: 'pooled',
        snapshotRead: true,
        uniqueConstraint: false,
        maxTransactionDurationMs: Number.POSITIVE_INFINITY,
      }),
      transaction: async (fn) => {
        await fn({
          query: () => Promise.resolve([]),
          execute: () => Promise.resolve(),
        });
        throw new Error('commit failed');
      },
      close: () => Promise.resolve(),
    };
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'write',
      (ctx) => {
        ctx.emit('never.sent', null);
        return Promise.resolve('written');
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('write', null, { connectionId: 'conn-1' }),
    );

    expect(error.message).toBe('commit failed');
  });

  test('rejects a post-timeout emit and keeps the request failed', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    let lateError = '';

    dispatcher.register(
      'slow',
      async (ctx) => {
        await sleep(40);
        try {
          ctx.emit('late', null);
        } catch (error) {
          lateError = error instanceof Error ? error.message : String(error);
        }
        return 'late result';
      },
      { access: 'public' },
    );

    await rejectionOf(dispatcher.dispatch('slow', null));
    await sleep(60);

    expect(lateError).toBe(EXPIRED_MESSAGE);
  });

  test('rejects a post-cancel emit', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let lateError = '';

    dispatcher.register(
      'slow',
      async (ctx) => {
        entered.resolve();
        await release.promise;
        try {
          ctx.emit('late', null);
        } catch (error) {
          lateError = error instanceof Error ? error.message : String(error);
        }
        return 'done';
      },
      { access: 'public' },
    );

    const pending = dispatcher.dispatch('slow', null, {
      requestId: 'req-1',
    });
    await entered.promise;
    expect(dispatcher.cancel('req-1', 'TRANSPORT_CLOSED')).toBe(true);
    release.resolve();
    await rejectionOf(pending);
    expect(lateError).toBe(EXPIRED_MESSAGE);
  });

  test('rejects invalid event names and session ids', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const errors: string[] = [];

    dispatcher.register(
      'invalid',
      (ctx) => {
        try {
          ctx.emit('', null);
        } catch (error) {
          errors.push(error instanceof Error ? error.message : String(error));
        }
        try {
          ctx.emitToSession('', 'chat.message', null);
        } catch (error) {
          errors.push(error instanceof Error ? error.message : String(error));
        }
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    const outcome = await dispatcher.dispatch('invalid', null);

    expect(errors).toEqual([
      'emit requires a non-empty event name',
      'emitToSession requires a non-empty session id',
    ]);
    expect(outcome).not.toHaveProperty('events');
  });

  test('caps the buffered events per request and lets the handler catch the overflow', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let overflow = '';

    dispatcher.register(
      'spam',
      (ctx) => {
        for (let index = 0; index < 1025; index += 1) {
          try {
            ctx.emit('tick', index);
          } catch (error) {
            overflow = error instanceof Error ? error.message : String(error);
          }
        }
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    const outcome = await dispatcher.dispatch('spam', null, {
      connectionId: 'conn-1',
    });

    expect(overflow).toMatch(/at most 1024 events/);
    expect(eventsOf(outcome)).toHaveLength(1024);
  });

  test('overflow fails the request when the handler does not catch it', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });

    dispatcher.register(
      'spam',
      (ctx) => {
        for (let index = 0; index <= 1024; index += 1) {
          ctx.emit('tick', index);
        }
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    const error = await rejectionOf(dispatcher.dispatch('spam', null));

    expect(error.message).toMatch(/at most 1024 events/);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('honours a configured maxEventsPerRequest across both emit paths and requests', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({
      backend,
      timeoutMs: 1000,
      maxEventsPerRequest: 2,
    });
    let overflow = '';

    dispatcher.register(
      'two',
      (ctx) => {
        ctx.emit('first', 1);
        ctx.emitToSession('session-2', 'second', 2);
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );
    dispatcher.register(
      'three',
      (ctx) => {
        for (let index = 0; index < 3; index += 1) {
          try {
            ctx.emit('tick', index);
          } catch (error) {
            overflow = error instanceof Error ? error.message : String(error);
          }
        }
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    const first = await dispatcher.dispatch('two', null, {
      connectionId: 'conn-1',
    });
    // A second request starts from zero even though the first filled the cap.
    const second = await dispatcher.dispatch('two', null, {
      connectionId: 'conn-1',
    });
    const caught = await dispatcher.dispatch('three', null, {
      connectionId: 'conn-1',
    });

    expect(eventsOf(first)).toHaveLength(2);
    expect(eventsOf(second)).toHaveLength(2);
    expect(overflow).toBe('a request may emit at most 2 events');
    expect(eventsOf(caught)).toHaveLength(2);
  });

  test('fails a request that exceeds a configured maxEventsPerRequest', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({
      backend,
      timeoutMs: 1000,
      maxEventsPerRequest: 2,
    });

    dispatcher.register(
      'three',
      (ctx) => {
        ctx.emit('one', 1);
        ctx.emit('two', 2);
        ctx.emit('three', 3);
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('three', null, { connectionId: 'conn-1' }),
    );

    expect(error.message).toBe('a request may emit at most 2 events');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });
});
