import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { quietDispatcher } from '../testing/dispatcher.ts';
import { Dispatcher } from './dispatcher.ts';

describe('Dispatcher.identity', () => {
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
});
