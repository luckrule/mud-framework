import { describe, expect, test } from 'bun:test';

import { CLIENT_REQUEST_ID_EMPTY_MESSAGE } from '../shared/client-request-id.ts';
import { rejectionOf } from '../testing/common.ts';
import { quietDispatcher } from '../testing/dispatcher.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { Dispatcher } from './dispatcher.ts';

describe('Dispatcher.identity', () => {
  test('generates a request id with the default factory', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    let seen = '';
    dispatcher.register(
      'inspect',
      async (ctx) => {
        seen = ctx.requestId;
        return null;
      },
      { access: 'public' },
    );

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
    dispatcher.register(
      'inspect',
      async (ctx) => {
        seen = ctx.requestId;
        return null;
      },
      { access: 'public' },
    );

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
    dispatcher.register(
      'inspect',
      async (ctx) => {
        seen.push(ctx.requestId);
        return null;
      },
      { access: 'public' },
    );

    await dispatcher.dispatch('inspect', null, { requestId: 'original-id' });
    await dispatcher.dispatch('inspect', null);

    expect(seen).toEqual(['original-id', 'fresh-id']);
  });

  test('carries connection, client request and session ids into the context', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let seen: Record<string, unknown> = {};
    dispatcher.register(
      'inspect',
      async (ctx) => {
        seen = {
          connectionId: ctx.connectionId,
          clientRequestId: ctx.clientRequestId,
          sessionId: ctx.sessionId,
        };
        return null;
      },
      { access: 'public' },
    );

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

  test('leaves the client request id absent when the caller supplied none', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let seen: Record<string, unknown> = {};
    let hasClientRequestId = true;
    dispatcher.register(
      'inspect',
      async (ctx) => {
        // Type-level: the handler sees the optional string, never a sentinel.
        const clientRequestId: string | undefined = ctx.clientRequestId;
        hasClientRequestId = Object.hasOwn(ctx, 'clientRequestId');
        seen = {
          connectionId: ctx.connectionId,
          clientRequestId,
          sessionId: ctx.sessionId,
        };
        return null;
      },
      { access: 'public' },
    );

    await dispatcher.dispatch('inspect', null);

    expect(hasClientRequestId).toBe(false);
    expect(seen).toEqual({
      connectionId: '',
      clientRequestId: undefined,
      sessionId: undefined,
    });
  });

  test('rejects an explicitly empty client request id', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let ran = false;
    dispatcher.register(
      'inspect',
      async () => {
        ran = true;
        return null;
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('inspect', null, { clientRequestId: '' }),
    );
    expect(error.message).toBe(CLIENT_REQUEST_ID_EMPTY_MESSAGE);
    expect(ran).toBe(false);
  });

  test('binds the request ids onto ctx.logger', async () => {
    const backend = new MemoryStorageBackend();
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 }, lines);
    dispatcher.register(
      'inspect',
      async (ctx) => {
        ctx.logger.info({ detail: 'inside handler' }, 'handler ran');
        return null;
      },
      { access: 'public' },
    );

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

  test('omits clientRequestId from the log bindings when absent', async () => {
    const backend = new MemoryStorageBackend();
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 }, lines);
    dispatcher.register(
      'inspect',
      async (ctx) => {
        ctx.logger.info({ detail: 'inside handler' }, 'handler ran');
        return null;
      },
      { access: 'public' },
    );

    await dispatcher.dispatch('inspect', null, { requestId: 'req-9' });

    const line = JSON.parse(lines[0] ?? '');
    expect(line).toMatchObject({
      msg: 'handler ran',
      requestId: 'req-9',
      detail: 'inside handler',
    });
    expect(line).not.toHaveProperty('clientRequestId');
  });
});
