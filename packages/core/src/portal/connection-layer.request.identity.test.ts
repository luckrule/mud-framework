import { describe, expect, test } from 'bun:test';

import { harness, lastFrame } from '../testing/connection-layer.ts';

describe('ConnectionLayer request', () => {
  test('gives the handler the connection and client request ids', async () => {
    const { transport, dispatcher } = await harness();
    let seen: Record<string, unknown> = {};
    dispatcher.register(
      'inspect',
      async (ctx) => {
        seen = {
          requestId: ctx.requestId,
          connectionId: ctx.connectionId,
          clientRequestId: ctx.clientRequestId,
          sessionId: ctx.sessionId,
        };
        return null;
      },
      { access: 'public' },
    );
    transport.connect('c1');

    await transport.receive('c1', '{"id":"client-7","method":"inspect"}');

    expect(seen.connectionId).toBe('c1');
    expect(seen.clientRequestId).toBe('client-7');
    expect(typeof seen.requestId).toBe('string');
    expect(String(seen.requestId).length).toBeGreaterThan(0);
    expect(seen.sessionId).toBeUndefined();
    // The wire reply still carries the client's id, not the framework's.
    expect(lastFrame(transport, 'c1')).toEqual({
      id: 'client-7',
      ok: true,
      result: null,
    });
  });

  test('uses the injected id factory and logs the same id everywhere', async () => {
    let counter = 0;
    const { transport, dispatcher, logs } = await harness({
      createRequestId: () => {
        counter += 1;
        return `req-${counter}`;
      },
    });
    dispatcher.register(
      'explode',
      async () => {
        throw new Error('boom');
      },
      { access: 'public' },
    );
    transport.connect('c1');

    await transport.receive('c1', '{"id":"client-1","method":"explode"}');

    expect(lastFrame(transport, 'c1')).toMatchObject({
      id: 'client-1',
      ok: false,
      error: { code: 'INTERNAL_ERROR' },
    });
    const failed = logs().filter((entry) => entry.code === 'INTERNAL_ERROR');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      level: 'error',
      requestId: 'req-1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      method: 'explode',
    });
    expect(typeof failed[0]?.durationMs).toBe('number');
  });
});
