import { describe, expect, test } from 'bun:test';

import { isRecord, parseFrame, sleep } from '../testing/common.ts';
import { harness, sessionHarness } from '../testing/connection-layer.ts';

describe('ConnectionLayer bounded input queue (SPEC 3.5)', () => {
  test('malformed frames consume the same bounded backlog without changing reply order or session binding', async () => {
    const { transport, dispatcher, layer } = await harness({
      maxQueuedPerConnection: 1,
      timeoutMs: 5000,
    });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    dispatcher.register(
      'bind',
      async (ctx) => {
        ctx.bindSession('s1');
        entered.resolve();
        await release.promise;
        return 'bound';
      },
      { access: 'public', mayChangeSession: true },
    );
    dispatcher.register('identity', async (ctx) => ctx.sessionId, {
      access: 'public',
    });
    transport.connect('c1');
    const slow = transport.receive('c1', '{"id":"slow","method":"bind"}');
    await entered.promise;
    const accepted = transport.receive('c1', 'not json');
    const refused = transport.receive(
      'c1',
      '{"method":"bind","sessionId":"other"}',
    );

    try {
      await sleep(0);
      expect(transport.sentTo('c1').map(parseFrame)).toEqual([
        {
          id: '',
          ok: false,
          error: {
            code: 'FRAMEWORK_QUEUE_FULL',
            message: 'input queue is full (1 waiting)',
          },
        },
      ]);
      release.resolve();
      await Promise.all([slow, accepted, refused]);
      await transport.receive('c1', '{"id":"next","method":"identity"}');
      expect(transport.sentTo('c1').map(parseFrame)).toMatchObject([
        { id: '', ok: false, error: { code: 'FRAMEWORK_QUEUE_FULL' } },
        { id: 'slow', ok: true, result: 'bound' },
        { id: '', ok: false, error: { code: 'INVALID_ENVELOPE' } },
        { id: 'next', ok: true, result: 's1' },
      ]);
    } finally {
      release.resolve();
      await Promise.all([slow, accepted, refused]);
      await layer.stop();
    }
  });

  test('allocates a request id when the input takes its queue seat, not when it starts', async () => {
    const created: string[] = [];
    const { transport, layer, controlled } = sessionHarness({
      maxQueuedPerConnection: 1,
      createRequestId: () => {
        const id = `req-${created.length + 1}`;
        created.push(id);
        return id;
      },
    });
    await layer.start();
    transport.connect('c1');

    // A malformed frame occupies a seat but is not a Request, so it gets no id.
    const bad = transport.receive('c1', 'not json');
    expect(created).toEqual([]);
    await bad;
    expect(created).toEqual([]);

    const first = transport.receive('c1', '{"id":"r0","method":"next"}');
    // SPEC 7.3: minted on entry, before the request waits for its turn.
    expect(created).toEqual(['req-1']);
    await first;
    expect(controlled.requests[0]?.requestId).toBe('req-1');
    expect(created).toEqual(['req-1']);
    await layer.stop();
  });

  test('a queue-full refusal carries the request id minted on entry', async () => {
    const created: string[] = [];
    const { transport, layer, controlled, logs } = sessionHarness({
      maxQueuedPerConnection: 1,
      createRequestId: () => {
        const id = `req-${created.length + 1}`;
        created.push(id);
        return id;
      },
    });
    await layer.start();
    transport.connect('c1');

    const first = transport.receive('c1', '{"id":"r0","method":"hang"}');
    await sleep(0);
    const second = transport.receive('c1', '{"id":"r1","method":"next"}');
    const refused = transport.receive('c1', '{"id":"r2","method":"next"}');
    await refused;

    const rejected = logs().filter(
      (entry) => entry.code === 'FRAMEWORK_QUEUE_FULL',
    );
    expect(rejected).toHaveLength(1);
    expect(rejected[0]).toMatchObject({
      connectionId: 'c1',
      clientRequestId: 'r2',
      requestId: 'req-3',
    });

    controlled.release();
    await Promise.all([first, second]);
    await layer.stop();
  });

  test('refuses a request with FRAMEWORK_QUEUE_FULL instead of executing it', async () => {
    const { transport, dispatcher } = await harness({
      maxQueuedPerConnection: 2,
      timeoutMs: 5000,
    });
    const gate = Promise.withResolvers<void>();
    const calls: string[] = [];
    dispatcher.register(
      'work',
      async (_ctx, payload) => {
        calls.push(String(payload));
        await gate.promise;
        return payload;
      },
      { access: 'public' },
    );
    transport.connect('c1');

    const pending = [
      transport.receive('c1', '{"id":"r0","method":"work","payload":"a"}'),
      transport.receive('c1', '{"id":"r1","method":"work","payload":"b"}'),
      transport.receive('c1', '{"id":"r2","method":"work","payload":"c"}'),
    ];
    gate.resolve();
    await Promise.all(pending);

    const frames = transport.sentTo('c1').map(parseFrame);
    const refused = frames.find((frame) => frame.id === 'r2');
    expect(refused?.ok).toBe(false);
    expect(refused?.error).toMatchObject({ code: 'FRAMEWORK_QUEUE_FULL' });
    expect(refused?.result).toBeUndefined();
    // The refused request never reached the handler.
    expect(calls).toEqual(['a', 'b']);
  });

  test('still executes the requests that were already queued', async () => {
    const { transport, dispatcher } = await harness({
      maxQueuedPerConnection: 2,
      timeoutMs: 5000,
    });
    const gate = Promise.withResolvers<void>();
    dispatcher.register(
      'work',
      async (_ctx, payload) => {
        await gate.promise;
        return payload;
      },
      { access: 'public' },
    );
    transport.connect('c1');

    const pending = [
      transport.receive('c1', '{"id":"r0","method":"work","payload":"a"}'),
      transport.receive('c1', '{"id":"r1","method":"work","payload":"b"}'),
      transport.receive('c1', '{"id":"r2","method":"work","payload":"c"}'),
    ];
    gate.resolve();
    await Promise.all(pending);

    const frames = transport.sentTo('c1').map(parseFrame);
    expect(frames.find((frame) => frame.id === 'r0')).toEqual({
      id: 'r0',
      ok: true,
      result: 'a',
    });
    expect(frames.find((frame) => frame.id === 'r1')).toEqual({
      id: 'r1',
      ok: true,
      result: 'b',
    });
  });

  test('accepts new requests once the queue drains', async () => {
    const { transport, dispatcher } = await harness({
      maxQueuedPerConnection: 1,
      timeoutMs: 5000,
    });
    const gate = Promise.withResolvers<void>();
    dispatcher.register(
      'work',
      async (_ctx, payload) => {
        await gate.promise;
        return payload;
      },
      { access: 'public' },
    );
    transport.connect('c1');

    const first = transport.receive(
      'c1',
      '{"id":"r0","method":"work","payload":"a"}',
    );
    const refused = transport.receive(
      'c1',
      '{"id":"r1","method":"work","payload":"b"}',
    );
    gate.resolve();
    await Promise.all([first, refused]);

    // The backlog is empty again, so the same connection can take work.
    await transport.receive('c1', '{"id":"r2","method":"work","payload":"c"}');

    const frames = transport.sentTo('c1').map(parseFrame);
    expect(frames.find((frame) => frame.id === 'r1')?.ok).toBe(false);
    expect(frames.find((frame) => frame.id === 'r1')?.error).toMatchObject({
      code: 'FRAMEWORK_QUEUE_FULL',
    });
    expect(frames.find((frame) => frame.id === 'r2')).toEqual({
      id: 'r2',
      ok: true,
      result: 'c',
    });
  });

  test('logs a queue rejection at warn with the queue depth', async () => {
    const { transport, dispatcher, logs } = await harness({
      maxQueuedPerConnection: 2,
      timeoutMs: 5000,
    });
    const gate = Promise.withResolvers<void>();
    dispatcher.register(
      'work',
      async () => {
        await gate.promise;
        return null;
      },
      { access: 'public' },
    );
    transport.connect('c1');

    const pending = [
      transport.receive('c1', '{"id":"r0","method":"work"}'),
      transport.receive('c1', '{"id":"r1","method":"work"}'),
      transport.receive('c1', '{"id":"r2","method":"work"}'),
    ];
    gate.resolve();
    await Promise.all(pending);

    const rejected = logs().filter(
      (entry) => entry.code === 'FRAMEWORK_QUEUE_FULL',
    );
    expect(rejected).toHaveLength(1);
    expect(rejected[0]).toMatchObject({
      level: 'warn',
      connectionId: 'c1',
      clientRequestId: 'r2',
      queued: 2,
      limit: 2,
    });
  });

  test('applies a default limit when no option is passed', async () => {
    const { transport, dispatcher, logs } = await harness({
      timeoutMs: 5000,
    });
    const gate = Promise.withResolvers<void>();
    dispatcher.register(
      'work',
      async (_ctx, payload) => {
        await gate.promise;
        return payload;
      },
      { access: 'public' },
    );
    transport.connect('c1');

    // One executing plus a thousand waiting fills the documented default;
    // the requests after that must be refused, not queued without bound.
    const pending: Promise<void>[] = [];
    for (let index = 0; index < 1002; index += 1) {
      pending.push(
        transport.receive('c1', `{"id":"r${index}","method":"work"}`),
      );
    }
    gate.resolve();
    await Promise.all(pending);

    const frames = transport.sentTo('c1').map(parseFrame);
    const refused = frames.filter((frame) => {
      const error = frame.error;
      return isRecord(error) && error.code === 'FRAMEWORK_QUEUE_FULL';
    });
    expect(refused).toHaveLength(2);
    for (const frame of refused) {
      expect(frame.ok).toBe(false);
    }
    // Everything else got a normal reply rather than silence.
    expect(frames.filter((frame) => frame.ok === true)).toHaveLength(1000);
    expect(
      logs().filter((entry) => entry.code === 'FRAMEWORK_QUEUE_FULL'),
    ).toHaveLength(2);
  });
});
