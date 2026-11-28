import { describe, expect, test } from 'bun:test';

import { isRecord, parseFrame, sleep } from '../testing/common.ts';
import { harness, sessionHarness } from '../testing/connection-layer.ts';

describe('ConnectionLayer bounded input queue (SPEC 3.5)', () => {
  test('malformed frames consume the same bounded backlog without changing reply order or session binding', async () => {
    const { transport, dispatcher, layer, logs } = await harness({
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
      // The refused frame had no readable id, so the log must not invent an
      // empty `clientRequestId`.
      const refusals = logs().filter(
        (entry) => entry.code === 'FRAMEWORK_QUEUE_FULL',
      );
      expect(refusals).toHaveLength(1);
      expect(refusals[0]).not.toHaveProperty('clientRequestId');
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

  test('a malformed frame releases its seat after the rejection is written', async () => {
    // The malformed path takes a seat without minting an identity, so its
    // release has no request to hang it on. If the seat leaked, the cap would
    // tighten by one on every bad frame and a healthy connection would start
    // refusing real requests.
    const { transport, dispatcher, layer } = await harness({
      maxQueuedPerConnection: 1,
      timeoutMs: 5000,
    });
    dispatcher.register('work', async () => null, { access: 'public' });
    transport.connect('c1');

    for (let index = 0; index < 5; index += 1) {
      await transport.receive('c1', 'not json');
    }
    // The seat is free again, so one request may still wait behind the running
    // one instead of being refused at a cap that leaked down to zero.
    const gate = Promise.withResolvers<void>();
    dispatcher.register(
      'hold',
      async () => {
        await gate.promise;
        return null;
      },
      { access: 'public' },
    );
    const running = transport.receive('c1', '{"id":"a","method":"hold"}');
    await sleep(0);
    const waiting = transport.receive('c1', '{"id":"b","method":"work"}');
    gate.resolve();
    await Promise.all([running, waiting]);

    const frames = transport.sentTo('c1').map(parseFrame);
    expect(
      frames.filter(
        (frame) =>
          isRecord(frame.error) && frame.error.code === 'FRAMEWORK_QUEUE_FULL',
      ),
    ).toEqual([]);
    await layer.stop();
  });

  test('a dropped malformed frame releases its seat at shutdown', async () => {
    // Shutdown discards queued work without running it, so the seat is released
    // by the drop hook instead of the work itself. A double release here would
    // make the counter negative and silently widen the bound.
    const h = sessionHarness({ shutdownTimeoutMs: 20 });
    await h.layer.start();
    h.transport.connect('c1');
    const state = (
      h.layer as unknown as {
        connections: Map<string, { queued: number }>;
      }
    ).connections.get('c1');

    const held = h.transport.holdSends('c1');
    h.controlled.emit({
      eventId: 'e:0',
      event: 'note',
      payload: 'x',
      target: { kind: 'connection', connectionId: 'c1' },
    });
    await held.entered;
    const bad = h.transport.receive('c1', 'not json');
    await sleep(0);
    expect(state?.queued).toBe(1);

    const stopping = h.layer.stop();
    await sleep(40);
    expect(state?.queued).toBe(0);
    held.release();
    await stopping;
    await bad;

    // Released once, not twice: the count is back to its starting value.
    expect(state?.queued).toBe(0);
    expect(
      h.logs().filter((entry) => entry.code === 'REQUEST_DROPPED_AT_SHUTDOWN'),
    ).toEqual([]);
  });

  test('a request abandoned at the Session barrier releases its seat', async () => {
    // The wait can end without the work running: the connection closes while
    // its request is parked behind another connection's unfinished Session
    // work. The seat is released on that path instead of inside the work.
    const h = sessionHarness({});
    await h.layer.start();
    h.transport.connect('c1');
    h.transport.connect('c2');
    const c2 = (
      h.layer as unknown as {
        connections: Map<string, { queued: number }>;
      }
    ).connections.get('c2');

    // c1 binds s1 and holds its barrier with a request that never finishes.
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');
    const holding = h.transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);
    // c2 takes s1 over; its next request must wait for c1's unfinished work.
    await h.transport.receive('c2', '{"id":"3","method":"bind"}');
    const waiting = h.transport.receive('c2', '{"id":"4","method":"next"}');
    await sleep(0);
    expect(c2?.queued).toBe(1);

    await h.transport.disconnect('c2', 'client left');
    h.controlled.release();
    await Promise.all([holding, waiting]);
    await sleep(0);

    expect(c2?.queued).toBe(0);
    expect(h.controlled.started).not.toContain('next');
    expect(
      h.logs().filter((entry) => entry.msg === 'request dropped before start'),
    ).toHaveLength(1);
    await h.layer.stop();
  });
});
