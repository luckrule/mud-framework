import { describe, expect, test } from 'bun:test';

import { isRecord, parseFrame } from '../test-support/common.ts';
import { harness } from '../test-support/connection-layer.ts';

describe('ConnectionLayer bounded input queue (SPEC 3.5)', () => {
  test('refuses a request with FRAMEWORK_QUEUE_FULL instead of executing it', async () => {
    const { transport, dispatcher } = await harness({
      maxQueuedPerConnection: 2,
      timeoutMs: 5000,
    });
    const gate = Promise.withResolvers<void>();
    const calls: string[] = [];
    dispatcher.register('work', async (_ctx, payload) => {
      calls.push(String(payload));
      await gate.promise;
      return payload;
    });
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
    dispatcher.register('work', async (_ctx, payload) => {
      await gate.promise;
      return payload;
    });
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
    dispatcher.register('work', async (_ctx, payload) => {
      await gate.promise;
      return payload;
    });
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
    dispatcher.register('work', async () => {
      await gate.promise;
      return null;
    });
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
    dispatcher.register('work', async (_ctx, payload) => {
      await gate.promise;
      return payload;
    });
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
