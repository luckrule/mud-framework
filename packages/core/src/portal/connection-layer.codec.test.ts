import { describe, expect, test } from 'bun:test';
import { Buffer } from 'node:buffer';

import { JsonCodec } from '../client/codec.ts';
import { parseFrame } from '../testing/common.ts';
import {
  Base64Codec,
  harness,
  lastFrame,
} from '../testing/connection-layer.ts';

describe('ConnectionLayer codec', () => {
  test('uses an injected codec instead of JSON', async () => {
    const { transport, dispatcher } = await harness({
      codec: new Base64Codec(),
    });
    dispatcher.register('echo', async (_ctx, payload) => payload);
    transport.connect('c1');

    const request = Buffer.from(
      '{"id":"r1","method":"echo","payload":42}',
      'utf8',
    ).toString('base64');
    await transport.receive('c1', request);

    const reply = transport.sentTo('c1').at(-1);
    if (reply === undefined) {
      throw new Error('expected a reply');
    }
    const decoded = Buffer.from(String(reply), 'base64').toString('utf8');
    expect(parseFrame(decoded)).toEqual({ id: 'r1', ok: true, result: 42 });
  });

  test('JSON codec rejects a non-object envelope', () => {
    const codec = new JsonCodec();

    expect(() => codec.decode('"hello"')).toThrow(/JSON object/);
    expect(() => codec.decode('[1,2]')).toThrow(/JSON object/);
  });

  test('JSON codec keeps a success with no value distinguishable', () => {
    const codec = new JsonCodec();

    const text = codec.encode({ id: 'r1', ok: true, result: undefined });

    // `ok:true` is what makes this different from "nothing was written".
    expect(parseFrame(text)).toEqual({ id: 'r1', ok: true });
  });

  test('JSON codec writes the error branch without a result field', () => {
    const codec = new JsonCodec();

    const text = codec.encode({
      id: 'r1',
      ok: false,
      error: { code: 'X', message: 'y' },
    });

    expect(parseFrame(text)).toEqual({
      id: 'r1',
      ok: false,
      error: { code: 'X', message: 'y' },
    });
  });

  test('answers invalid JSON with INVALID_ENVELOPE and an empty id', async () => {
    const { transport } = await harness();
    transport.connect('c1');

    await transport.receive('c1', 'not json');

    const frame = lastFrame(transport, 'c1');
    expect(frame.id).toBe('');
    expect(frame.ok).toBe(false);
    expect(frame.error).toMatchObject({ code: 'INVALID_ENVELOPE' });
  });

  test('answers a missing id or method with INVALID_ENVELOPE', async () => {
    const { transport } = await harness();
    transport.connect('c1');

    await transport.receive('c1', '{"method":"echo"}');
    await transport.receive('c1', '{"id":"r2"}');
    await transport.receive('c1', '{"id":"","method":"echo"}');

    const frames = transport.sentTo('c1').map(parseFrame);
    expect(frames).toHaveLength(3);
    for (const frame of frames) {
      expect(frame.id).toBe('');
      expect(frame.ok).toBe(false);
      expect(frame.error).toMatchObject({ code: 'INVALID_ENVELOPE' });
    }
  });

  test('logs a rejected envelope at warn with its length', async () => {
    const { transport, logs } = await harness();
    transport.connect('c1');

    await transport.receive('c1', 'not json');

    const rejected = logs().filter(
      (entry) => entry.code === 'INVALID_ENVELOPE',
    );
    expect(rejected).toHaveLength(1);
    expect(rejected[0]).toMatchObject({
      level: 'warn',
      connectionId: 'c1',
      rawLength: 8,
    });
  });
});
