import { describe, expect, test } from 'bun:test';
import { Buffer } from 'node:buffer';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { FakeTransport } from '../transport/fake.ts';
import type { Codec, Envelope, ResponseEnvelope } from './codec.ts';
import { JsonCodec } from './codec.ts';
import { ConnectionLayer } from './connection-layer.ts';
import { Dispatcher } from './dispatcher.ts';

const sleep = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function parseFrame(raw: string | Uint8Array): Record<string, unknown> {
  const text = typeof raw === 'string' ? raw : new TextDecoder().decode(raw);
  const parsed: unknown = JSON.parse(text);
  if (!isRecord(parsed)) {
    throw new Error(`frame is not a JSON object: ${text}`);
  }
  return parsed;
}

function lastFrame(
  transport: FakeTransport,
  connectionId: string,
): Record<string, unknown> {
  const frame = transport.sentTo(connectionId).at(-1);
  if (frame === undefined) {
    throw new Error(`no frame was sent to "${connectionId}"`);
  }
  return parseFrame(frame);
}

async function rejectionOf(promise: Promise<unknown>): Promise<Error> {
  try {
    await promise;
  } catch (error) {
    if (error instanceof Error) {
      return error;
    }
    throw new Error(`non-Error rejection: ${String(error)}`, { cause: error });
  }
  throw new Error('expected the promise to reject');
}

interface Harness {
  transport: FakeTransport;
  backend: MemoryStorageBackend;
  dispatcher: Dispatcher;
  layer: ConnectionLayer;
  /** Parsed log lines, in emission order. */
  logs: () => Record<string, unknown>[];
}

async function harness(
  options: {
    timeoutMs?: number;
    codec?: Codec;
    createRequestId?: () => string;
  } = {},
): Promise<Harness> {
  const transport = new FakeTransport();
  const backend = new MemoryStorageBackend();
  const lines: string[] = [];
  const logger: Logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  const dispatcher = new Dispatcher({
    backend,
    timeoutMs: options.timeoutMs ?? 1000,
    loggerFactory: {
      create: (loggerOptions) => logger.child(loggerOptions?.bindings ?? {}),
    },
    createRequestId: options.createRequestId,
  });
  const layer = new ConnectionLayer({
    transport,
    dispatcher,
    codec: options.codec,
    logger,
    createRequestId: options.createRequestId,
  });
  await layer.start();
  return {
    transport,
    backend,
    dispatcher,
    layer,
    logs: () => lines.map(parseFrame),
  };
}

/** Base64 frames: proves the codec is the only wire-format decision point. */
class Base64Codec implements Codec {
  encode(value: ResponseEnvelope): string {
    return Buffer.from(JSON.stringify(value), 'utf8').toString('base64');
  }

  decode(raw: string | Uint8Array): Envelope {
    const text =
      typeof raw === 'string' ? raw : Buffer.from(raw).toString('utf8');
    const parsed: unknown = JSON.parse(
      Buffer.from(text, 'base64').toString('utf8'),
    );
    if (
      !isRecord(parsed) ||
      typeof parsed.id !== 'string' ||
      typeof parsed.method !== 'string'
    ) {
      throw new Error('malformed base64 envelope');
    }
    return { id: parsed.id, method: parsed.method, payload: parsed.payload };
  }
}

describe('ConnectionLayer', () => {
  test('dispatches a request and replies with the handler result', async () => {
    const { transport, backend, dispatcher } = await harness();
    dispatcher.register('echo', async (ctx, payload) => {
      await ctx.storage.execute('INSERT 1', [payload]);
      return payload;
    });
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"echo","payload":"hi"}');

    expect(lastFrame(transport, 'c1')).toEqual({ id: 'r1', result: 'hi' });
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
  });

  test('processes requests on one connection strictly in order', async () => {
    const { transport, dispatcher } = await harness();
    const log: string[] = [];
    dispatcher.register('work', async (_ctx, payload) => {
      const label = String(payload);
      log.push(`${label}:start`);
      await sleep(25);
      log.push(`${label}:end`);
      return label;
    });
    transport.connect('c1');

    const first = transport.receive(
      'c1',
      '{"id":"1","method":"work","payload":"a"}',
    );
    const second = transport.receive(
      'c1',
      '{"id":"2","method":"work","payload":"b"}',
    );
    await Promise.all([first, second]);

    // The second request must not even start before the first has finished:
    // its writes have to be visible to it (SPEC 3.1).
    expect(log).toEqual(['a:start', 'a:end', 'b:start', 'b:end']);
    expect(transport.sentTo('c1').map(parseFrame)).toEqual([
      { id: '1', result: 'a' },
      { id: '2', result: 'b' },
    ]);
  });

  test('processes requests on different connections concurrently', async () => {
    const { transport, dispatcher } = await harness({ timeoutMs: 2000 });
    const barrier = Promise.withResolvers<void>();
    let active = 0;
    dispatcher.register('barrier', async () => {
      active += 1;
      if (active === 2) {
        barrier.resolve();
      }
      // Deadlocks (then times out) if the layer serialized across connections.
      await barrier.promise;
      return active;
    });
    transport.connect('c1');
    transport.connect('c2');

    await Promise.all([
      transport.receive('c1', '{"id":"1","method":"barrier"}'),
      transport.receive('c2', '{"id":"2","method":"barrier"}'),
    ]);

    expect(active).toBe(2);
    expect(lastFrame(transport, 'c1')).toEqual({ id: '1', result: 2 });
    expect(lastFrame(transport, 'c2')).toEqual({ id: '2', result: 2 });
  });

  test('maps a handler failure to INTERNAL_ERROR', async () => {
    const { transport, dispatcher } = await harness();
    dispatcher.register('explode', async () => {
      throw new Error('business failure');
    });
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"explode"}');

    expect(lastFrame(transport, 'c1')).toEqual({
      id: 'r1',
      error: { code: 'INTERNAL_ERROR', message: 'business failure' },
    });
  });

  test('maps an unregistered method to UNKNOWN_METHOD', async () => {
    const { transport } = await harness();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"missing"}');

    const frame = lastFrame(transport, 'c1');
    expect(frame.id).toBe('r1');
    expect(frame.error).toMatchObject({ code: 'UNKNOWN_METHOD' });
    expect(frame.result).toBeUndefined();
  });

  test('maps a timeout to FRAMEWORK_TIMEOUT', async () => {
    const { transport, dispatcher } = await harness({ timeoutMs: 15 });
    dispatcher.register('slow', async () => {
      await sleep(60);
      return 'late';
    });
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"slow"}');

    const frame = lastFrame(transport, 'c1');
    expect(frame.id).toBe('r1');
    expect(frame.error).toMatchObject({ code: 'FRAMEWORK_TIMEOUT' });
    expect(frame.result).toBeUndefined();
    // Let the abandoned handler settle before the next test starts.
    await sleep(60);
  });

  test('answers invalid JSON with INVALID_ENVELOPE and an empty id', async () => {
    const { transport } = await harness();
    transport.connect('c1');

    await transport.receive('c1', 'not json');

    const frame = lastFrame(transport, 'c1');
    expect(frame.id).toBe('');
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
      expect(frame.error).toMatchObject({ code: 'INVALID_ENVELOPE' });
    }
  });

  test('records a send failure after the connection closed', async () => {
    const { transport, dispatcher, logs } = await harness();
    const gate = Promise.withResolvers<void>();
    dispatcher.register('pending', async () => {
      await gate.promise;
      return 'done';
    });
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"pending"}');
    await transport.disconnect('c1', 'client left');
    gate.resolve();
    await inFlight;

    expect(transport.sentTo('c1')).toHaveLength(0);
    const failures = logs().filter(
      (entry) => entry.code === 'TRANSPORT_SEND_FAILED',
    );
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({ level: 'error', connectionId: 'c1' });
  });

  test('gives the handler the connection and client request ids', async () => {
    const { transport, dispatcher } = await harness();
    let seen: Record<string, unknown> = {};
    dispatcher.register('inspect', async (ctx) => {
      seen = {
        requestId: ctx.requestId,
        connectionId: ctx.connectionId,
        clientRequestId: ctx.clientRequestId,
        sessionId: ctx.sessionId,
      };
      return null;
    });
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
    dispatcher.register('explode', async () => {
      throw new Error('boom');
    });
    transport.connect('c1');

    await transport.receive('c1', '{"id":"client-1","method":"explode"}');

    expect(lastFrame(transport, 'c1')).toMatchObject({
      id: 'client-1',
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

  test('ignores messages for a connection it does not track', async () => {
    const { transport, dispatcher, layer } = await harness();
    dispatcher.register('echo', async (_ctx, payload) => payload);
    const connection = transport.connect('c1');
    await transport.disconnect('c1');

    await layer.onMessage(connection, '{"id":"r1","method":"echo"}');

    expect(transport.sentTo('c1')).toHaveLength(0);
  });

  test('stop closes the transport and the dispatcher', async () => {
    const { transport, dispatcher, layer } = await harness();
    dispatcher.register('echo', async (_ctx, payload) => payload);
    transport.connect('c1');

    await layer.stop();

    expect(() => transport.connect('c2')).toThrow(/stopped/);
    const error = await rejectionOf(dispatcher.dispatch('echo', null));
    expect(error.message).toMatch(/closed/);
    // A second stop is a no-op rather than a transport lifecycle error.
    await layer.stop();
  });

  test('refuses to start twice', async () => {
    const { layer } = await harness();

    const error = await rejectionOf(layer.start());

    expect(error.message).toMatch(/cannot start while started/);
  });

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
    expect(parseFrame(decoded)).toEqual({ id: 'r1', result: 42 });
  });

  test('JSON codec rejects a non-object envelope', () => {
    const codec = new JsonCodec();

    expect(() => codec.decode('"hello"')).toThrow(/JSON object/);
    expect(() => codec.decode('[1,2]')).toThrow(/JSON object/);
  });

  test('JSON codec omits result when an error is present', () => {
    const codec = new JsonCodec();

    const text = codec.encode({ id: 'r1', error: { code: 'X', message: 'y' } });

    expect(parseFrame(text)).toEqual({
      id: 'r1',
      error: { code: 'X', message: 'y' },
    });
  });
});
