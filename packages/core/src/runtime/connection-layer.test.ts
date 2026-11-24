import { describe, expect, test } from 'bun:test';
import { Buffer } from 'node:buffer';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger, LogLevel } from '../logging/types.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { FakeTransport } from '../transport/fake.ts';
import type { Transport, TransportConnection } from '../transport/types.ts';
import type { Codec, Envelope, ResponseEnvelope } from './codec.ts';
import { JsonCodec } from './codec.ts';
import { ConnectionLayer } from './connection-layer.ts';
import { Dispatcher } from './dispatcher.ts';
import { LocalExecutor } from './executor.ts';
import type { ExecutorOutcome, RequestExecutor } from './executor.ts';

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
    /** Defaults to the logger's own default (`info`). */
    level?: LogLevel;
    maxQueuedPerConnection?: number;
    shutdownTimeoutMs?: number;
    maxSessions?: number;
    sessionHandoffTimeoutMs?: number;
  } = {},
): Promise<Harness> {
  const transport = new FakeTransport();
  const backend = new MemoryStorageBackend();
  const lines: string[] = [];
  const logger: Logger = createJsonLogger({
    level: options.level,
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
    executor: new LocalExecutor({ dispatcher }),
    codec: options.codec,
    logger,
    createRequestId: options.createRequestId,
    maxQueuedPerConnection: options.maxQueuedPerConnection,
    shutdownTimeoutMs: options.shutdownTimeoutMs,
    maxSessions: options.maxSessions,
    sessionHandoffTimeoutMs: options.sessionHandoffTimeoutMs,
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

interface ScriptedExecutor {
  executor: RequestExecutor;
  order: string[];
  closeCalls: () => number;
}

/** Records start/close calls so shutdown ordering can be asserted. */
function scriptedExecutor(order: string[]): ScriptedExecutor {
  let closeCalls = 0;
  return {
    order,
    closeCalls: () => closeCalls,
    executor: {
      start: () => {
        order.push('executor.start');
        return Promise.resolve();
      },
      execute: () => Promise.resolve({ ok: true, result: null }),
      cancel: () => Promise.resolve(),
      close: () => {
        closeCalls += 1;
        order.push('executor.close');
        return Promise.resolve();
      },
    },
  };
}

interface ScriptedTransport {
  transport: Transport;
  stopCalls: () => number;
  failStop: (error: Error) => void;
}

/** Records start/stop calls, and can be told to make `stop` reject. */
function scriptedTransport(order: string[]): ScriptedTransport {
  let stopCalls = 0;
  let stopFailure: Error | undefined;
  return {
    transport: {
      start: () => {
        order.push('transport.start');
        return Promise.resolve();
      },
      stop: () => {
        stopCalls += 1;
        order.push('transport.stop');
        return stopFailure === undefined
          ? Promise.resolve()
          : Promise.reject(stopFailure);
      },
    },
    stopCalls: () => stopCalls,
    failStop: (error) => {
      stopFailure = error;
    },
  };
}

/** A connection that records frames, so a test can read the reply. */
function fakeConnection(id: string): {
  connection: TransportConnection;
  sent: string[];
} {
  const sent: string[] = [];
  return {
    sent,
    connection: {
      id,
      send: (payload) => {
        sent.push(typeof payload === 'string' ? payload : '<binary>');
        return Promise.resolve();
      },
      close: () => Promise.resolve(),
    },
  };
}

interface ControlledExecutor {
  executor: RequestExecutor;
  /** Methods whose execution started, in order. */
  started: string[];
  /** Methods whose execution returned, in order. */
  finished: string[];
  /** Settles every `hang` call that is waiting. */
  release: () => void;
}

/**
 * An executor that reports a Session binding for `bind` (the id is chosen by
 * `sessionOf`), hangs on `hang` until released, and succeeds immediately
 * otherwise. `cancel` is deliberately a no-op: the reconnect tests need a
 * request that outlives its connection, so the layer's own bounded waits are
 * what get measured rather than the executor's.
 */
function controlledExecutor(
  options: { sessionOf?: (method: string) => string | undefined } = {},
): ControlledExecutor {
  const started: string[] = [];
  const finished: string[] = [];
  const hang = Promise.withResolvers<void>();
  return {
    started,
    finished,
    release: () => {
      hang.resolve();
    },
    executor: {
      start: () => Promise.resolve(),
      execute: async (request) => {
        started.push(request.method);
        if (request.method === 'hang') {
          await hang.promise;
        }
        finished.push(request.method);
        const sessionId = options.sessionOf?.(request.method);
        return sessionId === undefined
          ? { ok: true, result: request.method }
          : { ok: true, result: request.method, sessionId };
      },
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
    },
  };
}

interface SessionHarness {
  transport: FakeTransport;
  layer: ConnectionLayer;
  controlled: ControlledExecutor;
  logs: () => Record<string, unknown>[];
}

/** A layer whose executor binds `bind` to a Session; see `controlledExecutor`. */
function sessionHarness(
  options: {
    maxSessions?: number;
    sessionHandoffTimeoutMs?: number;
    sessionOf?: (method: string) => string | undefined;
  } = {},
): SessionHarness {
  const transport = new FakeTransport();
  const lines: string[] = [];
  const logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  const controlled = controlledExecutor({
    sessionOf:
      options.sessionOf ?? ((method) => (method === 'bind' ? 's1' : undefined)),
  });
  const layer = new ConnectionLayer({
    transport,
    executor: controlled.executor,
    logger,
    maxSessions: options.maxSessions,
    sessionHandoffTimeoutMs: options.sessionHandoffTimeoutMs,
  });
  return {
    transport,
    layer,
    controlled,
    logs: () => lines.map(parseFrame),
  };
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

    expect(lastFrame(transport, 'c1')).toEqual({
      id: 'r1',
      ok: true,
      result: 'hi',
    });
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
      { id: '1', ok: true, result: 'a' },
      { id: '2', ok: true, result: 'b' },
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
    expect(lastFrame(transport, 'c1')).toEqual({
      id: '1',
      ok: true,
      result: 2,
    });
    expect(lastFrame(transport, 'c2')).toEqual({
      id: '2',
      ok: true,
      result: 2,
    });
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
      ok: false,
      error: { code: 'INTERNAL_ERROR', message: 'business failure' },
    });
  });

  test('replies with the executor failure without throwing', async () => {
    const transport = new FakeTransport();
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      execute: () =>
        Promise.resolve({
          ok: false,
          error: { code: 'STORAGE_UNAVAILABLE', message: 'backend is gone' },
        }),
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const layer = new ConnectionLayer({ transport, executor, logger });
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"echo"}');

    expect(lastFrame(transport, 'c1')).toEqual({
      id: 'r1',
      ok: false,
      error: { code: 'STORAGE_UNAVAILABLE', message: 'backend is gone' },
    });
    const failed = lines
      .map(parseFrame)
      .filter((entry) => entry.level === 'error');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      method: 'echo',
      code: 'STORAGE_UNAVAILABLE',
      reason: 'backend is gone',
      connectionId: 'c1',
      clientRequestId: 'r1',
    });
    expect(typeof failed[0]?.durationMs).toBe('number');
    await layer.stop();
  });

  test('answers an executor outcome that is missing its error instead of dropping the request', async () => {
    const transport = new FakeTransport();
    // The regression this covers: `{ok:false}` without `error` used to produce
    // no frame at all, so the client waited forever. The type says it cannot
    // happen; an executor is outside code, so it is checked anyway.
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      execute: () => {
        // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- untyped test double
        return Promise.resolve({ ok: false } as ExecutorOutcome);
      },
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const layer = new ConnectionLayer({ transport, executor });
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"echo"}');

    const frames = transport.sentTo('c1').map(parseFrame);
    expect(frames).toHaveLength(1);
    expect(frames[0]).toEqual({
      id: 'r1',
      ok: false,
      error: {
        code: 'INTERNAL_ERROR',
        message: 'executor returned a malformed outcome',
      },
    });
    await layer.stop();
  });

  test('maps a throwing executor to INTERNAL_ERROR instead of dropping the request', async () => {
    const transport = new FakeTransport();
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      execute: () => Promise.reject(new Error('boom')),
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const layer = new ConnectionLayer({ transport, executor, logger });
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"echo"}');

    expect(lastFrame(transport, 'c1')).toEqual({
      id: 'r1',
      ok: false,
      error: { code: 'INTERNAL_ERROR', message: 'boom' },
    });
    const failed = lines
      .map(parseFrame)
      .filter((entry) => entry.level === 'error');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      method: 'echo',
      code: 'INTERNAL_ERROR',
      reason: 'boom',
    });
    await layer.stop();
  });

  test('maps an unregistered method to UNKNOWN_METHOD', async () => {
    const { transport } = await harness();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"missing"}');

    const frame = lastFrame(transport, 'c1');
    expect(frame.id).toBe('r1');
    expect(frame.ok).toBe(false);
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
    expect(frame.ok).toBe(false);
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

  test('records a send failure when a successful result reaches a closed connection', async () => {
    const transport = new FakeTransport();
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });
    const gate = Promise.withResolvers<void>();
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      // An executor that ignores the cancel and still reports success: the
      // layer must not throw, and the failed write must still be recorded.
      execute: async () => {
        await gate.promise;
        return { ok: true, result: 'done' };
      },
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const layer = new ConnectionLayer({ transport, executor, logger });
    await layer.start();
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"pending"}');
    await sleep(0);
    await transport.disconnect('c1', 'client left');
    gate.resolve();
    await inFlight;

    expect(transport.sentTo('c1')).toHaveLength(0);
    const failures = lines
      .map(parseFrame)
      .filter((entry) => entry.code === 'TRANSPORT_SEND_FAILED');
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({ level: 'error', connectionId: 'c1' });
    await layer.stop();
  });

  test('ends a request quietly when its connection closed before the outcome', async () => {
    const { transport, dispatcher, logs } = await harness({ level: 'debug' });
    const entered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    dispatcher.register('pending', async () => {
      entered.resolve();
      await gate.promise;
      return 'done';
    });
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"pending"}');
    await entered.promise;
    // The disconnect cancels the request, so its outcome is TRANSPORT_CLOSED.
    await transport.disconnect('c1', 'client left');
    gate.resolve();
    await inFlight;
    await sleep(5);

    const entries = logs();
    expect(entries.filter((entry) => entry.level === 'error')).toHaveLength(0);
    expect(
      entries.filter((entry) => entry.code === 'TRANSPORT_SEND_FAILED'),
    ).toHaveLength(0);
    expect(transport.sentTo('c1')).toHaveLength(0);
    const ended = entries.filter(
      (entry) => entry.msg === 'request ended after the connection closed',
    );
    expect(ended).toHaveLength(1);
    expect(ended[0]).toMatchObject({
      level: 'debug',
      method: 'pending',
      code: 'TRANSPORT_CLOSED',
      connectionId: 'c1',
    });
    expect(typeof ended[0]?.durationMs).toBe('number');
  });

  test('still logs a send failure at error while the connection is tracked', async () => {
    const transport = new FakeTransport();
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      execute: () =>
        Promise.resolve({
          ok: false,
          error: { code: 'STORAGE_UNAVAILABLE', message: 'backend is gone' },
        }),
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const layer = new ConnectionLayer({ transport, executor, logger });
    await layer.start();
    const connection = transport.connect('c1');
    // The connection is still tracked, but the write itself fails: that is a
    // real transport fault and must stay an error (SPEC 7.1).
    connection.send = () => Promise.reject(new Error('socket is broken'));

    await transport.receive('c1', '{"id":"r1","method":"echo"}');

    const entries = lines.map(parseFrame);
    expect(
      entries.filter((entry) => entry.msg === 'request failed'),
    ).toHaveLength(1);
    const failedSend = entries.filter(
      (entry) => entry.code === 'TRANSPORT_SEND_FAILED',
    );
    expect(failedSend).toHaveLength(1);
    expect(failedSend[0]).toMatchObject({ level: 'error', connectionId: 'c1' });
    await layer.stop();
  });

  test('drops a request whose connection closed before it started', async () => {
    const { transport, dispatcher, logs } = await harness();
    let calls = 0;
    dispatcher.register('echo', (_ctx, payload) => {
      calls += 1;
      return Promise.resolve(payload);
    });
    transport.connect('c1');

    // Queued behind the chain, so the disconnect wins the race to start it.
    const inFlight = transport.receive('c1', '{"id":"r1","method":"echo"}');
    await transport.disconnect('c1', 'client left');
    await inFlight;

    // Executing it would commit writes for a player who is already gone.
    expect(calls).toBe(0);
    expect(transport.sentTo('c1')).toHaveLength(0);
    const dropped = logs().filter(
      (entry) => entry.msg === 'request dropped before start',
    );
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      level: 'warn',
      code: 'TRANSPORT_CLOSED',
      connectionId: 'c1',
    });
  });

  test('cancels an in-flight request when its connection closes', async () => {
    const { transport, dispatcher } = await harness();
    const entered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    let writeError = '';
    dispatcher.register('pending', async (ctx) => {
      entered.resolve();
      await gate.promise;
      try {
        await ctx.storage.execute('INSERT 1');
        writeError = 'write was accepted';
      } catch (error) {
        writeError = error instanceof Error ? error.message : String(error);
      }
      return 'done';
    });
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"pending"}');
    await entered.promise;
    // SPEC 3.6: a connection closing mid-request cancels it, so the handler's
    // later writes are rejected and its transaction rolls back.
    await transport.disconnect('c1', 'client left');
    gate.resolve();
    await inFlight;
    await sleep(5);

    expect(writeError).toMatch(/expired/);
    expect(transport.sentTo('c1')).toHaveLength(0);
  });

  test('calls the executor cancel with TRANSPORT_CLOSED on disconnect', async () => {
    const transport = new FakeTransport();
    const cancelled: { requestId: string; code: string }[] = [];
    const gate = Promise.withResolvers<void>();
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      execute: async () => {
        await gate.promise;
        return { ok: true, result: null };
      },
      cancel: (requestId, code) => {
        cancelled.push({ requestId, code });
        return Promise.resolve();
      },
      close: () => Promise.resolve(),
    };
    const layer = new ConnectionLayer({ transport, executor });
    await layer.start();
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"echo"}');
    await sleep(0);
    await transport.disconnect('c1', 'client left');
    gate.resolve();
    await inFlight;

    expect(cancelled).toHaveLength(1);
    expect(cancelled[0]?.code).toBe('TRANSPORT_CLOSED');
    expect(cancelled[0]?.requestId.length).toBeGreaterThan(0);
    await layer.stop();
  });

  test('a late result after the disconnect does not throw', async () => {
    const { transport, dispatcher } = await harness();
    const gate = Promise.withResolvers<void>();
    dispatcher.register('pending', async () => {
      await gate.promise;
      return 'done';
    });
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"pending"}');
    await transport.disconnect('c1', 'client left');
    // The reply path must survive a connection that is already gone.
    gate.resolve();
    await inFlight;

    expect(transport.sentTo('c1')).toHaveLength(0);
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
    dispatcher.register('explode', async () => {
      throw new Error('boom');
    });
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

  test('starts the executor before the transport', async () => {
    const order: string[] = [];
    const transport: Transport = {
      start: () => {
        order.push('transport');
        return Promise.resolve();
      },
      stop: () => Promise.resolve(),
    };
    const executor: RequestExecutor = {
      start: () => {
        order.push('executor');
        return Promise.resolve();
      },
      execute: () => Promise.resolve({ ok: true, result: null }),
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const layer = new ConnectionLayer({ transport, executor });

    await layer.start();

    // SPEC 6.2 step 3: no input before the backend is ready, so the transport
    // must not start first.
    expect(order).toEqual(['executor', 'transport']);
    await layer.stop();
  });

  test('does not start the transport when the executor fails to start', async () => {
    let transportStarted = false;
    const transport: Transport = {
      start: () => {
        transportStarted = true;
        return Promise.resolve();
      },
      stop: () => Promise.resolve(),
    };
    const executor: RequestExecutor = {
      start: () => Promise.reject(new Error('server never became ready')),
      execute: () => Promise.resolve({ ok: true, result: null }),
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
    };
    const layer = new ConnectionLayer({ transport, executor });

    const error = await rejectionOf(layer.start());

    expect(error.message).toMatch(/never became ready/);
    // A listener that accepted input for a backend that never started would be
    // exactly what SPEC 6.2 step 3 forbids.
    expect(transportStarted).toBe(false);
  });

  test('closes the executor when the transport fails to start', async () => {
    let executorClosed = false;
    const transport: Transport = {
      start: () => Promise.reject(new Error('port already in use')),
      stop: () => Promise.resolve(),
    };
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      execute: () => Promise.resolve({ ok: true, result: null }),
      cancel: () => Promise.resolve(),
      close: () => {
        executorClosed = true;
        return Promise.resolve();
      },
    };
    const layer = new ConnectionLayer({ transport, executor });

    const error = await rejectionOf(layer.start());

    // A Server process started for a layer that never came up has no owner.
    expect(error.message).toMatch(/port already in use/);
    expect(executorClosed).toBe(true);
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

  describe('bounded input queue (SPEC 3.5)', () => {
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
      await transport.receive(
        'c1',
        '{"id":"r2","method":"work","payload":"c"}',
      );

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

  describe('shutdown (SPEC 6.3)', () => {
    test('closes the executor after a failed start (2b)', async () => {
      const order: string[] = [];
      const { executor, closeCalls } = scriptedExecutor(order);
      const transport: Transport = {
        start: () => Promise.reject(new Error('port already in use')),
        stop: () => Promise.resolve(),
      };
      const layer = new ConnectionLayer({ transport, executor });

      await rejectionOf(layer.start());
      // The failed start already released the executor; a stop must not leak it
      // either, and must not close it a second time.
      await layer.stop();

      expect(closeCalls()).toBe(1);
    });

    test('still closes the executor when transport.stop throws, and a retry is idempotent (2b)', async () => {
      const order: string[] = [];
      const { executor, closeCalls } = scriptedExecutor(order);
      const scripted = scriptedTransport(order);
      scripted.failStop(new Error('socket refused to close'));
      const layer = new ConnectionLayer({
        transport: scripted.transport,
        executor,
      });
      await layer.start();

      const first = await rejectionOf(layer.stop());
      expect(first.message).toMatch(/refused to close/);
      expect(closeCalls()).toBe(1);
      // A second stop observes the same memoised run instead of starting over.
      const second = await rejectionOf(layer.stop());
      expect(second.message).toMatch(/refused to close/);
      expect(closeCalls()).toBe(1);
      expect(scripted.stopCalls()).toBe(1);
    });

    test('orders shutdown: refuse input, drain, close executor, stop transport', async () => {
      const order: string[] = [];
      const { executor } = scriptedExecutor(order);
      const scripted = scriptedTransport(order);
      const layer = new ConnectionLayer({
        transport: scripted.transport,
        executor,
        logger: createJsonLogger({ sink: () => undefined }),
      });
      await layer.start();
      const { connection } = fakeConnection('c1');
      await layer.onConnect(connection);

      await layer.stop();

      expect(order).toEqual([
        'executor.start',
        'transport.start',
        'executor.close',
        'transport.stop',
      ]);
    });

    test('drains an in-flight request before closing the executor', async () => {
      const order: string[] = [];
      const gate = Promise.withResolvers<void>();
      const state: { executor: RequestExecutor } = {
        executor: {
          start: () => Promise.resolve(),
          execute: async () => {
            order.push('execute.start');
            await gate.promise;
            order.push('execute.end');
            return { ok: true, result: 'done' };
          },
          cancel: () => Promise.resolve(),
          close: () => {
            order.push('executor.close');
            return Promise.resolve();
          },
        },
      };
      const scripted = scriptedTransport(order);
      const layer = new ConnectionLayer({
        transport: scripted.transport,
        executor: state.executor,
        logger: createJsonLogger({ sink: () => undefined }),
      });
      await layer.start();
      const { connection, sent } = fakeConnection('c1');
      await layer.onConnect(connection);

      const request = layer.onMessage(
        connection,
        '{"id":"r1","method":"work"}',
      );
      const stopping = layer.stop();
      gate.resolve();
      await Promise.all([request, stopping]);

      // The request settles before storage closes, so no write lands after it.
      expect(order.indexOf('execute.end')).toBeLessThan(
        order.indexOf('executor.close'),
      );
      expect(JSON.parse(sent[0] ?? '{}')).toEqual({
        id: 'r1',
        ok: true,
        result: 'done',
      });
    });

    test('answers a request that arrives during shutdown with FRAMEWORK_SHUTDOWN', async () => {
      const order: string[] = [];
      const { executor } = scriptedExecutor(order);
      const scripted = scriptedTransport(order);
      const lines: string[] = [];
      const layer = new ConnectionLayer({
        transport: scripted.transport,
        executor,
        logger: createJsonLogger({
          sink: (line) => {
            lines.push(line);
          },
        }),
      });
      await layer.start();
      const { connection, sent } = fakeConnection('c1');
      await layer.onConnect(connection);

      const stopping = layer.stop();
      await layer.onMessage(connection, '{"id":"late","method":"work"}');
      await stopping;

      const frame = parseFrame(sent[0] ?? '{}');
      expect(frame).toEqual({
        id: 'late',
        ok: false,
        error: {
          code: 'FRAMEWORK_SHUTDOWN',
          message: 'connection layer is shutting down',
        },
      });
      expect(
        lines
          .map(parseFrame)
          .filter((entry) => entry.code === 'FRAMEWORK_SHUTDOWN'),
      ).toHaveLength(1);
    });

    test('settles after shutdownTimeoutMs when a handler never returns (2c)', async () => {
      const order: string[] = [];
      const gate = Promise.withResolvers<void>();
      const lines: string[] = [];
      const executor: RequestExecutor = {
        start: () => Promise.resolve(),
        execute: () => gate.promise.then(() => ({ ok: true, result: null })),
        cancel: () => Promise.resolve(),
        close: () => {
          order.push('executor.close');
          return Promise.resolve();
        },
      };
      const scripted = scriptedTransport(order);
      const layer = new ConnectionLayer({
        transport: scripted.transport,
        executor,
        shutdownTimeoutMs: 20,
        logger: createJsonLogger({
          sink: (line) => {
            lines.push(line);
          },
        }),
      });
      await layer.start();
      const { connection } = fakeConnection('c1');
      await layer.onConnect(connection);

      const request = layer.onMessage(
        connection,
        '{"id":"r1","method":"work"}',
      );
      await layer.stop();

      // The wait is bounded, and the log names what held it up.
      const timedOut = lines
        .map(parseFrame)
        .filter((entry) => entry.level === 'error');
      expect(timedOut).toHaveLength(1);
      expect(timedOut[0]).toMatchObject({
        connectionIds: ['c1'],
        count: 1,
        timeoutMs: 20,
      });
      expect(order).toContain('executor.close');

      gate.resolve();
      await request;
    });

    test('gives the executor at least its declared close budget', async () => {
      // The bug this covers: `shutdownTimeoutMs` here (20ms) and the remote
      // executor's own worst case (200ms) were unrelated, so `stop()` returned
      // while the Server process was still draining and writing.
      const order: string[] = [];
      const serverStopped: string[] = [];
      const executor: RequestExecutor = {
        start: () => Promise.resolve(),
        execute: () => Promise.resolve({ ok: true, result: null }),
        cancel: () => Promise.resolve(),
        closeBudgetMs: 200,
        close: async () => {
          order.push('executor.close.start');
          await sleep(120);
          // Stands in for the executor's own Server termination: it must have
          // happened before `stop()` resolves.
          serverStopped.push('terminated');
          order.push('executor.close.end');
        },
      };
      const scripted = scriptedTransport(order);
      const lines: string[] = [];
      const layer = new ConnectionLayer({
        transport: scripted.transport,
        executor,
        shutdownTimeoutMs: 20,
        logger: createJsonLogger({
          sink: (line) => {
            lines.push(line);
          },
        }),
      });
      await layer.start();

      await layer.stop();

      expect(serverStopped).toEqual(['terminated']);
      expect(order).toEqual([
        'transport.start',
        'executor.close.start',
        'executor.close.end',
        'transport.stop',
      ]);
      // The layer's own 20ms ceiling did not cut the executor off.
      expect(
        lines
          .map(parseFrame)
          .filter(
            (entry) =>
              entry.msg ===
              'executor close did not settle before the shutdown timeout',
          ),
      ).toHaveLength(0);
    });

    test('logs the executor budget it gave up on', async () => {
      const executor: RequestExecutor = {
        start: () => Promise.resolve(),
        execute: () => Promise.resolve({ ok: true, result: null }),
        cancel: () => Promise.resolve(),
        closeBudgetMs: 30,
        close: () => Promise.reject(new Error('server refused to die')),
      };
      const scripted = scriptedTransport([]);
      const lines: string[] = [];
      const layer = new ConnectionLayer({
        transport: scripted.transport,
        executor,
        shutdownTimeoutMs: 20,
        logger: createJsonLogger({
          sink: (line) => {
            lines.push(line);
          },
        }),
      });
      await layer.start();

      await layer.stop();

      // 20ms own timeout is below the 30ms budget, so the budget wins.
      const failed = lines
        .map(parseFrame)
        .find((entry) => entry.msg === 'executor close failed');
      expect(failed).toMatchObject({ reason: 'server refused to die' });
    });

    test('logs a business failure at warn and a framework failure at error', async () => {
      const transport = new FakeTransport();
      const lines: string[] = [];
      const logger: Logger = createJsonLogger({
        sink: (line) => {
          lines.push(line);
        },
      });
      const outcomes: ExecutorOutcome[] = [
        {
          ok: false,
          error: { code: 'PLAYER_NOT_FOUND', message: 'no such player' },
        },
        {
          ok: false,
          error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
        },
        {
          ok: false,
          error: { code: 'INTERNAL_ERROR', message: 'boom' },
        },
      ];
      const executor: RequestExecutor = {
        start: () => Promise.resolve(),
        execute: () => {
          const next = outcomes.shift();
          if (next === undefined) {
            throw new Error('no outcome left');
          }
          return Promise.resolve(next);
        },
        cancel: () => Promise.resolve(),
        close: () => Promise.resolve(),
      };
      const layer = new ConnectionLayer({ transport, executor, logger });
      await layer.start();
      transport.connect('c1');

      await transport.receive('c1', '{"id":"r1","method":"look"}');
      await transport.receive('c1', '{"id":"r2","method":"look"}');
      await transport.receive('c1', '{"id":"r3","method":"look"}');

      // SPEC 7.1: an expected business refusal must not be recorded at error
      // level, or every "player not found" pages someone.
      const failures = lines
        .map(parseFrame)
        .filter((entry) => entry.msg === 'request failed');
      expect(failures.map((entry) => [entry.code, entry.level])).toEqual([
        ['PLAYER_NOT_FOUND', 'warn'],
        ['FRAMEWORK_TIMEOUT', 'error'],
        ['INTERNAL_ERROR', 'error'],
      ]);
      await layer.stop();
    });

    test('runs the shutdown once for concurrent and repeated calls', async () => {
      const order: string[] = [];
      const { executor, closeCalls } = scriptedExecutor(order);
      const scripted = scriptedTransport(order);
      const layer = new ConnectionLayer({
        transport: scripted.transport,
        executor,
      });
      await layer.start();

      await Promise.all([layer.stop(), layer.stop()]);
      await layer.stop();

      expect(closeCalls()).toBe(1);
      expect(scripted.stopCalls()).toBe(1);
    });
  });

  describe('session routing (SPEC 3.2, INV-004)', () => {
    test('a bound session makes a later connection rebind it as a handoff', async () => {
      const { transport, layer, logs } = sessionHarness();
      await layer.start();
      transport.connect('c1');
      await transport.receive('c1', '{"id":"1","method":"bind"}');

      transport.connect('c2');
      await transport.receive('c2', '{"id":"2","method":"bind"}');

      // The route exists because the second bind was recognised as a reconnect
      // rather than a first binding.
      const rebound = logs().filter(
        (entry) => entry.msg === 'session rebound to a new connection',
      );
      expect(rebound).toHaveLength(1);
      expect(rebound[0]).toMatchObject({
        level: 'warn',
        sessionId: 's1',
        previousConnectionId: 'c1',
        connectionId: 'c2',
      });
      expect(
        logs().filter((entry) => entry.msg === 'session bound'),
      ).toHaveLength(2);
      await layer.stop();
    });

    test('rebinding the same session on one connection is idempotent', async () => {
      const { transport, layer, logs } = sessionHarness();
      await layer.start();
      transport.connect('c1');

      await transport.receive('c1', '{"id":"1","method":"bind"}');
      await transport.receive('c1', '{"id":"2","method":"bind"}');
      await transport.receive('c1', '{"id":"3","method":"bind"}');

      // A long-lived connection binds once; repeating it must not log per
      // request or turn into a self-handoff.
      expect(
        logs().filter((entry) => entry.msg === 'session bound'),
      ).toHaveLength(1);
      expect(
        logs().filter(
          (entry) => entry.msg === 'session rebound to a new connection',
        ),
      ).toHaveLength(0);
      await layer.stop();
    });

    test('binding a new session on one connection drops the old route', async () => {
      let bound = 's1';
      const { transport, layer, logs } = sessionHarness({
        sessionOf: (method) => (method === 'bind' ? bound : undefined),
      });
      await layer.start();
      transport.connect('c1');

      await transport.receive('c1', '{"id":"1","method":"bind"}');
      bound = 's2';
      await transport.receive('c1', '{"id":"2","method":"bind"}');
      // c1 no longer holds s1, so this is a first binding, not a handoff.
      transport.connect('c2');
      bound = 's1';
      await transport.receive('c2', '{"id":"3","method":"bind"}');

      expect(
        logs().filter(
          (entry) => entry.msg === 'session rebound to a new connection',
        ),
      ).toHaveLength(0);
      await layer.stop();
    });

    test('a reconnecting connection waits for the previous connection to settle', async () => {
      const { transport, layer, controlled } = sessionHarness();
      await layer.start();
      transport.connect('c1');
      await transport.receive('c1', '{"id":"1","method":"bind"}');
      const hanging = transport.receive('c1', '{"id":"2","method":"hang"}');
      await sleep(0);
      expect(controlled.started).toEqual(['bind', 'hang']);

      // The binding request itself is not delayed: it already executed, and its
      // result belongs to the client that sent it.
      transport.connect('c2');
      await transport.receive('c2', '{"id":"3","method":"bind"}');
      expect(lastFrame(transport, 'c2')).toEqual({
        id: '3',
        ok: true,
        result: 'bind',
      });

      const next = transport.receive('c2', '{"id":"4","method":"next"}');
      await sleep(10);
      // SPEC 3.2: the new connection's next request must not start while the
      // old connection still has work without a terminal state.
      expect(controlled.started).toEqual(['bind', 'hang', 'bind']);

      controlled.release();
      await Promise.all([hanging, next]);
      expect(controlled.started).toEqual(['bind', 'hang', 'bind', 'next']);
      // The handoff ordering is what matters: the old connection's work has a
      // terminal state before the new connection's next request starts.
      expect(controlled.finished.indexOf('hang')).toBeLessThan(
        controlled.started.lastIndexOf('next'),
      );
      await layer.stop();
    });

    test('a connection that lost its session and takes it back is gated again', async () => {
      const { transport, layer, controlled, logs } = sessionHarness();
      await layer.start();
      transport.connect('c1');
      await transport.receive('c1', '{"id":"1","method":"bind"}');

      // c2 takes the Session over, so c1 is no longer the holder.
      transport.connect('c2');
      await transport.receive('c2', '{"id":"2","method":"bind"}');
      const hanging = transport.receive('c2', '{"id":"3","method":"hang"}');
      await sleep(0);

      // c1 binding s1 again is a reconnect from c1's side, not an idempotent
      // rebind of its own route: it must be gated on c2, which still has work.
      await transport.receive('c1', '{"id":"4","method":"bind"}');
      expect(
        logs().filter(
          (entry) => entry.msg === 'session rebound to a new connection',
        ),
      ).toHaveLength(2);

      const next = transport.receive('c1', '{"id":"5","method":"next"}');
      await sleep(10);
      expect(controlled.started).not.toContain('next');

      controlled.release();
      await Promise.all([hanging, next]);
      expect(controlled.started).toContain('next');
      await layer.stop();
    });

    test('releases a reconnect wait that outlives the handoff timeout', async () => {
      const { transport, layer, controlled, logs } = sessionHarness({
        sessionHandoffTimeoutMs: 20,
      });
      await layer.start();
      transport.connect('c1');
      await transport.receive('c1', '{"id":"1","method":"bind"}');
      const hanging = transport.receive('c1', '{"id":"2","method":"hang"}');
      await sleep(0);

      transport.connect('c2');
      await transport.receive('c2', '{"id":"3","method":"bind"}');
      const next = transport.receive('c2', '{"id":"4","method":"next"}');
      await sleep(60);

      // A handler nobody can preempt must not stall the session forever.
      expect(controlled.started).toContain('next');
      const timedOut = logs().filter(
        (entry) =>
          entry.msg === 'session handoff timed out; releasing the next request',
      );
      expect(timedOut).toHaveLength(1);
      expect(timedOut[0]).toMatchObject({
        level: 'warn',
        sessionId: 's1',
        previousConnectionId: 'c1',
        timeoutMs: 20,
      });

      controlled.release();
      await Promise.all([hanging, next]);
      await layer.stop();
    });

    test('a disconnect removes the route so the next binding is not a handoff', async () => {
      const { transport, layer, controlled, logs } = sessionHarness();
      await layer.start();
      transport.connect('c1');
      await transport.receive('c1', '{"id":"1","method":"bind"}');
      // Left in flight on purpose: its chain never settles, so any gate would
      // be observable.
      const abandoned = transport.receive('c1', '{"id":"2","method":"hang"}');
      await sleep(0);
      await transport.disconnect('c1', 'client left');

      transport.connect('c2');
      await transport.receive('c2', '{"id":"3","method":"bind"}');
      const next = transport.receive('c2', '{"id":"4","method":"next"}');
      await sleep(10);

      // SPEC 6.1: the route went with the connection, so nothing is inherited.
      expect(
        logs().filter(
          (entry) => entry.msg === 'session rebound to a new connection',
        ),
      ).toHaveLength(0);
      expect(controlled.started).toContain('next');

      controlled.release();
      await Promise.all([abandoned, next]);
      await layer.stop();
    });

    test('refuses a binding once the session table is full but still answers the request', async () => {
      let bound = 's1';
      const { transport, layer, logs } = sessionHarness({
        maxSessions: 1,
        sessionOf: (method) => (method === 'bind' ? bound : undefined),
      });
      await layer.start();
      transport.connect('c1');
      await transport.receive('c1', '{"id":"1","method":"bind"}');

      bound = 's2';
      await transport.receive('c1', '{"id":"2","method":"bind"}');

      // The request succeeded: a routing limit is not a business failure.
      expect(lastFrame(transport, 'c1')).toEqual({
        id: '2',
        ok: true,
        result: 'bind',
      });
      const refused = logs().filter(
        (entry) =>
          entry.msg === 'session binding refused: session table is full',
      );
      expect(refused).toHaveLength(1);
      expect(refused[0]).toMatchObject({
        level: 'warn',
        sessionId: 's2',
        connectionId: 'c1',
        limit: 1,
      });
      // The refused id left no route behind.
      expect(
        logs().filter((entry) => entry.msg === 'session bound'),
      ).toHaveLength(1);
      await layer.stop();
    });

    test('clears the session table on stop', async () => {
      const { transport, layer } = sessionHarness();
      await layer.start();
      transport.connect('c1');
      await transport.receive('c1', '{"id":"1","method":"bind"}');
      // Bracket access: the table is private, and shutdown leaves nothing a
      // client could observe it through (the connection is gone too).
      const sessions: Map<string, unknown> = layer['sessions'];
      expect(sessions.size).toBe(1);

      await layer.stop();

      expect(sessions.size).toBe(0);
    });
  });
});
