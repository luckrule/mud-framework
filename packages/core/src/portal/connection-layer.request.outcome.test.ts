import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { ExecutorOutcome, RequestExecutor } from '../shared/executor.ts';
import { parseFrame, sleep } from '../testing/common.ts';
import { harness, lastFrame } from '../testing/connection-layer.ts';
import { FakeTransport } from '../testing/fake-transport.ts';
import { ConnectionLayer } from './connection-layer.ts';

describe('ConnectionLayer request', () => {
  test('dispatches a request and replies with the handler result', async () => {
    const { transport, backend, dispatcher } = await harness();
    dispatcher.register(
      'echo',
      async (ctx, payload) => {
        await ctx.storage.execute('INSERT 1', [payload]);
        return payload;
      },
      { access: 'public' },
    );
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
    dispatcher.register(
      'work',
      async (_ctx, payload) => {
        const label = String(payload);
        log.push(`${label}:start`);
        await sleep(25);
        log.push(`${label}:end`);
        return label;
      },
      { access: 'public' },
    );
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
    dispatcher.register(
      'barrier',
      async () => {
        active += 1;
        if (active === 2) {
          barrier.resolve();
        }
        // Deadlocks (then times out) if the layer serialized across connections.
        await barrier.promise;
        return active;
      },
      { access: 'public' },
    );
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
    dispatcher.register(
      'explode',
      async () => {
        throw new Error('business failure');
      },
      { access: 'public' },
    );
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
      onEvent: () => () => undefined,
      execute: () =>
        Promise.resolve({
          ok: false,
          error: { code: 'INTERNAL_ERROR', message: 'backend is gone' },
        }),
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
      health: () => ({ state: 'ready', ready: true }),
    };
    const layer = new ConnectionLayer({ transport, executor, logger });
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"echo"}');

    expect(lastFrame(transport, 'c1')).toEqual({
      id: 'r1',
      ok: false,
      error: { code: 'INTERNAL_ERROR', message: 'backend is gone' },
    });
    const failed = lines
      .map(parseFrame)
      .filter((entry) => entry.level === 'error');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      method: 'echo',
      code: 'INTERNAL_ERROR',
      reason: 'backend is gone',
      connectionId: 'c1',
      clientRequestId: 'r1',
    });
    expect(failed[0]?.durationMs).toBeGreaterThanOrEqual(0);
    expect(Number.isFinite(failed[0]?.durationMs)).toBe(true);
    await layer.stop();
  });

  test('answers an executor outcome that is missing its error instead of dropping the request', async () => {
    const transport = new FakeTransport();
    // The regression this covers: `{ok:false}` without `error` used to produce
    // no frame at all, so the client waited forever. The type says it cannot
    // happen; an executor is outside code, so it is checked anyway.
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      execute: () => {
        return Promise.resolve({ ok: false } as ExecutorOutcome);
      },
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
      health: () => ({ state: 'ready', ready: true }),
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

  // One case per `return false` branch of `isExecutorOutcome` that the tests
  // above do not reach. Every case asserts a reply, because a rejected outcome
  // that produced no frame is exactly the silent drop SPEC 3.5 forbids.
  //
  // Two inputs are chosen to make their branch observable rather than merely
  // exercised: `error: null` has no readable fields, so without the
  // `isRecord(error)` guard `error.code` throws instead of rejecting cleanly;
  // `code: { length: 5 }` would pass the length check on its own, so only the
  // `typeof code === 'string'` guard refuses it.
  const malformedOutcomes: readonly (readonly [string, unknown])[] = [
    ['value is not an object at all', null],
    ['ok is neither true nor false', { ok: 'yes' }],
    ['error is not an object', { ok: false, error: null }],
    [
      'error code is not a string',
      { ok: false, error: { code: { length: 5 }, message: 'boom' } },
    ],
    [
      'error code is empty',
      { ok: false, error: { code: '', message: 'boom' } },
    ],
    [
      'error message is not a string',
      { ok: false, error: { code: 'INTERNAL_ERROR', message: 7 } },
    ],
    [
      'sessionChange is not an object',
      { ok: true, result: null, sessionChange: 'bind' },
    ],
    [
      'sessionChange type is unknown',
      { ok: true, result: null, sessionChange: { type: 'rebind' } },
    ],
    [
      'bind change has no session id',
      { ok: true, result: null, sessionChange: { type: 'bind' } },
    ],
    [
      'bind change has an empty session id',
      {
        ok: true,
        result: null,
        sessionChange: { type: 'bind', sessionId: '' },
      },
    ],
    [
      'unbind change carries a session id',
      {
        ok: true,
        result: null,
        sessionChange: { type: 'unbind', sessionId: 's1' },
      },
    ],
  ];

  for (const [label, outcome] of malformedOutcomes) {
    test(`answers an executor outcome whose ${label} instead of dropping the request`, async () => {
      const transport = new FakeTransport();
      const executor: RequestExecutor = {
        start: () => Promise.resolve(),
        onEvent: () => () => undefined,
        execute: () => Promise.resolve(outcome as ExecutorOutcome),
        cancel: () => Promise.resolve(),
        close: () => Promise.resolve(),
        health: () => ({ state: 'ready', ready: true }),
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
  }

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
      onEvent: () => () => undefined,
      execute: () => Promise.reject(new Error('boom')),
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
      health: () => ({ state: 'ready', ready: true }),
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
    dispatcher.register(
      'slow',
      async () => {
        await sleep(60);
        return 'late';
      },
      { access: 'public' },
    );
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

  test('answers a session-gated handler with AUTH_REQUIRED', async () => {
    const { transport, dispatcher } = await harness({
      createRequestId: () => 'req-refused',
    });
    dispatcher.register('inspect', async () => null);
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"inspect"}');

    expect(lastFrame(transport, 'c1')).toMatchObject({
      id: 'r1',
      ok: false,
      error: { code: 'AUTH_REQUIRED' },
    });
  });

  test('answers a policy refusal with FORBIDDEN', async () => {
    const { transport, dispatcher } = await harness({
      createRequestId: () => 'req-forbidden',
      authorize: () => false,
    });
    dispatcher.register('inspect', async () => null, { access: 'public' });
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"inspect"}');

    expect(lastFrame(transport, 'c1')).toMatchObject({
      id: 'r1',
      ok: false,
      error: { code: 'FORBIDDEN' },
    });
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
      onEvent: () => () => undefined,
      execute: () => {
        const next = outcomes.shift();
        if (next === undefined) {
          throw new Error('no outcome left');
        }
        return Promise.resolve(next);
      },
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
      health: () => ({ state: 'ready', ready: true }),
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
});
