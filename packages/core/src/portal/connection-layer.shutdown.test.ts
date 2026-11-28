import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import { closeBudget } from '../shared/budget.ts';
import type { Codec } from '../shared/codec.ts';
import { JsonCodec } from '../shared/codec.ts';
import type { ServerEvent } from '../shared/events.ts';
import type { RequestExecutor } from '../shared/executor.ts';
import { MAX_TIMER_DELAY_MS } from '../shared/validation.ts';
import { parseFrame, rejectionOf, sleep } from '../testing/common.ts';
import {
  fakeConnection,
  scriptedExecutor,
  scriptedTransport,
  sessionHarness,
} from '../testing/connection-layer.ts';
import { ConnectionLayer } from './connection-layer.ts';

function connectionEvent(eventId: string, connectionId = 'c1'): ServerEvent {
  return {
    eventId,
    target: { kind: 'connection', connectionId },
    event: 'note',
    payload: 'x',
  };
}

describe('ConnectionLayer shutdown (SPEC 6.3)', () => {
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
        onEvent: () => () => undefined,
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
        health: () => ({ state: 'ready', ready: true }),
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

    const request = layer.onMessage(connection, '{"id":"r1","method":"work"}');
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

  test('refuses an undecodable frame during shutdown without a clientRequestId log field', async () => {
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
    await layer.onMessage(connection, 'not json');
    await stopping;

    // The wire reply keeps the documented empty id; the log must not report an
    // empty `clientRequestId` for a frame that never had one.
    expect(parseFrame(sent[0] ?? '{}')).toEqual({
      id: '',
      ok: false,
      error: {
        code: 'FRAMEWORK_SHUTDOWN',
        message: 'connection layer is shutting down',
      },
    });
    const refusals = lines
      .map(parseFrame)
      .filter((entry) => entry.code === 'FRAMEWORK_SHUTDOWN');
    expect(refusals).toHaveLength(1);
    expect(refusals[0]).not.toHaveProperty('clientRequestId');
  });

  test('treats an empty id from a custom codec as uncorrelated during shutdown', async () => {
    const order: string[] = [];
    const { executor } = scriptedExecutor(order);
    const scripted = scriptedTransport(order);
    const lines: string[] = [];
    const json = new JsonCodec();
    const emptyIdCodec: Codec = {
      encode: (value) => json.encode(value),
      decode: () => ({ id: '', method: 'late', payload: null }),
    };
    const layer = new ConnectionLayer({
      transport: scripted.transport,
      executor,
      codec: emptyIdCodec,
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
    await layer.onMessage(connection, 'custom frame');
    await stopping;

    // Shutdown has no INVALID_ENVELOPE path: an unreadable id becomes "no
    // correlation", so the reply keeps the documented empty id and the log
    // omits `clientRequestId` rather than reporting an empty key.
    expect(parseFrame(sent[0] ?? '{}')).toEqual({
      id: '',
      ok: false,
      error: {
        code: 'FRAMEWORK_SHUTDOWN',
        message: 'connection layer is shutting down',
      },
    });
    const refusals = lines
      .map(parseFrame)
      .filter((entry) => entry.code === 'FRAMEWORK_SHUTDOWN');
    expect(refusals).toHaveLength(1);
    expect(refusals[0]).not.toHaveProperty('clientRequestId');
  });

  test('settles after shutdownTimeoutMs when a handler never returns (2c)', async () => {
    const order: string[] = [];
    const gate = Promise.withResolvers<void>();
    const lines: string[] = [];
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      execute: () => gate.promise.then(() => ({ ok: true, result: null })),
      cancel: () => Promise.resolve(),
      close: () => {
        order.push('executor.close');
        return Promise.resolve();
      },
      health: () => ({ state: 'ready', ready: true }),
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

    const request = layer.onMessage(connection, '{"id":"r1","method":"work"}');
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

  test('closes within a shutdown timeout at the timer ceiling', async () => {
    // `closeBudgetMs` grows with the shutdown timeout, so at the ceiling the
    // derived budget would exceed it and Bun would clamp the wait to 1ms: the
    // executor would be abandoned on the tick `stop()` started waiting and the
    // call would reject with a timeout that never happened. The derived value is
    // truncated instead, so a close that finishes inside its own budget still
    // settles and the shutdown succeeds.
    const order: string[] = [];
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      execute: () => Promise.resolve({ ok: true, result: null }),
      cancel: () => Promise.resolve(),
      closeBudgetMs: closeBudget(MAX_TIMER_DELAY_MS),
      close: async () => {
        order.push('executor.close');
        await sleep(20);
      },
      health: () => ({ state: 'ready', ready: true }),
    };
    const scripted = scriptedTransport(order);
    const lines: string[] = [];
    const layer = new ConnectionLayer({
      transport: scripted.transport,
      executor,
      shutdownTimeoutMs: MAX_TIMER_DELAY_MS,
      logger: createJsonLogger({ sink: (line) => lines.push(line) }),
    });
    await layer.start();

    await layer.stop();

    expect(order).toEqual([
      'transport.start',
      'executor.close',
      'transport.stop',
    ]);
    expect(
      lines
        .map(parseFrame)
        .filter(
          (entry) =>
            entry.msg ===
              'executor close did not settle before the shutdown timeout' ||
            entry.msg ===
              'transport stop did not settle before the shutdown timeout',
        ),
    ).toEqual([]);
  });

  test('gives the executor at least its declared close budget', async () => {
    // The bug this covers: `shutdownTimeoutMs` here (20ms) and the remote
    // executor's own worst case (200ms) were unrelated, so `stop()` returned
    // while the Server process was still draining and writing.
    const order: string[] = [];
    const serverStopped: string[] = [];
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
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
      health: () => ({ state: 'ready', ready: true }),
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

  test('rejects stop after executor close times out, stops transport once, and handles a late close rejection', async () => {
    const closing = Promise.withResolvers<void>();
    let closes = 0;
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      execute: () => Promise.resolve({ ok: true, result: null }),
      cancel: () => Promise.resolve(),
      close: () => {
        closes += 1;
        return closing.promise;
      },
      health: () => ({ state: 'ready', ready: true }),
    };
    const scripted = scriptedTransport([]);
    const lines: string[] = [];
    const layer = new ConnectionLayer({
      transport: scripted.transport,
      executor,
      shutdownTimeoutMs: 10,
      logger: createJsonLogger({ sink: (line) => lines.push(line) }),
    });
    await layer.start();

    try {
      const failure = await rejectionOf(layer.stop());
      expect(failure.message).toMatch(/executor close.*shutdown timeout/);
      expect(scripted.stopCalls()).toBe(1);
      expect(closes).toBe(1);
      expect(
        lines
          .map(parseFrame)
          .filter(
            (entry) =>
              entry.msg ===
              'executor close did not settle before the shutdown timeout',
          ),
      ).toMatchObject([{ timeoutMs: 10 }]);

      closing.reject(new Error('late executor close failed'));
      await sleep(0);
      expect(await rejectionOf(layer.stop())).toBe(failure);
      expect(scripted.stopCalls()).toBe(1);
      expect(closes).toBe(1);
    } finally {
      closing.resolve();
      await layer.stop().catch(() => undefined);
    }
  });

  test('propagates executor close failure only after the transport is stopped', async () => {
    const failure = new Error('server refused to die');
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      execute: () => Promise.resolve({ ok: true, result: null }),
      cancel: () => Promise.resolve(),
      closeBudgetMs: 30,
      close: () => Promise.reject(failure),
      health: () => ({ state: 'ready', ready: true }),
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

    const error = await rejectionOf(layer.stop());
    expect(error).toBe(failure);
    expect(scripted.stopCalls()).toBe(1);

    // 20ms own timeout is below the 30ms budget, so the budget wins.
    const failed = lines
      .map(parseFrame)
      .find((entry) => entry.msg === 'executor close failed');
    expect(failed).toMatchObject({ reason: 'server refused to die' });
  });

  test('preserves both close failures and repeats the same failed shutdown without closing again', async () => {
    const executorFailure = new Error('server refused to die');
    const transportFailure = new Error('socket refused to close');
    let closes = 0;
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      execute: () => Promise.resolve({ ok: true, result: null }),
      cancel: () => Promise.resolve(),
      close: () => {
        closes += 1;
        return Promise.reject(executorFailure);
      },
      health: () => ({ state: 'ready', ready: true }),
    };
    const scripted = scriptedTransport([]);
    scripted.failStop(transportFailure);
    const layer = new ConnectionLayer({
      transport: scripted.transport,
      executor,
      logger: createJsonLogger({ sink: () => undefined }),
    });
    await layer.start();

    const failure = await rejectionOf(layer.stop());
    expect(failure).toBeInstanceOf(AggregateError);
    expect(failure).toMatchObject({
      errors: [executorFailure, transportFailure],
    });
    expect(await rejectionOf(layer.stop())).toBe(failure);
    expect(closes).toBe(1);
    expect(scripted.stopCalls()).toBe(1);
  });

  test('rejects stop when transport stop never settles, calls it once, and repeats the same rejection', async () => {
    const never = Promise.withResolvers<void>();
    let stops = 0;
    const order: string[] = [];
    const { executor } = scriptedExecutor(order);
    const lines: string[] = [];
    const layer = new ConnectionLayer({
      transport: {
        start: () => Promise.resolve(),
        stop: () => {
          stops += 1;
          return never.promise;
        },
      },
      executor,
      shutdownTimeoutMs: 5,
      logger: createJsonLogger({ sink: (line) => lines.push(line) }),
    });
    await layer.start();

    const failure = await rejectionOf(layer.stop());
    expect(failure.message).toMatch(/transport stop.*shutdown timeout/);
    expect(stops).toBe(1);
    // The state still reflects the sequence reaching its end, but the caller
    // is told the transport close was never confirmed.
    expect(layer.health().state).toBe('stopped');
    expect(
      lines
        .map(parseFrame)
        .find(
          (entry) =>
            entry.msg ===
            'transport stop did not settle before the shutdown timeout',
        ),
    ).toMatchObject({ level: 'error', timeoutMs: 5 });

    expect(await rejectionOf(layer.stop())).toBe(failure);
    expect(stops).toBe(1);
    never.resolve();
  });

  test('preserves executor close failure and transport timeout together in a stable order', async () => {
    const executorFailure = new Error('server refused to die');
    const never = Promise.withResolvers<void>();
    let stops = 0;
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      execute: () => Promise.resolve({ ok: true, result: null }),
      cancel: () => Promise.resolve(),
      close: () => Promise.reject(executorFailure),
      health: () => ({ state: 'ready', ready: true }),
    };
    const layer = new ConnectionLayer({
      transport: {
        start: () => Promise.resolve(),
        stop: () => {
          stops += 1;
          return never.promise;
        },
      },
      executor,
      shutdownTimeoutMs: 5,
      logger: createJsonLogger({ sink: () => undefined }),
    });
    await layer.start();

    const failure = await rejectionOf(layer.stop());
    expect(failure).toBeInstanceOf(AggregateError);
    const errors = (failure as AggregateError).errors;
    expect(errors).toHaveLength(2);
    expect(errors[0]).toBe(executorFailure);
    expect(errors[1]?.message).toMatch(/transport stop.*shutdown timeout/);
    expect(stops).toBe(1);
    expect(await rejectionOf(layer.stop())).toBe(failure);
    never.resolve();
  });

  test('resolves stop when the transport stops within the budget', async () => {
    const order: string[] = [];
    const { executor } = scriptedExecutor(order);
    const scripted = scriptedTransport(order);
    const layer = new ConnectionLayer({
      transport: scripted.transport,
      executor,
      shutdownTimeoutMs: 5,
    });
    await layer.start();

    await layer.stop();

    expect(scripted.stopCalls()).toBe(1);
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

describe('ConnectionLayer shutdown drops requests that never started (SPEC 3.8, 6.3)', () => {
  test('drops a request queued behind an unfinished event frame, once and by name', async () => {
    const h = sessionHarness({
      shutdownTimeoutMs: 20,
      createRequestId: () => 'req-1',
    });
    await h.layer.start();
    h.transport.connect('c1');

    // One event frame is on the wire and never settles: the writer is inside
    // the frame's write, so the request behind it cannot start.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(connectionEvent('e:0'));
    await held.entered;
    const request = h.transport.receive('c1', '{"id":"r1","method":"work"}');
    await sleep(0);
    // Read before `stop()`: a stopped transport cannot be inspected, and the
    // held frame is the only one on the wire at this point.
    const beforeStop = h.transport.sentTo('c1').map(parseFrame);

    const stopping = h.layer.stop();
    await sleep(40);
    // The request was dropped, not left waiting on the writer; releasing the
    // frame lets the transport's own stop finish.
    held.release();
    await stopping;
    await request;
    await sleep(0);

    const dropped = h
      .logs()
      .filter((entry) => entry.code === 'REQUEST_DROPPED_AT_SHUTDOWN');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      level: 'warn',
      requestId: 'req-1',
      method: 'work',
      clientRequestId: 'r1',
      connectionId: 'c1',
      reason: expect.stringContaining('drain budget'),
    });
    // It never ran, so nothing was written and no late path reported it.
    expect(h.controlled.started).not.toContain('work');
    expect(beforeStop).toEqual([
      {
        kind: 'event',
        eventId: 'e:0',
        event: 'note',
        payload: 'x',
      },
    ]);
    expect(
      h.logs().filter((entry) => entry.msg === 'request dropped before start'),
    ).toEqual([]);
    expect(
      h.logs().filter((entry) => entry.code === 'TRANSPORT_CLOSED'),
    ).toEqual([]);
    // Exactly one line: the drop is reported by the shutdown path and nowhere
    // else, so the two paths cannot both claim the same request.
    expect(
      h.logs().filter((entry) => entry.msg === 'request dropped at shutdown'),
    ).toHaveLength(1);
    // Nothing is left counted after the close.
    expect(h.layer.health().connections).toBe(0);
    expect(h.layer.health().eventBacklog).toEqual({
      connections: 0,
      frames: 0,
      bytes: 0,
      dropped: 0,
    });
  });

  test('lets a started request finish with its receipt while a waiting one is dropped', async () => {
    const h = sessionHarness({
      shutdownTimeoutMs: 20,
      createRequestId: (() => {
        let next = 0;
        return () => `req-${(next += 1)}`;
      })(),
    });
    await h.layer.start();
    h.transport.connect('c1');
    h.transport.connect('c2');

    // c1's request is already running and finishes inside the drain budget.
    const started = h.transport.receive('c1', '{"id":"a1","method":"work"}');
    // c2's request waits behind a frame that never settles, so its turn never
    // comes: the drain budget runs out with it still queued.
    const held = h.transport.holdSends('c2');
    h.controlled.emit(connectionEvent('e:0', 'c2'));
    await held.entered;
    const waiting = h.transport.receive('c2', '{"id":"b1","method":"work"}');
    await sleep(0);

    const stopping = h.layer.stop();
    await started;
    const c1Frames = h.transport.sentTo('c1').map(parseFrame);
    await sleep(40);
    held.release();
    await stopping;
    await waiting;
    await sleep(0);

    // The started request ran to completion and was answered; it is not a
    // shutdown casualty (SPEC 6.3 step 2).
    expect(h.controlled.started).toContain('work');
    expect(c1Frames).toEqual([{ id: 'a1', ok: true, result: 'work' }]);
    expect(h.controlled.finished).toContain('work');

    const dropped = h
      .logs()
      .filter((entry) => entry.code === 'REQUEST_DROPPED_AT_SHUTDOWN');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      requestId: 'req-2',
      method: 'work',
      clientRequestId: 'b1',
      connectionId: 'c2',
    });
  });

  test('a graceful shutdown still runs and answers a request that was only queued', async () => {
    const h = sessionHarness({ shutdownTimeoutMs: 5000 });
    await h.layer.start();
    h.transport.connect('c1');

    // One request is running, one waits behind it. Neither is stuck on a frame,
    // so the drain budget covers both: this is the normal close, and dropping
    // the waiting one would turn a graceful shutdown into an abrupt one.
    const running = h.transport.receive('c1', '{"id":"a1","method":"hang"}');
    await sleep(0);
    const queued = h.transport.receive('c1', '{"id":"b1","method":"work"}');
    await sleep(0);
    expect(h.controlled.started).toEqual(['hang']);

    const stopping = h.layer.stop();
    await sleep(0);
    h.controlled.release();
    await Promise.all([running, queued]);
    // Behaviour first: a premature drop shows up here rather than as a read on
    // a transport that already stopped.
    expect(h.controlled.started).toEqual(['hang', 'work']);
    expect(h.controlled.finished).toEqual(['hang', 'work']);
    expect(
      h.logs().filter((entry) => entry.code === 'REQUEST_DROPPED_AT_SHUTDOWN'),
    ).toEqual([]);
    expect(
      h.logs().filter((entry) => entry.msg === 'request dropped before start'),
    ).toEqual([]);
    // Read while the transport is still running: `stop()` has not resolved yet.
    const frames = h.transport.sentTo('c1').map(parseFrame);
    await stopping;

    expect(frames).toEqual([
      { id: 'a1', ok: true, result: 'hang' },
      { id: 'b1', ok: true, result: 'work' },
    ]);
  });
});
