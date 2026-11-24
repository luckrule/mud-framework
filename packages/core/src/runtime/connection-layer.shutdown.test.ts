import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import { parseFrame, rejectionOf, sleep } from '../test-support/common.ts';
import {
  fakeConnection,
  scriptedExecutor,
  scriptedTransport,
} from '../test-support/connection-layer.ts';
import { ConnectionLayer } from './connection-layer.ts';
import type { RequestExecutor } from './executor.ts';

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
