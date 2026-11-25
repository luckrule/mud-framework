import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import { Dispatcher } from '../server/dispatcher.ts';
import { LocalExecutor } from '../server/local-executor.ts';
import type { RequestExecutor } from '../shared/executor.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf } from '../testing/common.ts';
import { harness, scriptedExecutor } from '../testing/connection-layer.ts';
import { FakeTransport } from '../transport/fake.ts';
import type { Transport } from '../transport/types.ts';
import { ConnectionLayer } from './connection-layer.ts';

describe('ConnectionLayer lifecycle', () => {
  test('a transport that finishes starting after stop is released once and never resurrects the layer', async () => {
    const listener = new FakeTransport();
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let stops = 0;
    const logger = createJsonLogger({ sink: () => undefined });
    const dispatcher = new Dispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
      logger,
    });
    const layer = new ConnectionLayer({
      executor: new LocalExecutor({ dispatcher }),
      transport: {
        start: async (handlers) => {
          entered.resolve();
          await release.promise;
          await listener.start(handlers);
        },
        stop: async () => {
          stops += 1;
          await listener.stop();
        },
      },
      shutdownTimeoutMs: 10,
      logger,
    });
    const starting = layer.start().then(
      () => undefined,
      (error: unknown) => error,
    );
    await entered.promise;

    try {
      await layer.stop();
      release.resolve();
      const startError = await starting;
      expect(() => listener.connect('late')).toThrow(/stopped/);
      expect(startError).toBeInstanceOf(Error);
      await layer.stop();
      expect(stops).toBe(1);
    } finally {
      release.resolve();
      await starting;
      await listener.stop().catch(() => undefined);
    }
  });

  test('stop reports a timeout but still closes late executor readiness without starting a listener afterwards', async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const listener = new FakeTransport();
    const logger = createJsonLogger({ sink: () => undefined });
    const dispatcher = new Dispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
      logger,
    });
    const local = new LocalExecutor({ dispatcher });
    let ready = false;
    let closedBeforeReady = false;
    let closes = 0;
    let listenerStarts = 0;
    const layer = new ConnectionLayer({
      executor: {
        start: async () => {
          entered.resolve();
          await release.promise;
          await local.start();
          ready = true;
        },
        execute: (request) => local.execute(request),
        cancel: (requestId, code) => local.cancel(requestId, code),
        close: async () => {
          closes += 1;
          closedBeforeReady = !ready;
          await local.close();
        },
      },
      transport: {
        start: (handlers) => {
          listenerStarts += 1;
          return listener.start(handlers);
        },
        stop: () => listener.stop(),
      },
      shutdownTimeoutMs: 10,
      logger,
    });
    const starting = layer.start().then(
      () => undefined,
      (error: unknown) => error,
    );
    await entered.promise;

    try {
      const stopError = await rejectionOf(layer.stop());
      expect(stopError.message).toMatch(/executor close.*shutdown timeout/);
      release.resolve();
      expect(await starting).toBeInstanceOf(Error);
      expect(await rejectionOf(layer.stop())).toBe(stopError);
      expect(closedBeforeReady).toBe(false);
      expect(closes).toBe(1);
      expect(listenerStarts).toBe(0);
      expect(() => listener.connect('late')).toThrow(/not started/);
      const error = await rejectionOf(dispatcher.dispatch('missing', null));
      expect(error.message).toMatch(/closed/);
    } finally {
      release.resolve();
      await starting;
      await listener.stop().catch(() => undefined);
    }
  });

  test('late transport start rejection remains observable without repeating cleanup or allowing a concurrent start', async () => {
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const failure = new Error('late listener startup failed');
    const { executor, closeCalls } = scriptedExecutor([]);
    let stops = 0;
    const layer = new ConnectionLayer({
      executor,
      transport: {
        start: async () => {
          entered.resolve();
          await release.promise;
          throw failure;
        },
        stop: async () => {
          stops += 1;
        },
      },
      shutdownTimeoutMs: 10,
      logger: createJsonLogger({ sink: () => undefined }),
    });
    const starting = layer.start();
    const rejected = rejectionOf(starting);
    await entered.promise;
    const concurrent = await rejectionOf(layer.start());
    expect(concurrent.message).toMatch(/cannot start while starting/);
    await layer.stop();
    release.resolve();

    expect(await rejected).toBe(failure);
    await layer.stop();
    expect(closeCalls()).toBe(1);
    expect(stops).toBe(0);
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
});
