import { describe, expect, test } from 'bun:test';

import type { RequestExecutor } from '../shared/executor.ts';
import { rejectionOf } from '../testing/common.ts';
import { harness, scriptedExecutor } from '../testing/connection-layer.ts';
import type { Transport } from '../transport/types.ts';
import { ConnectionLayer } from './connection-layer.ts';

describe('ConnectionLayer lifecycle', () => {
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
