import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { ExecutorHealth, RequestExecutor } from '../shared/executor.ts';
import { FakeTransport } from '../testing/fake-transport.ts';
import type { Transport } from '../transport/types.ts';
import { ConnectionLayer } from './connection-layer.ts';

function stubExecutor(health: () => ExecutorHealth): RequestExecutor {
  return {
    start: () => Promise.resolve(),
    onEvent: () => () => undefined,
    execute: () => Promise.resolve({ ok: true, result: null }),
    cancel: () => Promise.resolve(),
    close: () => Promise.resolve(),
    health,
  };
}

const READY: ExecutorHealth = { state: 'ready', ready: true };

describe('ConnectionLayer health', () => {
  test('reports an idle snapshot before start', () => {
    const layer = new ConnectionLayer({
      transport: new FakeTransport(),
      executor: stubExecutor(() => READY),
    });

    expect(layer.health()).toEqual({
      state: 'idle',
      ready: false,
      listening: false,
      connections: 0,
      executor: { state: 'ready', ready: true },
    });
  });

  test('reports starting while the transport has not started yet', async () => {
    const gate = Promise.withResolvers<void>();
    const transport: Transport = {
      start: () => gate.promise,
      stop: () => Promise.resolve(),
    };
    const layer = new ConnectionLayer({
      transport,
      executor: stubExecutor(() => READY),
    });

    const starting = layer.start();
    expect(layer.health()).toEqual({
      state: 'starting',
      ready: false,
      listening: false,
      connections: 0,
      executor: { state: 'ready', ready: true },
    });
    gate.resolve();
    await starting;
  });

  test('reports ready and the admitted connection count once started', async () => {
    const transport = new FakeTransport();
    const layer = new ConnectionLayer({
      transport,
      executor: stubExecutor(() => READY),
    });

    await layer.start();
    transport.connect('c1');
    transport.connect('c2');

    expect(layer.health()).toEqual({
      state: 'ready',
      ready: true,
      listening: true,
      connections: 2,
      executor: { state: 'ready', ready: true },
    });

    await transport.disconnect('c1');
    expect(layer.health().connections).toBe(1);

    await layer.stop();
  });

  test('reports degraded while the executor is not ready', async () => {
    const transport = new FakeTransport();
    const layer = new ConnectionLayer({
      transport,
      executor: stubExecutor(() => ({ state: 'recovering', ready: false })),
    });

    await layer.start();

    expect(layer.health()).toEqual({
      state: 'degraded',
      ready: false,
      listening: true,
      connections: 0,
      executor: { state: 'recovering', ready: false },
    });

    await layer.stop();
  });

  test('reports an unknown executor instead of throwing or logging on every poll', async () => {
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });
    const transport = new FakeTransport();
    const layer = new ConnectionLayer({
      transport,
      executor: stubExecutor(() => {
        throw new Error('health probe broke');
      }),
      logger,
    });
    await layer.start();

    for (let i = 0; i < 3; i += 1) {
      expect(layer.health()).toEqual({
        state: 'degraded',
        ready: false,
        listening: true,
        connections: 0,
        executor: { state: 'unknown', ready: false },
      });
    }
    expect(lines).toEqual([]);

    await layer.stop();
  });

  test('reports an unknown executor when its self-report is malformed', async () => {
    const layer = new ConnectionLayer({
      transport: new FakeTransport(),
      // Outside code can return anything; a shape the type forbids must not
      // make the probe read a field that is not there.
      executor: stubExecutor(
        () => ({ state: 'ready' }) as unknown as ExecutorHealth,
      ),
    });
    await layer.start();

    expect(layer.health().executor).toEqual({
      state: 'unknown',
      ready: false,
    });

    await layer.stop();
  });

  test('reports stopping while stop is in progress, then stopped', async () => {
    const layer = new ConnectionLayer({
      transport: new FakeTransport(),
      executor: stubExecutor(() => READY),
    });
    await layer.start();

    const stopping = layer.stop();
    expect(layer.health()).toEqual({
      state: 'stopping',
      ready: false,
      listening: false,
      connections: 0,
      executor: { state: 'ready', ready: true },
    });

    await stopping;
    expect(layer.health()).toEqual({
      state: 'stopped',
      ready: false,
      listening: false,
      connections: 0,
      executor: { state: 'ready', ready: true },
    });
  });
});
