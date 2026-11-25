import { describe, expect, test } from 'bun:test';

import type { RequestExecutor } from '../shared/executor.ts';
import { parseFrame, throwingLogger } from '../testing/common.ts';
import { lastFrame } from '../testing/connection-layer.ts';
import { FakeTransport } from '../testing/fake-transport.ts';
import { ConnectionLayer } from './connection-layer.ts';

function immediateExecutor(
  execute: RequestExecutor['execute'],
): RequestExecutor {
  return {
    start: () => Promise.resolve(),
    onEvent: () => () => undefined,
    execute,
    cancel: () => Promise.resolve(),
    close: () => Promise.resolve(),
    health: () => ({ state: 'ready', ready: true }),
  };
}

describe('ConnectionLayer logger safety', () => {
  test('replies with the unchanged failure when the logger throws', async () => {
    const transport = new FakeTransport();
    const layer = new ConnectionLayer({
      transport,
      executor: immediateExecutor(() =>
        Promise.resolve({
          ok: false,
          error: { code: 'PLAYER_NOT_FOUND', message: 'no such player' },
        }),
      ),
      logger: throwingLogger(),
    });
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"echo"}');

    expect(lastFrame(transport, 'c1')).toEqual({
      id: 'r1',
      ok: false,
      error: { code: 'PLAYER_NOT_FOUND', message: 'no such player' },
    });
    await layer.stop();
  }, 2000);

  test('refuses a full queue with the same code when the logger throws', async () => {
    const transport = new FakeTransport();
    const layer = new ConnectionLayer({
      transport,
      executor: immediateExecutor(() =>
        Promise.resolve({ ok: true, result: 'never runs' }),
      ),
      logger: throwingLogger(),
      maxQueuedPerConnection: 1,
    });
    await layer.start();
    transport.connect('c1');

    // The first request still occupies the executor's turn when the second
    // arrives, so the second is the one refused.
    const first = transport.receive('c1', '{"id":"r1","method":"echo"}');
    const second = transport.receive('c1', '{"id":"r2","method":"echo"}');
    await Promise.all([first, second]);

    expect(transport.sentTo('c1').map(parseFrame)).toContainEqual({
      id: 'r2',
      ok: false,
      error: {
        code: 'FRAMEWORK_QUEUE_FULL',
        message: 'input queue is full (1 waiting)',
      },
    });
    await layer.stop();
  }, 2000);
});
