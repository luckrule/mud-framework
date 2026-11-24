import { describe, expect, test } from 'bun:test';

import { throwingLogger } from '../test-support/common.ts';
import { lastFrame } from '../test-support/connection-layer.ts';
import { FakeTransport } from '../transport/fake.ts';
import { ConnectionLayer } from './connection-layer.ts';
import type { RequestExecutor } from './executor.ts';

function immediateExecutor(
  execute: RequestExecutor['execute'],
): RequestExecutor {
  return {
    start: () => Promise.resolve(),
    execute,
    cancel: () => Promise.resolve(),
    close: () => Promise.resolve(),
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
      maxQueuedPerConnection: 0,
    });
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"r1","method":"echo"}');

    expect(lastFrame(transport, 'c1')).toEqual({
      id: 'r1',
      ok: false,
      error: {
        code: 'FRAMEWORK_QUEUE_FULL',
        message: 'input queue is full (0 waiting)',
      },
    });
    await layer.stop();
  }, 2000);
});
