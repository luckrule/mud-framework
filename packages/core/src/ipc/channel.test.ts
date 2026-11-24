import { describe, expect, test } from 'bun:test';

import type { IpcChannel } from './channel.ts';
import { createInMemoryChannelPair, IpcChannelClosedError } from './channel.ts';
import type { IpcMessage } from './protocol.ts';

const hello: IpcMessage = { type: 'hello', portalGeneration: 1 };
const shutdown: IpcMessage = { type: 'shutdown', timeoutMs: 5000 };

function receive(channel: IpcChannel): unknown[] {
  const received: unknown[] = [];
  channel.onMessage((message) => {
    received.push(message);
  });
  return received;
}

describe('createInMemoryChannelPair', () => {
  test('carries messages from A to B', () => {
    const [a, b] = createInMemoryChannelPair();
    const received = receive(b);

    a.send(hello);

    expect(received).toEqual([hello]);
  });

  test('carries messages from B to A', () => {
    const [a, b] = createInMemoryChannelPair();
    const received = receive(a);

    b.send(shutdown);

    expect(received).toEqual([shutdown]);
  });

  test('preserves order and does not echo to the sender', () => {
    const [a, b] = createInMemoryChannelPair();
    const receivedA = receive(a);
    const receivedB = receive(b);

    a.send(hello);
    a.send(shutdown);

    expect(receivedB).toEqual([hello, shutdown]);
    expect(receivedA).toEqual([]);
  });

  test('keeps message identity intact', () => {
    const [a, b] = createInMemoryChannelPair();
    const received = receive(b);
    const message: IpcMessage = {
      type: 'result',
      requestId: 'req-1',
      ok: false,
      error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
    };

    a.send(message);

    expect(received[0]).toBe(message);
  });

  test('queues messages that arrive before the handler is registered', () => {
    const [a, b] = createInMemoryChannelPair();

    a.send(hello);
    a.send(shutdown);
    const received = receive(b);

    expect(received).toEqual([hello, shutdown]);
  });

  test('keeps undelivered messages when a replaying handler throws', () => {
    const [a, b] = createInMemoryChannelPair();
    a.send(hello);
    a.send(shutdown);

    expect(() => {
      b.onMessage(() => {
        throw new Error('handler is broken');
      });
    }).toThrow(/handler is broken/);

    const received = receive(b);

    expect(received).toEqual([hello, shutdown]);
  });

  test('stops replaying once the handler closes the channel', () => {
    const [a, b] = createInMemoryChannelPair();
    a.send(hello);
    a.send(shutdown);
    const received: unknown[] = [];

    b.onMessage((message) => {
      received.push(message);
      b.close('closed during replay');
    });

    expect(received).toEqual([hello]);
  });

  test('replaces the previous handler when onMessage is called twice', () => {
    const [a, b] = createInMemoryChannelPair();
    const first: unknown[] = [];
    const second: unknown[] = [];
    b.onMessage((message) => {
      first.push(message);
    });

    b.onMessage((message) => {
      second.push(message);
    });
    a.send(hello);

    expect(first).toEqual([]);
    expect(second).toEqual([hello]);
  });

  test('notifies the peer when one side closes', () => {
    const [a, b] = createInMemoryChannelPair();
    const reasons: string[] = [];
    b.onClose((reason) => {
      reasons.push(reason);
    });

    a.close('portal shutting down');

    expect(reasons).toEqual(['portal shutting down']);
  });

  test('does not notify the side that closed', () => {
    const [a, b] = createInMemoryChannelPair();
    let closedLocally = false;
    a.onClose(() => {
      closedLocally = true;
    });
    receive(b);

    a.close('going away');

    expect(closedLocally).toBe(false);
  });

  test('fires onClose only once even when both sides close', () => {
    const [a, b] = createInMemoryChannelPair();
    const reasonsB: string[] = [];
    const reasonsA: string[] = [];
    b.onClose((reason) => {
      reasonsB.push(reason);
    });
    a.onClose((reason) => {
      reasonsA.push(reason);
    });

    a.close('first');
    a.close('second');
    b.close('third');

    expect(reasonsB).toEqual(['first']);
    expect(reasonsA).toEqual([]);
  });

  test('reports the close to a handler registered after the close', () => {
    const [a, b] = createInMemoryChannelPair();

    a.close('gone');
    const reasons: string[] = [];
    b.onClose((reason) => {
      reasons.push(reason);
    });

    expect(reasons).toEqual(['gone']);
  });

  test('supplies a reason when close is called without one', () => {
    const [a, b] = createInMemoryChannelPair();
    const reasons: string[] = [];
    b.onClose((reason) => {
      reasons.push(reason);
    });

    a.close();

    expect(reasons).toHaveLength(1);
    expect(reasons[0]).toBe('closed by local side');
  });

  test('throws from send after the local side closed', () => {
    const [a] = createInMemoryChannelPair();

    a.close('done');

    expect(() => {
      a.send(hello);
    }).toThrow(IpcChannelClosedError);
  });

  test('throws from send after the peer closed', () => {
    const [a, b] = createInMemoryChannelPair();

    b.close('server died');

    expect(() => {
      a.send(hello);
    }).toThrow(IpcChannelClosedError);
  });

  test('names the close reason in the send error', () => {
    const [a, b] = createInMemoryChannelPair();
    b.close('server died');

    expect(() => {
      a.send(hello);
    }).toThrow(/server died/);
  });

  test('drops messages sent after close instead of delivering them', () => {
    const [a, b] = createInMemoryChannelPair();
    const received = receive(b);
    a.close('done');

    expect(() => {
      a.send(hello);
    }).toThrow(IpcChannelClosedError);
    expect(received).toEqual([]);
  });

  test('makes close idempotent', () => {
    const [a, b] = createInMemoryChannelPair();
    const reasons: string[] = [];
    b.onClose((reason) => {
      reasons.push(reason);
    });

    a.close('once');
    a.close('twice');

    expect(reasons).toEqual(['once']);
  });

  test('lets the close handler close its own side without recursing', () => {
    const [a, b] = createInMemoryChannelPair();
    let notifications = 0;
    a.onClose(() => {
      notifications += 1;
      a.close('acknowledged');
    });

    b.close('server died');

    expect(notifications).toBe(1);
    expect(() => {
      a.send(hello);
    }).toThrow(IpcChannelClosedError);
  });
});
