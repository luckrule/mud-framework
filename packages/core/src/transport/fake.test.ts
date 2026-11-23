import { describe, expect, test } from 'bun:test';

import { FakeTransport } from './fake.ts';
import type { TransportConnection, TransportHandlers } from './types.ts';

interface Recorded {
  connected: TransportConnection[];
  messages: { connection: TransportConnection; payload: string | Uint8Array }[];
  disconnected: { connection: TransportConnection; reason: string }[];
}

function recorder(): { handlers: TransportHandlers; recorded: Recorded } {
  const recorded: Recorded = { connected: [], messages: [], disconnected: [] };
  return {
    recorded,
    handlers: {
      onConnect: (connection) => {
        recorded.connected.push(connection);
      },
      onMessage: (connection, payload) => {
        recorded.messages.push({ connection, payload });
      },
      onDisconnect: (connection, reason) => {
        recorded.disconnected.push({ connection, reason });
      },
    },
  };
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

describe('FakeTransport', () => {
  test('declares the weakest honest capabilities by default', () => {
    const transport = new FakeTransport();

    expect(transport.capabilities()).toEqual({
      binaryFrames: false,
      fragmentation: false,
      maxMessageBytes: 1024,
      backpressure: false,
      orderedDelivery: true,
      heartbeat: 'none',
    });
  });

  test('lets tests inject capabilities', () => {
    const transport = new FakeTransport({
      capabilities: {
        binaryFrames: true,
        maxMessageBytes: 1024,
        heartbeat: 'protocol',
      },
    });

    const declared = transport.capabilities();

    expect(declared.binaryFrames).toBe(true);
    expect(declared.maxMessageBytes).toBe(1024);
    expect(declared.heartbeat).toBe('protocol');
    expect(declared.fragmentation).toBe(false);
    expect(declared.backpressure).toBe(false);
  });

  test('reports a connection to onConnect after start', async () => {
    const transport = new FakeTransport();
    const { handlers, recorded } = recorder();

    await transport.start(handlers);
    const connection = transport.connect('c1');

    expect(connection.id).toBe('c1');
    expect(recorded.connected).toEqual([connection]);
  });

  test('delivers string payloads unchanged to onMessage', async () => {
    const transport = new FakeTransport();
    const { handlers, recorded } = recorder();
    await transport.start(handlers);
    transport.connect('c1');

    await transport.receive('c1', 'look');

    expect(recorded.messages).toHaveLength(1);
    expect(recorded.messages[0]?.payload).toBe('look');
    expect(recorded.messages[0]?.connection.id).toBe('c1');
  });

  test('delivers binary payloads by identity to onMessage', async () => {
    const transport = new FakeTransport();
    const { handlers, recorded } = recorder();
    await transport.start(handlers);
    transport.connect('c1');
    const payload = new Uint8Array([0, 1, 255]);

    await transport.receive('c1', payload);

    expect(recorded.messages[0]?.payload).toBe(payload);
  });

  test('awaits an async onMessage handler', async () => {
    const transport = new FakeTransport();
    const seen: string[] = [];
    await transport.start({
      onConnect: () => undefined,
      onMessage: async (_connection, payload) => {
        await Promise.resolve();
        seen.push(String(payload));
      },
      onDisconnect: () => undefined,
    });
    transport.connect('c1');

    await transport.receive('c1', 'north');

    expect(seen).toEqual(['north']);
  });

  test('reports the disconnect reason to onDisconnect', async () => {
    const transport = new FakeTransport();
    const { handlers, recorded } = recorder();
    await transport.start(handlers);
    transport.connect('c1');

    await transport.disconnect('c1', 'client quit');

    expect(recorded.disconnected).toHaveLength(1);
    expect(recorded.disconnected[0]?.reason).toBe('client quit');
  });

  test('connection.close routes to onDisconnect', async () => {
    const transport = new FakeTransport();
    const { handlers, recorded } = recorder();
    await transport.start(handlers);
    const connection = transport.connect('c1');

    await connection.close(1000, 'normal');

    expect(recorded.disconnected[0]?.reason).toBe('normal');
  });

  test('records what the framework sends', async () => {
    const transport = new FakeTransport();
    await transport.start(recorder().handlers);
    const connection = transport.connect('c1');
    const frame = new Uint8Array([1, 2]);

    await connection.send('hello');
    await connection.send(frame);

    expect(transport.sentTo('c1')).toEqual(['hello', frame]);
  });

  test('rejects helper calls before start', () => {
    const transport = new FakeTransport();

    expect(() => transport.connect('c1')).toThrow(/not started/);
    expect(() => transport.receive('c1', 'x')).toThrow(/not started/);
    expect(() => transport.disconnect('c1')).toThrow(/not started/);
    expect(() => transport.sentTo('c1')).toThrow(/not started/);
  });

  test('rejects helper calls after stop', async () => {
    const transport = new FakeTransport();
    await transport.start(recorder().handlers);
    const connection = transport.connect('c1');
    await transport.stop();

    expect(() => transport.connect('c2')).toThrow(/stopped/);
    expect(() => transport.receive('c1', 'x')).toThrow(/stopped/);
    expect(() => transport.disconnect('c1')).toThrow(/stopped/);
    const sendError = await rejectionOf(connection.send('x'));
    expect(sendError.message).toMatch(/stopped/);
  });

  test('refuses to start twice or stop before start', async () => {
    const transport = new FakeTransport();

    const stopError = await rejectionOf(transport.stop());
    expect(stopError.message).toMatch(/cannot stop/);
    await transport.start(recorder().handlers);

    const startError = await rejectionOf(transport.start(recorder().handlers));
    expect(startError.message).toMatch(/cannot start/);
  });

  test('rejects unknown or duplicate connection ids', async () => {
    const transport = new FakeTransport();
    await transport.start(recorder().handlers);
    transport.connect('c1');

    expect(() => transport.connect('c1')).toThrow(/already exists/);
    expect(() => transport.receive('nope', 'x')).toThrow(/unknown connection/);
  });

  test('propagates a synchronous handler throw to the caller', async () => {
    const transport = new FakeTransport();
    await transport.start({
      onConnect: () => undefined,
      onMessage: () => {
        throw new Error('handler exploded');
      },
      onDisconnect: () => undefined,
    });
    transport.connect('c1');

    expect(() => transport.receive('c1', 'boom')).toThrow(/handler exploded/);
  });

  test('propagates an async handler rejection to the caller', async () => {
    const transport = new FakeTransport();
    await transport.start({
      onConnect: () => undefined,
      onMessage: async () => {
        throw new Error('async exploded');
      },
      onDisconnect: () => undefined,
    });
    transport.connect('c1');

    const error = await rejectionOf(transport.receive('c1', 'boom'));

    expect(error.message).toBe('async exploded');
  });

  test('waits for in-flight handler work on stop', async () => {
    const transport = new FakeTransport();
    const seen: string[] = [];
    await transport.start({
      onConnect: () => undefined,
      onMessage: async () => {
        await Promise.resolve();
        seen.push('done');
      },
      onDisconnect: () => undefined,
    });
    transport.connect('c1');

    void transport.receive('c1', 'x');
    await transport.stop();

    expect(seen).toEqual(['done']);
  });
});
