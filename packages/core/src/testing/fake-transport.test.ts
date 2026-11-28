import { describe, expect, test } from 'bun:test';

import { rejectionOf } from 'mud-framework-test-support';

import type {
  TransportConnection,
  TransportHandlers,
} from '../transport/types.ts';
import { FakeTransport } from './fake-transport.ts';

function recorder() {
  const connected: TransportConnection[] = [];
  const messages: {
    connection: TransportConnection;
    payload: string | Uint8Array;
  }[] = [];
  const disconnected: {
    connection: TransportConnection;
    reason: string;
  }[] = [];
  const handlers: TransportHandlers = {
    onConnect: (connection) => {
      connected.push(connection);
    },
    onMessage: (connection, payload) => {
      messages.push({ connection, payload });
    },
    onDisconnect: (connection, reason) => {
      disconnected.push({ connection, reason });
    },
  };
  return { handlers, connected, messages, disconnected };
}

describe('FakeTransport', () => {
  test('reports connections, payloads, disconnects and closes to the handlers', async () => {
    const transport = new FakeTransport();
    const { handlers, connected, messages, disconnected } = recorder();
    await transport.start(handlers);

    const connection = transport.connect('c1');
    expect(connection.id).toBe('c1');
    expect(connected).toEqual([connection]);

    const binary = new Uint8Array([0, 1, 255]);
    await transport.receive('c1', 'look');
    await transport.receive('c1', binary);
    expect(messages.map((entry) => entry.payload)).toEqual(['look', binary]);
    // Binary payloads are handed over by identity, not copied.
    expect(messages[1]?.payload).toBe(binary);

    await transport.disconnect('c1', 'client quit');
    expect(disconnected).toEqual([{ connection, reason: 'client quit' }]);

    const closing = transport.connect('c2');
    await closing.close(1000, 'normal');
    expect(transport.closedWith('c2')).toEqual({
      code: 1000,
      reason: 'normal',
    });
    expect(disconnected[1]).toEqual({ connection: closing, reason: 'normal' });
  });

  test('records what the framework sends and holds one write in flight', async () => {
    const transport = new FakeTransport();
    await transport.start(recorder().handlers);
    const connection = transport.connect('c1');
    const frame = new Uint8Array([1, 2]);

    await connection.send('hello');
    const held = transport.holdSends('c1');
    const sending = connection.send(frame);
    await held.entered;

    // The frame is recorded on entry, so "write started" is observable while
    // the send is still pending.
    expect(transport.sentTo('c1')).toEqual(['hello', frame]);
    let settled = false;
    void sending.then(() => {
      settled = true;
      return undefined;
    });
    await Promise.resolve();
    expect(settled).toBe(false);

    held.release();
    await sending;
    expect(settled).toBe(true);
  });

  test('enforces the start/stop state machine and one id per connection', async () => {
    const transport = new FakeTransport();
    const { handlers } = recorder();

    expect(() => transport.connect('c1')).toThrow(/not started/);
    expect(() => transport.receive('c1', 'x')).toThrow(/not started/);
    expect(() => transport.disconnect('c1')).toThrow(/not started/);
    expect(() => transport.sentTo('c1')).toThrow(/not started/);
    expect((await rejectionOf(transport.stop())).message).toMatch(
      /cannot stop/,
    );

    await transport.start(handlers);
    expect((await rejectionOf(transport.start(handlers))).message).toMatch(
      /cannot start/,
    );

    const connection = transport.connect('c1');
    expect(() => transport.connect('c1')).toThrow(/already exists/);
    expect(() => transport.receive('nope', 'x')).toThrow(/unknown connection/);

    await transport.stop();
    expect(() => transport.connect('c2')).toThrow(/stopped/);
    expect(() => transport.receive('c1', 'x')).toThrow(/stopped/);
    expect(() => transport.disconnect('c1')).toThrow(/stopped/);
    const sendError = await rejectionOf(connection.send('x'));
    expect(sendError.message).toMatch(/stopped/);
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

  test('propagates a handler throw or rejection to the caller', async () => {
    const throwing = new FakeTransport();
    await throwing.start({
      onConnect: () => undefined,
      onMessage: () => {
        throw new Error('handler exploded');
      },
      onDisconnect: () => undefined,
    });
    throwing.connect('c1');

    expect(() => throwing.receive('c1', 'boom')).toThrow(/handler exploded/);

    const rejecting = new FakeTransport();
    await rejecting.start({
      onConnect: () => undefined,
      onMessage: async () => {
        throw new Error('async exploded');
      },
      onDisconnect: () => undefined,
    });
    rejecting.connect('c1');

    const error = await rejectionOf(rejecting.receive('c1', 'boom'));

    expect(error.message).toBe('async exploded');
  });
});
