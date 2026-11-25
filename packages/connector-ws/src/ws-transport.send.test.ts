import { describe, expect, test } from 'bun:test';

import {
  openClient,
  rejectionOf,
  tick as settle,
} from 'mud-framework-test-support';

import {
  captureLogger,
  first,
  firstPending,
  floodConnection,
  rawClient,
  safeStop,
  startTransport,
  tracked,
  until,
} from './ws-transport.support.ts';

describe('WsTransport sends', () => {
  test('sends text and binary payloads the client receives', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');
      const received: unknown[] = [];
      client.addEventListener('message', (event) => {
        const data: unknown = event.data;
        received.push(data);
      });

      const connection = first(recorded.connected);
      await connection.send('hello');
      await connection.send(new Uint8Array([7, 8]));
      await until(() => received.length === 2, 'two client messages');

      expect(received[0]).toBe('hello');
      const binary = received[1];
      if (!(binary instanceof Uint8Array)) {
        throw new Error('expected a binary frame');
      }
      expect([...binary]).toEqual([7, 8]);
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('resolves a send immediately when the socket buffer has room', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');
      const received: unknown[] = [];
      client.addEventListener('message', (event) => {
        received.push(event.data);
      });

      const send = tracked(first(recorded.connected).send('quick'));
      await settle();

      expect(send.state()).toBe('resolved');
      await until(() => received.length === 1, 'one client message');
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('holds the send of a client that never reads, then drops it on timeout', async () => {
    const { transport, recorded } = await startTransport({
      sendTimeoutMs: 250,
    });
    const port = transport.port;
    if (port === undefined) {
      throw new Error('expected the transport to bind a port');
    }
    const socket = await rawClient(port);
    try {
      await until(() => recorded.connected.length === 1, 'onConnect');
      const flood = floodConnection(first(recorded.connected));
      await settle();
      // A paused client stops the kernel absorbing payloads, so some send is
      // still waiting on the socket.
      expect(flood.some((entry) => entry.state() === 'pending')).toBe(true);

      // The timeout drops the connection; nothing else closes this client.
      await until(() => recorded.disconnected.length === 1, 'onDisconnect');
      expect(first(recorded.disconnected).reason).toMatch(/1006/);
      // Every queued promise settles too, instead of hanging on a dead socket.
      await until(
        () => flood.every((entry) => entry.state() !== 'pending'),
        'the whole queue to settle',
      );

      const timedOut = flood.filter((entry) =>
        (entry.error()?.message ?? '').includes('timed out after 250ms'),
      );
      expect(timedOut.length).toBeGreaterThanOrEqual(1);
      const after = await rejectionOf(first(recorded.connected).send('later'));
      expect(after.message).toMatch(/not open|closed/);
    } finally {
      socket.destroy();
      await safeStop(transport);
    }
  });

  test('logs a send timeout at warn with the timeout and queue depth', async () => {
    const { logger, entries } = captureLogger();
    const { transport, recorded } = await startTransport({
      sendTimeoutMs: 250,
      logger,
    });
    const port = transport.port;
    if (port === undefined) {
      throw new Error('expected the transport to bind a port');
    }
    const socket = await rawClient(port);
    try {
      await until(() => recorded.connected.length === 1, 'onConnect');
      const connectionId = first(recorded.connected).id;
      floodConnection(first(recorded.connected));
      await settle();

      await until(() => recorded.disconnected.length === 1, 'onDisconnect');

      const warnings = entries().filter(
        (entry) => entry.msg === 'send timed out; terminating connection',
      );
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toMatchObject({
        level: 'warn',
        transport: 'ws',
        connectionId,
        sendTimeoutMs: 250,
      });
      expect(typeof warnings[0]?.queueDepth).toBe('number');
    } finally {
      socket.destroy();
      await safeStop(transport);
    }
  });

  test('rejects queued sends when the client disconnects', async () => {
    const { transport, recorded } = await startTransport({
      sendTimeoutMs: 10_000,
    });
    const port = transport.port;
    if (port === undefined) {
      throw new Error('expected the transport to bind a port');
    }
    const socket = await rawClient(port);
    try {
      await until(() => recorded.connected.length === 1, 'onConnect');
      const flood = floodConnection(first(recorded.connected));
      await settle();
      const blocked = firstPending(flood);
      expect(blocked.state()).toBe('pending');

      socket.destroy();
      await until(() => blocked.state() !== 'pending', 'the blocked send');

      const error = blocked.error();
      expect(error?.message).toMatch(/closed/);
      // The distinguishing detail: a disconnect is not a timeout.
      expect(error?.message).not.toMatch(/timed out/);
    } finally {
      socket.destroy();
      await safeStop(transport);
    }
  });
});
