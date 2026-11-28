import { describe, expect, test } from 'bun:test';

import type { TransportConnection } from 'mud-framework-core';
import {
  openClient,
  rejectionOf,
  tick as settle,
} from 'mud-framework-test-support';

import { SEND_BACKLOG_CLOSE_CODE } from './close-codes.ts';
import {
  captureLogger,
  first,
  floodConnection,
  lastPending,
  rawClient,
  safeStop,
  startTransport,
  tracked,
  until,
} from './ws-transport.support.ts';
import { WsTransport } from './ws-transport.ts';

/**
 * A connection registered through the transport's own upgrade path whose socket
 * is a test double, so a test can decide what Bun's `send` returns and read the
 * close the transport asks for.
 *
 * The outbound-overflow close code is only observable this way: the overflow
 * fires because the client's buffer is full, and in that state a real client
 * cannot receive the close frame the transport then writes. This borrows the
 * private callbacks the same way the message tests do.
 */
async function fakeConnection(
  transport: WsTransport,
  write: (payload: string | Uint8Array) => number,
): Promise<{
  connection: TransportConnection;
  closes: { code: number; reason: string }[];
}> {
  const internals = transport as unknown as {
    handleFetch(request: Request, server: unknown): Response | undefined;
    handleOpen(ws: {
      readonly data: { readonly id: string };
      close(code: number, reason: string): void;
      send(payload: string | Uint8Array): number;
    }): void | Promise<void>;
    connections: Map<string, { readonly connection: TransportConnection }>;
  };
  const closes: { code: number; reason: string }[] = [];
  let socket:
    | {
        readonly data: { readonly id: string };
        close(code: number, reason: string): void;
        send(payload: string | Uint8Array): number;
      }
    | undefined;
  const server = {
    // Bun's `upgrade(request, options)` takes `{ data }`, and the transport
    // stores its per-connection record there.
    upgrade: (
      _request: Request,
      options: {
        readonly data: {
          readonly id: string;
          readonly connection: TransportConnection;
        };
      },
    ): boolean => {
      socket = {
        data: options.data,
        send: write,
        close: (code, reason) => {
          closes.push({ code, reason });
        },
      };
      return true;
    },
  };
  internals.handleFetch(
    new Request('http://127.0.0.1/', { headers: { upgrade: 'websocket' } }),
    server,
  );
  if (socket === undefined) {
    throw new Error('expected the transport to upgrade the fake request');
  }
  await internals.handleOpen(socket);
  const connection = [...internals.connections.values()][0]?.connection;
  if (connection === undefined) {
    throw new Error('expected the fake connection to be registered');
  }
  return { connection, closes };
}

/**
 * The flood reaches tens of megabytes. Timeout and disconnect tests need it
 * to sit unsent, so they raise the capacity that would otherwise overflow
 * first; capacity behavior has its own tests below.
 */
const FLOOD_CAPACITY = {
  maxQueuedFrames: 256,
  maxQueuedBytes: 64 * 1024 * 1024,
} as const;

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
      ...FLOOD_CAPACITY,
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
      ...FLOOD_CAPACITY,
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
      // Socket-dependent, but a timed-out write must report a real count.
      expect(warnings[0]?.queueDepth).toBeGreaterThanOrEqual(0);
      expect(Number.isInteger(warnings[0]?.queueDepth)).toBe(true);
    } finally {
      socket.destroy();
      await safeStop(transport);
    }
  });

  test('closes a connection once when the send queue exceeds its byte limit', async () => {
    const { logger, entries } = captureLogger();
    // The queue must hold one maximum-size frame, so the byte limit can only be
    // exceeded by a second frame arriving behind a blocked head. A paused
    // client supplies that backpressure; the frame limit stays out of the way.
    const { transport, recorded } = await startTransport({
      maxQueuedFrames: 256,
      maxQueuedBytes: 1024 * 1024,
      sendTimeoutMs: 10_000,
      logger,
    });
    const port = transport.port;
    if (port === undefined) {
      throw new Error('expected the transport to bind a port');
    }
    const socket = await rawClient(port);
    try {
      await until(() => recorded.connected.length === 1, 'onConnect');
      const connection = first(recorded.connected);
      const flood = [
        ...floodConnection(connection),
        ...floodConnection(connection),
      ];
      await until(
        () =>
          flood.some((entry) =>
            (entry.error()?.message ?? '').includes('send queue overflow'),
          ),
        'the overflow rejection',
      );
      await until(
        () => flood.every((entry) => entry.state() !== 'pending'),
        'the whole queue to settle',
      );

      const overflowed = flood.filter((entry) =>
        (entry.error()?.message ?? '').includes('send queue overflow'),
      );
      // Every frame that was already queued when the limit broke is rejected
      // with the same error, so the count is not the assertion; the single
      // close below is.
      expect(overflowed.length).toBeGreaterThanOrEqual(1);
      expect(overflowed[0]?.error()?.message).toMatch(
        /overflow \(maxQueuedBytes\)/,
      );

      const warnings = entries().filter(
        (entry) => entry.msg === 'send queue overflow; closing connection',
      );
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toMatchObject({
        level: 'warn',
        transport: 'ws',
        connectionId: connection.id,
        limit: 'maxQueuedBytes',
        maxQueuedBytes: 1024 * 1024,
        incomingBytes: 900_000,
        // Only the blocked head is queued; the frame behind it is refused.
        queuedFrames: 1,
        queuedBytes: 900_000,
      });

      const after = await rejectionOf(connection.send('later'));
      expect(after.message).toMatch(/not open|overflow|closed/);
      expect(
        entries().filter(
          (entry) => entry.msg === 'send queue overflow; closing connection',
        ),
      ).toHaveLength(1);
    } finally {
      socket.destroy();
      await safeStop(transport);
    }
  });

  test('closes an overflowing connection with the send-backlog close code', async () => {
    // The code is this transport's choice, not a SPEC MUST: SPEC 5.2 only says
    // an exhausted outbound buffer MAY close with 1013. So the assertion is
    // behavioral — the close the transport requests equals the constant it
    // declares — rather than pinning the literal, which the admission path
    // already pins for SPEC 6.1. A full buffer is what triggers the overflow,
    // and a client in that state cannot read the close frame, so the socket is
    // a double and the assertion is on what the transport asked for.
    const transport = new WsTransport({
      maxQueuedFrames: 1,
      maxQueuedBytes: 1024 * 1024,
      sendTimeoutMs: 10_000,
    });
    await transport.start({
      onConnect: () => undefined,
      onMessage: () => undefined,
      onDisconnect: () => undefined,
    });
    try {
      // `-1` is Bun's "accepted, buffer now full", which holds the head and
      // makes the next frame overflow the one-frame capacity.
      const { connection, closes } = await fakeConnection(transport, () => -1);

      const blocked = tracked(connection.send(new Uint8Array(100)));
      const overflow = tracked(connection.send(new Uint8Array(100)));
      await until(
        () => overflow.state() !== 'pending',
        'the overflowing send to be rejected',
      );

      expect(overflow.error()?.message).toMatch(/send queue overflow/);
      expect(closes).toHaveLength(1);
      expect(closes[0]).toEqual({
        code: SEND_BACKLOG_CLOSE_CODE,
        reason: 'send queue full',
      });
      await blocked.promise.catch(() => undefined);
    } finally {
      await safeStop(transport);
    }
  });

  test('drops a paused client whose unread frames exceed the queue', async () => {
    const { logger, entries } = captureLogger();
    const { transport, recorded } = await startTransport({
      maxQueuedFrames: 1,
      sendTimeoutMs: 10_000,
      logger,
    });
    const port = transport.port;
    if (port === undefined) {
      throw new Error('expected the transport to bind a port');
    }
    const socket = await rawClient(port);
    try {
      await until(() => recorded.connected.length === 1, 'onConnect');
      const connection = first(recorded.connected);
      const filler = new Uint8Array(900_000);
      const flood = [
        ...floodConnection(connection),
        ...floodConnection(connection),
        // One blocked frame plus one more frame is enough to overflow; these
        // two guarantee a send follows the first `-1` whatever index it lands.
        tracked(connection.send(filler)),
        tracked(connection.send(filler)),
      ];
      await until(
        () =>
          flood.some((entry) =>
            (entry.error()?.message ?? '').includes('send queue overflow'),
          ),
        'the overflow rejection',
      );
      await until(
        () => flood.every((entry) => entry.state() !== 'pending'),
        'the whole queue to settle',
      );

      const warnings = entries().filter(
        (entry) => entry.msg === 'send queue overflow; closing connection',
      );
      expect(warnings).toHaveLength(1);
      expect(warnings[0]).toMatchObject({
        level: 'warn',
        transport: 'ws',
        connectionId: connection.id,
        limit: 'maxQueuedFrames',
        maxQueuedFrames: 1,
        queuedFrames: 1,
        queuedBytes: 900_000,
      });

      const after = await rejectionOf(connection.send('later'));
      expect(after.message).toMatch(/not open|overflow|closed/);
    } finally {
      socket.destroy();
      await safeStop(transport);
    }
  });

  test('rejects queued sends when the client disconnects', async () => {
    const { transport, recorded } = await startTransport({
      sendTimeoutMs: 10_000,
      ...FLOOD_CAPACITY,
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
      // The queue tail, not the head: Bun accepts the frame that fills the
      // socket buffer and holds it until `drain`, and a disconnect can deliver
      // `drain` before `close`, so the head may resolve instead of reject.
      // Everything behind it can only be written after the head clears.
      const pending = flood.filter((entry) => entry.state() === 'pending');
      // A lone pending frame could be that written head, which `drain` may
      // release; the sample is only sound with more queued behind it.
      expect(pending.length).toBeGreaterThan(1);
      const blocked = lastPending(flood);
      expect(blocked.state()).toBe('pending');

      socket.destroy();
      await until(() => blocked.state() !== 'pending', 'the queued send');
      // A resolve would mean the frame reached the socket before the
      // disconnect, so it could not show the close draining the queue.
      expect(blocked.state()).toBe('rejected');

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
