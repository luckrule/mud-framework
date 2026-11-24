import { describe, expect, test } from 'bun:test';
import { Buffer } from 'node:buffer';
import { randomBytes } from 'node:crypto';
import net from 'node:net';

import { createJsonLogger } from 'mud-framework-core';
import type {
  Logger,
  TransportConnection,
  TransportHandlers,
} from 'mud-framework-core';

import { WsTransport } from './ws-transport.ts';
import type { WsTransportOptions } from './ws-transport.ts';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** JSON logger writing into an array, so emitted events can be asserted. */
function captureLogger(): {
  logger: ReturnType<typeof createJsonLogger>;
  entries: () => Record<string, unknown>[];
} {
  const lines: string[] = [];
  const logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  return {
    logger,
    entries: () =>
      lines.map((line) => {
        const parsed: unknown = JSON.parse(line);
        if (!isRecord(parsed)) {
          throw new Error(`log line is not a JSON object: ${line}`);
        }
        return parsed;
      }),
  };
}

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

function first<T>(items: readonly T[]): T {
  const item = items[0];
  if (item === undefined) {
    throw new Error('expected at least one item');
  }
  return item;
}

/** A logger whose every method throws, including `child`. */
function throwingLogger(): Logger {
  return {
    debug: failLogging,
    info: failLogging,
    warn: failLogging,
    error: failLogging,
    child: failLogging,
  };
}

function failLogging(): never {
  throw new Error('logger exploded');
}

/** Polls instead of awaiting inside a loop, which the lint config forbids. */
async function until(
  predicate: () => boolean,
  label: string,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const poll = async (): Promise<void> => {
    if (predicate()) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error(`timed out waiting for ${label}`);
    }
    await new Promise<void>((resolve) => {
      setTimeout(resolve, 5);
    });
    return poll();
  };
  return poll();
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

interface Tracked {
  readonly promise: Promise<void>;
  state: () => 'pending' | 'resolved' | 'rejected';
  error: () => Error | undefined;
}

/** Observes a promise without letting its rejection go unhandled. */
function tracked(promise: Promise<void>): Tracked {
  let state: 'pending' | 'resolved' | 'rejected' = 'pending';
  let error: Error | undefined;
  void promise.then(
    () => {
      state = 'resolved';
      return undefined;
    },
    (rejection: unknown) => {
      state = 'rejected';
      error =
        rejection instanceof Error
          ? rejection
          : new Error(`non-Error rejection: ${String(rejection)}`);
      return undefined;
    },
  );
  return { promise, state: () => state, error: () => error };
}

async function settle(): Promise<void> {
  await new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });
}

/** Large enough to fill the kernel and Bun buffers on its own. */
const FILLER_BYTES = 900_000;
/** Enough payloads to overflow the socket buffer even on a paused client. */
const FLOOD_COUNT = 40;

/**
 * Sends until one payload cannot be written. The kernel and Bun buffers hold
 * megabytes before they push back, so a single large send is not enough.
 * Returns the tracked promises in send order.
 */
function floodConnection(connection: TransportConnection): Tracked[] {
  const filler = new Uint8Array(FILLER_BYTES);
  const sent: Tracked[] = [];
  for (let index = 0; index < FLOOD_COUNT; index += 1) {
    sent.push(tracked(connection.send(filler)));
  }
  return sent;
}

/** The oldest unsettled send: the queue head, which settles first. */
function firstPending(sent: readonly Tracked[]): Tracked {
  const pending = sent.find((entry) => entry.state() === 'pending');
  if (pending === undefined) {
    throw new Error('expected at least one pending send');
  }
  return pending;
}

/**
 * A raw TCP client that completes the WebSocket handshake and then, by
 * default, never reads. A paused socket leaves the server's send buffer full,
 * which is the only reliable way to make Bun report backpressure.
 */
function rawClient(port: number): Promise<net.Socket> {
  const key = randomBytes(16).toString('base64');
  return new Promise((resolve, reject) => {
    const socket = net.connect(port, '127.0.0.1', () => {
      socket.write(
        'GET / HTTP/1.1\r\nHost: 127.0.0.1\r\nUpgrade: websocket\r\n' +
          'Connection: Upgrade\r\n' +
          `Sec-WebSocket-Key: ${key}\r\n` +
          'Sec-WebSocket-Version: 13\r\n\r\n',
      );
    });
    socket.pause();
    socket.once('error', () => {
      reject(new Error(`raw client failed to connect to port ${port}`));
    });
    socket.once('connect', () => {
      resolve(socket);
    });
  });
}

function openClient(url: string): Promise<WebSocket> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    socket.addEventListener(
      'open',
      () => {
        resolve(socket);
      },
      { once: true },
    );
    socket.addEventListener(
      'error',
      () => {
        reject(new Error(`failed to connect to ${url}`));
      },
      { once: true },
    );
  });
}

function clientFailure(url: string): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = new WebSocket(url);
    socket.addEventListener(
      'error',
      () => {
        resolve();
      },
      { once: true },
    );
    socket.addEventListener(
      'open',
      () => {
        reject(new Error(`unexpectedly connected to ${url}`));
      },
      { once: true },
    );
  });
}

async function startTransport(
  options: WsTransportOptions = {},
): Promise<{ transport: WsTransport; recorded: Recorded; url: string }> {
  const transport = new WsTransport(options);
  const { handlers, recorded } = recorder();
  await transport.start(handlers);
  const port = transport.port;
  if (port === undefined) {
    throw new Error('expected the transport to bind a port');
  }
  return { transport, recorded, url: `ws://127.0.0.1:${port}/` };
}

async function safeStop(transport: WsTransport): Promise<void> {
  if (transport.port !== undefined) {
    await transport.stop();
  }
}

describe('WsTransport', () => {
  test('starts and stops with a logger whose every method throws', async () => {
    const { transport, recorded, url } = await startTransport({
      logger: throwingLogger(),
    });
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('defaults the message ceiling to one mebibyte', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      const oversize = new Uint8Array(1024 * 1024 + 1);
      const error = await rejectionOf(first(recorded.connected).send(oversize));

      expect(error.message).toMatch(/exceeds the 1048576 byte limit/);
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('has no port before start and a bound port after', async () => {
    const transport = new WsTransport();

    expect(transport.port).toBeUndefined();
    try {
      await transport.start(recorder().handlers);
      expect(transport.port).toBeGreaterThan(0);
    } finally {
      await safeStop(transport);
    }
  });

  test('reports the connection to onConnect', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');
      client.close();

      expect(first(recorded.connected).id).toBeString();
    } finally {
      await safeStop(transport);
    }
  });

  test('delivers a text message as a string', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      client.send('look');
      await until(() => recorded.messages.length === 1, 'onMessage');

      const received = first(recorded.messages);
      expect(received.payload).toBe('look');
      expect(received.connection).toBe(first(recorded.connected));
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('delivers a binary message as a plain Uint8Array', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      client.send(new Uint8Array([1, 2, 255]));
      await until(() => recorded.messages.length === 1, 'onMessage');

      const { payload } = first(recorded.messages);
      if (!(payload instanceof Uint8Array)) {
        throw new Error('expected a binary payload');
      }
      expect(Buffer.isBuffer(payload)).toBe(false);
      expect([...payload]).toEqual([1, 2, 255]);
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

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

  test('reports the close code and reason to onDisconnect', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      client.close(1000, 'bye');
      await until(() => recorded.disconnected.length === 1, 'onDisconnect');

      expect(first(recorded.disconnected).reason).toBe('1000 bye');
    } finally {
      await safeStop(transport);
    }
  });

  test('reports a bare close as its status code', async () => {
    const { transport, recorded, url } = await startTransport();
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      client.close();
      await until(() => recorded.disconnected.length === 1, 'onDisconnect');

      expect(first(recorded.disconnected).reason).toMatch(/^code 100[05]$/);
    } finally {
      await safeStop(transport);
    }
  });

  test('rejects a send above maxMessageBytes', async () => {
    const { transport, recorded, url } = await startTransport({
      maxMessageBytes: 8,
    });
    try {
      const client = await openClient(url);
      await until(() => recorded.connected.length === 1, 'onConnect');

      const error = await rejectionOf(
        first(recorded.connected).send('x'.repeat(9)),
      );

      expect(error.message).toMatch(/exceeds/);
      client.close();
    } finally {
      await safeStop(transport);
    }
  });

  test('closes live connections and refuses work after stop', async () => {
    const { transport, recorded, url } = await startTransport();
    const client = await openClient(url);
    try {
      await until(() => recorded.connected.length === 1, 'onConnect');
      const connection = first(recorded.connected);
      const closed = new Promise<number>((resolve) => {
        client.addEventListener(
          'close',
          (event) => {
            resolve(event.code);
          },
          { once: true },
        );
      });

      await transport.stop();

      expect(await closed).toBe(1001);
      expect(transport.port).toBeUndefined();
      const sendError = await rejectionOf(connection.send('x'));
      expect(sendError.message).toMatch(/stopped/);
      const stopError = await rejectionOf(transport.stop());
      expect(stopError.message).toMatch(/cannot stop/);
    } finally {
      await safeStop(transport);
    }
  });

  test('refuses to stop before start or start twice', async () => {
    const transport = new WsTransport();

    const stopError = await rejectionOf(transport.stop());
    expect(stopError.message).toMatch(/cannot stop/);

    await transport.start(recorder().handlers);
    try {
      const startError = await rejectionOf(
        transport.start(recorder().handlers),
      );
      expect(startError.message).toMatch(/cannot start/);
    } finally {
      await safeStop(transport);
    }
  });

  test('rejects an upgrade for a non-matching path', async () => {
    const { transport, recorded, url } = await startTransport({ path: '/ws' });
    try {
      await clientFailure(url);

      expect(recorded.connected).toHaveLength(0);
    } finally {
      await safeStop(transport);
    }
  });

  test('answers a plain HTTP request with a clear status', async () => {
    const { transport } = await startTransport();
    try {
      const port = transport.port;
      if (port === undefined) {
        throw new Error('expected the transport to bind a port');
      }

      const response = await fetch(`http://127.0.0.1:${port}/`);
      await response.text();

      expect(response.status).toBe(426);
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
