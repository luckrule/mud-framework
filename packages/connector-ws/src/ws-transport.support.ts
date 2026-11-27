import { randomBytes } from 'node:crypto';
import net from 'node:net';

import { createJsonLogger } from 'mud-framework-core';
import type {
  TransportConnection,
  TransportHandlers,
} from 'mud-framework-core';
import { parseJsonLine } from 'mud-framework-test-support';

import { WsTransport } from './ws-transport.ts';
import type { WsTransportOptions } from './ws-transport.ts';

/** JSON logger writing into an array, so emitted events can be asserted. */
export function captureLogger(): {
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
    entries: () => lines.map((line) => parseJsonLine(line, 'log line')),
  };
}

export interface Recorded {
  connected: TransportConnection[];
  messages: { connection: TransportConnection; payload: string | Uint8Array }[];
  disconnected: { connection: TransportConnection; reason: string }[];
}

export function recorder(): {
  handlers: TransportHandlers;
  recorded: Recorded;
} {
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

export function first<T>(items: readonly T[]): T {
  const item = items[0];
  if (item === undefined) {
    throw new Error('expected at least one item');
  }
  return item;
}

/** Polls instead of awaiting inside a loop, which the lint config forbids. */
export async function until(
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

export interface Tracked {
  readonly promise: Promise<void>;
  state: () => 'pending' | 'resolved' | 'rejected';
  error: () => Error | undefined;
}

/** Observes a promise without letting its rejection go unhandled. */
export function tracked(promise: Promise<void>): Tracked {
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

/**
 * Records process-level escapes a transport callback must never cause. Bun
 * terminates on an unhandled rejection or uncaught exception, so the test
 * installs listeners itself: they both observe and prevent that exit.
 */
export function captureProcessFailures(): {
  unhandledRejections: unknown[];
  uncaughtExceptions: unknown[];
  stop: () => void;
} {
  const unhandledRejections: unknown[] = [];
  const uncaughtExceptions: unknown[] = [];
  const onRejection = (reason: unknown): void => {
    unhandledRejections.push(reason);
  };
  const onException = (error: unknown): void => {
    uncaughtExceptions.push(error);
  };
  process.on('unhandledRejection', onRejection);
  process.on('uncaughtException', onException);
  return {
    unhandledRejections,
    uncaughtExceptions,
    stop: () => {
      process.off('unhandledRejection', onRejection);
      process.off('uncaughtException', onException);
    },
  };
}

/** Just enough of a server socket to drive a private transport callback. */
export interface FakeSocket {
  readonly data: { readonly id: string };
  close(code: number, reason: string): void;
}

export function closeRecorder(id: string): {
  ws: FakeSocket;
  closes: { code: number; reason: string }[];
} {
  const closes: { code: number; reason: string }[] = [];
  return {
    closes,
    ws: {
      data: { id },
      close: (code, reason) => {
        closes.push({ code, reason });
      },
    },
  };
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
export function floodConnection(connection: TransportConnection): Tracked[] {
  const filler = new Uint8Array(FILLER_BYTES);
  const sent: Tracked[] = [];
  for (let index = 0; index < FLOOD_COUNT; index += 1) {
    sent.push(tracked(connection.send(filler)));
  }
  return sent;
}

/**
 * The newest unsettled send: the queue tail. It is the frame most likely to
 * still be unsent — every payload ahead of it must clear first — so a test
 * that disconnects the client can rely on it staying queued. The queue head is
 * a poor choice: Bun accepts the frame that fills the buffer (`-1`) and holds
 * it until `drain`, and a disconnect can deliver `drain` before `close`, which
 * resolves that frame instead of rejecting it.
 */
export function lastPending(sent: readonly Tracked[]): Tracked {
  const pending = sent.findLast((entry) => entry.state() === 'pending');
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
export function rawClient(port: number): Promise<net.Socket> {
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

export function clientFailure(url: string): Promise<void> {
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

export async function startTransport(
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

export async function safeStop(transport: WsTransport): Promise<void> {
  if (transport.port !== undefined) {
    await transport.stop();
  }
}
