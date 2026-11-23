import { Buffer } from 'node:buffer';
import { randomUUID } from 'node:crypto';

import type {
  Transport,
  TransportCapabilities,
  TransportConnection,
  TransportHandlers,
} from 'mud-framework-core';

import { SendQueue } from './send-queue.ts';

export interface WsTransportOptions {
  /** Listening port; `0` (the default) lets the OS pick one. */
  port?: number;
  hostname?: string;
  /** Only upgrade requests for this path are accepted. */
  path?: string;
  /** Per-message ceiling for both directions. */
  maxMessageBytes?: number;
  /** Forwarded to Bun's WebSocket `idleTimeout`, in seconds. */
  idleTimeoutSeconds?: number;
  /**
   * How long a payload may wait behind a full socket buffer before its
   * `send` rejects and the connection is dropped. `Infinity` disables it.
   */
  sendTimeoutMs?: number;
}

interface WsData {
  readonly id: string;
  readonly connection: TransportConnection;
}

interface ConnectionState {
  readonly connection: TransportConnection;
  readonly queue: SendQueue;
  ws: Bun.ServerWebSocket<WsData> | undefined;
  open: boolean;
}

type Status = 'idle' | 'started' | 'stopped';

const DEFAULT_MAX_MESSAGE_BYTES = 1024 * 1024;
const DEFAULT_IDLE_TIMEOUT_SECONDS = 30;
const DEFAULT_SEND_TIMEOUT_MS = 30_000;

/**
 * WebSocket carries binary frames, fragments, ordered delivery and protocol
 * ping/pong, so those are declared true. Backpressure is declared true because
 * `ws.send` reports a full buffer and Bun exposes `drain`: a per-connection
 * queue holds payloads and leaves their promises pending until the socket has
 * room again (SPEC 5.2), instead of rejecting or dropping.
 */
export class WsTransport implements Transport {
  private readonly settings: {
    readonly port: number;
    readonly hostname: string;
    readonly path: string;
    readonly maxMessageBytes: number;
    readonly idleTimeoutSeconds: number;
    readonly sendTimeoutMs: number;
  };
  private readonly declared: TransportCapabilities;
  private readonly connections = new Map<string, ConnectionState>();
  private readonly pending = new Set<Promise<void>>();
  private status: Status = 'idle';
  private handlers: TransportHandlers | undefined;
  private server: Bun.Server<WsData> | undefined;

  constructor(options: WsTransportOptions = {}) {
    const maxMessageBytes =
      options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES;
    this.settings = {
      port: options.port ?? 0,
      hostname: options.hostname ?? '127.0.0.1',
      path: options.path ?? '/',
      maxMessageBytes,
      idleTimeoutSeconds:
        options.idleTimeoutSeconds ?? DEFAULT_IDLE_TIMEOUT_SECONDS,
      sendTimeoutMs: options.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS,
    };
    this.declared = {
      binaryFrames: true,
      fragmentation: true,
      maxMessageBytes,
      backpressure: true,
      orderedDelivery: true,
      heartbeat: 'protocol',
    };
  }

  /** The port actually bound, or `undefined` before `start` and after `stop`. */
  get port(): number | undefined {
    return this.server?.port;
  }

  capabilities(): TransportCapabilities {
    // Static declaration, readable after stop for shutdown ordering (SPEC 6.3).
    return this.declared;
  }

  async start(handlers: TransportHandlers): Promise<void> {
    if (this.status !== 'idle') {
      throw new Error(`transport cannot start while ${this.status}`);
    }
    this.handlers = handlers;
    this.status = 'started';
    try {
      this.server = Bun.serve<WsData>({
        port: this.settings.port,
        hostname: this.settings.hostname,
        websocket: {
          idleTimeout: this.settings.idleTimeoutSeconds,
          maxPayloadLength: this.settings.maxMessageBytes,
          open: (ws) => this.handleOpen(ws),
          message: (ws, message) => this.handleMessage(ws, message),
          drain: (ws) => this.handleDrain(ws),
          close: (ws, code, reason) => this.handleClose(ws, code, reason),
          error: (error) => this.handleError(error),
        },
        fetch: (request, server) => this.handleFetch(request, server),
      });
    } catch (error) {
      this.status = 'idle';
      this.handlers = undefined;
      this.server = undefined;
      throw toError(error);
    }
  }

  async stop(): Promise<void> {
    if (this.status !== 'started') {
      throw new Error(`transport cannot stop while ${this.status}`);
    }
    this.status = 'stopped';
    for (const state of this.connections.values()) {
      state.open = false;
      // Settle queued promises before the socket goes away; otherwise `stop`
      // would leave callers waiting on a connection that no longer exists.
      state.queue.fail(new Error(`connection "${state.connection.id}" closed`));
      state.ws?.close(1001, 'transport stopped');
    }
    const server = this.server;
    this.server = undefined;
    if (server !== undefined) {
      await server.stop(true);
    }
    // Handlers may have been mid-flight when the sockets closed.
    await Promise.allSettled(this.pending);
    this.pending.clear();
    this.connections.clear();
  }

  private handleFetch(
    request: Request,
    server: Bun.Server<WsData>,
  ): Response | undefined {
    if (new URL(request.url).pathname !== this.settings.path) {
      return new Response('not found', { status: 404 });
    }
    if (request.headers.get('upgrade')?.toLowerCase() !== 'websocket') {
      return new Response('websocket upgrade required', { status: 426 });
    }
    const id = randomUUID();
    const queue = new SendQueue(id, this.settings.sendTimeoutMs, {
      // The socket only exists after the upgrade, so look it up per write.
      write: (payload) => {
        const target = this.connections.get(id)?.ws;
        if (target === undefined) {
          throw new Error(`connection "${id}" has no socket`);
        }
        return target.send(payload);
      },
      abandon: (error) => {
        this.abandon(id, error);
      },
    });
    const state: ConnectionState = {
      connection: this.createConnection(id, queue),
      queue,
      ws: undefined,
      open: false,
    };
    this.connections.set(id, state);
    const upgraded = server.upgrade(request, {
      data: { id, connection: state.connection },
    });
    if (!upgraded) {
      this.connections.delete(id);
      return new Response('websocket upgrade failed', { status: 400 });
    }
    // The upgrade owns the response; returning a Response here would abort it.
    return undefined;
  }

  private handleOpen(ws: Bun.ServerWebSocket<WsData>): void | Promise<void> {
    const state = this.connections.get(ws.data.id);
    if (state === undefined) {
      throw new Error(`unknown connection "${ws.data.id}"`);
    }
    state.ws = ws;
    state.open = true;
    return this.invoke((handlers) => handlers.onConnect(state.connection));
  }

  private handleMessage(
    ws: Bun.ServerWebSocket<WsData>,
    message: string | Buffer<ArrayBuffer>,
  ): void | Promise<void> {
    const state = this.connections.get(ws.data.id);
    if (state === undefined) {
      throw new Error(`unknown connection "${ws.data.id}"`);
    }
    const payload = normalizePayload(message);
    return this.invoke((handlers) =>
      handlers.onMessage(state.connection, payload),
    );
  }

  private handleClose(
    ws: Bun.ServerWebSocket<WsData>,
    code: number,
    reason: string,
  ): void | Promise<void> {
    const state = this.connections.get(ws.data.id);
    if (state === undefined) {
      return undefined;
    }
    state.open = false;
    state.queue.fail(
      new Error(
        `connection "${ws.data.id}" closed (${describeClose(code, reason)})`,
      ),
    );
    this.connections.delete(ws.data.id);
    if (this.status !== 'started') {
      // Sockets closed by `stop` are not reported: shutdown ordering belongs to
      // the connection layer (SPEC 6.3), matching the fake transport.
      return undefined;
    }
    return this.invoke((handlers) =>
      handlers.onDisconnect(state.connection, describeClose(code, reason)),
    );
  }

  /** Bun fires this when a full socket buffer has room again. */
  private handleDrain(ws: Bun.ServerWebSocket<WsData>): void {
    this.connections.get(ws.data.id)?.queue.drain();
  }

  /**
   * Drops a connection whose client stopped reading. Terminating rather than
   * closing is deliberate: a graceful close would itself wait behind the
   * unsent frames this connection is being dropped for.
   */
  private abandon(id: string, cause: Error): void {
    const state = this.connections.get(id);
    if (state === undefined) {
      return;
    }
    state.open = false;
    state.queue.fail(
      new Error(`connection "${id}" closed after a send timeout`, {
        cause,
      }),
    );
    state.ws?.terminate();
  }

  private handleError(error: Error): void {
    // Bun calls this only for synchronous handler throws. Rethrowing inline
    // would re-enter this handler, so surface it as an uncaught exception
    // instead of swallowing it.
    queueMicrotask(() => {
      throw error;
    });
  }

  private createConnection(id: string, queue: SendQueue): TransportConnection {
    return {
      id,
      send: async (payload) => {
        if (this.status !== 'started') {
          throw statusError(this.status);
        }
        const state = this.connections.get(id);
        if (state === undefined || !state.open || state.ws === undefined) {
          throw new Error(`connection "${id}" is not open`);
        }
        const bytes = payloadBytes(payload);
        if (bytes > this.settings.maxMessageBytes) {
          throw new Error(
            `payload of ${bytes} bytes exceeds the ${this.settings.maxMessageBytes} byte limit`,
          );
        }
        // May stay pending for a long time: the socket buffer is full and only
        // `drain` (or `sendTimeoutMs`) settles it. Awaiting is the point —
        // that is how the transport pushes back on the writer.
        await queue.send(payload, bytes);
      },
      close: async (code, reason) => {
        if (this.status !== 'started') {
          throw statusError(this.status);
        }
        const state = this.connections.get(id);
        if (state === undefined || !state.open || state.ws === undefined) {
          throw new Error(`connection "${id}" is not open`);
        }
        state.open = false;
        state.queue.fail(new Error(`connection "${id}" closed`));
        state.ws.close(code, reason);
      },
    };
  }

  /**
   * Runs a handler and keeps the promise for `stop`. The promise is also
   * returned to Bun, which is the only channel a WebSocket callback has for a
   * rejection; observing settlement here would mark it handled, so a rejection
   * is re-raised on a detached promise to stay visible.
   */
  private invoke(
    call: (handlers: TransportHandlers) => void | Promise<void>,
  ): Promise<void> {
    const handlers = this.handlers;
    if (handlers === undefined) {
      return Promise.resolve();
    }
    const settled = Promise.resolve(call(handlers));
    this.pending.add(settled);
    void settled.then(
      () => {
        this.pending.delete(settled);
        return undefined;
      },
      (error: unknown) => {
        this.pending.delete(settled);
        void Promise.reject(toError(error));
        return undefined;
      },
    );
    return settled;
  }
}

function normalizePayload(
  message: string | Buffer<ArrayBuffer>,
): string | Uint8Array {
  if (typeof message === 'string') {
    return message;
  }
  // Buffer is a Uint8Array subclass backed by a pooled ArrayBuffer; copy so a
  // handler never sees neighbouring bytes or a later mutation of the pool.
  return new Uint8Array(message);
}

function payloadBytes(payload: string | Uint8Array): number {
  return typeof payload === 'string'
    ? Buffer.byteLength(payload)
    : payload.byteLength;
}

function describeClose(code: number, reason: string): string {
  return reason.length > 0 ? `${code} ${reason}` : `code ${code}`;
}

function statusError(status: Status): Error {
  return new Error(
    status === 'idle' ? 'transport is not started' : 'transport is stopped',
  );
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
