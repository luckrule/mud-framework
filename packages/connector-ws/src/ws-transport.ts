import { Buffer } from 'node:buffer';
import { randomUUID } from 'node:crypto';

import { safeLogger } from 'mud-framework-core';
import type {
  Logger,
  Transport,
  TransportConnection,
  TransportHandlers,
} from 'mud-framework-core';

import {
  SendQueue,
  assertPositiveInteger,
  payloadBytes,
} from './send-queue.ts';
import type { SendQueueOverflow } from './send-queue.ts';

export interface WsTransportOptions {
  /**
   * Listening port; `0` (the default) lets the OS pick one. Must be an
   * integer between 0 and 65535.
   */
  port?: number;
  /** Must be a non-empty string. */
  hostname?: string;
  /**
   * Only upgrade requests for this path are accepted. Must be a non-empty
   * string starting with `/`.
   */
  path?: string;
  /** Per-message ceiling for both directions. Must be a positive integer. */
  maxMessageBytes?: number;
  /**
   * Forwarded to Bun's WebSocket `idleTimeout`, in seconds. Must be a
   * non-negative integer; `0` disables the timeout.
   */
  idleTimeoutSeconds?: number;
  /**
   * How long a payload may wait behind a full socket buffer before its
   * `send` rejects and the connection is dropped. `Infinity` disables it.
   * Must be a positive finite number or `Infinity`.
   */
  sendTimeoutMs?: number;
  /**
   * Frames that may wait unsettled in one connection's outbound queue before
   * it is failed. Must be a positive integer; defaults to 1024.
   */
  maxQueuedFrames?: number;
  /**
   * Unsettled outbound bytes per connection before it is failed. Must be a
   * positive integer no smaller than `maxMessageBytes`, so one legal
   * maximum-size frame always fits; defaults to 4 MiB, raised to
   * `maxMessageBytes` when that is larger.
   */
  maxQueuedBytes?: number;
  /**
   * Optional: this package depends on core's types and the stateless
   * `safeLogger` wrapper only, never on a logger implementation; the caller
   * supplies the implementation. Without it nothing is logged.
   */
  logger?: Logger;
}

interface WsData {
  readonly id: string;
  readonly connection: TransportConnection;
}

interface ConnectionState {
  readonly connection: TransportConnection;
  readonly queue: SendQueue;
  /** Bound to `connectionId`; every per-connection event logs through it. */
  readonly logger: Logger | undefined;
  ws: Bun.ServerWebSocket<WsData> | undefined;
  open: boolean;
}

type Status = 'idle' | 'started' | 'stopped';

const DEFAULT_MAX_MESSAGE_BYTES = 1024 * 1024;
const DEFAULT_IDLE_TIMEOUT_SECONDS = 30;
const DEFAULT_SEND_TIMEOUT_MS = 30_000;
/**
 * The input queue bounds a connection at 1000 waiting requests, so one
 * response per request must fit. 1024 lets a refusal storm finish without
 * letting a client that never reads pin memory without bound.
 */
const DEFAULT_MAX_QUEUED_FRAMES = 1024;
/**
 * 4 MiB holds an ordinary burst (1000 refusal frames are ~80 KB) and several
 * maximum-size frames. The effective default is raised to `maxMessageBytes`
 * when a caller allows larger single frames.
 */
const DEFAULT_MAX_QUEUED_BYTES = 4 * 1024 * 1024;

/**
 * WebSocket carries binary frames, fragments, ordered delivery and protocol
 * ping/pong. Backpressure is handled by a per-connection queue that holds
 * payloads and leaves their promises pending until the socket has room again
 * (`ws.send` reports a full buffer and Bun exposes `drain`), instead of
 * rejecting or dropping.
 */
export class WsTransport implements Transport {
  private readonly settings: {
    readonly port: number;
    readonly hostname: string;
    readonly path: string;
    readonly maxMessageBytes: number;
    readonly idleTimeoutSeconds: number;
    readonly sendTimeoutMs: number;
    readonly maxQueuedFrames: number;
    readonly maxQueuedBytes: number;
  };
  private readonly connections = new Map<string, ConnectionState>();
  private readonly pending = new Set<Promise<void>>();
  private readonly logger: Logger | undefined;
  private status: Status = 'idle';
  private handlers: TransportHandlers | undefined;
  private server: Bun.Server<WsData> | undefined;

  constructor(options: WsTransportOptions = {}) {
    const port = options.port ?? 0;
    assertIntegerBetween(port, 'port', 0, 65_535);
    const maxMessageBytes =
      options.maxMessageBytes ?? DEFAULT_MAX_MESSAGE_BYTES;
    assertPositiveInteger(maxMessageBytes, 'maxMessageBytes');
    const idleTimeoutSeconds =
      options.idleTimeoutSeconds ?? DEFAULT_IDLE_TIMEOUT_SECONDS;
    assertNonNegativeInteger(idleTimeoutSeconds, 'idleTimeoutSeconds');
    const sendTimeoutMs = options.sendTimeoutMs ?? DEFAULT_SEND_TIMEOUT_MS;
    assertSendTimeout(sendTimeoutMs, 'sendTimeoutMs');
    const maxQueuedFrames =
      options.maxQueuedFrames ?? DEFAULT_MAX_QUEUED_FRAMES;
    assertPositiveInteger(maxQueuedFrames, 'maxQueuedFrames');
    const maxQueuedBytes =
      options.maxQueuedBytes ??
      Math.max(DEFAULT_MAX_QUEUED_BYTES, maxMessageBytes);
    assertPositiveInteger(maxQueuedBytes, 'maxQueuedBytes');
    if (maxQueuedBytes < maxMessageBytes) {
      // A queue smaller than one legal frame would fail that frame on arrival
      // and drop the connection, so the limit could never be reached legally.
      throw new RangeError(
        `maxQueuedBytes (${maxQueuedBytes}) must be at least maxMessageBytes (${maxMessageBytes}), or a maximum-size frame overflows the queue`,
      );
    }
    const hostname = options.hostname ?? '127.0.0.1';
    assertNonEmptyString(hostname, 'hostname');
    const path = options.path ?? '/';
    assertPath(path);
    this.settings = {
      port,
      hostname,
      path,
      maxMessageBytes,
      idleTimeoutSeconds,
      sendTimeoutMs,
      maxQueuedFrames,
      maxQueuedBytes,
    };
    this.logger =
      options.logger === undefined
        ? undefined
        : safeLogger(options.logger).child({ transport: 'ws' });
  }

  /** The port actually bound, or `undefined` before `start` and after `stop`. */
  get port(): number | undefined {
    return this.server?.port;
  }

  /**
   * The validated per-frame send bound (SPEC 3.8). Exposed so the connection
   * layer can hold a Session's serial slot at least as long as this transport
   * may take to fail a write, instead of releasing it mid-frame.
   */
  get sendTimeoutMs(): number {
    return this.settings.sendTimeoutMs;
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
          error: (error, ws) => this.handleError(error, ws),
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
    const connectionLogger = this.logger?.child({ connectionId: id });
    const queue = new SendQueue(
      id,
      this.settings.sendTimeoutMs,
      {
        maxQueuedFrames: this.settings.maxQueuedFrames,
        maxQueuedBytes: this.settings.maxQueuedBytes,
      },
      {
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
        overflow: (info) => {
          this.overflow(id, info);
        },
      },
    );
    const state: ConnectionState = {
      connection: this.createConnection(id, queue),
      queue,
      logger: connectionLogger,
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
      this.handleUnknownConnection(ws, 'open');
      return undefined;
    }
    state.ws = ws;
    state.open = true;
    return this.invoke(state, (handlers) =>
      handlers.onConnect(state.connection),
    );
  }

  private handleMessage(
    ws: Bun.ServerWebSocket<WsData>,
    message: string | Buffer<ArrayBuffer>,
  ): void | Promise<void> {
    const state = this.connections.get(ws.data.id);
    if (state === undefined) {
      this.handleUnknownConnection(ws, 'message');
      return undefined;
    }
    const payload = normalizePayload(message);
    return this.invoke(state, (handlers) =>
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
    return this.invoke(state, (handlers) =>
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
    // warn: the connection is dropped but the transport keeps serving others,
    // which is a degradation rather than a request failure (SPEC 7.1).
    state.logger?.warn(
      {
        sendTimeoutMs: this.settings.sendTimeoutMs,
        queueDepth: state.queue.depth,
        reason: cause.message,
      },
      'send timed out; terminating connection',
    );
    state.queue.fail(
      new Error(`connection "${id}" closed after a send timeout`, {
        cause,
      }),
    );
    state.ws?.terminate();
  }

  /**
   * Drops a connection whose outbound queue hit its capacity. A graceful
   * close is enough here: the overflowing frame was rejected before it was
   * written, so only the already-accepted head can precede the 1013 frame.
   * 1013 (Try Again Later) tells the client the connection is temporarily
   * unable to serve it, rather than that one message was malformed.
   */
  private overflow(id: string, info: SendQueueOverflow): void {
    const state = this.connections.get(id);
    if (state === undefined) {
      return;
    }
    state.open = false;
    // warn: one connection is dropped, the transport keeps serving others,
    // which is a degradation rather than a request failure (SPEC 7.1).
    state.logger?.warn(
      {
        limit: info.limit,
        queuedFrames: info.queuedFrames,
        queuedBytes: info.queuedBytes,
        maxQueuedFrames: info.maxQueuedFrames,
        maxQueuedBytes: info.maxQueuedBytes,
        incomingBytes: info.incomingBytes,
        reason: info.error.message,
      },
      'send queue overflow; closing connection',
    );
    closeQuietly(state.ws, 1013, 'send queue full');
  }

  private handleError(error: Error, ws: Bun.ServerWebSocket<WsData>): void {
    const state = this.connections.get(ws.data.id);
    const logger =
      state?.logger ?? this.logger?.child({ connectionId: ws.data.id });
    logger?.error(
      { code: 'TRANSPORT_ERROR', reason: error.message },
      'websocket handler failed',
    );
    // Bun reports only exceptions that escaped a socket callback. Escaping
    // again from here would terminate every connection, so the failure is
    // contained to this socket; its `close` callback still runs cleanup.
    if (state !== undefined) {
      state.open = false;
      state.queue.fail(
        new Error(`connection "${ws.data.id}" closed after a websocket error`, {
          cause: error,
        }),
      );
    }
    closeQuietly(ws, 1011, 'websocket handler failed');
  }

  private handleHandlerFailure(state: ConnectionState, error: Error): void {
    state.open = false;
    state.logger?.error(
      {
        code: 'TRANSPORT_HANDLER_FAILED',
        connectionId: state.connection.id,
        reason: error.message,
      },
      'transport handler failed; closing connection',
    );
    state.queue.fail(
      new Error(
        `connection "${state.connection.id}" closed after a handler failure`,
        { cause: error },
      ),
    );
    closeQuietly(state.ws, 1011, 'handler failed');
  }

  /**
   * A late `open`/`message` for an id that is no longer registered. Closing
   * the socket is best effort and keeps the event off the event loop.
   */
  private handleUnknownConnection(
    ws: Bun.ServerWebSocket<WsData>,
    event: 'open' | 'message',
  ): void {
    this.logger?.warn(
      {
        code: 'TRANSPORT_UNKNOWN_CONNECTION',
        connectionId: ws.data.id,
        event,
      },
      'websocket event for unknown connection; closing socket',
    );
    closeQuietly(ws, 1011, 'unknown connection');
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
        // that is how the transport pushes back on the writer. `bytes` is
        // already measured above, so the queue must not measure it again.
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
   * Runs a handler and keeps the promise for `stop`. Failures are contained
   * here: they are logged and the connection is closed, and the returned
   * promise always fulfills. A socket callback has no caller to propagate to —
   * Bun treats an escaped error or rejection as fatal — so re-raising would
   * take the whole process, and every other connection, down with it.
   */
  private invoke(
    state: ConnectionState,
    call: (handlers: TransportHandlers) => void | Promise<void>,
  ): Promise<void> {
    const handlers = this.handlers;
    if (handlers === undefined) {
      return Promise.resolve();
    }
    const settled = this.runHandler(state, handlers, call);
    this.pending.add(settled);
    void settled.then(
      () => {
        this.pending.delete(settled);
        return undefined;
      },
      () => {
        // `runHandler` absorbs failures; this arm only keeps a bug in the
        // failure path from becoming an unhandled rejection.
        this.pending.delete(settled);
        return undefined;
      },
    );
    return settled;
  }

  private async runHandler(
    state: ConnectionState,
    handlers: TransportHandlers,
    call: (handlers: TransportHandlers) => void | Promise<void>,
  ): Promise<void> {
    try {
      await call(handlers);
    } catch (error) {
      this.handleHandlerFailure(state, toError(error));
    }
  }
}

function assertIntegerBetween(
  value: unknown,
  name: string,
  min: number,
  max: number,
): void {
  const expected = `an integer between ${min} and ${max}`;
  if (typeof value !== 'number') {
    throw new TypeError(`${name} must be ${expected}, got ${String(value)}`);
  }
  if (!Number.isInteger(value) || value < min || value > max) {
    throw new RangeError(`${name} must be ${expected}, got ${String(value)}`);
  }
}

function assertNonEmptyString(value: unknown, name: string): string {
  if (typeof value !== 'string') {
    throw new TypeError(
      `${name} must be a non-empty string, got ${String(value)}`,
    );
  }
  if (value.length === 0) {
    throw new RangeError(`${name} must be a non-empty string, got ""`);
  }
  return value;
}

function assertPath(value: unknown): void {
  const path = assertNonEmptyString(value, 'path');
  if (!path.startsWith('/')) {
    throw new RangeError(
      `path must start with "/", got ${JSON.stringify(path)}`,
    );
  }
}

function assertSendTimeout(value: unknown, name: string): void {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a positive number or Infinity, got ${String(value)}`,
    );
  }
  const valid =
    value === Number.POSITIVE_INFINITY || (Number.isFinite(value) && value > 0);
  if (!valid) {
    throw new RangeError(
      `${name} must be a positive number or Infinity, got ${String(value)}`,
    );
  }
}

function assertNonNegativeInteger(value: unknown, name: string): void {
  if (typeof value !== 'number') {
    throw new TypeError(
      `${name} must be a non-negative integer, got ${String(value)}`,
    );
  }
  if (!Number.isInteger(value) || value < 0) {
    throw new RangeError(
      `${name} must be a non-negative integer, got ${String(value)}`,
    );
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

function describeClose(code: number, reason: string): string {
  return reason.length > 0 ? `${code} ${reason}` : `code ${code}`;
}

function closeQuietly(
  ws: Bun.ServerWebSocket<WsData> | undefined,
  code: number,
  reason: string,
): void {
  if (ws === undefined) {
    return;
  }
  try {
    ws.close(code, reason);
  } catch {
    // The failure path may run after the socket is already gone; a second
    // close must not turn a contained failure into a thrown one.
  }
}

function statusError(status: Status): Error {
  return new Error(
    status === 'idle' ? 'transport is not started' : 'transport is stopped',
  );
}

function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
