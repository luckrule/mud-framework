import { Buffer } from 'node:buffer';

import { isRecord, parseFrame } from 'mud-framework-test-support';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger, LogLevel } from '../logging/types.ts';
import { ConnectionLayer } from '../portal/connection-layer.ts';
import { DispatcherExecutor } from '../server/dispatcher-executor.ts';
import { Dispatcher } from '../server/dispatcher.ts';
import type { Codec, Envelope, ServerFrame } from '../shared/codec.ts';
import type { ServerEvent } from '../shared/events.ts';
import type {
  ExecutorRequest,
  RequestExecutor,
  SessionChange,
} from '../shared/executor.ts';
import type { Transport, TransportConnection } from '../transport/types.ts';
import { FakeTransport } from './fake-transport.ts';
import { MemoryStorageBackend } from './memory-backend.ts';

export function lastFrame(
  transport: FakeTransport,
  connectionId: string,
): Record<string, unknown> {
  const frame = transport.sentTo(connectionId).at(-1);
  if (frame === undefined) {
    throw new Error(`no frame was sent to "${connectionId}"`);
  }
  return parseFrame(frame);
}

export interface Harness {
  transport: FakeTransport;
  backend: MemoryStorageBackend;
  dispatcher: Dispatcher;
  layer: ConnectionLayer;
  /** Parsed log lines, in emission order. */
  logs: () => Record<string, unknown>[];
}

export async function harness(
  options: {
    timeoutMs?: number;
    codec?: Codec;
    /**
     * Replaces the collecting logger: `logs()` then stays empty and `level`
     * is ignored.
     */
    logger?: Logger;
    createRequestId?: () => string;
    /** Defaults to the logger's own default (`info`). */
    level?: LogLevel;
    maxQueuedPerConnection?: number;
    shutdownTimeoutMs?: number;
    maxConnections?: number;
    /** Access policy handed to the dispatcher (SPEC 3.7). */
    authorize?: ConstructorParameters<typeof Dispatcher>[0]['authorize'];
  } = {},
): Promise<Harness> {
  const transport = new FakeTransport();
  const backend = new MemoryStorageBackend();
  const lines: string[] = [];
  const logger: Logger =
    options.logger ??
    createJsonLogger({
      level: options.level,
      sink: (line) => {
        lines.push(line);
      },
    });
  const dispatcher = new Dispatcher({
    backend,
    timeoutMs: options.timeoutMs ?? 1000,
    logger,
    createRequestId: options.createRequestId,
    authorize: options.authorize,
  });
  const layer = new ConnectionLayer({
    transport,
    executor: new DispatcherExecutor({ dispatcher }),
    codec: options.codec,
    logger,
    createRequestId: options.createRequestId,
    maxQueuedPerConnection: options.maxQueuedPerConnection,
    shutdownTimeoutMs: options.shutdownTimeoutMs,
    maxConnections: options.maxConnections,
  });
  await layer.start();
  return {
    transport,
    backend,
    dispatcher,
    layer,
    logs: () => lines.map(parseFrame),
  };
}

/** Base64 frames: proves the codec is the only wire-format decision point. */
export class Base64Codec implements Codec {
  encode(value: ServerFrame): string {
    return Buffer.from(JSON.stringify(value), 'utf8').toString('base64');
  }

  decode(raw: string | Uint8Array): Envelope {
    const text =
      typeof raw === 'string' ? raw : Buffer.from(raw).toString('utf8');
    const parsed: unknown = JSON.parse(
      Buffer.from(text, 'base64').toString('utf8'),
    );
    if (
      !isRecord(parsed) ||
      typeof parsed.id !== 'string' ||
      typeof parsed.method !== 'string'
    ) {
      throw new Error('malformed base64 envelope');
    }
    return { id: parsed.id, method: parsed.method, payload: parsed.payload };
  }
}

export interface ScriptedExecutor {
  executor: RequestExecutor;
  order: string[];
  closeCalls: () => number;
}

/** Records start/close calls so shutdown ordering can be asserted. */
export function scriptedExecutor(order: string[]): ScriptedExecutor {
  let closeCalls = 0;
  return {
    order,
    closeCalls: () => closeCalls,
    executor: {
      start: () => {
        order.push('executor.start');
        return Promise.resolve();
      },
      execute: () => Promise.resolve({ ok: true, result: null }),
      onEvent: () => () => undefined,
      cancel: () => Promise.resolve(),
      close: () => {
        closeCalls += 1;
        order.push('executor.close');
        return Promise.resolve();
      },
      health: () => ({ state: 'ready', ready: true }),
    },
  };
}

export interface ScriptedTransport {
  transport: Transport;
  stopCalls: () => number;
  failStop: (error: Error) => void;
}

/** Records start/stop calls, and can be told to make `stop` reject. */
export function scriptedTransport(order: string[]): ScriptedTransport {
  let stopCalls = 0;
  let stopFailure: Error | undefined;
  return {
    transport: {
      start: () => {
        order.push('transport.start');
        return Promise.resolve();
      },
      stop: () => {
        stopCalls += 1;
        order.push('transport.stop');
        return stopFailure === undefined
          ? Promise.resolve()
          : Promise.reject(stopFailure);
      },
    },
    stopCalls: () => stopCalls,
    failStop: (error) => {
      stopFailure = error;
    },
  };
}

/** A connection that records frames, so a test can read the reply. */
export function fakeConnection(id: string): {
  connection: TransportConnection;
  sent: string[];
} {
  const sent: string[] = [];
  return {
    sent,
    connection: {
      id,
      send: (payload) => {
        sent.push(typeof payload === 'string' ? payload : '<binary>');
        return Promise.resolve();
      },
      close: () => Promise.resolve(),
    },
  };
}

export interface ControlledExecutor {
  executor: RequestExecutor;
  /** Methods whose execution started, in order. */
  started: string[];
  /** Methods whose execution returned, in order. */
  finished: string[];
  /** Every `ExecutorRequest` the layer handed over, in call order. */
  requests: ExecutorRequest[];
  /** Settles every `hang` call that is waiting. */
  release: () => void;
  /** Pushes one Server event to the registered sink (ADR-0008). */
  emit: (event: ServerEvent) => void;
  /** How many sinks the layer currently has registered. */
  listenerCount: () => number;
}

/**
 * An executor that reports a Session change for the methods the options name
 * (`sessionOf` binds, `changeOf` can return either change or none), hangs on
 * `hang` until released, and succeeds immediately otherwise. `cancel` is
 * deliberately a no-op: the reconnect tests need a request that outlives its
 * connection, so the layer's own bounded waits are what get measured rather
 * than the executor's.
 */
export function controlledExecutor(
  options: {
    sessionOf?: (method: string) => string | undefined;
    changeOf?: (method: string) => SessionChange | undefined;
  } = {},
): ControlledExecutor {
  const started: string[] = [];
  const finished: string[] = [];
  const requests: ExecutorRequest[] = [];
  const eventListeners = new Set<(event: ServerEvent) => void>();
  const hang = Promise.withResolvers<void>();
  return {
    started,
    finished,
    requests,
    release: () => {
      hang.resolve();
    },
    emit: (event) => {
      for (const listener of new Set(eventListeners)) {
        listener(event);
      }
    },
    listenerCount: () => eventListeners.size,
    executor: {
      start: () => Promise.resolve(),
      execute: async (request) => {
        started.push(request.method);
        requests.push(request);
        if (request.method === 'hang') {
          await hang.promise;
        }
        finished.push(request.method);
        const sessionId = options.sessionOf?.(request.method);
        const sessionChange =
          options.changeOf !== undefined
            ? options.changeOf(request.method)
            : sessionId === undefined
              ? undefined
              : { type: 'bind' as const, sessionId };
        return sessionChange === undefined
          ? { ok: true, result: request.method }
          : { ok: true, result: request.method, sessionChange };
      },
      onEvent: (listener) => {
        eventListeners.add(listener);
        return () => {
          eventListeners.delete(listener);
        };
      },
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
      health: () => ({ state: 'ready', ready: true }),
    },
  };
}

export interface SessionHarness {
  transport: FakeTransport;
  layer: ConnectionLayer;
  controlled: ControlledExecutor;
  logs: () => Record<string, unknown>[];
}

/** A layer whose executor changes the Session for the methods the options name. */
export function sessionHarness(
  options: {
    maxConnections?: number;
    maxQueuedPerConnection?: number;
    createRequestId?: () => string;
    sessionOf?: (method: string) => string | undefined;
    changeOf?: (method: string) => SessionChange | undefined;
    eventSendTimeoutMs?: number;
    shutdownTimeoutMs?: number;
    /** Declared as `Transport.sendTimeoutMs` on the harness transport. */
    transportSendTimeoutMs?: number;
  } = {},
): SessionHarness {
  const transport = new FakeTransport({
    sendTimeoutMs: options.transportSendTimeoutMs,
  });
  const lines: string[] = [];
  const logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  const controlled = controlledExecutor({
    sessionOf:
      options.sessionOf ?? ((method) => (method === 'bind' ? 's1' : undefined)),
    changeOf: options.changeOf,
  });
  const layer = new ConnectionLayer({
    transport,
    executor: controlled.executor,
    logger,
    maxConnections: options.maxConnections,
    maxQueuedPerConnection: options.maxQueuedPerConnection,
    createRequestId: options.createRequestId,
    eventSendTimeoutMs: options.eventSendTimeoutMs,
    shutdownTimeoutMs: options.shutdownTimeoutMs,
  });
  return {
    transport,
    layer,
    controlled,
    logs: () => lines.map(parseFrame),
  };
}
