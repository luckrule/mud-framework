import { parseFrame } from 'mud-framework-test-support';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger, LogLevel } from '../logging/types.ts';
import { ConnectionLayer } from '../portal/connection-layer.ts';
import { DispatcherExecutor } from '../server/dispatcher-executor.ts';
import { Dispatcher } from '../server/dispatcher.ts';
import type { Codec } from '../shared/codec.ts';
import type { SessionChange } from '../shared/executor.ts';
import { FakeTransport } from './fake-transport.ts';
import type { ControlledExecutor } from './layer-controlled.ts';
import { controlledExecutor } from './layer-controlled.ts';
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
    maxQueuedEvents?: number;
    maxQueuedEventBytes?: number;
    eventBacklogWarnBytes?: number;
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
    maxQueuedEvents: options.maxQueuedEvents,
    maxQueuedEventBytes: options.maxQueuedEventBytes,
    eventBacklogWarnBytes: options.eventBacklogWarnBytes,
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
