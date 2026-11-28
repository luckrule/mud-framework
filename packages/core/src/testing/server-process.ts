import {
  isRecord,
  parseJsonLine,
  rejectionOf,
} from 'mud-framework-test-support';

import type { IpcChannel } from '../ipc/channel.ts';
import { createInMemoryChannelPair } from '../ipc/channel.ts';
import type {
  IpcMessage,
  RequestMessage,
  ResultMessage,
} from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { DispatcherOptions } from '../server/dispatcher.ts';
import type { Extension, ExtensionRegistrar } from '../server/extension.ts';
import { runServerProcess } from '../server/server-process.ts';
import { ServerStartupError } from '../server/server-startup.ts';
import type { StorageBackend } from '../storage/types.ts';
import { MemoryStorageBackend } from './memory-backend.ts';

export function requestMessage(
  overrides: Partial<RequestMessage> = {},
): RequestMessage {
  return {
    type: 'request',
    requestId: 'req-1',
    connectionId: 'c1',
    clientRequestId: 'client-1',
    method: 'echo',
    payload: 'hi',
    timeoutMs: 1000,
    replayCount: 0,
    ...overrides,
  };
}

/**
 * The server validates everything it sends, so a test that parses its outbound
 * messages also proves they stay within the protocol.
 */
export function received(messages: readonly unknown[]): IpcMessage[] {
  return messages.map((raw) => {
    const parsed = parseIpcMessage(raw);
    if (!parsed.ok) {
      throw new Error(`server sent an invalid message: ${parsed.reason}`);
    }
    return parsed.message;
  });
}

export function results(messages: readonly unknown[]): ResultMessage[] {
  return received(messages).filter(
    (message): message is ResultMessage => message.type === 'result',
  );
}

/**
 * The message types Portal received, in order. Used where the exact shape
 * matters, e.g. a failed startup that must have sent `prepared` and nothing
 * else: asserting only "no `ready`" would accept a stray `closed` or `result`.
 */
export function messageTypes(messages: readonly unknown[]): string[] {
  return received(messages).map((message) => message.type);
}

export function sendRaw(portal: IpcChannel, raw: unknown): void {
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- the fake peer must be able to put a non-message on the wire; that is the case under test
  portal.send(raw as IpcMessage);
}

export interface HarnessOptions {
  extensions?: readonly Extension[];
  backend?: StorageBackend;
  timeoutMs?: number;
  /** Per-request event cap forwarded to the Server's dispatcher (SPEC 3.8). */
  maxEventsPerRequest?: number;
  /** Replaces the collecting logger: `logs()` then stays empty. */
  logger?: Logger;
  /** Observes every message Portal receives, for cross-array ordering checks. */
  onPortalMessage?: (raw: unknown) => void;
  /** Overrides the abort budget so a test need not wait the real 5s. */
  abortTimeoutMs?: number;
  /** Enables the parent-liveness poll; off by default (no real parent here). */
  parentCheckIntervalMs?: number;
  /** Lets a test pretend the process was reparented. */
  readParentPid?: () => number;
  /** Access policy forwarded to the Server's dispatcher (SPEC 3.7). */
  authorize?: DispatcherOptions['authorize'];
  /**
   * Drives `hello` and, once `prepared` arrives, `activate`. On by default so a
   * test that only cares about the started Server does not repeat the two-phase
   * handshake. Off for the tests that own the handshake themselves.
   */
  autoHandshake?: boolean;
}

export interface Harness {
  portal: IpcChannel;
  /** Raw messages Portal received, in arrival order. */
  messages: unknown[];
  /** Parsed log lines, in emission order. */
  logs: () => Record<string, unknown>[];
  /** Settles when the server process finishes or fails. */
  done: Promise<void>;
  backend: StorageBackend;
  /** Closes the link and waits for the server to settle. */
  stop: () => Promise<void>;
}

export function startServer(options: HarnessOptions = {}): Harness {
  const [portal, serverChannel] = createInMemoryChannelPair();
  const autoHandshake = options.autoHandshake ?? true;
  const messages: unknown[] = [];
  portal.onMessage((raw) => {
    messages.push(raw);
    options.onPortalMessage?.(raw);
    if (autoHandshake && isRecord(raw) && raw.type === 'prepared') {
      portal.send({ type: 'activate' });
    }
  });
  const lines: string[] = [];
  const logger =
    options.logger ??
    createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });
  const backend = options.backend ?? new MemoryStorageBackend();
  const done = runServerProcess({
    createBackend: () => backend,
    extensions: options.extensions ?? [],
    timeoutMs: options.timeoutMs ?? 1000,
    maxEventsPerRequest: options.maxEventsPerRequest,
    authorize: options.authorize,
    logger,
    channel: serverChannel,
    abortTimeoutMs: options.abortTimeoutMs,
    // The in-memory channel has no real parent to watch, and a background timer
    // would keep the test process alive.
    parentCheckIntervalMs: options.parentCheckIntervalMs ?? 0,
    readParentPid: options.readParentPid,
  });
  if (autoHandshake) {
    portal.send({ type: 'hello' });
  }
  return {
    portal,
    messages,
    logs: () => lines.map(parseLine),
    done,
    backend,
    async stop(): Promise<void> {
      portal.close('test finished');
      await done.catch(() => undefined);
    },
  };
}

export function parseLine(line: string): Record<string, unknown> {
  return parseJsonLine(line, 'log line');
}

/** Reads a numeric log field, failing loudly when it is missing or not a number. */
export function numberField(
  line: Record<string, unknown> | undefined,
  field: string,
): number {
  const value = line?.[field];
  if (typeof value !== 'number') {
    throw new Error(`log field "${field}" is not a number: ${String(value)}`);
  }
  return value;
}

export function echoRegistration(registrar: ExtensionRegistrar): void {
  registrar.register('echo', (_ctx, payload) => Promise.resolve(payload), {
    access: 'public',
  });
}

/** Narrows the rejection without an unchecked cast, so a wrong type fails loudly. */
export async function startupFailure(
  promise: Promise<unknown>,
): Promise<ServerStartupError> {
  const error = await rejectionOf(promise);
  if (!(error instanceof ServerStartupError)) {
    throw new Error(`expected ServerStartupError, got ${error.name}`);
  }
  return error;
}

/** SPEC 4.3 lines only, so unrelated warns do not change the counts. */
export function degradationWarnings(
  logs: readonly Record<string, unknown>[],
): Record<string, unknown>[] {
  return logs.filter((line) => line.msg === 'storage capability degraded');
}
