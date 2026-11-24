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
import type { Dispatcher } from '../runtime/dispatcher.ts';
import type { Extension } from '../runtime/extension.ts';
import { runServerProcess } from '../runtime/server-process.ts';
import { ServerStartupError } from '../runtime/server-startup.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import type { StorageBackend, StorageCapabilities } from '../storage/types.ts';
import { isRecord, rejectionOf } from './common.ts';

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

export function sendRaw(portal: IpcChannel, raw: unknown): void {
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- the fake peer must be able to put a non-message on the wire; that is the case under test
  portal.send(raw as IpcMessage);
}

export interface HarnessOptions {
  extensions?: readonly Extension[];
  backend?: StorageBackend;
  timeoutMs?: number;
  /** Observes every message Portal receives, for cross-array ordering checks. */
  onPortalMessage?: (raw: unknown) => void;
  /** Overrides the abort budget so a test need not wait the real 5s. */
  abortTimeoutMs?: number;
  /** Enables the parent-liveness poll; off by default (no real parent here). */
  parentCheckIntervalMs?: number;
  /** Lets a test pretend the process was reparented. */
  readParentPid?: () => number;
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
  const messages: unknown[] = [];
  portal.onMessage((raw) => {
    messages.push(raw);
    options.onPortalMessage?.(raw);
  });
  const lines: string[] = [];
  const logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  const backend = options.backend ?? new MemoryStorageBackend();
  const done = runServerProcess({
    createBackend: () => backend,
    extensions: options.extensions ?? [],
    timeoutMs: options.timeoutMs ?? 1000,
    logger,
    channel: serverChannel,
    abortTimeoutMs: options.abortTimeoutMs,
    // The in-memory channel has no real parent to watch, and a background timer
    // would keep the test process alive.
    parentCheckIntervalMs: options.parentCheckIntervalMs ?? 0,
    readParentPid: options.readParentPid,
  });
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
  const parsed: unknown = JSON.parse(line);
  if (!isRecord(parsed)) {
    throw new Error(`log line is not a JSON object: ${line}`);
  }
  return parsed;
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

export function echoRegistration(dispatcher: Dispatcher): void {
  dispatcher.register('echo', (_ctx, payload) => Promise.resolve(payload));
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

/** A logger that keeps the singleton test's warning out of the output. */
export function silentLogger(): Logger {
  return createJsonLogger({ sink: () => undefined });
}

/** SPEC 4.3 lines only, so unrelated warns do not change the counts. */
export function degradationWarnings(
  logs: readonly Record<string, unknown>[],
): Record<string, unknown>[] {
  return logs.filter((line) => line.msg === 'storage capability degraded');
}

/**
 * A backend whose declaration is deliberately invalid. The cast is the test's
 * only way to hand an unusable declaration to the self-check.
 */
export function brokenBackend(declared: unknown): StorageBackend {
  return {
    capabilities: (): StorageCapabilities => {
      // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- the self-check is given a declaration no real backend could return
      return declared as StorageCapabilities;
    },
    transaction: <T>(): Promise<T> =>
      Promise.reject(new Error('broken backend must not run transactions')),
    close: (): Promise<void> => Promise.resolve(),
  };
}
