import type { IpcChannel } from '../ipc/channel.ts';
import { createInMemoryChannelPair } from '../ipc/channel.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import type { ServerExitInfo, SpawnedServer } from '../ipc/spawn-port.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { parseLine, received } from '../testing/server-process.ts';
import type { Extension } from './extension.ts';
import { runServerProcess } from './server-process.ts';

interface ActivationHarnessOptions {
  extensions?: readonly Extension[];
  /** Gates backend creation so a test can hold the Server in `starting`. */
  createBackend?: () => Promise<MemoryStorageBackend>;
  /** Enables the parent-liveness poll; off by default (no real parent here). */
  parentCheckIntervalMs?: number;
  /** Lets a test pretend the process was reparented. */
  readParentPid?: () => number;
  /** Wraps the Server endpoint to break one part of the link. */
  wrapChannel?: (channel: IpcChannel) => IpcChannel;
}

export function activationHarness(options: ActivationHarnessOptions = {}) {
  const [portal, channel] = createInMemoryChannelPair();
  const messages: unknown[] = [];
  portal.onMessage((raw) => {
    messages.push(raw);
  });
  const lines: string[] = [];
  const logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  let created = 0;
  const done = runServerProcess({
    createBackend: async () => {
      created += 1;
      return options.createBackend?.() ?? new MemoryStorageBackend();
    },
    extensions: options.extensions ?? [],
    timeoutMs: 1000,
    logger,
    channel: options.wrapChannel?.(channel) ?? channel,
    parentCheckIntervalMs: options.parentCheckIntervalMs ?? 0,
    readParentPid: options.readParentPid,
  });
  return {
    portal,
    messages,
    logs: () => lines.map(parseLine),
    done,
    created: () => created,
    async stop(): Promise<void> {
      portal.close('test finished');
      await done.catch(() => undefined);
    },
  };
}

export function hello(h: ReturnType<typeof activationHarness>): void {
  h.portal.send({ type: 'hello' });
}

export function activate(h: ReturnType<typeof activationHarness>): void {
  h.portal.send({ type: 'activate' });
}

export function types(h: ReturnType<typeof activationHarness>): string[] {
  return received(h.messages).map((message) => message.type);
}

/**
 * A Server handle for `channel` that reports its exit when the Server sends
 * `closed`. The Portal side of an in-memory pair delivers synchronously, which
 * is what makes "deliver the instant `ready` arrives" reproducible here.
 */
export function linkedServer(channel: IpcChannel): SpawnedServer {
  const exit = Promise.withResolvers<ServerExitInfo>();
  let info: ServerExitInfo | undefined;
  const record = (value: ServerExitInfo): void => {
    if (info !== undefined) {
      return;
    }
    info = value;
    exit.resolve(value);
  };
  return {
    channel: {
      send: (message) => channel.send(message),
      onMessage: (handler) =>
        channel.onMessage((raw) => {
          const parsed = parseIpcMessage(raw);
          if (parsed.ok && parsed.message.type === 'closed') {
            record({ code: 0, signal: null, observed: true });
          }
          handler(raw);
        }),
      onClose: (handler) => channel.onClose(handler),
      close: (reason) => channel.close(reason),
    },
    exited: exit.promise,
    lastExit: () => info,
    kill: () => record({ code: null, signal: 'SIGKILL', observed: true }),
  };
}
