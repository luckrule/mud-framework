import { expect } from 'bun:test';

import type { IpcChannel } from '../ipc/channel.ts';
import { createInMemoryChannelPair } from '../ipc/channel.ts';
import type { IpcMessage, RequestMessage } from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import type { ServerExitInfo, SpawnedServer } from '../ipc/spawn-port.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { ExecutorOutcome, ExecutorRequest } from '../runtime/executor.ts';
import { IpcExecutor } from '../runtime/ipc-executor.ts';
import { isRecord, tick } from './common.ts';

/**
 * The in-memory channel delivers synchronously, but crash detection, the exit
 * wait, the restart timer and the ready handshake each cross a task boundary.
 * Five macrotask hops are enough for the whole chain, with no wall-clock wait.
 */
export async function flushTicks(): Promise<void> {
  await tick();
  await tick();
  await tick();
  await tick();
  await tick();
}

/** Enough microtask hops for a timer callback's continuation to finish. */
export async function flushMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

/**
 * Waits for a condition that only a real timer can satisfy. The restart backoff
 * is a wall-clock delay now that `ready` no longer resets it, so a fixed number
 * of ticks is not enough for the second and later restarts.
 */
export async function waitFor(
  condition: () => boolean,
  timeoutMs = 2000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const poll = async (): Promise<void> => {
    if (condition()) {
      return;
    }
    if (Date.now() > deadline) {
      throw new Error('condition was not met before the timeout');
    }
    await tick();
    return poll();
  };
  return poll();
}

export function readyMessage(maxTransactionDurationMs = 5000): IpcMessage {
  return {
    type: 'ready',
    pid: 4242,
    capabilities: {
      transactions: 'full',
      concurrency: 'pooled',
      snapshotRead: true,
      uniqueConstraint: true,
      maxTransactionDurationMs,
    },
  };
}

export function successMessage(requestId: string, result: unknown): IpcMessage {
  return { type: 'result', requestId, ok: true, result };
}

export function boundSuccessMessage(
  requestId: string,
  result: unknown,
  sessionId: string,
): IpcMessage {
  return { type: 'result', requestId, ok: true, result, sessionId };
}

export function request(
  overrides: Partial<ExecutorRequest> = {},
): ExecutorRequest {
  return {
    requestId: 'req-1',
    connectionId: 'c1',
    clientRequestId: 'client-1',
    method: 'echo',
    payload: null,
    ...overrides,
  };
}

export function failureOf(outcome: ExecutorOutcome): {
  code: string;
  message: string;
} {
  if (outcome.ok) {
    throw new Error('expected a failure outcome');
  }
  return outcome.error;
}

export function messagesOf(server: FakeServerProcess): IpcMessage[] {
  return server.received.flatMap((raw) => {
    const parsed = parseIpcMessage(raw);
    if (!parsed.ok) {
      throw new Error(`portal sent an unparsable message: ${parsed.reason}`);
    }
    return [parsed.message];
  });
}

export function requestMessages(server: FakeServerProcess): RequestMessage[] {
  return messagesOf(server).flatMap((message) =>
    message.type === 'request' ? [message] : [],
  );
}

export function requestIds(server: FakeServerProcess): string[] {
  return requestMessages(server).map((message) => message.requestId);
}

/** Asserts the server received exactly one message and that it is `hello`. */
export function expectOnlyHello(server: FakeServerProcess): void {
  const messages = messagesOf(server);
  expect(messages).toHaveLength(1);
  const [first] = messages;
  if (first?.type !== 'hello') {
    throw new Error(`expected hello, got ${first?.type ?? 'no message'}`);
  }
  expect(first.portalGeneration).toBeGreaterThan(0);
}

export interface FakeServerOptions {
  /** Ack `shutdown` with `closed`, like a cooperative Server. */
  autoCloseOnShutdown: boolean;
  /** Record the exit right after `closed`. False models a process that lingers. */
  exitOnShutdown?: boolean;
  /** Reply `ready` to `hello` and exit at once: the crash-on-first-request loop. */
  crashOnHello?: boolean;
  /** Default true. False models a process that survives SIGKILL. */
  killExits?: boolean;
  /**
   * Model the real spawn's ordering: `recordExit` closes the channel (which
   * fires the Portal's `onClose` synchronously) before the `exited` promise
   * settles. Without it a test cannot exercise the crash path where the
   * channel close wins the race against the promise continuation.
   */
  closeChannelOnExit?: boolean;
}

/**
 * A stand-in Server process: the far end of an in-memory channel pair, with a
 * hand-resolvable `exited` promise so a crash is deterministic.
 */
export class FakeServerProcess {
  readonly received: unknown[] = [];
  readonly handle: SpawnedServer;
  private readonly channel: IpcChannel;
  private readonly exit = Promise.withResolvers<ServerExitInfo>();
  private readonly autoCloseOnShutdown: boolean;
  private readonly exitOnShutdown: boolean;
  private readonly crashOnHello: boolean;
  private readonly killExits: boolean;
  private readonly closeChannelOnExit: boolean;
  private exitRecorded = false;
  private exitInfo: ServerExitInfo | undefined;
  private requestCount = 0;
  private closeOnRequestAt: number | undefined;
  private kills = 0;

  constructor(options: FakeServerOptions) {
    const [portal, server] = createInMemoryChannelPair();
    this.autoCloseOnShutdown = options.autoCloseOnShutdown;
    this.exitOnShutdown = options.exitOnShutdown ?? true;
    this.crashOnHello = options.crashOnHello ?? false;
    this.killExits = options.killExits ?? true;
    this.closeChannelOnExit = options.closeChannelOnExit ?? false;
    this.channel = server;
    this.handle = {
      channel: portal,
      exited: this.exit.promise,
      // The real spawn records the exit before it notifies the channel close, so
      // this is populated by the time `onClose` runs.
      lastExit: () => this.exitInfo,
      kill: () => {
        this.kills += 1;
        // A real SIGKILL ends the process. Without this the executor would pay
        // its exit-wait timeout on every kill.
        if (this.killExits) {
          this.recordExit({ code: null, signal: 'SIGKILL', observed: true });
        }
      },
    };
    server.onMessage((raw) => {
      this.received.push(raw);
      const parsed = parseIpcMessage(raw);
      if (!parsed.ok) {
        return;
      }
      if (parsed.message.type === 'request') {
        this.requestCount += 1;
        if (this.requestCount === this.closeOnRequestAt) {
          // A crash while handling a request: the link dies mid-flush.
          this.channel.close('test crash on request');
        }
      }
      if (this.autoCloseOnShutdown && parsed.message.type === 'shutdown') {
        this.send({ type: 'closed', reason: 'test shutdown' });
        // The real Server exits after `closed`; without this the executor would
        // wait out its exit timeout on every close().
        if (this.exitOnShutdown) {
          this.recordExit({ code: 0, signal: null, observed: true });
        }
      }
      if (this.crashOnHello && parsed.message.type === 'hello') {
        this.send(readyMessage());
        this.recordExit({ code: 1, signal: null, observed: true });
      }
    });
  }

  send(message: IpcMessage): void {
    this.channel.send(message);
  }

  /** Bypasses the message type: the real peer can put anything on the wire. */
  sendRaw(raw: unknown): void {
    // Method parameters are bivariant, so this widens the send signature
    // without an assertion: the channel treats the value as opaque.
    const sender: { send(message: unknown): void } = this.channel;
    sender.send(raw);
  }

  crash(code: number | null = 1): void {
    this.recordExit({ code, signal: null, observed: true });
  }

  /**
   * An exit that could not be observed: the process may or may not be gone.
   * Used to check the executor keeps its defensive kill rather than assuming
   * the process exited.
   */
  unobservableExit(): void {
    this.recordExit({ code: null, signal: null, observed: false });
  }

  /** A dead link without an exit event: the other half of the crash race. */
  closeChannel(): void {
    this.channel.close('test channel closed');
  }

  /** Kills the link once this many requests have been received. */
  closeOnRequest(count: number): void {
    this.closeOnRequestAt = count;
  }

  /** Records a real exit after a delay, modelling a slow SIGKILL. */
  exitAfter(
    delayMs: number,
    info: ServerExitInfo = {
      code: null,
      signal: 'SIGKILL',
      observed: true,
    },
  ): void {
    setTimeout(() => {
      this.recordExit(info);
    }, delayMs);
  }

  get killCount(): number {
    return this.kills;
  }

  private recordExit(info: ServerExitInfo): void {
    if (this.exitRecorded) {
      return;
    }
    this.exitRecorded = true;
    this.exitInfo = info;
    if (this.closeChannelOnExit) {
      // Mirrors `spawnBunServer`: `markClosed` runs before `exit.resolve`, so
      // the Portal's `onClose` sees the exit synchronously while the promise's
      // continuation has not run yet.
      this.channel.close('server process exited');
    }
    this.exit.resolve(info);
  }
}

export interface HarnessOptions {
  timeoutMs: number;
  /** Replaces the collecting logger: `logs()` then stays empty. */
  logger?: Logger;
  maxReplays?: number;
  autoClose?: boolean;
  crashOnHello?: boolean;
  restartBackoffMs?: number;
  shutdownTimeoutMs?: number;
  stableResetMs?: number;
  killExits?: boolean;
  exitOnShutdown?: boolean;
  startTimeoutMs?: number;
  maxPending?: number;
  closeChannelOnExit?: boolean;
}

export interface Harness {
  executor: IpcExecutor;
  servers: FakeServerProcess[];
  logs: () => Record<string, unknown>[];
}

export function harness(options: Partial<HarnessOptions> = {}): Harness {
  const lines: string[] = [];
  const logger: Logger =
    options.logger ??
    createJsonLogger({
      level: 'debug',
      sink: (line) => {
        lines.push(line);
      },
    });
  const servers: FakeServerProcess[] = [];
  const executor = new IpcExecutor({
    spawn: () => {
      const server = new FakeServerProcess({
        autoCloseOnShutdown: options.autoClose ?? true,
        exitOnShutdown: options.exitOnShutdown,
        crashOnHello: options.crashOnHello,
        killExits: options.killExits,
        closeChannelOnExit: options.closeChannelOnExit,
      });
      servers.push(server);
      return server.handle;
    },
    timeoutMs: options.timeoutMs ?? 1000,
    maxReplays: options.maxReplays,
    restartBackoffMs: options.restartBackoffMs,
    shutdownTimeoutMs: options.shutdownTimeoutMs,
    stableResetMs: options.stableResetMs,
    startTimeoutMs: options.startTimeoutMs,
    maxPending: options.maxPending,
    logger,
  });
  return {
    executor,
    servers,
    logs: () =>
      lines.map((line) => {
        const parsed: unknown = JSON.parse(line);
        if (!isRecord(parsed)) {
          throw new Error(`log line is not an object: ${line}`);
        }
        return parsed;
      }),
  };
}

export function serverAt(h: Harness, index: number): FakeServerProcess {
  const server = h.servers[index];
  if (server === undefined) {
    throw new Error(`server ${index} was never spawned`);
  }
  return server;
}

/** Starts the executor and completes the handshake with one Server. */
export async function started(
  h: Harness,
  caps?: number,
): Promise<FakeServerProcess> {
  const startPromise = h.executor.start();
  const server = serverAt(h, 0);
  server.send(readyMessage(caps));
  await startPromise;
  return server;
}
