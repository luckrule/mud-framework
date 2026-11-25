import { expect } from 'bun:test';

import { parseJsonLine, tick } from 'mud-framework-test-support';

import type { IpcChannel } from '../ipc/channel.ts';
import { createInMemoryChannelPair } from '../ipc/channel.ts';
import type { IpcMessage, RequestMessage } from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import type { ServerExitInfo, SpawnedServer } from '../ipc/spawn-port.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import { IpcExecutor } from '../portal/executor/ipc-executor.ts';
import type {
  ExecutorOutcome,
  ExecutorRequest,
  SessionChange,
} from '../shared/executor.ts';

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

export function preparedMessage(pid = 4242): IpcMessage {
  return { type: 'prepared', pid };
}

export function readyMessage(
  maxTransactionDurationMs = 5000,
  pid = 4242,
): IpcMessage {
  return {
    type: 'ready',
    pid,
    capabilities: {
      transactions: 'full',
      concurrency: 'pooled',
      snapshotRead: true,
      uniqueConstraint: true,
      maxTransactionDurationMs,
    },
  };
}

export function successMessage(
  requestId: string,
  result: unknown,
  sessionChange?: SessionChange,
): IpcMessage {
  return sessionChange === undefined
    ? { type: 'result', requestId, ok: true, result }
    : { type: 'result', requestId, ok: true, result, sessionChange };
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
}

/**
 * Asserts the completed two-phase handshake and nothing else: `hello` followed
 * by `activate` (ADR-0007). A request at this point would mean the Portal
 * delivered before `ready`.
 */
export function expectHandshake(server: FakeServerProcess): void {
  const messages = messagesOf(server);
  expect(messages.map((message) => message.type)).toEqual([
    'hello',
    'activate',
  ]);
}

export interface FakeServerOptions {
  /** Ack `shutdown` with `closed`, like a cooperative Server. */
  autoCloseOnShutdown: boolean;
  /** Record the exit right after `closed`. False models a process that lingers. */
  exitOnShutdown?: boolean;
  /**
   * Answer `hello` with `prepared` (ADR-0007). On by default: the two-phase
   * handshake is the only startup path, so a fake that stayed silent on `hello`
   * would model a Server that can never start.
   */
  autoPrepare?: boolean;
  /**
   * Answer `prepared` to `hello`, `ready` to `activate`, and exit at once: the
   * "ready, then die on the first request" loop.
   */
  crashOnActivate?: boolean;
  /** Default true. False models a process that survives SIGKILL. */
  killExits?: boolean;
  /**
   * Model the real spawn's ordering: `recordExit` closes the channel (which
   * fires the Portal's `onClose` synchronously) before the `exited` promise
   * settles. Without it a test cannot exercise the crash path where the
   * channel close wins the race against the promise continuation.
   */
  closeChannelOnExit?: boolean;
  /** Ignore `hello`: models a spawn that never reaches `prepared`. */
  silentOnHello?: boolean;
  /** Answer `activate` with `ready` (after `prepared`). Default false. */
  readyOnActivate?: boolean;
  /** Reported in `prepared`/`ready`; defaults to 4242. */
  pid?: number;
  /** Shared event log for cross-process ordering assertions. */
  timeline?: string[];
  /** Prefix for this process's timeline entries. */
  label?: string;
  /**
   * Hold delivered requests and answer them with a success only when `shutdown`
   * arrives, before `closed`. Models a Server draining its in-flight work during
   * a switch, so the test can assert where a request settled.
   */
  holdRequests?: boolean;
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
  private readonly autoPrepare: boolean;
  private readonly crashOnActivate: boolean;
  private readonly killExits: boolean;
  private readonly closeChannelOnExit: boolean;
  private readonly silentOnHello: boolean;
  private readonly readyOnActivate: boolean;
  private readonly pid: number;
  private readonly timeline: string[] | undefined;
  private readonly label: string;
  private readonly holdRequests: boolean;
  /** Request ids delivered while `holdRequests` is on, in arrival order. */
  private readonly held: string[] = [];
  private exitRecorded = false;
  private exitInfo: ServerExitInfo | undefined;
  private requestCount = 0;
  private closeOnRequestAt: number | undefined;
  private kills = 0;

  constructor(options: FakeServerOptions) {
    const [portal, server] = createInMemoryChannelPair();
    this.autoCloseOnShutdown = options.autoCloseOnShutdown;
    this.exitOnShutdown = options.exitOnShutdown ?? true;
    this.autoPrepare = options.autoPrepare ?? true;
    this.crashOnActivate = options.crashOnActivate ?? false;
    this.killExits = options.killExits ?? true;
    this.closeChannelOnExit = options.closeChannelOnExit ?? false;
    this.silentOnHello = options.silentOnHello ?? false;
    this.readyOnActivate = options.readyOnActivate ?? false;
    this.pid = options.pid ?? 4242;
    this.timeline = options.timeline;
    this.label = options.label ?? `server-${String(this.pid)}`;
    this.holdRequests = options.holdRequests ?? false;
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
      this.mark(`recv:${parsed.message.type}`);
      if (parsed.message.type === 'request') {
        this.requestCount += 1;
        if (this.holdRequests) {
          this.held.push(parsed.message.requestId);
        }
        if (this.requestCount === this.closeOnRequestAt) {
          // A crash while handling a request: the link dies mid-flush.
          this.channel.close('test crash on request');
        }
      }
      if (this.autoCloseOnShutdown && parsed.message.type === 'shutdown') {
        if (this.holdRequests) {
          // Drain first: every held request settles before `closed`, like a
          // Server that finishes in-flight work during its graceful close.
          for (const requestId of this.held.splice(0)) {
            this.send({
              type: 'result',
              requestId,
              ok: true,
              result: `drained-${requestId}`,
            });
          }
        }
        this.send({ type: 'closed', reason: 'test shutdown' });
        this.mark('closed');
        // The real Server exits after `closed`; without this the executor would
        // wait out its exit timeout on every close().
        if (this.exitOnShutdown) {
          this.recordExit({ code: 0, signal: null, observed: true });
        }
      }
      if (this.autoPrepare && parsed.message.type === 'hello') {
        if (!this.silentOnHello) {
          this.send(preparedMessage(this.pid));
        }
      }
      if (parsed.message.type === 'activate') {
        this.mark('activate');
        if (this.crashOnActivate) {
          this.send(readyMessage(5000, this.pid));
          this.recordExit({ code: 1, signal: null, observed: true });
        } else if (this.readyOnActivate) {
          this.send(readyMessage(5000, this.pid));
        }
      }
    });
  }

  /** Appends one entry to the shared timeline, when a test supplied one. */
  private mark(event: string): void {
    this.timeline?.push(`${this.label}:${event}`);
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
    this.mark('exit');
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
  autoPrepare?: boolean;
  crashOnActivate?: boolean;
  restartBackoffMs?: number;
  shutdownTimeoutMs?: number;
  stableResetMs?: number;
  killExits?: boolean;
  exitOnShutdown?: boolean;
  startTimeoutMs?: number;
  maxPending?: number;
  closeChannelOnExit?: boolean;
  /** Answer `activate` with `ready` for the initial spawn. */
  readyOnActivate?: boolean;
  /** Hold delivered requests until `shutdown` for the initial spawn. */
  holdRequests?: boolean;
  /** Shared event log forwarded to every fake this harness spawns. */
  timeline?: string[];
  /** Label for the initial fake's timeline entries; defaults per pid. */
  label?: string;
}

export interface Harness {
  executor: IpcExecutor;
  servers: FakeServerProcess[];
  logs: () => Record<string, unknown>[];
  /**
   * Builds a fake process with the harness defaults plus overrides, recording
   * it in `servers`. `switchTo` supplies its own spawn, so a test uses this to
   * describe the replacement instead of relying on the initial factory.
   */
  makeServer: (overrides?: Partial<FakeServerOptions>) => SpawnedServer;
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
  const makeServer = (
    overrides: Partial<FakeServerOptions> = {},
  ): SpawnedServer => {
    const server = new FakeServerProcess({
      autoCloseOnShutdown: options.autoClose ?? true,
      exitOnShutdown: options.exitOnShutdown,
      autoPrepare: options.autoPrepare,
      crashOnActivate: options.crashOnActivate,
      killExits: options.killExits,
      closeChannelOnExit: options.closeChannelOnExit,
      readyOnActivate: options.readyOnActivate,
      holdRequests: options.holdRequests,
      timeline: options.timeline,
      label: options.label,
      ...overrides,
    });
    servers.push(server);
    return server.handle;
  };
  const executor = new IpcExecutor({
    spawn: () => makeServer(),
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
    logs: () => lines.map((line) => parseJsonLine(line, 'log line')),
    makeServer,
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
