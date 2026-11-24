import { describe, expect, jest, test } from 'bun:test';

import type { IpcChannel } from '../ipc/channel.ts';
import { createInMemoryChannelPair } from '../ipc/channel.ts';
import type { IpcMessage, RequestMessage } from '../ipc/protocol.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import type { ServerExitInfo, SpawnedServer } from '../ipc/spawn-port.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { ExecutorOutcome, ExecutorRequest } from './executor.ts';
import { IpcExecutor } from './ipc-executor.ts';

const tick = (): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, 0);
  });

/**
 * The in-memory channel delivers synchronously, but crash detection, the exit
 * wait, the restart timer and the ready handshake each cross a task boundary.
 * Five macrotask hops are enough for the whole chain, with no wall-clock wait.
 */
async function flushTicks(): Promise<void> {
  await tick();
  await tick();
  await tick();
  await tick();
  await tick();
}

/** Enough microtask hops for a timer callback's continuation to finish. */
async function flushMicrotasks(): Promise<void> {
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
async function waitFor(
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

async function rejectionOf(promise: Promise<unknown>): Promise<Error> {
  try {
    await promise;
  } catch (error) {
    if (error instanceof Error) {
      return error;
    }
    throw new Error(`non-Error rejection: ${String(error)}`, { cause: error });
  }
  throw new Error('expected the promise to reject');
}

function readyMessage(maxTransactionDurationMs = 5000): IpcMessage {
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

function successMessage(requestId: string, result: unknown): IpcMessage {
  return { type: 'result', requestId, ok: true, result };
}

function boundSuccessMessage(
  requestId: string,
  result: unknown,
  sessionId: string,
): IpcMessage {
  return { type: 'result', requestId, ok: true, result, sessionId };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function request(overrides: Partial<ExecutorRequest> = {}): ExecutorRequest {
  return {
    requestId: 'req-1',
    connectionId: 'c1',
    clientRequestId: 'client-1',
    method: 'echo',
    payload: null,
    ...overrides,
  };
}

function failureOf(outcome: ExecutorOutcome): {
  code: string;
  message: string;
} {
  if (outcome.ok) {
    throw new Error('expected a failure outcome');
  }
  return outcome.error;
}

function messagesOf(server: FakeServerProcess): IpcMessage[] {
  return server.received.flatMap((raw) => {
    const parsed = parseIpcMessage(raw);
    if (!parsed.ok) {
      throw new Error(`portal sent an unparsable message: ${parsed.reason}`);
    }
    return [parsed.message];
  });
}

function requestMessages(server: FakeServerProcess): RequestMessage[] {
  return messagesOf(server).flatMap((message) =>
    message.type === 'request' ? [message] : [],
  );
}

function requestIds(server: FakeServerProcess): string[] {
  return requestMessages(server).map((message) => message.requestId);
}

/** Asserts the server received exactly one message and that it is `hello`. */
function expectOnlyHello(server: FakeServerProcess): void {
  const messages = messagesOf(server);
  expect(messages).toHaveLength(1);
  const [first] = messages;
  if (first?.type !== 'hello') {
    throw new Error(`expected hello, got ${first?.type ?? 'no message'}`);
  }
  expect(first.portalGeneration).toBeGreaterThan(0);
}

interface FakeServerOptions {
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
class FakeServerProcess {
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

interface HarnessOptions {
  timeoutMs: number;
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

interface Harness {
  executor: IpcExecutor;
  servers: FakeServerProcess[];
  logs: () => Record<string, unknown>[];
}

function harness(options: Partial<HarnessOptions> = {}): Harness {
  const lines: string[] = [];
  const logger: Logger = createJsonLogger({
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

function serverAt(h: Harness, index: number): FakeServerProcess {
  const server = h.servers[index];
  if (server === undefined) {
    throw new Error(`server ${index} was never spawned`);
  }
  return server;
}

/** Starts the executor and completes the handshake with one Server. */
async function started(h: Harness, caps?: number): Promise<FakeServerProcess> {
  const startPromise = h.executor.start();
  const server = serverAt(h, 0);
  server.send(readyMessage(caps));
  await startPromise;
  return server;
}

describe('IpcExecutor', () => {
  test('sends hello and holds requests until the server is ready', async () => {
    const h = harness({ timeoutMs: 500 });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);

    expectOnlyHello(server);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    await flushTicks();
    // The recovery barrier (SPEC 6.2): queued, not failed, not dropped.
    expect(server.received).toHaveLength(1);

    server.send(readyMessage());
    await startPromise;
    await flushTicks();
    expect(requestIds(server)).toEqual(['req-1']);

    server.send(successMessage('req-1', 'ok'));
    expect(await outcomePromise).toEqual({ ok: true, result: 'ok' });
    await h.executor.close();
  });

  test('returns the server result as a success outcome', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(
      request({
        requestId: 'req-1',
        connectionId: 'c9',
        clientRequestId: 'client-9',
        sessionId: 's1',
        method: 'look',
        payload: { at: 'rock' },
      }),
    );

    const [message] = requestMessages(server);
    expect(message).toMatchObject({
      type: 'request',
      requestId: 'req-1',
      connectionId: 'c9',
      clientRequestId: 'client-9',
      sessionId: 's1',
      method: 'look',
      payload: { at: 'rock' },
      replayCount: 0,
    });
    // The Portal sends the remaining budget, which may already be a millisecond
    // below the ceiling by the time the message is built.
    expect(message?.timeoutMs).toBeGreaterThan(0);
    expect(message?.timeoutMs).toBeLessThanOrEqual(1000);
    server.send(successMessage('req-1', { hp: 3 }));
    expect(await outcomePromise).toEqual({ ok: true, result: { hp: 3 } });
    await h.executor.close();
  });

  test('passes a business failure through unchanged', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    server.send({
      type: 'result',
      requestId: 'req-1',
      ok: false,
      error: { code: 'NOT_ENOUGH_GOLD', message: 'you cannot afford that' },
    });

    const outcome = await outcomePromise;
    expect(outcome.ok).toBe(false);
    expect(failureOf(outcome)).toEqual({
      code: 'NOT_ENOUGH_GOLD',
      message: 'you cannot afford that',
    });
    await h.executor.close();
  });

  test('carries the session id a result reports into the outcome', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    server.send(boundSuccessMessage('req-1', 'welcome', 'session-1'));

    expect(await outcomePromise).toEqual({
      ok: true,
      result: 'welcome',
      sessionId: 'session-1',
    });
    const received = h.logs().find((line) => line.msg === 'result received');
    expect(received).toMatchObject({ sessionId: 'session-1' });
    await h.executor.close();
  });

  test('omits the session id when the result reports none', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    server.send(successMessage('req-1', 'ok'));

    expect(await outcomePromise).toEqual({ ok: true, result: 'ok' });
    const received = h.logs().find((line) => line.msg === 'result received');
    expect(received).not.toHaveProperty('sessionId');
    await h.executor.close();
  });

  test('times out, cancels, and discards the late result', async () => {
    const h = harness({ timeoutMs: 30 });
    const server = await started(h);

    const outcome = await h.executor.execute(request({ requestId: 'req-1' }));

    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    expect(failureOf(outcome).message).toContain('timed out');
    // The Server is told to stop working on a request Portal already settled.
    expect(messagesOf(server)).toContainEqual({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });

    // A result that arrives after the terminal state must be dropped quietly.
    server.send(successMessage('req-1', 'too late'));
    await flushTicks();
    expect(h.logs().some((line) => line.msg === 'late result discarded')).toBe(
      true,
    );
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg === 'late result discarded' && line.level === 'debug',
        ),
    ).toBe(true);
    await h.executor.close();
  });

  test('a late result does not settle the request a second time', async () => {
    const h = harness({ timeoutMs: 30 });
    const server = await started(h);
    let settles = 0;

    const outcome = await h.executor
      .execute(request({ requestId: 'req-1' }))
      .then((value) => {
        settles += 1;
        return value;
      });

    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    server.send(successMessage('req-1', 'too late'));
    await flushTicks();

    expect(settles).toBe(1);
    // Still the timeout: the late result must not replace the settled outcome.
    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    await h.executor.close();
  });

  test('takes the backend ceiling when it is below the portal ceiling', async () => {
    const h = harness({ timeoutMs: 1000 });
    const server = await started(h, 30);

    const outcome = await h.executor.execute(request({ requestId: 'req-1' }));

    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    const timeoutMs = requestMessages(server)[0]?.timeoutMs;
    expect(timeoutMs).toBeGreaterThan(0);
    expect(timeoutMs).toBeLessThanOrEqual(30);
    await h.executor.close();
  });

  test('keeps the portal ceiling when it is below the backend ceiling', async () => {
    const h = harness({ timeoutMs: 25 });
    const server = await started(h, 60_000);

    const outcome = await h.executor.execute(request({ requestId: 'req-1' }));

    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    const timeoutMs = requestMessages(server)[0]?.timeoutMs;
    expect(timeoutMs).toBeGreaterThan(0);
    expect(timeoutMs).toBeLessThanOrEqual(25);
    await h.executor.close();
  });

  test('a later server cannot extend an existing deadline', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h, 100);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    const firstTimeout = requestMessages(first)[0]?.timeoutMs;
    expect(firstTimeout).toBeGreaterThan(0);
    expect(firstTimeout).toBeLessThanOrEqual(100);

    first.crash();
    await flushTicks();
    const second = serverAt(h, 1);
    // A longer ceiling must not reopen the budget the Portal already promised
    // (SPEC 3.4): the replay still gets what is left of the original deadline.
    second.send(readyMessage(60_000));
    await flushTicks();

    const replay = requestMessages(second)[0];
    expect(replay?.replayCount).toBe(1);
    expect(replay?.timeoutMs).toBeGreaterThan(0);
    expect(replay?.timeoutMs).toBeLessThanOrEqual(100);

    second.send(successMessage('req-1', null));
    expect((await outcomePromise).ok).toBe(true);
    await h.executor.close();
  });

  test('restarts after a crash and replays the unacknowledged request', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    expect(requestIds(first)).toEqual(['req-1']);

    first.crash();
    await flushTicks();

    expect(h.servers).toHaveLength(2);
    const second = serverAt(h, 1);
    expectOnlyHello(second);

    second.send(readyMessage());
    await flushTicks();

    // Same id, incremented replay count (SPEC 7.3): the replay is the same
    // Request, not a new one.
    const [replay] = requestMessages(second);
    expect(replay?.requestId).toBe('req-1');
    expect(replay?.replayCount).toBe(1);
    expect(
      h
        .logs()
        .some(
          (line) => line.msg === 'request replayed' && line.replayCount === 1,
        ),
    ).toBe(true);

    second.send(successMessage('req-1', 'after restart'));
    expect(await outcomePromise).toEqual({
      ok: true,
      result: 'after restart',
    });
    await h.executor.close();
  });

  test('gives up with FRAMEWORK_REPLAY_EXHAUSTED after maxReplays', async () => {
    const h = harness({ timeoutMs: 5000, maxReplays: 1 });
    const first = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));

    first.crash();
    // The second restart already pays a real backoff: `ready` no longer resets
    // the crash streak, so the first restart is immediate and later ones are not.
    await waitFor(() => h.servers.length === 2);
    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();
    expect(requestMessages(second)).toHaveLength(1);
    expect(requestMessages(second)[0]?.replayCount).toBe(1);

    second.crash();
    await waitFor(() => h.servers.length === 3);
    const third = serverAt(h, 2);
    third.send(readyMessage());
    await flushTicks();

    // Budget spent: no third delivery, a terminal state instead (SPEC 6.5).
    expect(requestMessages(third)).toHaveLength(0);
    const outcome = await outcomePromise;
    expect(failureOf(outcome).code).toBe('FRAMEWORK_REPLAY_EXHAUSTED');
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg === 'request replay exhausted' && line.level === 'error',
        ),
    ).toBe(true);
    await h.executor.close();
  });

  test('replays one connection in its original order', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    // Two requests on c1 with a c2 request between them: the interleaving is
    // what a timed-out chain produces (the chain releases at the timeout, so
    // several unacknowledged requests can coexist on one connection).
    void h.executor.execute(request({ requestId: 'a1', connectionId: 'c1' }));
    void h.executor.execute(request({ requestId: 'b1', connectionId: 'c2' }));
    void h.executor.execute(request({ requestId: 'a2', connectionId: 'c1' }));
    expect(requestIds(first)).toEqual(['a1', 'b1', 'a2']);

    first.crash();
    await flushTicks();
    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();

    // Grouped by connection, ascending per-connection sequence inside a group.
    expect(requestIds(second)).toEqual(['a1', 'a2', 'b1']);
    for (const message of requestMessages(second)) {
      expect(message.replayCount).toBe(1);
    }

    for (const requestId of ['a1', 'a2', 'b1']) {
      second.send(successMessage(requestId, null));
    }
    await h.executor.close();
  });

  test('orders a replay before a request queued during the restart', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    const firstOutcome = h.executor.execute(
      request({ requestId: 'A', connectionId: 'c1' }),
    );
    expect(requestIds(first)).toEqual(['A']);

    first.crash();
    await flushTicks();
    const second = serverAt(h, 1);
    // Queued while the replacement is still starting: a fresh request with a
    // higher per-connection sequence than the replay.
    const secondOutcome = h.executor.execute(
      request({ requestId: 'B', connectionId: 'c1' }),
    );

    second.send(readyMessage());
    await flushTicks();

    const messages = requestMessages(second);
    expect(messages.map((message) => message.requestId)).toEqual(['A', 'B']);
    expect(messages.map((message) => message.replayCount)).toEqual([1, 0]);

    second.send(successMessage('A', null));
    second.send(successMessage('B', null));
    await Promise.all([firstOutcome, secondOutcome]);
    await h.executor.close();
  });

  test('resumes the flush after a mid-flush delivery failure', async () => {
    const h = harness({ timeoutMs: 5000 });
    const startPromise = h.executor.start();
    const first = serverAt(h, 0);
    // The second delivery kills the link: the flush must stop, then replay
    // everything unsent on the replacement without dropping or duplicating.
    first.closeOnRequest(2);

    const outcomes = ['A', 'B', 'C'].map((requestId) =>
      h.executor.execute(request({ requestId, connectionId: 'c1' })),
    );

    first.send(readyMessage());
    await startPromise;
    await flushTicks();

    expect(requestIds(first)).toEqual(['A', 'B']);
    expect(h.servers).toHaveLength(2);

    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();

    const replayed = requestMessages(second);
    expect(replayed.map((message) => message.requestId)).toEqual([
      'A',
      'B',
      'C',
    ]);
    // A and B reached the first Server (its close ran inside B's send), so both
    // are replays; C never left the Portal.
    expect(replayed.map((message) => message.replayCount)).toEqual([1, 1, 0]);

    for (const requestId of ['A', 'B', 'C']) {
      second.send(successMessage(requestId, null));
    }
    const settled = await Promise.all(outcomes);
    expect(settled.every((outcome) => outcome.ok)).toBe(true);
    await h.executor.close();
  });

  test('a request timeoutMs tightens the deadline and cannot extend it', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h, 60_000);

    // Smaller than the ceiling: the caller's deadline wins, and the Server is
    // given the same budget so it rolls back at the same instant.
    const outcome = await h.executor.execute(
      request({ requestId: 'req-1', timeoutMs: 20 }),
    );
    expect(failureOf(outcome).code).toBe('FRAMEWORK_TIMEOUT');
    const sent = requestMessages(server)[0]?.timeoutMs;
    expect(sent).toBeGreaterThan(0);
    expect(sent).toBeLessThanOrEqual(20);

    // Larger than the ceiling: the executor's own promise is unchanged.
    const second = h.executor.execute(
      request({ requestId: 'req-2', timeoutMs: 60_000 }),
    );
    const secondTimeout = requestMessages(server)[1]?.timeoutMs;
    expect(secondTimeout).toBeGreaterThan(20);
    expect(secondTimeout).toBeLessThanOrEqual(5000);
    await h.executor.cancel('req-2', 'TRANSPORT_CLOSED');
    await second;
    await h.executor.close();
  });

  test('close sends shutdown and waits for closed', async () => {
    const h = harness();
    const server = await started(h);

    await h.executor.close();

    expect(messagesOf(server)).toContainEqual({ type: 'shutdown' });
    expect(server.killCount).toBe(0);
  });

  test('kills the server when closed never arrives', async () => {
    // Fake timers so the 5s shutdown grace period costs no wall-clock time.
    jest.useFakeTimers();
    try {
      const h = harness({ timeoutMs: 5000, autoClose: false });
      const server = await started(h);

      const closing = h.executor.close();
      await flushMicrotasks();
      expect(messagesOf(server)).toContainEqual({ type: 'shutdown' });
      expect(server.killCount).toBe(0);

      jest.advanceTimersByTime(5000);
      await flushMicrotasks();
      await closing;

      expect(server.killCount).toBe(1);
    } finally {
      jest.useRealTimers();
    }
  });

  test('honours the shutdown timeout option', async () => {
    const h = harness({
      timeoutMs: 5000,
      autoClose: false,
      shutdownTimeoutMs: 20,
    });
    const server = await started(h);
    const before = Date.now();

    await h.executor.close();

    expect(server.killCount).toBe(1);
    expect(Date.now() - before).toBeLessThan(1000);
  });

  test('rejects requests after close instead of hanging', async () => {
    const h = harness();
    await started(h);
    await h.executor.close();

    const outcome = await h.executor.execute(request({ requestId: 'late' }));

    expect(failureOf(outcome).code).toBe('FRAMEWORK_SHUTDOWN');
    // Idempotent: a second close resolves rather than throwing.
    await h.executor.close();
  });

  test('fails immediately when execute is called before start', async () => {
    const h = harness({ timeoutMs: 5000 });

    const outcome = await h.executor.execute(request({ requestId: 'early' }));

    // Not a silent wait for the deadline: nothing was ever spawned, and that
    // must be visible in the outcome and the logs (SPEC 6.2 step 3).
    expect(failureOf(outcome).code).toBe('INTERNAL_ERROR');
    expect(failureOf(outcome).message).toContain('never started');
    expect(h.servers).toHaveLength(0);
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg === 'request rejected' &&
            line.code === 'INTERNAL_ERROR' &&
            line.level === 'error',
        ),
    ).toBe(true);
  });

  test('rejects start when close wins the race to readiness', async () => {
    const h = harness({ timeoutMs: 5000 });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);
    expectOnlyHello(server);

    await h.executor.close();

    // "Ready" and "closed" must not look the same to a caller.
    const error = await rejectionOf(startPromise);
    expect(error.message).toMatch(/closed before the server became ready/);
    expect(server.killCount).toBe(1);
  });

  test('rejects a duplicate requestId with a logged code', async () => {
    const h = harness();
    const server = await started(h);

    void h.executor.execute(request({ requestId: 'dup' }));
    const duplicate = await h.executor.execute(request({ requestId: 'dup' }));

    expect(failureOf(duplicate).code).toBe('INTERNAL_ERROR');
    const rejected = h.logs().find((line) => line.msg === 'request rejected');
    expect(rejected).toMatchObject({ level: 'warn', code: 'INTERNAL_ERROR' });

    server.send(successMessage('dup', null));
    await h.executor.close();
  });

  test('cancel sends a cancel message to the server', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h);

    void h.executor.execute(request({ requestId: 'req-1' }));
    expect(requestIds(server)).toEqual(['req-1']);

    await h.executor.cancel('req-1', 'TRANSPORT_CLOSED');

    expect(messagesOf(server)).toContainEqual({
      type: 'cancel',
      requestId: 'req-1',
      code: 'TRANSPORT_CLOSED',
    });
    const cancelled = h.logs().find((line) => line.msg === 'request cancelled');
    expect(cancelled).toMatchObject({
      level: 'warn',
      requestId: 'req-1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      code: 'TRANSPORT_CLOSED',
    });
    expect(typeof cancelled?.durationMs).toBe('number');
    await h.executor.close();
  });

  test('cancel settles locally and stops replaying the request', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    expect(requestIds(first)).toEqual(['req-1']);

    await h.executor.cancel('req-1', 'TRANSPORT_CLOSED');
    const outcome = await outcomePromise;

    expect(outcome.ok).toBe(false);
    expect(failureOf(outcome)).toEqual({
      code: 'TRANSPORT_CLOSED',
      message: 'request req-1 was cancelled',
    });

    // The buffer no longer holds the id, so a crash cannot replay it: its
    // answer was already discarded, and a replay would be work nobody reads.
    first.crash();
    await flushTicks();
    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();
    expect(requestIds(second)).toEqual([]);

    // A result for the cancelled id is a late result, not a second settlement.
    second.send(successMessage('req-1', 'too late'));
    await flushTicks();
    expect(h.logs().some((line) => line.msg === 'late result discarded')).toBe(
      true,
    );
    await h.executor.close();
  });

  test('cancel for an unknown requestId is a no-op', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h);

    await h.executor.cancel('never-seen', 'TRANSPORT_CLOSED');

    // Nothing was accepted under that id, so there is nothing to tell the
    // Server: only the handshake ever went out.
    expectOnlyHello(server);
    expect(h.logs().some((line) => line.msg === 'request cancelled')).toBe(
      false,
    );
    await h.executor.close();
  });

  test('close fails a request that never reached a server', async () => {
    const h = harness({ timeoutMs: 5000 });
    // Started but never ready: the request is buffered, not delivered.
    void h.executor.start().catch(() => undefined);
    await flushTicks();

    const outcomePromise = h.executor.execute(request({ requestId: 'queued' }));
    await h.executor.close();

    expect(failureOf(await outcomePromise).code).toBe('FRAMEWORK_SHUTDOWN');
    expect(requestIds(serverAt(h, 0))).toEqual([]);
  });

  test('ignores malformed messages and keeps running', async () => {
    const h = harness();
    const server = await started(h);

    server.sendRaw({ type: 'not-a-message' });
    server.sendRaw({ type: 'result' });
    await flushTicks();

    const warnings = h
      .logs()
      .filter(
        (line) => line.msg === 'ipc message rejected' && line.level === 'warn',
      );
    expect(warnings).toHaveLength(2);
    expect(warnings[0]?.reason).toEqual(expect.any(String));

    // Still functional after the bad input.
    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    server.send(successMessage('req-1', 'alive'));
    expect(await outcomePromise).toEqual({ ok: true, result: 'alive' });
    await h.executor.close();
  });

  test('treats exit and channel close as one crash', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    // Both signals fire, in the order that leaves the channel handler last:
    // one restart, not two.
    first.closeChannel();
    first.crash();
    await flushTicks();

    expect(h.servers).toHaveLength(2);
    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();

    // Still exactly one buffered request: a double restart would have lost or
    // duplicated it.
    expect(requestIds(second)).toEqual(['req-1']);
    second.send(successMessage('req-1', 'once'));
    expect(await outcomePromise).toEqual({ ok: true, result: 'once' });
    await h.executor.close();
  });

  test('kills an orphaned server before restarting', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);
    void h.executor.execute(request({ requestId: 'req-1' }));

    // The pipe dies but the process is still alive: restarting beside it would
    // leave two storage writers (INV-002).
    first.closeChannel();
    await flushTicks();

    expect(first.killCount).toBe(1);
    expect(h.servers).toHaveLength(2);

    // Settle the replay so close() does not wait out the 5s deadline.
    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();
    second.send(successMessage('req-1', null));
    await h.executor.close();
  });

  test('does not restart while closing when the server dies', async () => {
    const h = harness({ timeoutMs: 5000, autoClose: false });
    const server = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    const closing = h.executor.close();
    await flushMicrotasks();

    // The crash lands mid-shutdown: no restart, and the request settles.
    server.crash();
    await flushMicrotasks();
    await closing;

    expect(h.servers).toHaveLength(1);
    expect(failureOf(await outcomePromise).code).toBe('FRAMEWORK_SHUTDOWN');
    // Already gone: killing a corpse would be a no-op at best, and the point
    // is that the shutdown path noticed the exit instead of waiting 5s.
    expect(server.killCount).toBe(0);
  });

  test('waits for the process to exit before restarting after a lost channel', async () => {
    // The replacement must not open storage while the old process is still
    // holding it (INV-002). A kill that has not taken effect yet is the exact
    // window this covers.
    const h = harness({ timeoutMs: 5000, killExits: false });
    const first = await started(h);

    first.closeChannel();
    await flushTicks();

    expect(first.killCount).toBe(1);
    // Killed but not yet gone: no replacement may be spawned.
    expect(h.servers).toHaveLength(1);

    first.crash();
    await waitFor(() => h.servers.length === 2);
    expect(h.servers).toHaveLength(2);
    // Let the replacement handshake so close() takes the graceful path.
    serverAt(h, 1).send(readyMessage());
    await flushTicks();
    await h.executor.close();
  });

  test('still kills a server whose exit could not be observed', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    // An unobservable exit means "the process may still be alive". Treating it
    // as gone would skip the only kill and start a second writer beside it
    // (INV-002), so the defensive kill must still run.
    first.unobservableExit();
    await flushTicks();

    expect(first.killCount).toBe(1);
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg ===
            'server channel lost while the process was alive; killing it',
        ),
    ).toBe(true);
    // And the restart is not blocked forever: the replacement appears.
    await waitFor(() => h.servers.length === 2);
    serverAt(h, 1).send(readyMessage());
    await flushTicks();
    await h.executor.close();
  });

  test('close waits for the process exit after the server acknowledges', async () => {
    // `closed` without an exit: the acknowledgement must not be treated as
    // "the process is gone", or the storage would be reopened too early.
    const h = harness({ timeoutMs: 5000, exitOnShutdown: false });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);
    server.send(readyMessage());
    await startPromise;

    let closed = false;
    const closing = h.executor.close().then(() => {
      closed = true;
      return undefined;
    });
    await flushTicks();

    expect(closed).toBe(false);
    server.crash(0);
    await closing;
    expect(closed).toBe(true);
  });

  test('a server that dies on every request is not restarted in a tight loop', async () => {
    // The bug this covers: resetting the crash streak on `ready` made
    // "ready, then die on the first request" restart with zero backoff, which
    // spawned ~275 processes in 300ms.
    const h = harness({
      timeoutMs: 1000,
      crashOnHello: true,
      restartBackoffMs: 200,
    });
    void h.executor.start().catch(() => undefined);
    await flushTicks();

    await new Promise<void>((resolve) => {
      setTimeout(resolve, 300);
    });

    expect(h.servers.length).toBeLessThanOrEqual(3);
    await h.executor.close();
  });

  test('kills a server that acknowledges closed but never exits', async () => {
    // `closed` means "finished writing", not "the process is gone". close() must
    // not return while the process may still hold the storage (INV-002).
    jest.useFakeTimers();
    try {
      const h = harness({ timeoutMs: 5000, exitOnShutdown: false });
      const startPromise = h.executor.start();
      const server = serverAt(h, 0);
      server.send(readyMessage());
      await startPromise;

      const closing = h.executor.close();
      await flushMicrotasks();
      expect(server.killCount).toBe(0);

      jest.advanceTimersByTime(2000);
      await flushMicrotasks();
      await closing;

      expect(server.killCount).toBe(1);
      expect(
        h
          .logs()
          .some(
            (line) =>
              line.msg ===
              'server process did not exit; killing it before close returns',
          ),
      ).toBe(true);
    } finally {
      jest.useRealTimers();
    }
  });

  test('fails start after startTimeoutMs when the server never becomes ready', async () => {
    const h = harness({ timeoutMs: 5000, startTimeoutMs: 50 });
    const beganAt = Date.now();

    const error = await rejectionOf(h.executor.start());

    // A broken entry script must be visible in seconds, not hang forever while
    // the restart loop keeps spawning processes.
    expect(error.message).toMatch(/did not become ready within 50ms/);
    expect(Date.now() - beganAt).toBeLessThan(2000);
    const logged = h
      .logs()
      .find(
        (line) =>
          line.msg === 'server did not become ready before the start timeout',
      );
    expect(logged).toMatchObject({ level: 'error', timeoutMs: 50 });
    expect(typeof logged?.attempts).toBe('number');
    await h.executor.close();
  });

  test('stops spawning once the start timeout fired', async () => {
    const h = harness({
      timeoutMs: 5000,
      startTimeoutMs: 50,
      restartBackoffMs: 5,
    });
    await rejectionOf(h.executor.start());
    const spawnedAtReject = h.servers.length;

    await new Promise<void>((resolve) => {
      setTimeout(resolve, 150);
    });

    expect(h.servers.length).toBe(spawnedAtReject);
    await h.executor.close();
  });

  test('execute fails immediately after a failed start instead of buffering', async () => {
    const h = harness({ timeoutMs: 5000, startTimeoutMs: 20 });
    await rejectionOf(h.executor.start());

    const beganAt = Date.now();
    const outcome = await h.executor.execute(request({ requestId: 'late' }));

    expect(failureOf(outcome).code).toBe('INTERNAL_ERROR');
    expect(failureOf(outcome).message).toContain('failed to start');
    expect(Date.now() - beganAt).toBeLessThan(1000);
    await h.executor.close();
  });

  test('refuses a request once the pending buffer is full', async () => {
    const h = harness({ timeoutMs: 5000, maxPending: 2 });
    // Started but never ready, so nothing is delivered and the buffer fills.
    void h.executor.start().catch(() => undefined);
    await flushTicks();

    void h.executor.execute(request({ requestId: 'req-1' }));
    void h.executor.execute(request({ requestId: 'req-2' }));
    const refused = await h.executor.execute(request({ requestId: 'req-3' }));

    expect(failureOf(refused).code).toBe('FRAMEWORK_QUEUE_FULL');
    const logged = h
      .logs()
      .find((line) => line.code === 'FRAMEWORK_QUEUE_FULL');
    expect(logged).toMatchObject({
      level: 'warn',
      pending: 2,
      limit: 2,
    });
    await h.executor.close();
  });

  test('reports the real exit code when the channel close wins the race', async () => {
    // The real spawn closes the channel before `exited` settles, and the close
    // handler is synchronous while the promise continuation is a microtask.
    // Without a synchronous view of the exit the crash line had
    // `exitCode: null` and the code survived only inside the free-text reason.
    const h = harness({ timeoutMs: 5000, closeChannelOnExit: true });
    await started(h);

    serverAt(h, 0).crash(17);
    await flushTicks();

    const lost = h.logs().find((line) => line.msg === 'server process lost');
    expect(lost).toMatchObject({ level: 'error', exitCode: 17, signal: null });
    serverAt(h, 1).send(readyMessage());
    await flushTicks();
    await h.executor.close();
  });

  test('exposes a close budget covering the drain, grace and exit wait', async () => {
    const h = harness({ timeoutMs: 5000, shutdownTimeoutMs: 30 });

    // drain + grace + two bounded exit waits (the second after the kill): a
    // caller must give the executor at least this much, or it returns while the
    // Server is still writing.
    expect(h.executor.closeBudgetMs).toBe(30 * 2 + 2000 * 2);
    await h.executor.close();
  });

  test('fails pending requests instead of waiting past its own close budget', async () => {
    // The Server accepts the request and then never answers: `doClose` used to
    // wait out the whole request deadline, which is what made a caller with a
    // shorter shutdown timeout return while the Server was still working.
    const h = harness({
      timeoutMs: 5000,
      shutdownTimeoutMs: 30,
      autoClose: false,
    });
    const server = await started(h);
    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));

    const startedAt = Date.now();
    await h.executor.close();

    expect(Date.now() - startedAt).toBeLessThan(3000);
    expect(failureOf(await outcomePromise).code).toBe('FRAMEWORK_SHUTDOWN');
    expect(server.killCount).toBe(1);
  });

  test('works without a logger option, using the default stdout logger', async () => {
    // The default must be observable (SPEC §7.1, INV-013): an executor that is
    // silent unless the caller injects a logger leaves crash, replay and
    // shutdown events invisible in the documented minimal setup. This asserts
    // the default is usable; the stdout side effect is the intended behaviour.
    const server = new FakeServerProcess({ autoCloseOnShutdown: true });
    const executor = new IpcExecutor({
      spawn: () => server.handle,
      timeoutMs: 1000,
    });

    const startPromise = executor.start();
    server.send(readyMessage());
    await startPromise;

    const outcomePromise = executor.execute(request({ requestId: 'req-1' }));
    server.send(successMessage('req-1', 'ok'));

    expect(await outcomePromise).toEqual({ ok: true, result: 'ok' });
    await executor.close();
  });
});
