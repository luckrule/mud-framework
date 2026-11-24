import { describe, expect, test } from 'bun:test';

import { createProcessChannel } from '../ipc/bun-child.ts';
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
import { MemoryStorageBackend } from '../storage/memory.ts';
import type { StorageBackend, StorageCapabilities } from '../storage/types.ts';
import type { Dispatcher } from './dispatcher.ts';
import type { Extension } from './extension.ts';
import {
  resolveShutdownBudgetMs,
  runServerProcess,
  ServerChannelClosedError,
  ServerStartupError,
} from './server-process.ts';
import { BusinessError } from './wire-codes.ts';

const sleep = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

/** Flushes the microtasks an accepted request needs before it reports. */
const tick = (): Promise<void> => sleep(0);

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function requestMessage(
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
function received(messages: readonly unknown[]): IpcMessage[] {
  return messages.map((raw) => {
    const parsed = parseIpcMessage(raw);
    if (!parsed.ok) {
      throw new Error(`server sent an invalid message: ${parsed.reason}`);
    }
    return parsed.message;
  });
}

function results(messages: readonly unknown[]): ResultMessage[] {
  return received(messages).filter(
    (message): message is ResultMessage => message.type === 'result',
  );
}

function sendRaw(portal: IpcChannel, raw: unknown): void {
  // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- the test sends a malformed message on purpose
  portal.send(raw as IpcMessage);
}

interface HarnessOptions {
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

interface Harness {
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

function startServer(options: HarnessOptions = {}): Harness {
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

function parseLine(line: string): Record<string, unknown> {
  const parsed: unknown = JSON.parse(line);
  if (!isRecord(parsed)) {
    throw new Error(`log line is not a JSON object: ${line}`);
  }
  return parsed;
}

/** Reads a numeric log field, failing loudly when it is missing or not a number. */
function numberField(
  line: Record<string, unknown> | undefined,
  field: string,
): number {
  const value = line?.[field];
  if (typeof value !== 'number') {
    throw new Error(`log field "${field}" is not a number: ${String(value)}`);
  }
  return value;
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

function echoRegistration(dispatcher: Dispatcher): void {
  dispatcher.register('echo', (_ctx, payload) => Promise.resolve(payload));
}

/** Narrows the rejection without an unchecked cast, so a wrong type fails loudly. */
async function startupFailure(
  promise: Promise<unknown>,
): Promise<ServerStartupError> {
  const error = await rejectionOf(promise);
  if (!(error instanceof ServerStartupError)) {
    throw new Error(`expected ServerStartupError, got ${error.name}`);
  }
  return error;
}

describe('createProcessChannel singleton', () => {
  test('refuses a second channel in the same process', () => {
    // `process.send` and the `message`/`disconnect` events are process-global,
    // so a second channel would add a second listener and deliver every inbound
    // message to both — two Servers executing the same request (INV-002).
    createProcessChannel({ logger: silentLogger() });

    expect(() => createProcessChannel({ logger: silentLogger() })).toThrow(
      /already exists/,
    );
  });
});

/** A logger that keeps the singleton test's warning out of the output. */
function silentLogger(): Logger {
  return createJsonLogger({ sink: () => undefined });
}

describe('runServerProcess startup', () => {
  test('sends ready with the backend declaration', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: {
        transactions: 'limited',
        concurrency: 'single',
        snapshotRead: false,
        uniqueConstraint: true,
        maxTransactionDurationMs: 250,
      },
    });
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    const ready = received(harness.messages).filter(
      (message) => message.type === 'ready',
    );
    expect(ready).toHaveLength(1);
    expect(ready[0]).toEqual({
      type: 'ready',
      pid: process.pid,
      capabilities: backend.capabilities(),
    });
    await harness.stop();
  });

  test('sends ready only after register returned', async () => {
    let readyCountDuringRegister: number | undefined;
    const harness = startServer({
      extensions: [
        {
          name: 'echo',
          register: (dispatcher) => {
            dispatcher.register('echo', (_ctx, payload) =>
              Promise.resolve(payload),
            );
            readyCountDuringRegister = harness.messages.length;
          },
        },
      ],
    });
    await tick();

    expect(readyCountDuringRegister).toBe(0);
    expect(received(harness.messages)).toHaveLength(1);
    await harness.stop();
  });

  test('fails without ready when maxTransactionDurationMs is NaN', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: Number.NaN },
    });
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });

    const error = await startupFailure(harness.done);

    expect(error.reason).toContain('maxTransactionDurationMs');
    expect(received(harness.messages)).toEqual([]);
    const failures = harness
      .logs()
      .filter((line) => line.code === 'SERVER_STARTUP_FAILED');
    expect(failures).toHaveLength(1);
  });

  test('fails without ready when maxTransactionDurationMs is zero', async () => {
    // The protocol refuses a non-positive ceiling (ipc/protocol.ts), so passing
    // the self-check here would send a `ready` Portal rejects: Portal would
    // never reach its barrier and every request would end at its deadline.
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: 0 },
    });
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });

    const error = await startupFailure(harness.done);

    expect(error.reason).toContain('maxTransactionDurationMs');
    expect(received(harness.messages)).toEqual([]);
    const failures = harness
      .logs()
      .filter((line) => line.code === 'SERVER_STARTUP_FAILED');
    expect(failures).toHaveLength(1);
  });

  test('fails without ready when maxTransactionDurationMs is negative', async () => {
    const values = [-1, -0.5, Number.NEGATIVE_INFINITY];
    const harnesses: Harness[] = [];
    const pending: Promise<ServerStartupError>[] = [];
    for (const value of values) {
      const backend = new MemoryStorageBackend({
        capabilities: { maxTransactionDurationMs: value },
      });
      const harness = startServer({
        backend,
        extensions: [{ name: 'echo', register: echoRegistration }],
      });
      harnesses.push(harness);
      pending.push(startupFailure(harness.done));
    }

    const errors = await Promise.all(pending);

    for (const error of errors) {
      expect(error.reason).toContain('maxTransactionDurationMs');
    }
    for (const harness of harnesses) {
      expect(received(harness.messages)).toEqual([]);
    }
  });

  test('accepts an infinite maxTransactionDurationMs as unbounded', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: Number.POSITIVE_INFINITY },
    });
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    const ready = received(harness.messages).filter(
      (message) => message.type === 'ready',
    );
    expect(ready).toHaveLength(1);
    await harness.stop();
  });

  test('fails without ready when a capability value is outside its enum', async () => {
    const backend = brokenBackend({
      transactions: 'partial',
      concurrency: 'pooled',
      snapshotRead: true,
      uniqueConstraint: false,
      maxTransactionDurationMs: 1000,
    });
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });

    const error = await startupFailure(harness.done);

    expect(error.reason).toContain('transactions');
    expect(received(harness.messages)).toEqual([]);
  });

  test('fails without ready when register throws', async () => {
    const harness = startServer({
      extensions: [
        {
          name: 'refusing',
          register: () => {
            throw new Error('extension refused');
          },
        },
      ],
    });

    const error = await startupFailure(harness.done);

    expect(error.reason).toContain('extension refused');
    expect(received(harness.messages)).toEqual([]);
  });
});

describe('runServerProcess extension capabilities', () => {
  test('registers an extension whose requirements the backend meets', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'full', concurrency: 'pooled' },
    });
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'banking',
          requires: { transactions: 'full' },
          register: (dispatcher) => {
            dispatcher.register('deposit', (_ctx, payload) =>
              Promise.resolve({ deposited: payload }),
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'deposit' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: { deposited: 'hi' },
      },
    ]);
    const registered = harness
      .logs()
      .filter((line) => line.msg === 'extension registered');
    expect(registered).toHaveLength(1);
    expect(registered[0]).toMatchObject({
      level: 'info',
      extension: 'banking',
    });
    await harness.stop();
  });

  test('skips an extension whose requirements are unmet, and none of its handlers exist', async () => {
    // The defect this covers: a business requiring `transactions: full` ran
    // silently on a `none` backend (INV-010).
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'none' },
    });
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'banking',
          requires: { transactions: 'full' },
          register: (dispatcher) => {
            dispatcher.register('deposit', () => Promise.resolve('deposited'));
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'deposit' }));
    await tick();

    const [outcome] = results(harness.messages);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure result');
    }
    expect(outcome.error.code).toBe('UNKNOWN_METHOD');
    const skipped = harness
      .logs()
      .filter(
        (line) => line.msg === 'extension skipped: storage capabilities unmet',
      );
    expect(skipped).toHaveLength(1);
    expect(skipped[0]).toMatchObject({
      level: 'warn',
      extension: 'banking',
      unmet: ['transactions: needs full, backend declares none'],
    });
    // The process still started: skipping one extension is not a startup failure.
    expect(
      received(harness.messages).filter((message) => message.type === 'ready'),
    ).toHaveLength(1);
    await harness.stop();
  });

  test('a skipped extension does not stop later ones from registering', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'limited' },
    });
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'banking',
          requires: { transactions: 'full' },
          register: (dispatcher) => {
            dispatcher.register('deposit', () => Promise.resolve('deposited'));
          },
        },
        {
          name: 'queries',
          register: echoRegistration,
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ payload: 'later' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'later' },
    ]);
    await harness.stop();
  });

  test('a required extension with unmet requirements refuses startup', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'limited' },
    });
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'banking',
          requires: { transactions: 'full' },
          required: true,
          register: (dispatcher) => {
            dispatcher.register('deposit', () => Promise.resolve('deposited'));
          },
        },
      ],
    });

    const error = await startupFailure(harness.done);

    // SPEC 6.2 step 2: the process cannot run without it, so no `ready` and a
    // ServerStartupError for the caller to map to a non-zero exit.
    expect(error.reason).toContain('banking');
    expect(received(harness.messages)).toEqual([]);
    const failures = harness
      .logs()
      .filter((line) => line.msg === 'required extension cannot start');
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      level: 'error',
      extension: 'banking',
      unmet: ['transactions: needs full, backend declares limited'],
    });
  });

  test('starts with no extensions at all and warns about it', async () => {
    const harness = startServer({ extensions: [] });
    await tick();

    expect(
      received(harness.messages).filter((message) => message.type === 'ready'),
    ).toHaveLength(1);
    // No Extension was declared, so nothing was skipped; the all-skipped line
    // is about declared-but-refused extensions, not an empty list.
    expect(
      harness
        .logs()
        .filter(
          (line) =>
            line.msg === 'no extension registered: every extension was skipped',
        ),
    ).toHaveLength(0);
    await harness.stop();
  });

  test('warns when every declared extension was skipped', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'none', concurrency: 'single' },
    });
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'banking',
          requires: { transactions: 'full' },
          register: () => undefined,
        },
        {
          name: 'reporting',
          requires: { concurrency: 'pooled' },
          register: () => undefined,
        },
      ],
    });
    await tick();

    // A Server with zero handlers is useless but must not crash: Portal is
    // waiting on it, and UNKNOWN_METHOD is a diagnosable answer.
    expect(
      received(harness.messages).filter((message) => message.type === 'ready'),
    ).toHaveLength(1);
    const warnings = harness
      .logs()
      .filter(
        (line) =>
          line.msg === 'no extension registered: every extension was skipped',
      );
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatchObject({ level: 'warn', total: 2, skipped: 2 });
    await harness.stop();
  });
});

describe('runServerProcess requests', () => {
  test('executes a request and answers with a result', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();
    harness.portal.send({ type: 'hello', portalGeneration: 1 });

    harness.portal.send(requestMessage({ payload: 'ping' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'ping' },
    ]);
    await harness.stop();
  });

  test('maps a thrown handler error to a failure result with its code', async () => {
    const harness = startServer({
      extensions: [
        {
          name: 'failures',
          register: (dispatcher) => {
            dispatcher.register('explode', () =>
              Promise.reject(new Error('business failure')),
            );
            dispatcher.register('denied', () =>
              Promise.reject(
                new BusinessError('BUSINESS_DENIED', 'not allowed'),
              ),
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'explode' }));
    harness.portal.send(
      requestMessage({ requestId: 'req-2', method: 'denied' }),
    );
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: false,
        error: { code: 'INTERNAL_ERROR', message: 'business failure' },
      },
      {
        type: 'result',
        requestId: 'req-2',
        ok: false,
        error: { code: 'BUSINESS_DENIED', message: 'not allowed' },
      },
    ]);
    await harness.stop();
  });

  test('maps an unknown method to UNKNOWN_METHOD', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'missing' }));
    await tick();

    const [outcome] = results(harness.messages);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure result');
    }
    expect(outcome.error.code).toBe('UNKNOWN_METHOD');
    await harness.stop();
  });

  test('carries the session id a handler bound into the result message', async () => {
    const harness = startServer({
      extensions: [
        {
          name: 'auth',
          register: (dispatcher) => {
            dispatcher.register('login', async (ctx) => {
              ctx.bindSession('session-1');
              return 'welcome';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'login' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: 'welcome',
        sessionId: 'session-1',
      },
    ]);
    await harness.stop();
  });

  test('omits the session id from the result when the handler binds nothing', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    harness.portal.send(requestMessage());
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: 'hi',
      },
    ]);
    // The outcome is spread into the message, so an unbound success carries the
    // key with `undefined` rather than omitting it. JSON drops the key and
    // structured clone keeps it with the same meaning as absence (SPEC 2), so
    // the wire assertion is on the value.
    const raw = harness.messages.find(
      (message) => isRecord(message) && message.type === 'result',
    );
    if (!isRecord(raw)) {
      throw new Error('expected a raw result message');
    }
    expect(raw.sessionId).toBeUndefined();
    await harness.stop();
  });

  test('maps a request timeout to FRAMEWORK_TIMEOUT', async () => {
    const harness = startServer({
      timeoutMs: 10,
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async () => {
              await sleep(40);
              return 'late';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow' }));
    await sleep(20);

    const [outcome] = results(harness.messages);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure result');
    }
    expect(outcome.error.code).toBe('FRAMEWORK_TIMEOUT');
    // Let the abandoned handler settle before closing the backend.
    await sleep(40);
    await harness.stop();
  });

  test('executes a request that arrives before hello', async () => {
    // Executing is the only way to answer: the protocol has no "not ready"
    // reply, and dropping would leave Portal replaying the request forever.
    // The missing handshake is logged instead of silently tolerated.
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    harness.portal.send(requestMessage());
    await tick();

    expect(results(harness.messages)).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'hi' },
    ]);
    const warnings = harness
      .logs()
      .filter((line) => line.code === 'REQUEST_BEFORE_HELLO');
    expect(warnings).toHaveLength(1);
    await harness.stop();
  });

  test('logs REQUEST_BEFORE_HELLO once, not once per request', async () => {
    // The missing handshake is a timing fault, so it stays diagnosable without
    // letting the log volume track the request rate (SPEC 7.1).
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    for (let index = 0; index < 5; index += 1) {
      harness.portal.send(requestMessage({ requestId: `req-${index}` }));
    }
    await tick();

    expect(results(harness.messages)).toHaveLength(5);
    const warnings = harness
      .logs()
      .filter((line) => line.code === 'REQUEST_BEFORE_HELLO');
    expect(warnings).toHaveLength(1);
    await harness.stop();
  });

  test('does not repeat REQUEST_BEFORE_HELLO once hello arrives', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    harness.portal.send(requestMessage({ requestId: 'req-1' }));
    await tick();
    harness.portal.send({ type: 'hello', portalGeneration: 1 });
    await tick();
    harness.portal.send(requestMessage({ requestId: 'req-2' }));
    await tick();

    const warnings = harness
      .logs()
      .filter((line) => line.code === 'REQUEST_BEFORE_HELLO');
    expect(warnings).toHaveLength(1);
    await harness.stop();
  });

  test('runs a duplicated request id only once', async () => {
    let calls = 0;
    const gate = Promise.withResolvers<void>();
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async () => {
              calls += 1;
              await gate.promise;
              return 'done';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow', replayCount: 0 }));
    harness.portal.send(requestMessage({ method: 'slow', replayCount: 1 }));
    gate.resolve();
    await tick();

    expect(calls).toBe(1);
    expect(results(harness.messages)).toHaveLength(1);
    const duplicates = harness
      .logs()
      .filter((line) => line.code === 'DUPLICATE_REQUEST');
    expect(duplicates).toHaveLength(1);
    expect(duplicates[0]?.replayCount).toBe(1);
    await harness.stop();
  });

  test('logs DUPLICATE_REQUEST once per request id, not once per replay', async () => {
    const gate = Promise.withResolvers<void>();
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async () => {
              await gate.promise;
              return 'done';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow', replayCount: 0 }));
    for (let replayCount = 1; replayCount <= 5; replayCount += 1) {
      harness.portal.send(requestMessage({ method: 'slow', replayCount }));
    }
    gate.resolve();
    await tick();

    // One line for the id, however many replays of it arrived (SPEC 7.1).
    const duplicates = harness
      .logs()
      .filter((line) => line.code === 'DUPLICATE_REQUEST');
    expect(duplicates).toHaveLength(1);
    await harness.stop();
  });

  test('logs DUPLICATE_REQUEST again after the id went terminal', async () => {
    // The suppression set is pruned when a request reaches a terminal state, so
    // a later burst of the same id is reported again instead of being silenced
    // for the rest of the process lifetime.
    let gate = Promise.withResolvers<void>();
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async () => {
              await gate.promise;
              return 'done';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow', replayCount: 0 }));
    harness.portal.send(requestMessage({ method: 'slow', replayCount: 1 }));
    gate.resolve();
    await tick();

    gate = Promise.withResolvers<void>();
    harness.portal.send(requestMessage({ method: 'slow', replayCount: 2 }));
    harness.portal.send(requestMessage({ method: 'slow', replayCount: 3 }));
    gate.resolve();
    await tick();

    const duplicates = harness
      .logs()
      .filter((line) => line.code === 'DUPLICATE_REQUEST');
    expect(duplicates).toHaveLength(2);
    await harness.stop();
  });
});

describe('runServerProcess cancel', () => {
  test('skips a handler when the cancel arrives in the same delivery batch', async () => {
    let calls = 0;
    const harness = startServer({
      extensions: [
        {
          name: 'echo',
          register: (dispatcher) => {
            dispatcher.register('echo', (_ctx, payload) => {
              calls += 1;
              return Promise.resolve(payload);
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage());
    harness.portal.send({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });
    await tick();

    expect(calls).toBe(0);
    expect(results(harness.messages)).toEqual([]);
    await harness.stop();
  });

  test('does not answer a request cancelled while it runs', async () => {
    let calls = 0;
    const gate = Promise.withResolvers<void>();
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async () => {
              calls += 1;
              await gate.promise;
              return 'done';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow' }));
    await tick();
    harness.portal.send({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });
    gate.resolve();
    await tick();

    // Portal already settled the id when it sent the cancel; a late result
    // could be mistaken for the answer to a replay of the same id.
    expect(calls).toBe(1);
    expect(results(harness.messages)).toEqual([]);
    await harness.stop();
  });

  test('rolls back the writes of a request cancelled while it runs', async () => {
    // The regression this covers: the cancel only muted the result, so the
    // transaction still committed and the Portal's FRAMEWORK_TIMEOUT was a lie.
    let lateWriteError = '';
    const entered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    const backend = new MemoryStorageBackend();
    const harness = startServer({
      backend,
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async (ctx) => {
              await ctx.storage.execute('INSERT 1');
              entered.resolve();
              await gate.promise;
              try {
                await ctx.storage.execute('INSERT 2');
                lateWriteError = 'write was accepted';
              } catch (error) {
                lateWriteError =
                  error instanceof Error ? error.message : String(error);
              }
              return 'done';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow' }));
    await entered.promise;
    harness.portal.send({
      type: 'cancel',
      requestId: 'req-1',
      code: 'FRAMEWORK_TIMEOUT',
    });
    gate.resolve();
    await tick();

    const stats = backend.stats();
    expect(stats.committed).toBe(0);
    expect(stats.rolledBack).toBeGreaterThanOrEqual(1);
    expect(lateWriteError).toMatch(/expired/);
    expect(results(harness.messages)).toEqual([]);
    const applied = harness
      .logs()
      .filter((line) => line.msg === 'cancel applied to a running handler');
    expect(applied).toHaveLength(1);
    expect(applied[0]).toMatchObject({
      level: 'warn',
      code: 'FRAMEWORK_TIMEOUT',
      requestId: 'req-1',
    });
    await harness.stop();
  });

  test('honours the request timeoutMs instead of the configured ceiling', async () => {
    // The regression this covers: Portal settled at 10ms, the Server kept going
    // for 60ms and committed, so the player saw a failure for a state change
    // that happened.
    const backend = new MemoryStorageBackend();
    const harness = startServer({
      backend,
      timeoutMs: 5000,
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async (ctx) => {
              await ctx.storage.execute('INSERT 1');
              await sleep(60);
              return 'late';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow', timeoutMs: 10 }));
    await sleep(30);

    const [outcome] = results(harness.messages);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure result');
    }
    expect(outcome.error.code).toBe('FRAMEWORK_TIMEOUT');
    // Let the abandoned handler settle so the rollback is observable.
    await sleep(60);
    const stats = backend.stats();
    expect(stats.committed).toBe(0);
    expect(stats.rolledBack).toBeGreaterThanOrEqual(1);
    await harness.stop();
  });

  test('a request timeoutMs larger than the configuration does not extend it', async () => {
    const harness = startServer({
      timeoutMs: 10,
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async () => {
              await sleep(60);
              return 'late';
            });
          },
        },
      ],
    });
    await tick();

    // 5000ms on the wire, 10ms configured: the Server's own ceiling wins.
    harness.portal.send(requestMessage({ method: 'slow', timeoutMs: 5000 }));
    await sleep(30);

    const [outcome] = results(harness.messages);
    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure result');
    }
    expect(outcome.error.code).toBe('FRAMEWORK_TIMEOUT');
    await sleep(60);
    await harness.stop();
  });
});

describe('runServerProcess lifecycle', () => {
  test('stops accepting, closes the backend, then sends closed', async () => {
    const events: string[] = [];
    const inner = new MemoryStorageBackend();
    const backend: StorageBackend = {
      capabilities: () => inner.capabilities(),
      transaction: (fn) => inner.transaction(fn),
      async close(): Promise<void> {
        events.push('backend.close');
        await inner.close();
      },
    };
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
      onPortalMessage: (raw) => {
        const parsed = parseIpcMessage(raw);
        if (parsed.ok) {
          events.push(`message.${parsed.message.type}`);
        }
      },
    });
    await tick();

    harness.portal.send(requestMessage());
    harness.portal.send({ type: 'shutdown', timeoutMs: 2000 });
    harness.portal.send(requestMessage({ requestId: 'req-late' }));
    await harness.done;

    // The accepted request still reports before `closed`, the late one is
    // refused with a stable code, and the backend is closed before the
    // announcement.
    expect(events).toEqual([
      'message.ready',
      'message.result',
      'message.result',
      'backend.close',
      'message.closed',
    ]);
    const outcomes = results(harness.messages);
    expect(outcomes).toHaveLength(2);
    expect(outcomes.find((outcome) => outcome.requestId === 'req-1')).toEqual({
      type: 'result',
      requestId: 'req-1',
      ok: true,
      result: 'hi',
    });
    expect(
      outcomes.find((outcome) => outcome.requestId === 'req-late'),
    ).toEqual({
      type: 'result',
      requestId: 'req-late',
      ok: false,
      error: { code: 'FRAMEWORK_SHUTDOWN', message: 'server is shutting down' },
    });
    const refused = harness
      .logs()
      .filter((line) => line.code === 'SERVER_SHUTTING_DOWN');
    expect(refused).toHaveLength(1);
    const closed = received(harness.messages).at(-1);
    expect(closed).toEqual({ type: 'closed', reason: 'shutdown' });
  });

  test('answers every request that arrives during shutdown with FRAMEWORK_SHUTDOWN', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    harness.portal.send({ type: 'shutdown', timeoutMs: 2000 });
    harness.portal.send(requestMessage({ requestId: 'req-a' }));
    harness.portal.send(requestMessage({ requestId: 'req-b' }));
    await harness.done;

    const outcomes = results(harness.messages);
    expect(outcomes).toHaveLength(2);
    for (const outcome of outcomes) {
      expect(outcome.ok).toBe(false);
      if (outcome.ok) {
        throw new Error('expected a failure result');
      }
      expect(outcome.error.code).toBe('FRAMEWORK_SHUTDOWN');
    }
  });

  test('lets an in-flight request finish with its real result during shutdown', async () => {
    // A duplicate of an id already running is owed that execution's result:
    // refusing it would report a failure for work that still succeeds, and the
    // real result would then arrive for an id Portal had already settled.
    const gate = Promise.withResolvers<void>();
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async () => {
              await gate.promise;
              return 'done';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow' }));
    await tick();
    harness.portal.send({ type: 'shutdown', timeoutMs: 2000 });
    harness.portal.send(requestMessage({ method: 'slow', replayCount: 1 }));
    await tick();

    gate.resolve();
    await harness.done;

    expect(results(harness.messages)).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'done' },
    ]);
    const duplicates = harness
      .logs()
      .filter((line) => line.code === 'DUPLICATE_REQUEST');
    expect(duplicates).toHaveLength(1);
    expect(
      harness.logs().filter((line) => line.code === 'SERVER_SHUTTING_DOWN'),
    ).toHaveLength(0);
  });

  test('waits for an in-flight request before closing', async () => {
    const gate = Promise.withResolvers<void>();
    const events: string[] = [];
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async () => {
              await gate.promise;
              return 'done';
            });
          },
        },
      ],
      onPortalMessage: (raw) => {
        const parsed = parseIpcMessage(raw);
        if (parsed.ok) {
          events.push(`message.${parsed.message.type}`);
        }
      },
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow' }));
    await tick();
    harness.portal.send({ type: 'shutdown', timeoutMs: 2000 });
    await tick();

    // Still draining: the backend must not close under a running handler.
    expect(events).not.toContain('message.closed');

    gate.resolve();
    await harness.done;

    expect(events).toEqual([
      'message.ready',
      'message.result',
      'message.closed',
    ]);
  });

  test('rejects when the link closes without shutdown', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    harness.portal.close('portal gone');

    const error = await rejectionOf(harness.done);
    if (!(error instanceof ServerChannelClosedError)) {
      throw new Error(`expected ServerChannelClosedError, got ${error.name}`);
    }
    expect(error.reason).toBe('portal gone');
  });

  test('bounds the shutdown close when the backend waits on a stuck handler', async () => {
    // A backend whose `close` awaits its queue (SQLite does) makes
    // `executor.close()` wait for the handler as well, so an unbounded close
    // would keep `closed` from ever being sent when a handler never returns.
    const release = Promise.withResolvers<void>();
    const inner = new MemoryStorageBackend();
    const backend: StorageBackend = {
      capabilities: () => inner.capabilities(),
      transaction: (fn) => inner.transaction(fn),
      async close(): Promise<void> {
        await release.promise;
        await inner.close();
      },
    };
    const harness = startServer({
      backend,
      timeoutMs: 10,
      extensions: [
        {
          name: 'stuck',
          register: (dispatcher) => {
            dispatcher.register('stuck', async () => {
              await release.promise;
              return 'late';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'stuck' }));
    await tick();
    harness.portal.send({ type: 'shutdown', timeoutMs: 20 });
    // The declared budget bounds the close, so `closed` must arrive while the
    // handler is still holding the backend.
    await harness.done;

    expect(received(harness.messages).at(-1)).toEqual({
      type: 'closed',
      reason: 'shutdown',
    });
    const closeTimeouts = harness
      .logs()
      .filter((line) => line.code === 'STORAGE_CLOSE_TIMEOUT');
    expect(closeTimeouts).toHaveLength(1);
    // The drain settled first (the dispatcher answers at `timeoutMs` even while
    // the handler runs), so the close step got the budget minus the drain time:
    // at most the declared 20ms, never twice it.
    const closeBudget = numberField(closeTimeouts[0], 'timeoutMs');
    expect(closeBudget).toBeGreaterThan(0);
    expect(closeBudget).toBeLessThanOrEqual(20);
    // The dispatcher already answered the request at `timeoutMs` while the
    // handler kept running, so nothing is left in flight to name here.
    expect(closeTimeouts[0]?.pendingRequests).toBe(0);
    release.resolve();
    await tick();
  });

  test('shares one deadline between the drain and the backend close', async () => {
    // The defect this covers: drain and close each got the full budget, so a
    // Server that spent its whole declared budget draining then got a second,
    // fresh budget for the close — exactly the overrun that made the Portal kill
    // a Server closing correctly. Here the drain consumes the whole budget, so
    // the close must see only the remainder.
    const release = Promise.withResolvers<void>();
    const inner = new MemoryStorageBackend();
    const backend: StorageBackend = {
      capabilities: () => inner.capabilities(),
      transaction: (fn) => inner.transaction(fn),
      async close(): Promise<void> {
        await release.promise;
        await inner.close();
      },
    };
    const harness = startServer({
      backend,
      // The request ceiling is far above the declared budget, so the drain
      // cannot settle by the dispatcher's timeout and runs out the budget.
      timeoutMs: 5000,
      extensions: [
        {
          name: 'stuck',
          register: (dispatcher) => {
            dispatcher.register('stuck', async () => {
              await release.promise;
              return 'late';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'stuck' }));
    await tick();
    const startedAt = performance.now();
    harness.portal.send({ type: 'shutdown', timeoutMs: 80 });
    await harness.done;
    const elapsedMs = performance.now() - startedAt;

    // A per-step budget would spend 80ms twice (160ms). One shared deadline ends
    // near 80ms, which is what the Portal is actually waiting for.
    expect(elapsedMs).toBeLessThan(130);
    const drainTimeouts = harness
      .logs()
      .filter((line) => line.code === 'SHUTDOWN_DRAIN_TIMEOUT');
    expect(drainTimeouts).toHaveLength(1);
    const closeTimeouts = harness
      .logs()
      .filter((line) => line.code === 'STORAGE_CLOSE_TIMEOUT');
    expect(closeTimeouts).toHaveLength(1);
    // The drain consumed the budget, so the close step is left with less than
    // the declared 80ms — not a fresh 80ms of its own.
    const closeBudget = numberField(closeTimeouts[0], 'timeoutMs');
    expect(closeBudget).toBeLessThan(80);
    expect(closeBudget).toBeGreaterThan(0);
    release.resolve();
    await tick();
  });

  test('resends ready when a newer portal generation arrives', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();
    harness.portal.send({ type: 'hello', portalGeneration: 1 });
    await tick();

    harness.portal.send({ type: 'hello', portalGeneration: 2 });
    await tick();

    const readies = received(harness.messages).filter(
      (message) => message.type === 'ready',
    );
    // The startup barrier plus one per new generation: a restarted Portal was
    // not around for the first one and would wait for readiness forever.
    expect(readies).toHaveLength(2);
    await harness.stop();
  });

  test('ignores a hello that does not advance the generation', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();
    harness.portal.send({ type: 'hello', portalGeneration: 3 });
    await tick();

    harness.portal.send({ type: 'hello', portalGeneration: 3 });
    await tick();

    const readies = received(harness.messages).filter(
      (message) => message.type === 'ready',
    );
    expect(readies).toHaveLength(1);
    const stale = harness.logs().filter((line) => line.code === 'STALE_HELLO');
    expect(stale).toHaveLength(1);
    await harness.stop();
  });

  test('logs a warning for an invalid message and keeps running', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    sendRaw(harness.portal, { type: 'nonsense' });
    sendRaw(harness.portal, { type: 'request' });
    harness.portal.send(requestMessage());
    await tick();

    expect(results(harness.messages)).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'hi' },
    ]);
    const rejected = harness
      .logs()
      .filter((line) => line.code === 'INVALID_IPC_MESSAGE');
    expect(rejected).toHaveLength(2);
    expect(rejected[0]?.reason).toContain('nonsense');
    await harness.stop();
  });

  test('logs a business refusal at warn and a framework failure at error', async () => {
    const harness = startServer({
      extensions: [
        {
          name: 'failures',
          register: (dispatcher) => {
            dispatcher.register('denied', () =>
              Promise.reject(
                new BusinessError('PLAYER_NOT_FOUND', 'no such player'),
              ),
            );
            dispatcher.register('explode', () =>
              Promise.reject(new Error('internal boom')),
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'denied' }));
    harness.portal.send(
      requestMessage({ requestId: 'req-2', method: 'explode' }),
    );
    await tick();

    // SPEC 7.1: an expected business refusal must not be recorded at error
    // level. `PLAYER_NOT_FOUND` is not a framework code, so it is a refusal.
    const failures = harness
      .logs()
      .filter((line) => line.msg === 'request failed');
    expect(failures.map((line) => [line.code, line.level])).toEqual([
      ['PLAYER_NOT_FOUND', 'warn'],
      ['INTERNAL_ERROR', 'error'],
    ]);
    await harness.stop();
  });

  test('aborts within a fixed short budget when the channel closes with a stuck handler', async () => {
    // The bug this covers: the abort budget was `2 * timeoutMs`, which is four
    // minutes at `timeoutMs = 60s`. A Portal that died is not waiting for this
    // process, and every extra second is a second of two writers (INV-002).
    const release = Promise.withResolvers<void>();
    const inner = new MemoryStorageBackend();
    const backend: StorageBackend = {
      capabilities: () => inner.capabilities(),
      transaction: (fn) => inner.transaction(fn),
      async close(): Promise<void> {
        await release.promise;
        await inner.close();
      },
    };
    const harness = startServer({
      backend,
      timeoutMs: 60_000,
      abortTimeoutMs: 20,
      extensions: [
        {
          name: 'stuck',
          register: (dispatcher) => {
            dispatcher.register('stuck', async () => {
              await release.promise;
              return 'late';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'stuck' }));
    await tick();
    const started = Date.now();
    harness.portal.close('portal died');

    const error = await rejectionOf(harness.done);

    expect(error).toBeInstanceOf(ServerChannelClosedError);
    // Not four minutes: the fixed abort budget, injected here as 20ms per step.
    expect(Date.now() - started).toBeLessThan(2000);
    const drainTimeouts = harness
      .logs()
      .filter((line) => line.code === 'SHUTDOWN_DRAIN_TIMEOUT');
    expect(drainTimeouts).toHaveLength(1);
    expect(drainTimeouts[0]?.timeoutMs).toBe(20);
    release.resolve();
    await tick();
  });

  test('aborts when the parent process is gone', async () => {
    // `disconnect` does not fire for an abrupt parent death, so the poll is the
    // only signal an orphan gets; without it the process keeps writing.
    let parentPid = 1000;
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
      parentCheckIntervalMs: 10,
      readParentPid: () => parentPid,
    });
    await tick();

    parentPid = 2000;
    const error = await rejectionOf(harness.done);

    expect(error).toBeInstanceOf(ServerChannelClosedError);
    expect(error.message).toContain('parent process is gone');
    const logged = harness
      .logs()
      .filter((line) => line.code === 'IPC_PARENT_GONE');
    expect(logged).toHaveLength(1);
    expect(logged[0]?.level).toBe('error');
  });
});

describe('resolveShutdownBudgetMs', () => {
  test('uses the budget the Portal declared', () => {
    const lines: string[] = [];
    const logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });

    expect(resolveShutdownBudgetMs(1500, 5000, logger)).toBe(1500);
    // No degradation, so nothing to report.
    expect(lines).toEqual([]);
  });

  test('falls back to 4 * timeoutMs and warns when no budget was declared', () => {
    // The wire parser requires the field, so this is the legacy-peer path: it
    // still has to close, but the larger, unrelated budget must be visible.
    const lines: string[] = [];
    const logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });

    expect(resolveShutdownBudgetMs(undefined, 5000, logger)).toBe(20_000);
    const warnings = lines.map(parseLine);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatchObject({
      level: 'warn',
      code: 'SHUTDOWN_BUDGET_UNDECLARED',
      timeoutMs: 20_000,
    });
  });
});

/**
 * A backend whose declaration is deliberately invalid. The cast is the test's
 * only way to hand an unusable declaration to the self-check.
 */
function brokenBackend(declared: unknown): StorageBackend {
  return {
    // oxlint-disable-next-line typescript/no-unsafe-type-assertion -- test double for an invalid declaration
    capabilities: (): StorageCapabilities => declared as StorageCapabilities,
    transaction: <T>(): Promise<T> =>
      Promise.reject(new Error('broken backend must not run transactions')),
    close: (): Promise<void> => Promise.resolve(),
  };
}
