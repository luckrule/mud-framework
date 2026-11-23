import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger, LoggerFactory } from '../logging/types.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { Dispatcher, FrameworkTimeoutError } from './dispatcher.ts';

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/** Parsed log lines, in emission order. */
function parseLines(lines: readonly string[]): Record<string, unknown>[] {
  return lines.map((line) => {
    const parsed: unknown = JSON.parse(line);
    if (!isRecord(parsed)) {
      throw new Error(`log line is not a JSON object: ${line}`);
    }
    return parsed;
  });
}

/** Dispatcher whose logs go to `lines` instead of stdout. */
function quietDispatcher(
  options: ConstructorParameters<typeof Dispatcher>[0],
  lines: string[] = [],
): { dispatcher: Dispatcher; lines: string[] } {
  const logger: Logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  const loggerFactory: LoggerFactory = {
    create: (loggerOptions) => logger.child(loggerOptions?.bindings ?? {}),
  };
  return {
    dispatcher: new Dispatcher({ ...options, loggerFactory }),
    lines,
  };
}

function deferred<T>(): {
  promise: Promise<T>;
  resolve: (value: T) => void;
} {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

const sleep = (ms: number): Promise<void> =>
  new Promise<void>((resolve) => {
    setTimeout(resolve, ms);
  });

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

describe('Dispatcher', () => {
  test('runs a handler inside a transaction and returns its result', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('echo', async (ctx, payload) => {
      await ctx.storage.execute('INSERT 1', [payload]);
      return payload;
    });

    const result = await dispatcher.dispatch('echo', 'hello');

    expect(result).toBe('hello');
    expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
  });

  test('rejects and rolls back when the handler throws', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('fail', async () => {
      throw new Error('business failure');
    });

    const error = await rejectionOf(dispatcher.dispatch('fail', null));

    expect(error.message).toBe('business failure');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects unknown handler names instead of returning undefined', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher, lines } = quietDispatcher({ backend, timeoutMs: 1000 });

    const error = await rejectionOf(dispatcher.dispatch('missing', null));

    expect(error.message).toMatch(/no handler registered/);
    expect(lines).toHaveLength(1);
    expect(JSON.parse(lines[0] ?? '')).toMatchObject({
      level: 'warn',
      method: 'missing',
    });
  });

  test('fails the request at the timeout and invalidates later writes', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    const handlerDone = deferred<void>();
    let lateWriteError = '';

    dispatcher.register('slow', async (ctx) => {
      await sleep(40);
      try {
        await ctx.storage.execute('INSERT 1');
        lateWriteError = 'write was accepted';
      } catch (error) {
        lateWriteError = error instanceof Error ? error.message : String(error);
      }
      handlerDone.resolve();
      return 'late result';
    });

    const error = await rejectionOf(dispatcher.dispatch('slow', null));
    await handlerDone.promise;
    // The backend rolls back only after the abandoned handler returns.
    await sleep(5);

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    expect(lateWriteError).toMatch(/expired/);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('uses the smaller of the configured timeout and the backend ceiling', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: 5 },
    });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 10_000 });
    dispatcher.register('slow', async () => {
      await sleep(50);
      return 'late';
    });

    const error = await rejectionOf(dispatcher.dispatch('slow', null));

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.timeoutMs).toBe(5);
      expect(error.code).toBe('FRAMEWORK_TIMEOUT');
    }
  });

  test('rejects a non-finite or non-positive timeout', () => {
    const backend = new MemoryStorageBackend();

    expect(
      () => new Dispatcher({ backend, timeoutMs: Number.POSITIVE_INFINITY }),
    ).toThrow(/positive finite/);
    expect(() => new Dispatcher({ backend, timeoutMs: 0 })).toThrow(
      /positive finite/,
    );
  });

  test('close shuts down the backend', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });

    await dispatcher.close();

    expect(() => backend.capabilities()).toThrow(/closed/);
    const error = await rejectionOf(dispatcher.dispatch('anything', null));
    expect(error.message).toMatch(/closed/);
  });

  test('generates a request id with the default factory', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = new Dispatcher({ backend, timeoutMs: 1000 });
    let seen = '';
    dispatcher.register('inspect', async (ctx) => {
      seen = ctx.requestId;
      return null;
    });

    await dispatcher.dispatch('inspect', null);

    expect(seen).toMatch(/^[0-9a-f-]{36}$/);
  });

  test('uses an injected request id factory', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({
      backend,
      timeoutMs: 1000,
      createRequestId: () => 'fixed-id',
    });
    let seen = '';
    dispatcher.register('inspect', async (ctx) => {
      seen = ctx.requestId;
      return null;
    });

    await dispatcher.dispatch('inspect', null);

    expect(seen).toBe('fixed-id');
  });

  test('prefers meta.requestId so a replay keeps the original id', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({
      backend,
      timeoutMs: 1000,
      createRequestId: () => 'fresh-id',
    });
    const seen: string[] = [];
    dispatcher.register('inspect', async (ctx) => {
      seen.push(ctx.requestId);
      return null;
    });

    await dispatcher.dispatch('inspect', null, { requestId: 'original-id' });
    await dispatcher.dispatch('inspect', null);

    expect(seen).toEqual(['original-id', 'fresh-id']);
  });

  test('carries connection, client request and session ids into the context', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let seen: Record<string, unknown> = {};
    dispatcher.register('inspect', async (ctx) => {
      seen = {
        connectionId: ctx.connectionId,
        clientRequestId: ctx.clientRequestId,
        sessionId: ctx.sessionId,
      };
      return null;
    });

    await dispatcher.dispatch('inspect', null, {
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
    });

    expect(seen).toEqual({
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
    });
  });

  test('defaults connection and client ids to empty strings when absent', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    let seen: Record<string, unknown> = {};
    dispatcher.register('inspect', async (ctx) => {
      seen = {
        connectionId: ctx.connectionId,
        clientRequestId: ctx.clientRequestId,
        sessionId: ctx.sessionId,
      };
      return null;
    });

    await dispatcher.dispatch('inspect', null);

    expect(seen).toEqual({
      connectionId: '',
      clientRequestId: '',
      sessionId: undefined,
    });
  });

  test('binds the request ids onto ctx.logger', async () => {
    const backend = new MemoryStorageBackend();
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 }, lines);
    dispatcher.register('inspect', async (ctx) => {
      ctx.logger.info({ detail: 'inside handler' }, 'handler ran');
      return null;
    });

    await dispatcher.dispatch('inspect', null, {
      requestId: 'req-9',
      connectionId: 'c9',
      clientRequestId: 'client-9',
    });

    expect(JSON.parse(lines[0] ?? '')).toMatchObject({
      level: 'info',
      msg: 'handler ran',
      requestId: 'req-9',
      connectionId: 'c9',
      clientRequestId: 'client-9',
      detail: 'inside handler',
    });
  });

  test('logs a timeout at warn with the effective timeout', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: 5 },
    });
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher(
      { backend, timeoutMs: 10_000 },
      lines,
    );
    dispatcher.register('slow', async () => {
      await sleep(40);
      return 'late';
    });

    await rejectionOf(dispatcher.dispatch('slow', null, { requestId: 'r1' }));

    const timedOut = parseLines(lines).filter(
      (entry) => entry.msg === 'request timed out',
    );
    expect(timedOut).toHaveLength(1);
    expect(timedOut[0]).toMatchObject({
      level: 'warn',
      requestId: 'r1',
      method: 'slow',
      timeoutMs: 5,
    });
  });

  test('FrameworkTimeoutError carries the id that was actually used', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    dispatcher.register('slow', async () => {
      await sleep(40);
      return 'late';
    });

    const error = await rejectionOf(
      dispatcher.dispatch('slow', null, { requestId: 'given-id' }),
    );

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    if (error instanceof FrameworkTimeoutError) {
      expect(error.requestId).toBe('given-id');
    }
  });
});
