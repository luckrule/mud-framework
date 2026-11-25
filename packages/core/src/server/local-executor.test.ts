import { describe, expect, test } from 'bun:test';

import { sleep } from 'mud-framework-test-support';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { ExecutorRequest } from '../shared/executor.ts';
import { BusinessError } from '../shared/wire-codes.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { Dispatcher } from './dispatcher.ts';
import type { DispatchMeta } from './dispatcher.ts';
import { LocalExecutor } from './local-executor.ts';

function quietDispatcher(
  options: ConstructorParameters<typeof Dispatcher>[0],
): Dispatcher {
  const logger: Logger = createJsonLogger({ sink: () => undefined });
  return new Dispatcher({ ...options, logger });
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

describe('LocalExecutor', () => {
  test('start resolves without spawning anything', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    dispatcher.register('echo', async (_ctx, payload) => payload);
    const executor = new LocalExecutor({ dispatcher });

    // In process there is no backend to wait for; the interface still requires
    // a start so callers cannot forget it (SPEC 6.2 step 3).
    await executor.start();
    const outcome = await executor.execute(request({ payload: 'hi' }));

    expect(outcome).toEqual({ ok: true, result: 'hi' });
    await executor.close();
  });

  test('returns the handler result as a success outcome', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    dispatcher.register('echo', async (_ctx, payload) => payload);
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ payload: 'hi' }));

    expect(outcome).toEqual({ ok: true, result: 'hi' });
    await executor.close();
  });

  test('maps a timeout to FRAMEWORK_TIMEOUT', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 15,
    });
    dispatcher.register('slow', async () => {
      await sleep(60);
      return 'late';
    });
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ method: 'slow' }));

    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure outcome');
    }
    expect(outcome.error.code).toBe('FRAMEWORK_TIMEOUT');
    expect(outcome.error.message).toContain('timed out');
    // Let the abandoned handler settle before the next test starts.
    await sleep(60);
    await executor.close();
  });

  test('maps an unregistered method to UNKNOWN_METHOD', async () => {
    const executor = new LocalExecutor({
      dispatcher: quietDispatcher({
        backend: new MemoryStorageBackend(),
        timeoutMs: 1000,
      }),
    });

    const outcome = await executor.execute(request({ method: 'missing' }));

    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure outcome');
    }
    expect(outcome.error.code).toBe('UNKNOWN_METHOD');
    expect(outcome.error.message).toContain('missing');
    await executor.close();
  });

  test('maps an ordinary Error to INTERNAL_ERROR', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    dispatcher.register('explode', async () => {
      throw new Error('business failure');
    });
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ method: 'explode' }));

    expect(outcome).toEqual({
      ok: false,
      error: { code: 'INTERNAL_ERROR', message: 'business failure' },
    });
    await executor.close();
  });

  test('does not leak a driver error code into the wire code', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    dispatcher.register('explode', async () => {
      // Shaped like a SQLite driver error: a duck-typed `code` must not become
      // the wire code, or downstream alerts would key on a driver detail
      // (INV-014, SPEC 7.2).
      throw Object.assign(new Error('UNIQUE constraint failed'), {
        code: 'SQLITE_CONSTRAINT_UNIQUE',
      });
    });
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ method: 'explode' }));

    expect(outcome).toEqual({
      ok: false,
      error: {
        code: 'INTERNAL_ERROR',
        message: 'UNIQUE constraint failed',
      },
    });
    await executor.close();
  });

  test('passes a BusinessError code through unchanged', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    dispatcher.register('explode', async () => {
      throw new BusinessError('PLAYER_NOT_FOUND', 'no such player');
    });
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ method: 'explode' }));

    expect(outcome).toEqual({
      ok: false,
      error: { code: 'PLAYER_NOT_FOUND', message: 'no such player' },
    });
    await executor.close();
  });

  test('degrades a BusinessError with an empty code to INTERNAL_ERROR', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    dispatcher.register('explode', async () => {
      throw new BusinessError('', 'unclassifiable failure');
    });
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ method: 'explode' }));

    expect(outcome).toEqual({
      ok: false,
      error: { code: 'INTERNAL_ERROR', message: 'unclassifiable failure' },
    });
    await executor.close();
  });

  test('passes the request id through instead of minting one', async () => {
    const seen: string[] = [];
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
      createRequestId: () => {
        seen.push('minted');
        return 'minted-id';
      },
    });
    let contextRequestId = '';
    dispatcher.register('inspect', async (ctx) => {
      contextRequestId = ctx.requestId;
      return null;
    });
    const executor = new LocalExecutor({ dispatcher });

    await executor.execute(request({ method: 'inspect', requestId: 'req-42' }));

    expect(contextRequestId).toBe('req-42');
    expect(seen).toEqual([]);
    await executor.close();
  });

  test('passes connection, client request and session ids through', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    let seen: Record<string, unknown> = {};
    dispatcher.register('inspect', async (ctx) => {
      seen = {
        connectionId: ctx.connectionId,
        clientRequestId: ctx.clientRequestId,
        sessionId: ctx.sessionId,
      };
      return null;
    });
    const executor = new LocalExecutor({ dispatcher });

    await executor.execute(request({ method: 'inspect', sessionId: 's1' }));

    expect(seen).toEqual({
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
    });
    await executor.close();
  });

  test('carries the session id a handler bound into the success outcome', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('session-1');
      return 'welcome';
    });
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ method: 'login' }));

    expect(outcome).toEqual({
      ok: true,
      result: 'welcome',
      sessionId: 'session-1',
    });
    await executor.close();
  });

  test('omits the session id when the handler binds nothing', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    dispatcher.register('echo', async (_ctx, payload) => payload);
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ payload: 'hi' }));

    // Exactly equal, so an `undefined` field would fail: the absence is part of
    // the contract (SPEC 2).
    expect(outcome).toEqual({ ok: true, result: 'hi' });
    await executor.close();
  });

  test('does not carry a session id on a failure', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('half-made');
      throw new BusinessError('LOGIN_REFUSED', 'nope');
    });
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ method: 'login' }));

    expect(outcome).toEqual({
      ok: false,
      error: { code: 'LOGIN_REFUSED', message: 'nope' },
    });
    await executor.close();
  });

  test('close closes the dispatcher and through it the backend', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = quietDispatcher({ backend, timeoutMs: 1000 });
    const executor = new LocalExecutor({ dispatcher });

    await executor.close();

    expect(() => backend.capabilities()).toThrow(/closed/);
    // A second close is a no-op rather than a lifecycle error.
    await executor.close();
  });

  test('passes request.timeoutMs through as the dispatch deadline', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 10_000,
    });
    dispatcher.register('slow', async () => {
      await sleep(60);
      return 'late';
    });
    const executor = new LocalExecutor({ dispatcher });

    const startedAt = performance.now();
    const outcome = await executor.execute(
      request({ method: 'slow', timeoutMs: 10 }),
    );
    const elapsed = performance.now() - startedAt;

    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure outcome');
    }
    expect(outcome.error.code).toBe('FRAMEWORK_TIMEOUT');
    // The per-request deadline wins over the executor's own 10s ceiling.
    expect(elapsed).toBeLessThan(1000);
    await sleep(60);
    await executor.close();
  });

  test('strips the code version from the handler metadata', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    const metas: DispatchMeta[] = [];
    const dispatch = dispatcher.dispatch.bind(dispatcher);
    dispatcher.dispatch = (method, payload, meta) => {
      const observed = meta ?? {};
      metas.push(observed);
      return dispatch(method, payload, observed);
    };
    dispatcher.register('echo', async (_ctx, payload) => payload);
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ codeVersion: 'build-1' }));

    expect(outcome).toEqual({ ok: true, result: null });
    expect(metas).toHaveLength(1);
    // `codeVersion` is IPC envelope metadata: it must not reach the dispatcher
    // as an undeclared `DispatchMeta` key.
    expect(metas[0]).not.toHaveProperty('codeVersion');
    await executor.close();
  });

  test('cancel rolls back an in-flight request', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    dispatcher.register('slow', async (ctx) => {
      await ctx.storage.execute('INSERT 1');
      entered.resolve();
      await release.promise;
      return 'late';
    });
    const executor = new LocalExecutor({ dispatcher });

    const outcomePromise = executor.execute(request({ method: 'slow' }));
    await entered.promise;
    await executor.cancel('req-1', 'TRANSPORT_CLOSED');
    release.resolve();

    const outcome = await outcomePromise;
    await sleep(5);

    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure outcome');
    }
    expect(outcome.error.code).toBe('TRANSPORT_CLOSED');
    expect(outcome.error.message).toContain('cancelled');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
    await executor.close();
  });

  test('cancel for an unknown request id is a no-op', async () => {
    const executor = new LocalExecutor({
      dispatcher: quietDispatcher({
        backend: new MemoryStorageBackend(),
        timeoutMs: 1000,
      }),
    });

    await executor.cancel('never-seen', 'TRANSPORT_CLOSED');

    await executor.close();
  });
});
