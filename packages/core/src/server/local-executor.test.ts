import { describe, expect, test } from 'bun:test';

import { sleep } from 'mud-framework-test-support';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { ExecutorRequest } from '../shared/executor.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { Dispatcher } from './dispatcher.ts';
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
    dispatcher.register('echo', async (_ctx, payload) => payload, {
      access: 'public',
    });
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
    dispatcher.register('echo', async (_ctx, payload) => payload, {
      access: 'public',
    });
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ payload: 'hi' }));

    expect(outcome).toEqual({ ok: true, result: 'hi' });
    await executor.close();
  });

  test('passes connection, client request and session ids through', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    let seen: Record<string, unknown> = {};
    dispatcher.register(
      'inspect',
      async (ctx) => {
        seen = {
          connectionId: ctx.connectionId,
          clientRequestId: ctx.clientRequestId,
          sessionId: ctx.sessionId,
        };
        return null;
      },
      { access: 'public' },
    );
    const executor = new LocalExecutor({ dispatcher });

    await executor.execute(request({ method: 'inspect', sessionId: 's1' }));

    expect(seen).toEqual({
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
    });
    await executor.close();
  });

  test('carries the session change a handler bound into the success outcome', async () => {
    const dispatcher = quietDispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
    });
    dispatcher.register(
      'login',
      async (ctx) => {
        ctx.bindSession('session-1');
        return 'welcome';
      },
      { access: 'public', mayChangeSession: true },
    );
    const executor = new LocalExecutor({ dispatcher });

    const outcome = await executor.execute(request({ method: 'login' }));

    expect(outcome).toEqual({
      ok: true,
      result: 'welcome',
      sessionChange: { type: 'bind', sessionId: 'session-1' },
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
    dispatcher.register(
      'slow',
      async () => {
        await sleep(60);
        return 'late';
      },
      { access: 'public' },
    );
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

  test('cancel rolls back an in-flight request', async () => {
    const backend = new MemoryStorageBackend();
    const dispatcher = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    dispatcher.register(
      'slow',
      async (ctx) => {
        await ctx.storage.execute('INSERT 1');
        entered.resolve();
        await release.promise;
        return 'late';
      },
      { access: 'public' },
    );
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
