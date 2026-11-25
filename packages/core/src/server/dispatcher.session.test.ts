import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import {
  FrameworkTimeoutError,
  RequestCancelledError,
} from '../shared/wire-codes.ts';
import { rejectionOf, sleep } from '../testing/common.ts';
import {
  deferred,
  parseLines,
  quietDispatcher,
} from '../testing/dispatcher.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { Dispatcher } from './dispatcher.ts';

const LOGIN = { access: 'public', mayChangeSession: true } as const;

describe('Dispatcher.session', () => {
  test('reports the session change the handler bound', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'login',
      async (ctx) => {
        ctx.bindSession('session-1');
        return 'welcome';
      },
      LOGIN,
    );

    const outcome = await dispatcher.dispatch('login', null);

    expect(outcome).toEqual({
      result: 'welcome',
      sessionChange: { type: 'bind', sessionId: 'session-1' },
    });
  });

  test('reports an unbind the handler declared', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'logout',
      async (ctx) => {
        ctx.unbindSession();
        return 'bye';
      },
      LOGIN,
    );

    const outcome = await dispatcher.dispatch('logout', null);

    expect(outcome).toEqual({
      result: 'bye',
      sessionChange: { type: 'unbind' },
    });
  });

  test('leaves the session change undefined when the handler declares none', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('look', async () => 'a rock', { access: 'public' });

    const outcome = await dispatcher.dispatch('look', null);

    expect(outcome.result).toBe('a rock');
    expect(outcome.sessionChange).toBeUndefined();
  });

  test('does not report the bind when the handler throws', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'login',
      async (ctx) => {
        ctx.bindSession('half-made');
        throw new Error('login refused');
      },
      LOGIN,
    );

    const error = await rejectionOf(dispatcher.dispatch('login', null));

    expect(error.message).toBe('login refused');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('does not report the unbind when the handler throws', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'logout',
      async (ctx) => {
        ctx.unbindSession();
        throw new Error('logout failed');
      },
      LOGIN,
    );

    const error = await rejectionOf(dispatcher.dispatch('logout', null));

    expect(error.message).toBe('logout failed');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('does not report the bind when the request times out', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    const handlerDone = deferred<void>();
    dispatcher.register(
      'slow-login',
      async (ctx) => {
        ctx.bindSession('too-late');
        await sleep(40);
        handlerDone.resolve();
        return 'late';
      },
      LOGIN,
    );

    const error = await rejectionOf(dispatcher.dispatch('slow-login', null));
    // The backend rolls back only after the abandoned handler returns.
    await handlerDone.promise;
    await sleep(5);

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects a bind when the handler did not declare mayChangeSession', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'login',
      async (ctx) => {
        ctx.bindSession('s1');
        return null;
      },
      { access: 'public' },
    );

    const error = await rejectionOf(dispatcher.dispatch('login', null));

    expect(error.message).toMatch(/mayChangeSession/);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects an unbind when the handler did not declare mayChangeSession', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'logout',
      async (ctx) => {
        ctx.unbindSession();
        return null;
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('logout', null, { sessionId: 's1' }),
    );

    expect(error.message).toMatch(/mayChangeSession/);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects an empty session id', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'login',
      async (ctx) => {
        ctx.bindSession('');
        return null;
      },
      LOGIN,
    );

    const error = await rejectionOf(dispatcher.dispatch('login', null));

    expect(error.message).toMatch(/non-empty session id/);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects a second bind in the same request and keeps the first', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'login',
      async (ctx) => {
        ctx.bindSession('first');
        ctx.bindSession('second');
        return null;
      },
      LOGIN,
    );

    const error = await rejectionOf(dispatcher.dispatch('login', null));

    expect(error.message).toMatch(/at most once/);
    expect(error.message).toContain('first');
    expect(error.message).toContain('second');
    // A rejected request commits nothing, so neither id escapes.
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects a bind after an unbind in the same request', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'switch',
      async (ctx) => {
        ctx.unbindSession();
        ctx.bindSession('late');
        return null;
      },
      LOGIN,
    );

    const error = await rejectionOf(
      dispatcher.dispatch('switch', null, { sessionId: 's1' }),
    );

    expect(error.message).toMatch(/at most once/);
    expect(error.message).toContain('unbindSession()');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects an unbind after a bind in the same request', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register(
      'switch',
      async (ctx) => {
        ctx.bindSession('next');
        ctx.unbindSession();
        return null;
      },
      LOGIN,
    );

    const error = await rejectionOf(dispatcher.dispatch('switch', null));

    expect(error.message).toMatch(/at most once/);
    expect(error.message).toContain('bindSession("next")');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('does not report a bind when the request is cancelled', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = deferred<void>();
    const release = deferred<void>();
    dispatcher.register(
      'login',
      async (ctx) => {
        ctx.bindSession('s1');
        entered.resolve();
        await release.promise;
        return 'late';
      },
      LOGIN,
    );

    const pending = dispatcher.dispatch('login', null, { requestId: 'r1' });
    await entered.promise;
    expect(dispatcher.cancel('r1', 'TRANSPORT_CLOSED')).toBe(true);
    const error = await rejectionOf(pending);
    release.resolve();
    await sleep(5);

    expect(error).toBeInstanceOf(RequestCancelledError);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('does not report an unbind when the request is cancelled', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    const entered = deferred<void>();
    const release = deferred<void>();
    dispatcher.register(
      'logout',
      async (ctx) => {
        ctx.unbindSession();
        entered.resolve();
        await release.promise;
        return 'late';
      },
      LOGIN,
    );

    const pending = dispatcher.dispatch('logout', null, {
      requestId: 'r2',
      sessionId: 's1',
    });
    await entered.promise;
    expect(dispatcher.cancel('r2', 'TRANSPORT_CLOSED')).toBe(true);
    const error = await rejectionOf(pending);
    release.resolve();
    await sleep(5);

    expect(error).toBeInstanceOf(RequestCancelledError);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects a bind after the request already timed out', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    let bindError = '';
    dispatcher.register(
      'slow',
      async (ctx) => {
        await sleep(40);
        try {
          ctx.bindSession('late');
          bindError = 'bind was accepted';
        } catch (error) {
          bindError = error instanceof Error ? error.message : String(error);
        }
        return 'late';
      },
      LOGIN,
    );

    await rejectionOf(dispatcher.dispatch('slow', null));
    await sleep(60);

    // The transaction is expired, so a binding made after the deadline is
    // rejected like any other late write (SPEC 3.3, 3.4).
    expect(bindError).toMatch(/expired/);
  });

  test('logs the recorded bind as a sessionChange', async () => {
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      level: 'debug',
      sink: (line) => {
        lines.push(line);
      },
    });
    const dispatcher = new Dispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
      logger,
    });
    dispatcher.register(
      'login',
      async (ctx) => {
        ctx.bindSession('session-9');
        return null;
      },
      LOGIN,
    );

    await dispatcher.dispatch('login', null, { requestId: 'req-9' });

    const recorded = parseLines(lines).filter(
      (entry) => entry.msg === 'session change recorded',
    );
    expect(recorded).toHaveLength(1);
    expect(recorded[0]).toMatchObject({
      level: 'debug',
      requestId: 'req-9',
      sessionChange: { type: 'bind', sessionId: 'session-9' },
    });
    // No entry Session, so the log names none; the change is the only place
    // the new id appears.
    expect(recorded[0]).not.toHaveProperty('sessionId');
  });

  test('logs a recorded unbind as a sessionChange', async () => {
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      level: 'debug',
      sink: (line) => {
        lines.push(line);
      },
    });
    const dispatcher = new Dispatcher({
      backend: new MemoryStorageBackend(),
      timeoutMs: 1000,
      logger,
    });
    dispatcher.register(
      'logout',
      async (ctx) => {
        ctx.unbindSession();
        return null;
      },
      LOGIN,
    );

    await dispatcher.dispatch('logout', null, {
      requestId: 'req-10',
      sessionId: 'session-9',
    });

    const recorded = parseLines(lines).filter(
      (entry) => entry.msg === 'session change recorded',
    );
    expect(recorded).toHaveLength(1);
    expect(recorded[0]).toMatchObject({
      level: 'debug',
      requestId: 'req-10',
      sessionId: 'session-9',
      sessionChange: { type: 'unbind' },
    });
  });
});
