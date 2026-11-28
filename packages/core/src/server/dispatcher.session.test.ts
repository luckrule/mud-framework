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

  test.each(['bind', 'unbind'] as const)(
    'does not report the %s when the handler throws',
    async (change) => {
      const backend = new MemoryStorageBackend();
      const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
      dispatcher.register(
        'change',
        async (ctx) => {
          if (change === 'bind') {
            ctx.bindSession('half-made');
          } else {
            ctx.unbindSession();
          }
          throw new Error(`${change} refused`);
        },
        LOGIN,
      );

      const error = await rejectionOf(dispatcher.dispatch('change', null));

      expect(error.message).toBe(`${change} refused`);
      expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
    },
  );

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

  test.each(['bind', 'unbind'] as const)(
    'rejects a %s when the handler did not declare mayChangeSession',
    async (change) => {
      const backend = new MemoryStorageBackend();
      const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
      dispatcher.register(
        'change',
        async (ctx) => {
          if (change === 'bind') {
            ctx.bindSession('s1');
          } else {
            ctx.unbindSession();
          }
          return null;
        },
        { access: 'public' },
      );

      const error = await rejectionOf(
        dispatcher.dispatch(
          'change',
          null,
          change === 'unbind' ? { sessionId: 's1' } : undefined,
        ),
      );

      expect(error.message).toMatch(/mayChangeSession/);
      expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
    },
  );

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

  test.each(['bind', 'unbind'] as const)(
    'keeps the first change when the handler swallows a second %s',
    async (second) => {
      const backend = new MemoryStorageBackend();
      const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
      let message = '';
      dispatcher.register(
        'change',
        async (ctx) => {
          ctx.bindSession('first');
          try {
            if (second === 'bind') {
              ctx.bindSession('second');
            } else {
              ctx.unbindSession();
            }
          } catch (error) {
            message = error instanceof Error ? error.message : String(error);
          }
          return 'ok';
        },
        LOGIN,
      );

      const outcome = await dispatcher.dispatch('change', null, {
        sessionId: 'entry',
      });

      // Catching the second-call error does not make the second change valid;
      // the first change is the one the request reports.
      expect(outcome).toEqual({
        result: 'ok',
        sessionChange: { type: 'bind', sessionId: 'first' },
      });
      expect(message).toMatch(/at most once/);
      expect(backend.stats()).toEqual({ committed: 1, rolledBack: 0 });
    },
  );

  test.each(['bind', 'unbind'] as const)(
    'rejects a %s after the other change in the same request',
    async (second) => {
      const backend = new MemoryStorageBackend();
      const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
      dispatcher.register(
        'switch',
        async (ctx) => {
          if (second === 'bind') {
            ctx.unbindSession();
            ctx.bindSession('late');
          } else {
            ctx.bindSession('next');
            ctx.unbindSession();
          }
          return null;
        },
        LOGIN,
      );

      const error = await rejectionOf(
        dispatcher.dispatch(
          'switch',
          null,
          second === 'bind' ? { sessionId: 's1' } : undefined,
        ),
      );

      expect(error.message).toMatch(/at most once/);
      expect(error.message).toContain(
        second === 'bind' ? 'unbindSession()' : 'bindSession("next")',
      );
      expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
    },
  );

  test.each(['bind', 'unbind'] as const)(
    'does not report a %s when the request is cancelled',
    async (change) => {
      const backend = new MemoryStorageBackend();
      const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
      const entered = deferred<void>();
      const release = deferred<void>();
      dispatcher.register(
        'change',
        async (ctx) => {
          if (change === 'bind') {
            ctx.bindSession('s1');
          } else {
            ctx.unbindSession();
          }
          entered.resolve();
          await release.promise;
          return 'late';
        },
        LOGIN,
      );

      const pending = dispatcher.dispatch(
        'change',
        null,
        change === 'unbind'
          ? { requestId: 'r1', sessionId: 's1' }
          : { requestId: 'r1' },
      );
      await entered.promise;
      expect(dispatcher.cancel('r1', 'TRANSPORT_CLOSED')).toBe(true);
      const error = await rejectionOf(pending);
      release.resolve();
      await sleep(5);

      expect(error).toBeInstanceOf(RequestCancelledError);
      expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
    },
  );

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

  test.each(['bind', 'unbind'] as const)(
    'logs a recorded %s as a sessionChange',
    async (change) => {
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
        'change',
        async (ctx) => {
          if (change === 'bind') {
            ctx.bindSession('session-9');
          } else {
            ctx.unbindSession();
          }
          return null;
        },
        LOGIN,
      );

      await dispatcher.dispatch(
        'change',
        null,
        change === 'unbind'
          ? { requestId: 'req-9', sessionId: 'session-9' }
          : { requestId: 'req-9' },
      );

      const recorded = parseLines(lines).filter(
        (entry) => entry.msg === 'session change recorded',
      );
      expect(recorded).toHaveLength(1);
      if (change === 'bind') {
        expect(recorded[0]).toMatchObject({
          level: 'debug',
          requestId: 'req-9',
          sessionChange: { type: 'bind', sessionId: 'session-9' },
        });
        // No entry Session, so the log names none; the change is the only place
        // the new id appears.
        expect(recorded[0]).not.toHaveProperty('sessionId');
      } else {
        expect(recorded[0]).toMatchObject({
          level: 'debug',
          requestId: 'req-9',
          sessionId: 'session-9',
          sessionChange: { type: 'unbind' },
        });
      }
    },
  );
});
