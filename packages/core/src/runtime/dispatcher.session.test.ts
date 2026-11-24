import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf, sleep } from '../test-support/common.ts';
import {
  deferred,
  parseLines,
  quietDispatcher,
} from '../test-support/dispatcher.ts';
import { Dispatcher, FrameworkTimeoutError } from './dispatcher.ts';

describe('Dispatcher.session', () => {
  test('reports the session id the handler bound', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('session-1');
      return 'welcome';
    });

    const outcome = await dispatcher.dispatch('login', null);

    expect(outcome).toEqual({ result: 'welcome', boundSessionId: 'session-1' });
  });

  test('leaves the session id undefined when the handler binds nothing', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('look', async () => 'a rock');

    const outcome = await dispatcher.dispatch('look', null);

    expect(outcome.result).toBe('a rock');
    expect(outcome.boundSessionId).toBeUndefined();
  });

  test('does not commit the binding when the handler throws', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('half-made');
      throw new Error('login refused');
    });

    const error = await rejectionOf(dispatcher.dispatch('login', null));

    expect(error.message).toBe('login refused');
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('does not commit the binding when the request times out', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    const handlerDone = deferred<void>();
    dispatcher.register('slow-login', async (ctx) => {
      ctx.bindSession('too-late');
      await sleep(40);
      handlerDone.resolve();
      return 'late';
    });

    const error = await rejectionOf(dispatcher.dispatch('slow-login', null));
    // The backend rolls back only after the abandoned handler returns.
    await handlerDone.promise;
    await sleep(5);

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects an empty session id', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('');
      return null;
    });

    const error = await rejectionOf(dispatcher.dispatch('login', null));

    expect(error.message).toMatch(/non-empty session id/);
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects a second bind in the same request and keeps the first', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('first');
      ctx.bindSession('second');
      return null;
    });

    const error = await rejectionOf(dispatcher.dispatch('login', null));

    expect(error.message).toMatch(/bindSession called twice/);
    expect(error.message).toContain('first');
    expect(error.message).toContain('second');
    // A rejected request commits nothing, so neither id escapes.
    expect(backend.stats()).toEqual({ committed: 0, rolledBack: 1 });
  });

  test('rejects a bind after the request already timed out', async () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 10 });
    let bindError = '';
    dispatcher.register('slow', async (ctx) => {
      await sleep(40);
      try {
        ctx.bindSession('late');
        bindError = 'bind was accepted';
      } catch (error) {
        bindError = error instanceof Error ? error.message : String(error);
      }
      return 'late';
    });

    await rejectionOf(dispatcher.dispatch('slow', null));
    await sleep(60);

    // The transaction is expired, so a binding made after the deadline is
    // rejected like any other late write (SPEC 3.3, 3.4).
    expect(bindError).toMatch(/expired/);
  });

  test('logs the binding at debug with the session id', async () => {
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
    dispatcher.register('login', async (ctx) => {
      ctx.bindSession('session-9');
      return null;
    });

    await dispatcher.dispatch('login', null, { requestId: 'req-9' });

    const bound = parseLines(lines).filter(
      (entry) => entry.msg === 'session bound',
    );
    expect(bound).toHaveLength(1);
    expect(bound[0]).toMatchObject({
      level: 'debug',
      requestId: 'req-9',
      sessionId: 'session-9',
    });
  });
});
