import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { parseLines, quietDispatcher } from '../testing/dispatcher.ts';

describe('Dispatcher handler metadata', () => {
  test('exposes the registering Extension in the context and request logs', async () => {
    const backend = new MemoryStorageBackend();
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 }, lines);
    let seen: string | undefined = 'unset';

    dispatcher.register(
      'deposit',
      async (ctx) => {
        seen = ctx.extension;
        ctx.logger.info({ detail: 'seen' }, 'handler ran');
        return null;
      },
      { extension: 'banking' },
    );

    await dispatcher.dispatch('deposit', null, {
      requestId: 'r-banking',
      connectionId: 'c1',
      clientRequestId: 'client-1',
    });

    expect(seen).toBe('banking');
    const handlerLines = parseLines(lines).filter(
      (entry) => entry.msg === 'handler ran',
    );
    expect(handlerLines).toHaveLength(1);
    expect(handlerLines[0]).toMatchObject({
      extension: 'banking',
      requestId: 'r-banking',
      connectionId: 'c1',
      clientRequestId: 'client-1',
    });
  });

  test('reports the registering Extension for a method, without running it', () => {
    const backend = new MemoryStorageBackend();
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 });

    dispatcher.register('deposit', async () => null, {
      extension: 'banking',
    });
    dispatcher.register('plain', async () => null);

    expect(dispatcher.extensionOf('deposit')).toBe('banking');
    // Absence is an answer too: the caller writes the field only when one exists.
    expect(dispatcher.extensionOf('plain')).toBeUndefined();
    expect(dispatcher.extensionOf('never-registered')).toBeUndefined();
  });

  test('leaves the Extension unset when the handler declares none', async () => {
    const backend = new MemoryStorageBackend();
    const lines: string[] = [];
    const { dispatcher } = quietDispatcher({ backend, timeoutMs: 1000 }, lines);
    let seen: string | undefined = 'unset';

    dispatcher.register('plain', async (ctx) => {
      seen = ctx.extension;
      ctx.logger.info({ detail: 'seen' }, 'handler ran');
      return null;
    });

    await dispatcher.dispatch('plain', null, { requestId: 'r-plain' });

    expect(seen).toBeUndefined();
    const line = parseLines(lines).find((entry) => entry.msg === 'handler ran');
    if (line === undefined) {
      throw new Error('expected the handler log line');
    }
    // An absent field, not `extension: undefined`: a JSON line must not claim
    // an ownership it does not have (SPEC 7.1).
    expect(Object.hasOwn(line, 'extension')).toBe(false);
  });
});
