import { describe, expect, test } from 'bun:test';

import { tick } from '../testing/common.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import {
  requestMessage,
  received,
  results,
} from '../testing/server-process.ts';
import {
  activate,
  activationHarness,
  hello,
  types,
} from './server-process.activation.support.ts';

describe('runServerProcess two-phase activation', () => {
  test('answers hello with prepared without creating the backend', async () => {
    const registered: string[] = [];
    const h = activationHarness({
      extensions: [
        {
          name: 'echo',
          register: () => {
            registered.push('echo');
          },
        },
      ],
    });

    hello(h);

    expect(received(h.messages)).toEqual([
      { type: 'prepared', pid: process.pid },
    ]);
    // INV-002: no storage is opened before the Portal asks for activation.
    expect(h.created()).toBe(0);
    expect(registered).toEqual([]);
    await h.stop();
  });

  test('opens the backend, registers extensions and sends ready only after activate', async () => {
    const registered: string[] = [];
    const h = activationHarness({
      extensions: [
        {
          name: 'echo',
          register: (registrar) => {
            registered.push('echo');
            registrar.register(
              'echo',
              (_ctx, payload) => Promise.resolve(payload),
              { access: 'public' },
            );
          },
        },
      ],
    });

    hello(h);
    expect(h.created()).toBe(0);
    expect(registered).toEqual([]);

    activate(h);
    await tick();

    expect(h.created()).toBe(1);
    expect(registered).toEqual(['echo']);
    expect(types(h)).toEqual(['prepared', 'ready']);
    const ready = received(h.messages).find(
      (message) => message.type === 'ready',
    );
    expect(ready).toMatchObject({ type: 'ready', pid: process.pid });

    // The full request path is only attached after the readiness barrier.
    h.portal.send(requestMessage({ payload: 'ping' }));
    await tick();
    expect(results(h.messages)).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'ping' },
    ]);

    h.portal.send({ type: 'shutdown', timeoutMs: 1000 });
    await h.done;
  });

  test('closes the storage it just opened when shutdown arrives during the start', async () => {
    const gate = Promise.withResolvers<void>();
    const backend = new MemoryStorageBackend();
    const h = activationHarness({
      createBackend: async () => {
        await gate.promise;
        return backend;
      },
    });

    hello(h);
    activate(h);
    await tick();
    // Parked while the backend is opening: the start owns the close, so this
    // must not close the link under it.
    h.portal.send({ type: 'shutdown', timeoutMs: 1000 });
    await tick();
    expect(types(h)).toEqual(['prepared']);

    gate.resolve();
    await h.done;

    // No `ready`: the shutdown was already in flight when the start finished,
    // so announcing readiness would promise work this process will not take.
    expect(types(h)).toEqual(['prepared', 'closed']);
    expect(() => backend.transaction(() => Promise.resolve())).toThrow(
      /storage backend is closed/,
    );
  });

  test('shuts down during prepare without opening the backend', async () => {
    const h = activationHarness();

    hello(h);
    h.portal.send({ type: 'shutdown', timeoutMs: 1000 });
    await h.done;

    expect(h.created()).toBe(0);
    expect(types(h)).toEqual(['prepared', 'closed']);
  });
});
