import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { sleep, tick } from '../testing/common.ts';
import {
  requestMessage,
  results,
  startServer,
} from '../testing/server-process.ts';

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
    expect(outcome).toMatchObject({
      ok: false,
      error: { code: 'FRAMEWORK_TIMEOUT' },
    });
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
    expect(outcome).toMatchObject({
      ok: false,
      error: { code: 'FRAMEWORK_TIMEOUT' },
    });
    await sleep(60);
    await harness.stop();
  });
});
