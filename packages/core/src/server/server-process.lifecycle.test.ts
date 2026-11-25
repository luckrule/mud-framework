import { describe, expect, test } from 'bun:test';

import { createProcessChannel } from '../ipc/bun-child.ts';
import { BusinessError } from '../shared/wire-codes.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import type { StorageBackend } from '../storage/types.ts';
import { rejectionOf, throwingLogger, tick } from '../testing/common.ts';
import {
  echoRegistration,
  readies,
  requestMessage,
  results,
  sendRaw,
  startServer,
} from '../testing/server-process.ts';
import { ServerChannelClosedError } from './server-process.ts';

describe('createProcessChannel singleton', () => {
  test('refuses a second channel in the same process', () => {
    // `process.send` and the `message`/`disconnect` events are process-global,
    // so a second channel would add a second listener and deliver every inbound
    // message to both — two Servers executing the same request (INV-002).
    //
    // `bun test` has no `process.send`, so construction logs `IPC_UNAVAILABLE`;
    // a throwing logger proves that warn cannot change the singleton result.
    createProcessChannel({ logger: throwingLogger() });

    expect(() => createProcessChannel({ logger: throwingLogger() })).toThrow(
      /already exists/,
    );
  });
});

describe('runServerProcess lifecycle', () => {
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

  test('resends ready when a newer portal generation arrives', async () => {
    const harness = startServer({
      autoHandshake: false,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    harness.portal.send({ type: 'hello', portalGeneration: 1 });
    harness.portal.send({ type: 'activate' });
    await tick();

    harness.portal.send({ type: 'hello', portalGeneration: 2 });
    await tick();

    // The startup barrier plus one per new generation: a restarted Portal was
    // not around for the first one and would wait for readiness forever.
    expect(readies(harness.messages)).toHaveLength(2);
    await harness.stop();
  });

  test('ignores a hello that does not advance the generation', async () => {
    const harness = startServer({
      autoHandshake: false,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    harness.portal.send({ type: 'hello', portalGeneration: 3 });
    harness.portal.send({ type: 'activate' });
    await tick();

    harness.portal.send({ type: 'hello', portalGeneration: 3 });
    await tick();

    expect(readies(harness.messages)).toHaveLength(1);
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
            dispatcher.register(
              'denied',
              () =>
                Promise.reject(
                  new BusinessError('PLAYER_NOT_FOUND', 'no such player'),
                ),
              { access: 'public' },
            );
            dispatcher.register(
              'explode',
              () => Promise.reject(new Error('internal boom')),
              { access: 'public' },
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
            dispatcher.register(
              'stuck',
              async () => {
                await release.promise;
                return 'late';
              },
              { access: 'public' },
            );
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
