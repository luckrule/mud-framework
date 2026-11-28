import { describe, expect, test } from 'bun:test';

import { createInMemoryChannelPair } from '../ipc/channel.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { IpcExecutor } from '../portal/executor/ipc-executor.ts';
import { tick } from '../testing/common.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { requestMessage, results } from '../testing/server-process.ts';
import {
  activate,
  activationHarness,
  hello,
  linkedServer,
  types,
} from './server-process.activation.support.ts';
import { runServerProcessWithTestOptions } from './server-process.ts';

describe('runServerProcess two-phase activation', () => {
  test('ignores requests and cancels while preparing, and logs the fault once', async () => {
    let calls = 0;
    const h = activationHarness({
      extensions: [
        {
          name: 'echo',
          register: (registrar) => {
            registrar.register(
              'echo',
              (_ctx, payload) => {
                calls += 1;
                return Promise.resolve(payload);
              },
              { access: 'public' },
            );
          },
        },
      ],
    });

    hello(h);
    h.portal.send(requestMessage({ requestId: 'req-a' }));
    h.portal.send(requestMessage({ requestId: 'req-b' }));
    h.portal.send({
      type: 'cancel',
      requestId: 'req-a',
      code: 'FRAMEWORK_TIMEOUT',
    });
    await tick();

    expect(calls).toBe(0);
    expect(results(h.messages)).toEqual([]);
    expect(h.created()).toBe(0);
    // One flag for both, so a pre-ready request and a pre-ready cancel cannot
    // each produce a line: the volume must not track the input rate (SPEC 7.1).
    const logs = h.logs();
    expect(
      logs.filter(
        (line) =>
          line.code === 'REQUEST_BEFORE_READY' ||
          line.code === 'CANCEL_BEFORE_READY',
      ),
    ).toHaveLength(1);

    // Activation still works on its own; the ignored input did not poison it.
    activate(h);
    await tick();
    h.portal.send(requestMessage({ requestId: 'req-c' }));
    await tick();
    expect(calls).toBe(1);
    expect(results(h.messages)).toHaveLength(1);
    await h.stop();
  });

  test('logs a pre-ready cancel at most once', async () => {
    const h = activationHarness();

    for (let index = 0; index < 5; index += 1) {
      h.portal.send({
        type: 'cancel',
        requestId: `req-${index}`,
        code: 'FRAMEWORK_TIMEOUT',
      });
    }
    await tick();

    // A cancel before the barrier is the same fault as a request before it, so
    // it shares the one flag instead of adding a line per message (SPEC 7.1).
    const cancels = h
      .logs()
      .filter((line) => line.code === 'CANCEL_BEFORE_READY');
    expect(cancels).toHaveLength(1);
    expect(cancels[0]).toMatchObject({
      level: 'warn',
      requestId: 'req-0',
      cancelCode: 'FRAMEWORK_TIMEOUT',
    });
    await h.stop();
  });

  test('ignores a request that arrives while the activation is still starting', async () => {
    const gate = Promise.withResolvers<void>();
    const h = activationHarness({
      createBackend: async () => {
        await gate.promise;
        return new MemoryStorageBackend();
      },
    });

    hello(h);
    activate(h);
    h.portal.send(requestMessage());
    await tick();
    expect(results(h.messages)).toEqual([]);
    expect(
      h.logs().filter((line) => line.code === 'REQUEST_BEFORE_READY'),
    ).toHaveLength(1);

    gate.resolve();
    await tick();
    expect(types(h)).toEqual(['prepared', 'ready']);
    await h.stop();
  });

  test('ignores a second hello after the handshake started', async () => {
    const h = activationHarness();

    hello(h);
    expect(types(h)).toEqual(['prepared']);
    activate(h);
    await tick();
    expect(types(h)).toEqual(['prepared', 'ready']);

    // Every launch opens a new channel, so the handshake arrives exactly once.
    // A second hello is out of protocol: warn and ignore, never resend a
    // barrier or reopen the prepare state.
    hello(h);
    await tick();
    expect(types(h)).toEqual(['prepared', 'ready']);
    const ignored = h
      .logs()
      .filter((line) => line.code === 'UNEXPECTED_IPC_MESSAGE');
    expect(ignored).toHaveLength(1);
    expect(ignored[0]).toMatchObject({
      level: 'warn',
      msg: 'ipc message ignored',
      reason: 'server received "hello" while active',
    });

    h.portal.send({ type: 'shutdown', timeoutMs: 1000 });
    await h.done;
  });

  test('the pre-activation handler ignores server-to-portal message types', async () => {
    const h = activationHarness();
    // Valid shape, wrong direction: the pre-activation handler must not treat
    // it as `closed` or `ready`.
    h.portal.send({ type: 'closed', reason: 'not from a server' });
    await tick();

    const unexpected = h
      .logs()
      .filter((line) => line.code === 'UNEXPECTED_IPC_MESSAGE');
    expect(unexpected).toHaveLength(1);
    await h.stop();
  });

  test('ignores an activate that arrives before hello', async () => {
    const h = activationHarness();

    activate(h);
    await tick();

    expect(h.created()).toBe(0);
    const ignored = h.logs().filter((line) => line.code === 'ACTIVATE_IGNORED');
    expect(ignored).toHaveLength(1);
    expect(ignored[0]).toMatchObject({
      level: 'warn',
      reason: 'activate arrived while awaiting-hello',
    });
    await h.stop();
  });

  test('ignores a prepared that arrives after ready', async () => {
    const h = activationHarness();

    hello(h);
    activate(h);
    await tick();
    expect(types(h)).toEqual(['prepared', 'ready']);

    // A Portal never sends `prepared`; the pre- and post-activation handlers
    // must both treat it as out of protocol rather than as a new barrier.
    h.portal.send({ type: 'prepared', pid: 1 });
    await tick();

    expect(types(h)).toEqual(['prepared', 'ready']);
    expect(
      h.logs().filter((line) => line.code === 'UNEXPECTED_IPC_MESSAGE'),
    ).toHaveLength(1);
    await h.stop();
  });
});

describe('readiness barrier ordering', () => {
  test('executes a request buffered before ready that is flushed on ready', async () => {
    // The bug this covers: `ready` was sent before the request handler was
    // installed. Portal flushes its buffer synchronously when it sees `ready`
    // (SPEC 6.2 entitles it to deliver immediately), so the request arrived
    // while the pre-activation handler was still installed, was ignored, and
    // ended as FRAMEWORK_TIMEOUT with no replay. Over the in-memory channel the
    // flush lands inside the `ready` delivery, so this reproduces every run.
    const [portal, serverChannel] = createInMemoryChannelPair();
    let backend: MemoryStorageBackend | undefined;
    const done = runServerProcessWithTestOptions({
      createBackend: () => {
        backend = new MemoryStorageBackend();
        return backend;
      },
      extensions: [
        {
          name: 'echo',
          register: (registrar) => {
            registrar.register(
              'echo',
              (_ctx, payload) => Promise.resolve(payload),
              { access: 'public' },
            );
          },
        },
      ],
      timeoutMs: 200,
      logger: createJsonLogger({ sink: () => undefined }),
      channel: serverChannel,
      parentCheckIntervalMs: 0,
    });
    const executor = new IpcExecutor({
      spawn: () => linkedServer(portal),
      timeoutMs: 200,
      logger: createJsonLogger({ sink: () => undefined }),
    });

    // Buffered before the Server is ready, so Portal can only deliver it on
    // `ready` — the exact ordering the barrier is supposed to make safe.
    const startPromise = executor.start();
    const outcomePromise = executor.execute({
      requestId: 'req-after-ready',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      method: 'echo',
      payload: 'ping',
    });
    await startPromise;
    const outcome = await outcomePromise;

    expect(outcome).toEqual({ ok: true, result: 'ping' });
    await executor.close();
    await done.catch(() => undefined);
    expect(backend).toBeDefined();
  });
});
