import { describe, expect, test } from 'bun:test';

import { withDeadline } from 'mud-framework-test-support';

import { createInMemoryChannelPair } from '../ipc/channel.ts';
import type { IpcChannel } from '../ipc/channel.ts';
import { parseIpcMessage } from '../ipc/protocol.ts';
import type { ServerExitInfo, SpawnedServer } from '../ipc/spawn-port.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { IpcExecutor } from '../portal/executor/ipc-executor.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf, tick } from '../testing/common.ts';
import {
  parseLine,
  received,
  requestMessage,
  results,
  startupFailure,
} from '../testing/server-process.ts';
import type { Extension } from './extension.ts';
import {
  runServerProcess,
  ServerChannelClosedError,
} from './server-process.ts';
import { ServerStartupError } from './server-startup.ts';

interface ActivationHarnessOptions {
  extensions?: readonly Extension[];
  codeVersion?: string;
  /** Gates backend creation so a test can hold the Server in `starting`. */
  createBackend?: () => Promise<MemoryStorageBackend>;
  /** Enables the parent-liveness poll; off by default (no real parent here). */
  parentCheckIntervalMs?: number;
  /** Lets a test pretend the process was reparented. */
  readParentPid?: () => number;
  /** Wraps the Server endpoint to break one part of the link. */
  wrapChannel?: (channel: IpcChannel) => IpcChannel;
}

function activationHarness(options: ActivationHarnessOptions = {}) {
  const [portal, channel] = createInMemoryChannelPair();
  const messages: unknown[] = [];
  portal.onMessage((raw) => {
    messages.push(raw);
  });
  const lines: string[] = [];
  const logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  let created = 0;
  const done = runServerProcess({
    createBackend: async () => {
      created += 1;
      return options.createBackend?.() ?? new MemoryStorageBackend();
    },
    extensions: options.extensions ?? [],
    timeoutMs: 1000,
    codeVersion: options.codeVersion,
    logger,
    channel: options.wrapChannel?.(channel) ?? channel,
    parentCheckIntervalMs: options.parentCheckIntervalMs ?? 0,
    readParentPid: options.readParentPid,
  });
  return {
    portal,
    messages,
    logs: () => lines.map(parseLine),
    done,
    created: () => created,
    async stop(): Promise<void> {
      portal.close('test finished');
      await done.catch(() => undefined);
    },
  };
}

function hello(h: ReturnType<typeof activationHarness>, generation = 1): void {
  h.portal.send({ type: 'hello', portalGeneration: generation });
}

function activate(h: ReturnType<typeof activationHarness>): void {
  h.portal.send({ type: 'activate' });
}

function types(h: ReturnType<typeof activationHarness>): string[] {
  return received(h.messages).map((message) => message.type);
}

describe('runServerProcess two-phase activation', () => {
  test('answers hello with prepared without creating the backend', async () => {
    const registered: string[] = [];
    const h = activationHarness({
      codeVersion: 'build-1',
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
      { type: 'prepared', pid: process.pid, codeVersion: 'build-1' },
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

  test('logs a pre-ready cancel at most once per portal generation', async () => {
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
    // it shares the one-per-generation flag instead of adding a line per
    // message (SPEC 7.1).
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

  test('does not resend ready for the initial hello, but does for a newer generation', async () => {
    const h = activationHarness();

    hello(h);
    expect(types(h)).toEqual(['prepared']);
    activate(h);
    await tick();
    expect(types(h)).toEqual(['prepared', 'ready']);

    hello(h, 2);
    await tick();
    expect(types(h)).toEqual(['prepared', 'ready', 'ready']);

    // Same generation again: stale, and the resend must not repeat.
    hello(h, 2);
    await tick();
    expect(types(h)).toEqual(['prepared', 'ready', 'ready']);
    expect(h.logs().filter((line) => line.code === 'STALE_HELLO')).toHaveLength(
      1,
    );
    expect(
      h.logs().filter((line) => line.code === 'PORTAL_GENERATION_CHANGED'),
    ).toHaveLength(1);

    h.portal.send({ type: 'shutdown', timeoutMs: 1000 });
    await h.done;
  });

  test('fails without opening the backend when the link closes during prepare', async () => {
    const h = activationHarness();

    hello(h);
    h.portal.close('portal gone');

    const error = await rejectionOf(h.done);
    if (!(error instanceof ServerChannelClosedError)) {
      throw new Error(`expected ServerChannelClosedError, got ${error.name}`);
    }
    expect(error.reason).toBe('portal gone');
    expect(h.created()).toBe(0);
  });

  test('ignores a request that arrives before hello', async () => {
    const h = activationHarness();

    h.portal.send(requestMessage());
    await tick();

    expect(results(h.messages)).toEqual([]);
    expect(
      h.logs().filter((line) => line.code === 'REQUEST_BEFORE_READY'),
    ).toHaveLength(1);
    expect(h.created()).toBe(0);
    await h.stop();
  });

  test('reports a startup failure after activate as ServerStartupError', async () => {
    const h = activationHarness({
      createBackend: () => Promise.reject(new Error('disk unavailable')),
    });

    hello(h);
    activate(h);

    const error = await startupFailure(h.done);
    expect(error).toBeInstanceOf(ServerStartupError);
    expect(error.message).toContain('disk unavailable');
    // The Portal saw the Server was prepared, but never ready.
    expect(types(h)).toEqual(['prepared']);
  });

  test('fails the startup when the ready barrier cannot be sent', async () => {
    // A send that throws while the link stays open: the old code logged
    // `IPC_SEND_FAILED`, marked the process active and parked forever, leaving
    // the backend open until Portal's start timeout killed it.
    const backend = new MemoryStorageBackend();
    const h = activationHarness({
      createBackend: () => Promise.resolve(backend),
      wrapChannel: (channel) => ({
        ...channel,
        send: (message) => {
          if (message.type === 'ready') {
            throw new Error('wire refused ready');
          }
          channel.send(message);
        },
      }),
    });

    hello(h);
    activate(h);

    const error = await withDeadline(
      startupFailure(h.done),
      1000,
      'runServerProcess did not fail after the ready send threw',
    );
    expect(error).toBeInstanceOf(ServerStartupError);
    expect(error.reason).toContain('ready could not be sent');
    expect(error.reason).toContain('wire refused ready');
    // Never active: Portal saw the barrier it was waiting for fail, so no
    // request may be accepted and no second `ready` may appear.
    expect(types(h)).toEqual(['prepared']);
    expect(
      h.logs().filter((line) => line.msg === 'server process ready'),
    ).toEqual([]);
    // One failure line, not one per step: the same contract `startServer`
    // failures follow.
    expect(
      h.logs().filter((line) => line.code === 'SERVER_STARTUP_FAILED'),
    ).toHaveLength(1);
    // The storage this start opened must not outlive it (INV-002).
    expect(() => backend.transaction(() => Promise.resolve())).toThrow(
      /storage backend is closed/,
    );
    // The process is gone: the link was closed on the failure path.
    expect(() => h.portal.send({ type: 'hello', portalGeneration: 2 })).toThrow(
      /closed/,
    );
  });

  test('fails the startup when the prepared barrier cannot be sent', async () => {
    const h = activationHarness({
      wrapChannel: (channel) => ({
        ...channel,
        send: (message) => {
          if (message.type === 'prepared') {
            throw new Error('wire refused prepared');
          }
          channel.send(message);
        },
      }),
    });

    hello(h);

    const error = await withDeadline(
      startupFailure(h.done),
      1000,
      'runServerProcess did not fail after the prepared send threw',
    );
    expect(error).toBeInstanceOf(ServerStartupError);
    expect(error.reason).toContain('prepared could not be sent');
    // ADR-0007: prepare never opened storage, so there is nothing to close and
    // nothing was created.
    expect(h.created()).toBe(0);
    expect(types(h)).toEqual([]);
  });

  test('serves a second portal generation that arrives during prepare', async () => {
    const h = activationHarness();

    hello(h);
    expect(types(h)).toEqual(['prepared']);
    hello(h, 2);

    // The second Portal was not around for the first `prepared`; without a
    // second one it would wait for a barrier it already missed.
    expect(types(h)).toEqual(['prepared', 'prepared']);
    activate(h);
    await tick();
    expect(types(h)).toEqual(['prepared', 'prepared', 'ready']);
    await h.stop();
  });

  test('closes storage and fails when the link dies while the start is running', async () => {
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
    h.portal.close('portal gone');
    gate.resolve();

    const error = await rejectionOf(h.done);
    expect(error).toBeInstanceOf(ServerChannelClosedError);
    expect(error.message).toContain('portal gone');
    // The storage opened during the start must not outlive it (INV-002).
    expect(() => backend.transaction(() => Promise.resolve())).toThrow(
      /storage backend is closed/,
    );
  });

  test('rejects an invalid code version before answering hello', async () => {
    const h = activationHarness({ codeVersion: '' });

    hello(h);

    const error = await startupFailure(h.done);
    expect(error.reason).toContain('codeVersion');
    // Nothing was put on the wire: `prepared` would carry the same value.
    expect(h.messages).toEqual([]);
    expect(h.created()).toBe(0);
  });
});

describe('activation handshake over a caller-driven channel', () => {
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

/**
 * A Server handle for `channel` that reports its exit when the Server sends
 * `closed`. The Portal side of an in-memory pair delivers synchronously, which
 * is what makes "deliver the instant `ready` arrives" reproducible here.
 */
function linkedServer(channel: IpcChannel): SpawnedServer {
  const exit = Promise.withResolvers<ServerExitInfo>();
  let info: ServerExitInfo | undefined;
  const record = (value: ServerExitInfo): void => {
    if (info !== undefined) {
      return;
    }
    info = value;
    exit.resolve(value);
  };
  return {
    channel: {
      send: (message) => channel.send(message),
      onMessage: (handler) =>
        channel.onMessage((raw) => {
          const parsed = parseIpcMessage(raw);
          if (parsed.ok && parsed.message.type === 'closed') {
            record({ code: 0, signal: null, observed: true });
          }
          handler(raw);
        }),
      onClose: (handler) => channel.onClose(handler),
      close: (reason) => channel.close(reason),
    },
    exited: exit.promise,
    lastExit: () => info,
    kill: () => record({ code: null, signal: 'SIGKILL', observed: true }),
  };
}

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
    const done = runServerProcess({
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

describe('parent liveness before ready', () => {
  test('rejects within a bounded time when the parent disappears while prepared', async () => {
    let parentPid = 1000;
    const h = activationHarness({
      parentCheckIntervalMs: 5,
      readParentPid: () => parentPid,
    });

    hello(h);
    expect(h.created()).toBe(0);

    parentPid = 2000;
    const error = await withDeadline(
      rejectionOf(h.done),
      1000,
      'runServerProcess did not return after the parent disappeared',
    );

    if (!(error instanceof ServerChannelClosedError)) {
      throw new Error(`expected ServerChannelClosedError, got ${error.name}`);
    }
    expect(error.message).toContain('parent process is gone');
    // ADR-0007: prepare holds no storage, so a parent that vanished cannot
    // leave a writer behind — and the process must not keep running either.
    expect(h.created()).toBe(0);
    const logged = h.logs().filter((line) => line.code === 'IPC_PARENT_GONE');
    expect(logged).toHaveLength(1);
  });

  test('closes the backend it just opened when the parent disappears during the start', async () => {
    let parentPid = 1000;
    const gate = Promise.withResolvers<void>();
    const backend = new MemoryStorageBackend();
    const h = activationHarness({
      createBackend: async () => {
        await gate.promise;
        return backend;
      },
      parentCheckIntervalMs: 5,
      readParentPid: () => parentPid,
    });

    hello(h);
    activate(h);
    await tick();
    parentPid = 2000;
    // Let the poll fire while the backend is still opening.
    await withDeadline(
      (async () => {
        while (h.logs().every((line) => line.code !== 'IPC_PARENT_GONE')) {
          await tick();
        }
      })(),
      1000,
      'the parent-gone poll did not fire',
    );
    gate.resolve();

    const error = await withDeadline(
      rejectionOf(h.done),
      2000,
      'runServerProcess did not return after the parent disappeared',
    );
    expect(error).toBeInstanceOf(ServerChannelClosedError);
    // No `ready`: there is no Portal left to announce it to.
    expect(types(h)).toEqual(['prepared']);
    expect(() => backend.transaction(() => Promise.resolve())).toThrow(
      /storage backend is closed/,
    );
  });
});
