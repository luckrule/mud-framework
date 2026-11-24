import { describe, expect, test } from 'bun:test';

import type { IpcChannel } from '../ipc/channel.ts';
import { createInMemoryChannelPair } from '../ipc/channel.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { rejectionOf } from '../test-support/common.ts';
import {
  parseLine,
  received,
  requestMessage,
} from '../test-support/server-process.ts';
import type { Extension } from './extension.ts';
import type { ServerProcessOptions } from './server-process.ts';
import {
  type StartupDeps,
  type StartupResult,
  ServerStartupError,
  startServer,
} from './server-startup.ts';

interface Harness {
  /** The Portal end: observes what startup sent. */
  portal: IpcChannel;
  /** The Server end handed to `startServer`. */
  channel: IpcChannel;
  messages: unknown[];
  logs: () => Record<string, unknown>[];
  deps: (overrides?: Partial<ServerProcessOptions>) => StartupDeps;
}

function harness(extensions: readonly Extension[] = []): Harness {
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
  return {
    portal,
    channel,
    messages,
    logs: () => lines.map(parseLine),
    deps: (overrides = {}) => ({
      options: {
        createBackend: () => new MemoryStorageBackend(),
        extensions,
        timeoutMs: 1000,
        logger,
        channel,
        ...overrides,
      },
      logger,
      channel,
    }),
  };
}

function echoExtension(name = 'echo'): Extension {
  return {
    name,
    register: (registrar) => {
      registrar.register('echo', (_ctx, payload) => Promise.resolve(payload));
    },
  };
}

describe('startServer', () => {
  test('returns the runtime and sends ready as the only outbound message', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { concurrency: 'pooled' },
    });
    const test1 = harness([echoExtension()]);
    const result: StartupResult = await startServer(
      test1.deps({ createBackend: () => backend }),
    );

    expect(received(test1.messages)).toEqual([
      {
        type: 'ready',
        pid: process.pid,
        capabilities: backend.capabilities(),
      },
    ]);
    expect(result.runtime.capabilities).toEqual(backend.capabilities());
    expect(result.skippedExtensions).toBe(0);
    // The extension registered against the returned dispatcher.
    const outcome = await result.runtime.executor.execute({
      requestId: 'req-1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      method: 'echo',
      payload: 'hi',
      timeoutMs: 1000,
    });
    expect(outcome).toEqual({ ok: true, result: 'hi' });

    await result.runtime.executor.close();
  });

  test('tags an Extension handler with the Extension name in context and logs', async () => {
    const test1 = harness([
      {
        name: 'banking',
        register: (registrar) => {
          registrar.register('deposit', async (ctx) => {
            ctx.logger.info({ detail: 'seen' }, 'handler ran');
            return ctx.extension;
          });
        },
      },
    ]);

    const { runtime } = await startServer(test1.deps());
    const outcome = await runtime.executor.execute({
      requestId: 'req-1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      method: 'deposit',
      payload: null,
      timeoutMs: 1000,
    });

    expect(outcome).toEqual({ ok: true, result: 'banking' });
    const handlerLines = test1
      .logs()
      .filter((line) => line.msg === 'handler ran');
    expect(handlerLines).toHaveLength(1);
    expect(handlerLines[0]).toMatchObject({
      extension: 'banking',
      requestId: 'req-1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
    });

    await runtime.executor.close();
  });

  test('leaves inbound handlers unattached so messages stay buffered', async () => {
    // The readiness barrier is `ready`; the caller attaches `onMessage` after
    // `startServer` returns, and the channel buffers anything that arrived in
    // between. A handler installed here would deliver requests before `ready`.
    const test1 = harness([echoExtension()]);
    const { runtime } = await startServer(test1.deps());

    test1.portal.send(requestMessage());
    await Promise.resolve();

    expect(received(test1.messages)).toHaveLength(1);
    expect(received(test1.messages)[0]?.type).toBe('ready');

    await runtime.executor.close();
  });

  test('throws ServerStartupError, sends no ready and closes the backend when the self-check fails', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: Number.NaN },
    });
    const test1 = harness([echoExtension()]);

    const error = await rejectionOf(
      startServer(test1.deps({ createBackend: () => backend })),
    );

    if (!(error instanceof ServerStartupError)) {
      throw new Error(`expected ServerStartupError, got ${error.name}`);
    }
    expect(error.reason).toContain('maxTransactionDurationMs');
    expect(received(test1.messages)).toEqual([]);
    // The backend was created, so it must not stay open (SPEC 6.2).
    expect(() => backend.capabilities()).toThrow(/closed/);
    expect(
      test1.logs().filter((line) => line.code === 'SERVER_STARTUP_FAILED'),
    ).toHaveLength(1);
  });

  test('throws ServerStartupError when the backend cannot be created', async () => {
    const test1 = harness();

    const error = await rejectionOf(
      startServer(
        test1.deps({
          createBackend: () => {
            throw new Error('disk unavailable');
          },
        }),
      ),
    );

    expect(error).toBeInstanceOf(ServerStartupError);
    expect(error.message).toContain('disk unavailable');
    expect(received(test1.messages)).toEqual([]);
  });

  test('reports every skipped extension but still sends ready', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'none', concurrency: 'single' },
    });
    const test1 = harness([
      {
        name: 'banking',
        requires: { transactions: 'full' },
        register: () => undefined,
      },
      {
        name: 'reporting',
        requires: { concurrency: 'pooled' },
        register: () => undefined,
      },
    ]);

    const result = await startServer(
      test1.deps({ createBackend: () => backend }),
    );

    expect(result.skippedExtensions).toBe(2);
    expect(received(test1.messages).map((message) => message.type)).toEqual([
      'ready',
    ]);
    const warnings = test1
      .logs()
      .filter(
        (line) =>
          line.msg === 'no extension registered: every extension was skipped',
      );
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatchObject({ total: 2, skipped: 2 });

    await result.runtime.executor.close();
  });

  test('a required extension with unmet requirements throws without ready', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { transactions: 'limited' },
    });
    const test1 = harness([
      {
        name: 'banking',
        requires: { transactions: 'full' },
        required: true,
        register: () => undefined,
      },
    ]);

    const error = await rejectionOf(
      startServer(test1.deps({ createBackend: () => backend })),
    );

    expect(error).toBeInstanceOf(ServerStartupError);
    expect(received(test1.messages)).toEqual([]);
    expect(() => backend.capabilities()).toThrow(/closed/);
  });

  test('warns once per degraded capability at startup', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: {
        transactions: 'full',
        concurrency: 'single',
        snapshotRead: true,
        uniqueConstraint: true,
      },
    });
    const test1 = harness();

    const { runtime } = await startServer(
      test1.deps({ createBackend: () => backend }),
    );

    const warnings = test1
      .logs()
      .filter((line) => line.msg === 'storage capability degraded');
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatchObject({
      level: 'warn',
      capability: 'concurrency',
      declared: 'single',
    });

    await runtime.executor.close();
  });
});
