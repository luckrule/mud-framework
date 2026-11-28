import { describe, expect, test } from 'bun:test';

import type { IpcChannel } from '../ipc/channel.ts';
import { createInMemoryChannelPair } from '../ipc/channel.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { rejectionOf } from '../testing/common.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import {
  parseLine,
  received,
  requestMessage,
} from '../testing/server-process.ts';
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
    }),
  };
}

function echoExtension(name = 'echo'): Extension {
  return {
    name,
    register: (registrar) => {
      registrar.register('echo', (_ctx, payload) => Promise.resolve(payload), {
        access: 'public',
      });
    },
  };
}

describe('startServer', () => {
  test('returns the runtime and sends nothing', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { concurrency: 'pooled' },
    });
    const test1 = harness([echoExtension()]);
    const result: StartupResult = await startServer(
      test1.deps({ createBackend: () => backend }),
    );

    // The readiness barrier belongs to the caller: sending it here would cross
    // it before a request handler exists, and Portal is entitled to deliver the
    // instant it sees `ready`.
    expect(received(test1.messages)).toEqual([]);
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
          registrar.register(
            'deposit',
            async (ctx) => {
              ctx.logger.info({ detail: 'seen' }, 'handler ran');
              return ctx.extension;
            },
            { access: 'public' },
          );
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
    // The readiness barrier and the handler are the caller's, in that order. A
    // handler installed here would deliver requests before the caller can say
    // the process is active.
    const test1 = harness([echoExtension()]);
    const { runtime } = await startServer(test1.deps());

    test1.portal.send(requestMessage());
    await Promise.resolve();

    expect(received(test1.messages)).toEqual([]);

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
});
