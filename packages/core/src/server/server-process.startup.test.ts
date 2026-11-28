import { describe, expect, test } from 'bun:test';

import { tick } from '../testing/common.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import {
  echoRegistration,
  messageTypes,
  received,
  startServer,
  startupFailure,
} from '../testing/server-process.ts';

describe('runServerProcess startup', () => {
  test('sends ready with the backend declaration', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: {
        transactions: 'limited',
        concurrency: 'single',
        snapshotRead: false,
        uniqueConstraint: true,
        maxTransactionDurationMs: 250,
      },
    });
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    const ready = received(harness.messages).filter(
      (message) => message.type === 'ready',
    );
    expect(ready).toHaveLength(1);
    expect(ready[0]).toEqual({
      type: 'ready',
      pid: process.pid,
      capabilities: backend.capabilities(),
    });
    await harness.stop();
  });

  test('sends ready only after register returned', async () => {
    let readiesDuringRegister: number | undefined;
    const harness = startServer({
      extensions: [
        {
          name: 'echo',
          register: (dispatcher) => {
            dispatcher.register(
              'echo',
              (_ctx, payload) => Promise.resolve(payload),
              { access: 'public' },
            );
            readiesDuringRegister = received(harness.messages).filter(
              (message) => message.type === 'ready',
            ).length;
          },
        },
      ],
    });
    await tick();

    expect(readiesDuringRegister).toBe(0);
    // `prepared` precedes the activation; `ready` is the last message.
    expect(received(harness.messages).map((message) => message.type)).toEqual([
      'prepared',
      'ready',
    ]);
    await harness.stop();
  });

  test('fails without ready when maxTransactionDurationMs is NaN', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: Number.NaN },
    });
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });

    const error = await startupFailure(harness.done);

    expect(error.reason).toContain('maxTransactionDurationMs');
    expect(messageTypes(harness.messages)).toEqual(['prepared']);
    const failures = harness
      .logs()
      .filter((line) => line.code === 'SERVER_STARTUP_FAILED');
    expect(failures).toHaveLength(1);
  });

  test('fails without ready when register throws', async () => {
    const harness = startServer({
      extensions: [
        {
          name: 'refusing',
          register: () => {
            throw new Error('extension refused');
          },
        },
      ],
    });

    const error = await startupFailure(harness.done);

    expect(error.reason).toContain('extension refused');
    expect(messageTypes(harness.messages)).toEqual(['prepared']);
  });

  test.each([
    ['zero', 0],
    ['negative', -1],
    ['fractional', 1.5],
    ['NaN', Number.NaN],
    ['Infinity', Number.POSITIVE_INFINITY],
    ['non-number', '2'],
  ] as const)(
    'fails without ready when maxEventsPerRequest is %s',
    async (_name, value) => {
      const harness = startServer({
        extensions: [{ name: 'echo', register: echoRegistration }],
        maxEventsPerRequest: value as number,
      });

      const error = await startupFailure(harness.done);

      expect(error.reason).toContain('maxEventsPerRequest');
      expect(messageTypes(harness.messages)).toEqual(['prepared']);
    },
  );
});
