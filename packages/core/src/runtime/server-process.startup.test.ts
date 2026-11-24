import { describe, expect, test } from 'bun:test';

import { MemoryStorageBackend } from '../storage/memory.ts';
import { tick } from '../test-support/common.ts';
import type { Harness } from '../test-support/server-process.ts';
import {
  brokenBackend,
  echoRegistration,
  received,
  startServer,
  startupFailure,
} from '../test-support/server-process.ts';
import type { ServerStartupError } from './server-startup.ts';

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
    let readyCountDuringRegister: number | undefined;
    const harness = startServer({
      extensions: [
        {
          name: 'echo',
          register: (dispatcher) => {
            dispatcher.register('echo', (_ctx, payload) =>
              Promise.resolve(payload),
            );
            readyCountDuringRegister = harness.messages.length;
          },
        },
      ],
    });
    await tick();

    expect(readyCountDuringRegister).toBe(0);
    expect(received(harness.messages)).toHaveLength(1);
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
    expect(received(harness.messages)).toEqual([]);
    const failures = harness
      .logs()
      .filter((line) => line.code === 'SERVER_STARTUP_FAILED');
    expect(failures).toHaveLength(1);
  });

  test('fails without ready when maxTransactionDurationMs is zero', async () => {
    // The protocol refuses a non-positive ceiling (ipc/protocol.ts), so passing
    // the self-check here would send a `ready` Portal rejects: Portal would
    // never reach its barrier and every request would end at its deadline.
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: 0 },
    });
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });

    const error = await startupFailure(harness.done);

    expect(error.reason).toContain('maxTransactionDurationMs');
    expect(received(harness.messages)).toEqual([]);
    const failures = harness
      .logs()
      .filter((line) => line.code === 'SERVER_STARTUP_FAILED');
    expect(failures).toHaveLength(1);
  });

  test('fails without ready when maxTransactionDurationMs is negative', async () => {
    const values = [-1, -0.5, Number.NEGATIVE_INFINITY];
    const harnesses: Harness[] = [];
    const pending: Promise<ServerStartupError>[] = [];
    for (const value of values) {
      const backend = new MemoryStorageBackend({
        capabilities: { maxTransactionDurationMs: value },
      });
      const harness = startServer({
        backend,
        extensions: [{ name: 'echo', register: echoRegistration }],
      });
      harnesses.push(harness);
      pending.push(startupFailure(harness.done));
    }

    const errors = await Promise.all(pending);

    for (const error of errors) {
      expect(error.reason).toContain('maxTransactionDurationMs');
    }
    for (const harness of harnesses) {
      expect(received(harness.messages)).toEqual([]);
    }
  });

  test('accepts an infinite maxTransactionDurationMs as unbounded', async () => {
    const backend = new MemoryStorageBackend({
      capabilities: { maxTransactionDurationMs: Number.POSITIVE_INFINITY },
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
    await harness.stop();
  });

  test('fails without ready when a capability value is outside its enum', async () => {
    const backend = brokenBackend({
      transactions: 'partial',
      concurrency: 'pooled',
      snapshotRead: true,
      uniqueConstraint: false,
      maxTransactionDurationMs: 1000,
    });
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });

    const error = await startupFailure(harness.done);

    expect(error.reason).toContain('transactions');
    expect(received(harness.messages)).toEqual([]);
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
    expect(received(harness.messages)).toEqual([]);
  });
});
