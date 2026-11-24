import { describe, expect, test } from 'bun:test';

import { parseIpcMessage } from '../ipc/protocol.ts';
import { createJsonLogger } from '../logging/json-logger.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import type { StorageBackend } from '../storage/types.ts';
import { tick } from '../test-support/common.ts';
import {
  echoRegistration,
  numberField,
  parseLine,
  received,
  requestMessage,
  results,
  startServer,
} from '../test-support/server-process.ts';
import { resolveShutdownBudgetMs } from './server-process.ts';

describe('runServerProcess shutdown', () => {
  test('stops accepting, closes the backend, then sends closed', async () => {
    const events: string[] = [];
    const inner = new MemoryStorageBackend();
    const backend: StorageBackend = {
      capabilities: () => inner.capabilities(),
      transaction: (fn) => inner.transaction(fn),
      async close(): Promise<void> {
        events.push('backend.close');
        await inner.close();
      },
    };
    const harness = startServer({
      backend,
      extensions: [{ name: 'echo', register: echoRegistration }],
      onPortalMessage: (raw) => {
        const parsed = parseIpcMessage(raw);
        if (parsed.ok) {
          events.push(`message.${parsed.message.type}`);
        }
      },
    });
    await tick();

    harness.portal.send(requestMessage());
    harness.portal.send({ type: 'shutdown', timeoutMs: 2000 });
    harness.portal.send(requestMessage({ requestId: 'req-late' }));
    await harness.done;

    // The accepted request still reports before `closed`, the late one is
    // refused with a stable code, and the backend is closed before the
    // announcement.
    expect(events).toEqual([
      'message.ready',
      'message.result',
      'message.result',
      'backend.close',
      'message.closed',
    ]);
    const outcomes = results(harness.messages);
    expect(outcomes).toHaveLength(2);
    expect(outcomes.find((outcome) => outcome.requestId === 'req-1')).toEqual({
      type: 'result',
      requestId: 'req-1',
      ok: true,
      result: 'hi',
    });
    expect(
      outcomes.find((outcome) => outcome.requestId === 'req-late'),
    ).toEqual({
      type: 'result',
      requestId: 'req-late',
      ok: false,
      error: { code: 'FRAMEWORK_SHUTDOWN', message: 'server is shutting down' },
    });
    const refused = harness
      .logs()
      .filter((line) => line.code === 'SERVER_SHUTTING_DOWN');
    expect(refused).toHaveLength(1);
    const closed = received(harness.messages).at(-1);
    expect(closed).toEqual({ type: 'closed', reason: 'shutdown' });
  });

  test('answers every request that arrives during shutdown with FRAMEWORK_SHUTDOWN', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    harness.portal.send({ type: 'shutdown', timeoutMs: 2000 });
    harness.portal.send(requestMessage({ requestId: 'req-a' }));
    harness.portal.send(requestMessage({ requestId: 'req-b' }));
    await harness.done;

    const outcomes = results(harness.messages);
    expect(outcomes).toHaveLength(2);
    for (const outcome of outcomes) {
      expect(outcome).toMatchObject({
        ok: false,
        error: { code: 'FRAMEWORK_SHUTDOWN' },
      });
    }
  });

  test('lets an in-flight request finish with its real result during shutdown', async () => {
    // A duplicate of an id already running is owed that execution's result:
    // refusing it would report a failure for work that still succeeds, and the
    // real result would then arrive for an id Portal had already settled.
    const gate = Promise.withResolvers<void>();
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async () => {
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
    harness.portal.send({ type: 'shutdown', timeoutMs: 2000 });
    harness.portal.send(requestMessage({ method: 'slow', replayCount: 1 }));
    await tick();

    gate.resolve();
    await harness.done;

    expect(results(harness.messages)).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'done' },
    ]);
    const duplicates = harness
      .logs()
      .filter((line) => line.code === 'DUPLICATE_REQUEST');
    expect(duplicates).toHaveLength(1);
    expect(
      harness.logs().filter((line) => line.code === 'SERVER_SHUTTING_DOWN'),
    ).toHaveLength(0);
  });

  test('waits for an in-flight request before closing', async () => {
    const gate = Promise.withResolvers<void>();
    const events: string[] = [];
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register('slow', async () => {
              await gate.promise;
              return 'done';
            });
          },
        },
      ],
      onPortalMessage: (raw) => {
        const parsed = parseIpcMessage(raw);
        if (parsed.ok) {
          events.push(`message.${parsed.message.type}`);
        }
      },
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow' }));
    await tick();
    harness.portal.send({ type: 'shutdown', timeoutMs: 2000 });
    await tick();

    // Still draining: the backend must not close under a running handler.
    expect(events).not.toContain('message.closed');

    gate.resolve();
    await harness.done;

    expect(events).toEqual([
      'message.ready',
      'message.result',
      'message.closed',
    ]);
  });

  test('bounds the shutdown close when the backend waits on a stuck handler', async () => {
    // A backend whose `close` awaits its queue (SQLite does) makes
    // `executor.close()` wait for the handler as well, so an unbounded close
    // would keep `closed` from ever being sent when a handler never returns.
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
      timeoutMs: 10,
      extensions: [
        {
          name: 'stuck',
          register: (dispatcher) => {
            dispatcher.register('stuck', async () => {
              await release.promise;
              return 'late';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'stuck' }));
    await tick();
    harness.portal.send({ type: 'shutdown', timeoutMs: 20 });
    // The declared budget bounds the close, so `closed` must arrive while the
    // handler is still holding the backend.
    await harness.done;

    expect(received(harness.messages).at(-1)).toEqual({
      type: 'closed',
      reason: 'shutdown',
    });
    const closeTimeouts = harness
      .logs()
      .filter((line) => line.code === 'STORAGE_CLOSE_TIMEOUT');
    expect(closeTimeouts).toHaveLength(1);
    // The drain settled first (the dispatcher answers at `timeoutMs` even while
    // the handler runs), so the close step got the budget minus the drain time:
    // at most the declared 20ms, never twice it.
    const closeBudget = numberField(closeTimeouts[0], 'timeoutMs');
    expect(closeBudget).toBeGreaterThan(0);
    expect(closeBudget).toBeLessThanOrEqual(20);
    // The dispatcher already answered the request at `timeoutMs` while the
    // handler kept running, so nothing is left in flight to name here.
    expect(closeTimeouts[0]?.pendingRequests).toBe(0);
    release.resolve();
    await tick();
  });

  test('shares one deadline between the drain and the backend close', async () => {
    // The defect this covers: drain and close each got the full budget, so a
    // Server that spent its whole declared budget draining then got a second,
    // fresh budget for the close — exactly the overrun that made the Portal kill
    // a Server closing correctly. Here the drain consumes the whole budget, so
    // the close must see only the remainder.
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
      // The request ceiling is far above the declared budget, so the drain
      // cannot settle by the dispatcher's timeout and runs out the budget.
      timeoutMs: 5000,
      extensions: [
        {
          name: 'stuck',
          register: (dispatcher) => {
            dispatcher.register('stuck', async () => {
              await release.promise;
              return 'late';
            });
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'stuck' }));
    await tick();
    const startedAt = performance.now();
    harness.portal.send({ type: 'shutdown', timeoutMs: 80 });
    await harness.done;
    const elapsedMs = performance.now() - startedAt;

    // A per-step budget would spend 80ms twice (160ms). One shared deadline ends
    // near 80ms, which is what the Portal is actually waiting for.
    expect(elapsedMs).toBeLessThan(130);
    const drainTimeouts = harness
      .logs()
      .filter((line) => line.code === 'SHUTDOWN_DRAIN_TIMEOUT');
    expect(drainTimeouts).toHaveLength(1);
    const closeTimeouts = harness
      .logs()
      .filter((line) => line.code === 'STORAGE_CLOSE_TIMEOUT');
    expect(closeTimeouts).toHaveLength(1);
    // The drain consumed the budget, so the close step is left with less than
    // the declared 80ms — not a fresh 80ms of its own.
    const closeBudget = numberField(closeTimeouts[0], 'timeoutMs');
    expect(closeBudget).toBeLessThan(80);
    expect(closeBudget).toBeGreaterThan(0);
    release.resolve();
    await tick();
  });
});

describe('resolveShutdownBudgetMs', () => {
  test('uses the budget the Portal declared', () => {
    const lines: string[] = [];
    const logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });

    expect(resolveShutdownBudgetMs(1500, 5000, logger)).toBe(1500);
    // No degradation, so nothing to report.
    expect(lines).toEqual([]);
  });

  test('falls back to 4 * timeoutMs and warns when no budget was declared', () => {
    // The wire parser requires the field, so this is the legacy-peer path: it
    // still has to close, but the larger, unrelated budget must be visible.
    const lines: string[] = [];
    const logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });

    expect(resolveShutdownBudgetMs(undefined, 5000, logger)).toBe(20_000);
    const warnings = lines.map(parseLine);
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatchObject({
      level: 'warn',
      code: 'SHUTDOWN_BUDGET_UNDECLARED',
      timeoutMs: 20_000,
    });
  });
});
