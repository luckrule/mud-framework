import { describe, expect, jest, test } from 'bun:test';

import {
  failureOf,
  flushMicrotasks,
  flushTicks,
  harness,
  messagesOf,
  readyMessage,
  request,
  requestIds,
  serverAt,
  started,
} from '../test-support/ipc-executor.ts';

describe('IpcExecutor close', () => {
  test('close sends shutdown and waits for closed', async () => {
    const h = harness();
    const server = await started(h);

    await h.executor.close();

    expect(messagesOf(server)).toContainEqual({
      type: 'shutdown',
      timeoutMs: 5000,
    });
    expect(server.killCount).toBe(0);
  });

  test('kills the server when closed never arrives', async () => {
    // Fake timers so the 5s shutdown grace period costs no wall-clock time.
    jest.useFakeTimers();
    try {
      const h = harness({ timeoutMs: 5000, autoClose: false });
      const server = await started(h);

      const closing = h.executor.close();
      await flushMicrotasks();
      expect(messagesOf(server)).toContainEqual({
        type: 'shutdown',
        timeoutMs: 5000,
      });
      expect(server.killCount).toBe(0);

      jest.advanceTimersByTime(5000 + 250);
      await flushMicrotasks();
      await closing;

      expect(server.killCount).toBe(1);
    } finally {
      jest.useRealTimers();
    }
  });

  test('honours the shutdown timeout option', async () => {
    const h = harness({
      timeoutMs: 5000,
      autoClose: false,
      shutdownTimeoutMs: 20,
    });
    const server = await started(h);
    const before = Date.now();

    await h.executor.close();

    expect(server.killCount).toBe(1);
    expect(Date.now() - before).toBeLessThan(1000);
  });

  test('rejects requests after close instead of hanging', async () => {
    const h = harness();
    await started(h);
    await h.executor.close();

    const outcome = await h.executor.execute(request({ requestId: 'late' }));

    expect(failureOf(outcome).code).toBe('FRAMEWORK_SHUTDOWN');
    // Idempotent: a second close resolves rather than throwing.
    await h.executor.close();
  });

  test('close fails a request that never reached a server', async () => {
    const h = harness({ timeoutMs: 5000 });
    // Started but never ready: the request is buffered, not delivered.
    void h.executor.start().catch(() => undefined);
    await flushTicks();

    const outcomePromise = h.executor.execute(request({ requestId: 'queued' }));
    await h.executor.close();

    expect(failureOf(await outcomePromise).code).toBe('FRAMEWORK_SHUTDOWN');
    expect(requestIds(serverAt(h, 0))).toEqual([]);
  });

  test('does not restart while closing when the server dies', async () => {
    const h = harness({ timeoutMs: 5000, autoClose: false });
    const server = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    const closing = h.executor.close();
    await flushMicrotasks();

    // The crash lands mid-shutdown: no restart, and the request settles.
    server.crash();
    await flushMicrotasks();
    await closing;

    expect(h.servers).toHaveLength(1);
    expect(failureOf(await outcomePromise).code).toBe('FRAMEWORK_SHUTDOWN');
    // Already gone: killing a corpse would be a no-op at best, and the point
    // is that the shutdown path noticed the exit instead of waiting 5s.
    expect(server.killCount).toBe(0);
  });

  test('close waits for the process exit after the server acknowledges', async () => {
    // `closed` without an exit: the acknowledgement must not be treated as
    // "the process is gone", or the storage would be reopened too early.
    const h = harness({ timeoutMs: 5000, exitOnShutdown: false });
    const startPromise = h.executor.start();
    const server = serverAt(h, 0);
    server.send(readyMessage());
    await startPromise;

    let closed = false;
    const closing = h.executor.close().then(() => {
      closed = true;
      return undefined;
    });
    await flushTicks();

    expect(closed).toBe(false);
    server.crash(0);
    await closing;
    expect(closed).toBe(true);
  });

  test('kills a server that acknowledges closed but never exits', async () => {
    // `closed` means "finished writing", not "the process is gone". close() must
    // not return while the process may still hold the storage (INV-002).
    jest.useFakeTimers();
    try {
      const h = harness({ timeoutMs: 5000, exitOnShutdown: false });
      const startPromise = h.executor.start();
      const server = serverAt(h, 0);
      server.send(readyMessage());
      await startPromise;

      const closing = h.executor.close();
      await flushMicrotasks();
      expect(server.killCount).toBe(0);

      jest.advanceTimersByTime(2000);
      await flushMicrotasks();
      await closing;

      expect(server.killCount).toBe(1);
      expect(
        h
          .logs()
          .some(
            (line) =>
              line.msg ===
              'server process did not exit; killing it before close returns',
          ),
      ).toBe(true);
    } finally {
      jest.useRealTimers();
    }
  });

  test('exposes a close budget covering the drain, grace and exit wait', async () => {
    const h = harness({ timeoutMs: 5000, shutdownTimeoutMs: 30 });

    // drain + grace (+ the IPC-hop slack) + two bounded exit waits (the second
    // after the kill): a caller must give the executor at least this much, or it
    // returns while the Server is still writing.
    expect(h.executor.closeBudgetMs).toBe(30 * 2 + 250 + 2000 * 2);
    await h.executor.close();
  });

  test('declares a grace that covers the Server-side worst-case close', async () => {
    // The bug this pins: the Server derived its graceful-close budget from its
    // own `timeoutMs` (4x), the Portal derived its patience from
    // `shutdownTimeoutMs`, and nothing compared the two. A Server closing inside
    // its own budget was killed by a Portal that had already given up.
    const h = harness({ timeoutMs: 5000, shutdownTimeoutMs: 30 });
    const server = await started(h);

    await h.executor.close();

    // The declared grace IS the Server's whole graceful close now, so the
    // inequality to protect is "the budget contains that grace plus every other
    // step this side performs". A budget that dropped below it would start
    // killing Servers that were closing correctly.
    const shutdown = messagesOf(server).find(
      (message) => message.type === 'shutdown',
    );
    if (shutdown?.type !== 'shutdown') {
      throw new Error('the executor did not send a shutdown message');
    }
    const declaredGraceMs = shutdown.timeoutMs;
    const shutdownTimeoutMs = 30;
    const exitWaitMs = 2000;
    const ackSlackMs = 250;
    // The grace is exactly `shutdownTimeoutMs`, so neither side can be retuned
    // without the other noticing.
    expect(declaredGraceMs).toBe(shutdownTimeoutMs);
    // The budget has to contain the Server's share plus every other step this
    // side performs; a budget that dropped below that would start killing
    // Servers that were closing correctly.
    expect(h.executor.closeBudgetMs).toBe(
      shutdownTimeoutMs + declaredGraceMs + ackSlackMs + exitWaitMs * 2,
    );
  });

  test('fails pending requests instead of waiting past its own close budget', async () => {
    // The Server accepts the request and then never answers: `doClose` used to
    // wait out the whole request deadline, which is what made a caller with a
    // shorter shutdown timeout return while the Server was still working.
    const h = harness({
      timeoutMs: 5000,
      shutdownTimeoutMs: 30,
      autoClose: false,
    });
    const server = await started(h);
    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));

    const startedAt = Date.now();
    await h.executor.close();

    expect(Date.now() - startedAt).toBeLessThan(3000);
    expect(failureOf(await outcomePromise).code).toBe('FRAMEWORK_SHUTDOWN');
    expect(server.killCount).toBe(1);
  });
});
