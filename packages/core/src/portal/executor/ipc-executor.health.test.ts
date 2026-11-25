import { describe, expect, jest, test } from 'bun:test';

import { rejectionOf } from '../../testing/common.ts';
import {
  flushMicrotasks,
  flushTicks,
  harness,
  readyMessage,
  serverAt,
  started,
  waitFor,
} from '../../testing/ipc-executor.ts';

describe('IpcExecutor health', () => {
  test('is idle before start', () => {
    const h = harness({ timeoutMs: 5000 });

    expect(h.executor.health()).toEqual({ state: 'idle', ready: false });
  });

  test('is starting while the first handshake is still pending', async () => {
    const h = harness({ timeoutMs: 5000 });
    const start = h.executor.start();

    expect(h.executor.health()).toEqual({ state: 'starting', ready: false });
    await flushTicks();
    expect(h.executor.health()).toEqual({ state: 'starting', ready: false });

    serverAt(h, 0).send(readyMessage());
    await start;
    await h.executor.close();
  });

  test('is ready once the server completed the handshake', async () => {
    const h = harness({ timeoutMs: 5000 });
    await started(h);

    expect(h.executor.health()).toEqual({ state: 'ready', ready: true });

    await h.executor.close();
  });

  test('is recovering while a replacement server is not ready', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    first.closeChannel();
    first.crash();
    await waitFor(() => h.servers.length === 2);

    expect(h.executor.health()).toEqual({ state: 'recovering', ready: false });

    serverAt(h, 1).send(readyMessage());
    await flushTicks();
    expect(h.executor.health()).toEqual({ state: 'ready', ready: true });

    await h.executor.close();
  });

  test('is recovering while a replacement is being prepared', async () => {
    const h = harness({ timeoutMs: 5000 });
    await started(h);
    const switching = h.executor.switchTo({
      spawn: () => h.makeServer({ silentOnHello: true }),
    });
    await flushTicks();

    expect(h.executor.health()).toEqual({ state: 'recovering', ready: false });

    const closing = h.executor.close();
    expect((await rejectionOf(switching)).message).toMatch(/closing/);
    await closing;
  });

  test('is blocked when a lost writer exit is unconfirmed', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    first.unobservableExit();
    await flushTicks();

    expect(h.executor.health()).toEqual({ state: 'blocked', ready: false });
    expect(h.servers).toHaveLength(1);

    // The same condition makes close() fail, which is the terminal signal; the
    // health snapshot must not promise readiness in the meantime.
    await rejectionOf(h.executor.close());
  });

  test('is closed while shutdown is in progress and after it completes', async () => {
    const h = harness({ timeoutMs: 5000 });
    await started(h);

    const closing = h.executor.close();
    expect(h.executor.health()).toEqual({ state: 'closed', ready: false });

    await closing;
    expect(h.executor.health()).toEqual({ state: 'closed', ready: false });
  });

  test('converges to closed after a successful close that follows a blocked recovery', async () => {
    jest.useFakeTimers();
    try {
      const h = harness({ timeoutMs: 5000, killExits: false });
      const first = await started(h);
      first.closeChannel();
      await flushMicrotasks();
      jest.advanceTimersByTime(2001);
      await flushMicrotasks();
      expect(h.executor.health()).toEqual({ state: 'blocked', ready: false });

      const closing = h.executor.close();
      await flushMicrotasks();
      // The retained writer finally confirms its exit, so close() succeeds; the
      // stale blocked flag must not outlive the close that cleared the barrier.
      first.crash();
      await flushMicrotasks();
      await closing;
      expect(h.executor.health()).toEqual({ state: 'closed', ready: false });
    } finally {
      jest.useRealTimers();
    }
  });

  test('reports blocked, not closed, when close fails on an unconfirmed exit', async () => {
    jest.useFakeTimers();
    try {
      const h = harness({
        timeoutMs: 5000,
        exitOnShutdown: false,
        killExits: false,
        shutdownTimeoutMs: 20,
      });
      const startPromise = h.executor.start();
      serverAt(h, 0).send(readyMessage());
      await startPromise;

      const closing = h.executor.close();
      // Let the sequence arm its first exit wait, then run each bounded wait in
      // turn: the next timer is armed only after the previous one settles.
      await flushMicrotasks();
      jest.advanceTimersByTime(2001);
      await flushMicrotasks();
      jest.advanceTimersByTime(2001);
      await flushMicrotasks();
      jest.advanceTimersByTime(2001);
      await flushMicrotasks();
      const error = await rejectionOf(closing);

      expect(error.message).toMatch(/could not confirm/);
      // The only observable signal that a writer may still be alive (INV-002):
      // reporting `closed` here would erase it.
      expect(h.executor.health()).toEqual({ state: 'blocked', ready: false });
    } finally {
      jest.useRealTimers();
    }
  });

  test('reports a terminal failed start as closed', async () => {
    const h = harness({
      timeoutMs: 5000,
      autoPrepare: false,
      startTimeoutMs: 20,
    });

    const error = await rejectionOf(h.executor.start());

    expect(error.message).toMatch(/did not become ready/);
    expect(h.executor.health()).toEqual({ state: 'closed', ready: false });
  });
});
