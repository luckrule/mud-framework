import { describe, expect, jest, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import {
  FakeServerProcess,
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
import { IpcExecutor } from './ipc-executor.ts';

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
    const rejected = h.logs().filter((line) => line.msg === 'request rejected');
    expect(rejected).toHaveLength(1);
    expect(rejected[0]).toMatchObject({
      level: 'warn',
      code: 'FRAMEWORK_SHUTDOWN',
      reason: 'executor closed',
      requestId: 'late',
      connectionId: 'c1',
      clientRequestId: 'client-1',
    });
    expect(rejected[0]).not.toHaveProperty('sessionId');
    // Idempotent: a second close resolves rather than throwing.
    await h.executor.close();
  });

  test('rejects requests that arrive while closing and says which state', async () => {
    const h = harness({ timeoutMs: 5000 });
    await started(h);

    // `close()` flips the status synchronously; the shutdown handshake follows.
    const closing = h.executor.close();
    const outcome = await h.executor.execute(
      request({ requestId: 'mid-close', sessionId: 's-close' }),
    );
    await closing;

    expect(failureOf(outcome).code).toBe('FRAMEWORK_SHUTDOWN');
    const rejected = h.logs().filter((line) => line.msg === 'request rejected');
    expect(rejected).toHaveLength(1);
    expect(rejected[0]).toMatchObject({
      level: 'warn',
      code: 'FRAMEWORK_SHUTDOWN',
      reason: 'executor closing',
      requestId: 'mid-close',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's-close',
    });
  });

  test('a throwing sink does not change the rejection while closing or closed', async () => {
    // `execute()` is not async: a throw from the injected sink would escape as
    // an exception instead of the failure outcome, and the connection layer
    // would report INTERNAL_ERROR where the contract says FRAMEWORK_SHUTDOWN.
    const spawned: FakeServerProcess[] = [];
    const lines: string[] = [];
    let sinkThrows = 0;
    const executor = new IpcExecutor({
      spawn: () => {
        const server = new FakeServerProcess({ autoCloseOnShutdown: true });
        spawned.push(server);
        return server.handle;
      },
      timeoutMs: 5000,
      logger: createJsonLogger({
        level: 'debug',
        sink: (line) => {
          lines.push(line);
          if (line.includes('"msg":"request rejected"')) {
            sinkThrows += 1;
            throw new Error('sink exploded');
          }
        },
      }),
    });
    const startPromise = executor.start();
    const server = spawned[0];
    if (server === undefined) {
      throw new Error('server was never spawned');
    }
    server.send(readyMessage());
    await startPromise;

    // `close()` flips the status synchronously, so this call sees `closing`.
    const closing = executor.close();
    const whileClosing = await executor.execute(
      request({ requestId: 'while-closing' }),
    );
    await closing;
    const afterClose = await executor.execute(request({ requestId: 'late' }));

    expect(failureOf(whileClosing).code).toBe('FRAMEWORK_SHUTDOWN');
    expect(failureOf(afterClose).code).toBe('FRAMEWORK_SHUTDOWN');
    // The reason names the state, which proves both branches ran rather than
    // the same one twice.
    expect(
      lines.some((line) => line.includes('"reason":"executor closing"')),
    ).toBe(true);
    expect(
      lines.some((line) => line.includes('"reason":"executor closed"')),
    ).toBe(true);
    // Proves the sink really threw; a renamed message would let the outcome
    // assertions pass without exercising the guarded path.
    expect(sinkThrows).toBeGreaterThanOrEqual(1);
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
    // Exactly one per-request line: a request failed before delivery must say
    // why it ended, and no other path may claim the same failure.
    const failed = h
      .logs()
      .filter((line) => line.msg === 'pending request failed');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      level: 'warn',
      code: 'FRAMEWORK_SHUTDOWN',
      reason: 'executor closing before delivery',
      requestId: 'queued',
      connectionId: 'c1',
      clientRequestId: 'client-1',
    });
    expect(typeof failed[0]?.durationMs).toBe('number');
    expect(failed[0]).not.toHaveProperty('sessionId');
  });

  test('a throwing logger sink does not block settlement', async () => {
    // The logger is injectable, so its sink is arbitrary code. A throw on the
    // first failed request must not abort the loop and strand the ones after it.
    const spawned: FakeServerProcess[] = [];
    let sinkThrows = 0;
    const executor = new IpcExecutor({
      spawn: () => {
        const server = new FakeServerProcess({ autoCloseOnShutdown: true });
        spawned.push(server);
        return server.handle;
      },
      timeoutMs: 5000,
      shutdownTimeoutMs: 20,
      logger: createJsonLogger({
        level: 'debug',
        sink: (line) => {
          if (line.includes('"msg":"pending request failed"')) {
            sinkThrows += 1;
            throw new Error('sink exploded');
          }
        },
      }),
    });
    const startPromise = executor.start();
    const server = spawned[0];
    if (server === undefined) {
      throw new Error('server was never spawned');
    }
    server.send(readyMessage());
    await startPromise;

    // Delivered but never answered: the drain times out and fails both, which
    // is where a throwing sink used to cut the settlement loop short.
    const first = executor.execute(request({ requestId: 'req-a' }));
    const second = executor.execute(request({ requestId: 'req-b' }));
    await executor.close();

    expect(failureOf(await first).code).toBe('FRAMEWORK_SHUTDOWN');
    expect(failureOf(await second).code).toBe('FRAMEWORK_SHUTDOWN');
    // Proves the sink really threw: if the log message is ever renamed, the
    // settlement assertions above could pass because the sink never fired.
    expect(sinkThrows).toBeGreaterThanOrEqual(1);
  });

  test('a throwing sink on the drain timeout summary still closes', async () => {
    // The summary sits before failAllPending, shutdownServer(), status =
    // 'closed' and rejectStart: a throw there would skip all of them and leave
    // the Server holding storage (INV-002).
    const spawned: FakeServerProcess[] = [];
    const lines: string[] = [];
    let sinkThrows = 0;
    const executor = new IpcExecutor({
      spawn: () => {
        const server = new FakeServerProcess({ autoCloseOnShutdown: true });
        spawned.push(server);
        return server.handle;
      },
      timeoutMs: 5000,
      shutdownTimeoutMs: 20,
      logger: createJsonLogger({
        level: 'debug',
        sink: (line) => {
          lines.push(line);
          if (
            line.includes(
              '"msg":"pending requests did not settle before the shutdown timeout"',
            )
          ) {
            sinkThrows += 1;
            throw new Error('sink exploded');
          }
        },
      }),
    });
    const startPromise = executor.start();
    const server = spawned[0];
    if (server === undefined) {
      throw new Error('server was never spawned');
    }
    server.send(readyMessage());
    await startPromise;

    // Delivered but never answered: the drain runs into its 20ms budget.
    const first = executor.execute(request({ requestId: 'req-a' }));
    const second = executor.execute(request({ requestId: 'req-b' }));
    await executor.close();

    expect(sinkThrows).toBeGreaterThanOrEqual(1);
    expect(failureOf(await first).code).toBe('FRAMEWORK_SHUTDOWN');
    expect(failureOf(await second).code).toBe('FRAMEWORK_SHUTDOWN');
    // The late rejection says `closed`: the throw did not leave the executor
    // stuck in `closing`.
    const late = await executor.execute(request({ requestId: 'late' }));
    expect(failureOf(late).code).toBe('FRAMEWORK_SHUTDOWN');
    expect(
      lines.some((line) => line.includes('"reason":"executor closed"')),
    ).toBe(true);
  });

  test('does not restart while closing when the server dies', async () => {
    const h = harness({ timeoutMs: 5000, autoClose: false });
    const server = await started(h);

    const outcomePromise = h.executor.execute(
      request({ requestId: 'req-1', sessionId: 's-crash' }),
    );
    const closing = h.executor.close();
    await flushMicrotasks();

    // The crash lands mid-shutdown: no restart, and the request settles.
    server.crash();
    await flushMicrotasks();
    await closing;

    expect(h.servers).toHaveLength(1);
    expect(failureOf(await outcomePromise).code).toBe('FRAMEWORK_SHUTDOWN');
    const failed = h
      .logs()
      .filter((line) => line.msg === 'pending request failed');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      level: 'warn',
      code: 'FRAMEWORK_SHUTDOWN',
      reason: 'server lost during shutdown',
      requestId: 'req-1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's-crash',
    });
    expect(typeof failed[0]?.durationMs).toBe('number');
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

  test('exposes a close budget covering the switch abort, drain, grace and exit waits', async () => {
    const h = harness({ timeoutMs: 5000, shutdownTimeoutMs: 30 });

    // switch abort + drain + grace (+ the IPC-hop slack) + two bounded exit
    // waits for the current Server (the second after the kill) + the bounded
    // wait→kill→wait for an abandoned activated replacement: a caller must give
    // the executor at least this much, or it returns while a Server is still
    // writing.
    expect(h.executor.closeBudgetMs).toBe(30 * 3 + 250 + 2000 * 4);
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
    // Servers that were closing correctly. The grace is the third
    // `shutdownTimeoutMs` term (switch abort, drain, grace).
    expect(h.executor.closeBudgetMs).toBe(
      shutdownTimeoutMs * 3 + ackSlackMs + exitWaitMs * 4,
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
    const failed = h
      .logs()
      .filter((line) => line.msg === 'pending request failed');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      level: 'warn',
      code: 'FRAMEWORK_SHUTDOWN',
      reason: 'shutdown drain timed out',
      requestId: 'req-1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
    });
    expect(typeof failed[0]?.durationMs).toBe('number');
  });
});
