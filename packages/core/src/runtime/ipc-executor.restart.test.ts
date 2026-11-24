import { describe, expect, test } from 'bun:test';

import { rejectionOf } from '../test-support/common.ts';
import {
  failureOf,
  flushTicks,
  harness,
  readyMessage,
  request,
  requestIds,
  serverAt,
  started,
  successMessage,
  waitFor,
} from '../test-support/ipc-executor.ts';

describe('IpcExecutor restart', () => {
  test('treats exit and channel close as one crash', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    // Both signals fire, in the order that leaves the channel handler last:
    // one restart, not two.
    first.closeChannel();
    first.crash();
    await flushTicks();

    expect(h.servers).toHaveLength(2);
    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();

    // Still exactly one buffered request: a double restart would have lost or
    // duplicated it.
    expect(requestIds(second)).toEqual(['req-1']);
    second.send(successMessage('req-1', 'once'));
    expect(await outcomePromise).toEqual({ ok: true, result: 'once' });
    await h.executor.close();
  });

  test('kills an orphaned server before restarting', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);
    void h.executor.execute(request({ requestId: 'req-1' }));

    // The pipe dies but the process is still alive: restarting beside it would
    // leave two storage writers (INV-002).
    first.closeChannel();
    await flushTicks();

    expect(first.killCount).toBe(1);
    expect(h.servers).toHaveLength(2);

    // Settle the replay so close() does not wait out the 5s deadline.
    const second = serverAt(h, 1);
    second.send(readyMessage());
    await flushTicks();
    second.send(successMessage('req-1', null));
    await h.executor.close();
  });

  test('waits for the process to exit before restarting after a lost channel', async () => {
    // The replacement must not open storage while the old process is still
    // holding it (INV-002). A kill that has not taken effect yet is the exact
    // window this covers.
    const h = harness({ timeoutMs: 5000, killExits: false });
    const first = await started(h);

    first.closeChannel();
    await flushTicks();

    expect(first.killCount).toBe(1);
    // Killed but not yet gone: no replacement may be spawned.
    expect(h.servers).toHaveLength(1);

    first.crash();
    await waitFor(() => h.servers.length === 2);
    expect(h.servers).toHaveLength(2);
    // Let the replacement handshake so close() takes the graceful path.
    serverAt(h, 1).send(readyMessage());
    await flushTicks();
    await h.executor.close();
  });

  test('still kills a server whose exit could not be observed', async () => {
    const h = harness({ timeoutMs: 5000 });
    const first = await started(h);

    // An unobservable exit means "the process may still be alive". Treating it
    // as gone would skip the only kill and start a second writer beside it
    // (INV-002), so the defensive kill must still run.
    first.unobservableExit();
    await flushTicks();

    expect(first.killCount).toBe(1);
    expect(
      h
        .logs()
        .some(
          (line) =>
            line.msg ===
            'server channel lost while the process was alive; killing it',
        ),
    ).toBe(true);
    // And the restart is not blocked forever: the replacement appears.
    await waitFor(() => h.servers.length === 2);
    serverAt(h, 1).send(readyMessage());
    await flushTicks();
    await h.executor.close();
  });

  test('a server that dies on every request is not restarted in a tight loop', async () => {
    // The bug this covers: resetting the crash streak on `ready` made
    // "ready, then die on the first request" restart with zero backoff, which
    // spawned ~275 processes in 300ms.
    const h = harness({
      timeoutMs: 1000,
      crashOnHello: true,
      restartBackoffMs: 200,
    });
    void h.executor.start().catch(() => undefined);
    await flushTicks();

    await new Promise<void>((resolve) => {
      setTimeout(resolve, 300);
    });

    expect(h.servers.length).toBeLessThanOrEqual(3);
    await h.executor.close();
  });

  test('fails start after startTimeoutMs when the server never becomes ready', async () => {
    const h = harness({ timeoutMs: 5000, startTimeoutMs: 50 });
    const beganAt = Date.now();

    const error = await rejectionOf(h.executor.start());

    // A broken entry script must be visible in seconds, not hang forever while
    // the restart loop keeps spawning processes.
    expect(error.message).toMatch(/did not become ready within 50ms/);
    expect(Date.now() - beganAt).toBeLessThan(2000);
    const logged = h
      .logs()
      .find(
        (line) =>
          line.msg === 'server did not become ready before the start timeout',
      );
    expect(logged).toMatchObject({ level: 'error', timeoutMs: 50 });
    expect(typeof logged?.attempts).toBe('number');
    await h.executor.close();
  });

  test('stops spawning once the start timeout fired', async () => {
    const h = harness({
      timeoutMs: 5000,
      startTimeoutMs: 50,
      restartBackoffMs: 5,
    });
    await rejectionOf(h.executor.start());
    const spawnedAtReject = h.servers.length;

    await new Promise<void>((resolve) => {
      setTimeout(resolve, 150);
    });

    expect(h.servers.length).toBe(spawnedAtReject);
    await h.executor.close();
  });

  test('execute fails immediately after a failed start instead of buffering', async () => {
    const h = harness({ timeoutMs: 5000, startTimeoutMs: 20 });
    await rejectionOf(h.executor.start());

    const beganAt = Date.now();
    const outcome = await h.executor.execute(request({ requestId: 'late' }));

    expect(failureOf(outcome).code).toBe('INTERNAL_ERROR');
    expect(failureOf(outcome).message).toContain('failed to start');
    expect(Date.now() - beganAt).toBeLessThan(1000);
    await h.executor.close();
  });

  test('reports the real exit code when the channel close wins the race', async () => {
    // The real spawn closes the channel before `exited` settles, and the close
    // handler is synchronous while the promise continuation is a microtask.
    // Without a synchronous view of the exit the crash line had
    // `exitCode: null` and the code survived only inside the free-text reason.
    const h = harness({ timeoutMs: 5000, closeChannelOnExit: true });
    await started(h);

    serverAt(h, 0).crash(17);
    await flushTicks();

    const lost = h.logs().find((line) => line.msg === 'server process lost');
    expect(lost).toMatchObject({ level: 'error', exitCode: 17, signal: null });
    serverAt(h, 1).send(readyMessage());
    await flushTicks();
    await h.executor.close();
  });
});
