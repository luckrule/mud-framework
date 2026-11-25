import { describe, expect, jest, test } from 'bun:test';

import type { IpcChannel } from '../../ipc/channel.ts';
import { IpcChannelClosedError } from '../../ipc/channel.ts';
import { rejectionOf } from '../../testing/common.ts';
import {
  flushMicrotasks,
  flushTicks,
  readyMessage,
  request,
  requestIds,
  serverAt,
  started,
  successMessage,
  waitFor,
} from '../../testing/ipc-executor.ts';
import {
  failFirstDeliveryAfterActivate,
  labelledServer,
  messagesTo,
  SWITCH_FAILED,
  SWITCH_LOST,
  SWITCH_ROLLBACK,
  switchable,
} from './version-switch.support.ts';

describe('IpcExecutor switchTo', () => {
  test('keeps the old server serving and rejects when the replacement spawn throws', async () => {
    const h = switchable();
    const old = await started(h);

    const error = await rejectionOf(
      h.executor.switchTo({
        spawn: () => {
          throw new Error('no such binary');
        },
      }),
    );
    expect(error.message).toMatch(/spawn failed/);
    expect(old.killCount).toBe(0);

    // The old process was never told to stop and still answers.
    expect(messagesTo(old)).toEqual(['hello', 'activate']);
    const outcome = h.executor.execute(request({ requestId: 'after' }));
    expect(requestIds(old)).toEqual(['after']);
    old.send(successMessage('after', 'still-old'));
    expect(await outcome).toEqual({ ok: true, result: 'still-old' });

    expect(
      h
        .logs()
        .some((line) => line.msg === SWITCH_FAILED && line.level === 'warn'),
    ).toBe(true);
    await h.executor.close();
  });

  test('cleans up the replacement and keeps the old server when prepared never arrives', async () => {
    const h = switchable({ startTimeoutMs: 40 });
    const old = await started(h);

    const error = await rejectionOf(
      h.executor.switchTo({
        spawn: () => labelledServer(h, 'new', { silentOnHello: true }),
      }),
    );
    expect(error.message).toMatch(/did not prepare/);

    const replacement = serverAt(h, 1);
    expect(replacement.killCount).toBeGreaterThanOrEqual(1);
    // The old process still holds the storage and was never asked to stop.
    expect(messagesTo(old)).toEqual(['hello', 'activate']);
    expect(old.killCount).toBe(0);

    const outcome = h.executor.execute(request({ requestId: 'after' }));
    old.send(successMessage('after', 'still-old'));
    expect(await outcome).toEqual({ ok: true, result: 'still-old' });
    await h.executor.close();
  });

  test('rolls back to the previous spawn when ready never arrives after activate', async () => {
    const h = switchable({ startTimeoutMs: 40 });
    await started(h);

    const error = await rejectionOf(
      h.executor.switchTo({
        // Prepares but never answers `activate` with `ready`.
        spawn: () => labelledServer(h, 'new', { readyOnActivate: false }),
      }),
    );
    expect(error.message).toMatch(/did not become ready/);

    // The old process is retired; the framework restarts the previous version
    // through the crash path rather than leaving no Server at all.
    await waitFor(() => h.servers.length === 3);
    const replacement = serverAt(h, 2);
    replacement.send(readyMessage());

    const outcome = h.executor.execute(request({ requestId: 'after' }));
    await flushTicks();
    expect(requestIds(replacement)).toEqual(['after']);
    replacement.send(successMessage('after', 'rolled-back'));
    expect(await outcome).toEqual({ ok: true, result: 'rolled-back' });

    expect(
      h
        .logs()
        .some((line) => line.msg === SWITCH_ROLLBACK && line.level === 'warn'),
    ).toBe(true);
    await h.executor.close();
  });

  test('does not restart the previous version until the activated replacement is gone', async () => {
    // The replacement received `activate` (so it may have opened storage) but
    // never answered `ready`. The restart must wait for its exit, or two
    // processes would briefly write to the same storage (INV-002).
    const h = switchable({ startTimeoutMs: 40, killExits: false });
    await started(h);

    const error = await rejectionOf(
      h.executor.switchTo({
        spawn: () => labelledServer(h, 'new', { readyOnActivate: false }),
      }),
    );
    expect(error.message).toMatch(/did not become ready/);

    // Abandoning it issued a kill, but its exit has not been observed yet.
    const activated = serverAt(h, 1);
    expect(activated.killCount).toBeGreaterThanOrEqual(1);
    await flushTicks();
    expect(h.servers).toHaveLength(2);

    // Only once the process is gone may the previous version restart.
    activated.crash(0);
    await waitFor(() => h.servers.length === 3);
    serverAt(h, 2).send(readyMessage());
    await flushTicks();
    await h.executor.close();
  });

  test('retains a replacement when activate is delivered before send throws', async () => {
    const h = switchable({ killExits: false });
    await started(h);
    const error = await rejectionOf(
      h.executor.switchTo({
        spawn: () => {
          const handle = labelledServer(h, 'new');
          const channel: IpcChannel = {
            send(message): void {
              handle.channel.send(message);
              if (message.type === 'activate') {
                throw new IpcChannelClosedError('activate delivery uncertain');
              }
            },
            onMessage: (handler) => {
              handle.channel.onMessage(handler);
            },
            onClose: (handler) => {
              handle.channel.onClose(handler);
            },
            close: (reason) => {
              handle.channel.close(reason);
            },
          };
          return { ...handle, channel };
        },
      }),
    );

    expect(error.message).toMatch(/activate failed/);
    expect(h.logs().find((line) => line.msg === SWITCH_ROLLBACK)).toMatchObject(
      { pendingExits: 1 },
    );
    expect(h.servers).toHaveLength(2);
    serverAt(h, 1).crash();
    await waitFor(() => h.servers.length === 3);
    serverAt(h, 2).send(readyMessage());
    await h.executor.close();
  });

  test('rollback retains an activated replacement after its exit wait times out', async () => {
    jest.useFakeTimers();
    try {
      const h = switchable({ startTimeoutMs: 40, killExits: false });
      await started(h);
      const switching = h.executor.switchTo({
        spawn: () => labelledServer(h, 'new', { readyOnActivate: false }),
      });
      await flushMicrotasks();
      jest.advanceTimersByTime(41);
      await flushMicrotasks();
      expect((await rejectionOf(switching)).message).toMatch(
        /did not become ready/,
      );
      const replacement = serverAt(h, 1);

      jest.advanceTimersByTime(2001);
      await flushMicrotasks();
      jest.advanceTimersByTime(1);
      await flushMicrotasks();
      expect(h.servers).toHaveLength(2);

      let closed = false;
      const closing = h.executor.close().then(() => {
        closed = true;
        return undefined;
      });
      await flushMicrotasks();
      expect(closed).toBe(false);
      replacement.crash();
      await flushMicrotasks();
      await closing;
      jest.advanceTimersByTime(5000);
      await flushMicrotasks();
      expect(h.servers).toHaveLength(2);
    } finally {
      jest.useRealTimers();
    }
  });

  test('does not restart until the lost previous process has exited', async () => {
    // The old process lost its link but survived the kill (INV-002): it may
    // still hold storage, so the rollback restart must wait for its exit like
    // the crash path does, instead of launching a second writer beside it.
    const h = switchable({
      startTimeoutMs: 5000,
      restartBackoffMs: 5,
      killExits: false,
    });
    const old = await started(h);

    const switching = h.executor.switchTo({
      spawn: () =>
        labelledServer(h, 'new', { silentOnHello: true, killExits: true }),
    });
    // A dead link without an exit event: the old process is unreachable and a
    // kill cannot be confirmed, so it must be treated as possibly alive.
    old.closeChannel();

    const error = await rejectionOf(switching);
    expect(error.message).toMatch(/previous server lost/);
    expect(
      h
        .logs()
        .some((line) => line.msg === SWITCH_LOST && line.level === 'warn'),
    ).toBe(true);

    // The kill was issued but its exit was not observed: nothing may launch yet.
    expect(old.killCount).toBeGreaterThanOrEqual(1);
    await flushTicks();
    expect(h.servers).toHaveLength(2);

    // Only once the old process is confirmed gone may the rollback spawn.
    old.crash(1);
    await waitFor(() => h.servers.length === 3);
    serverAt(h, 2).send(readyMessage());
    await flushTicks();
    await h.executor.close();
  });

  test('settles requests accepted during a failed switch without waiting for the deadline', async () => {
    const h = switchable({ startTimeoutMs: 40 });
    const old = await started(h);

    const buffered = h.executor.execute(request({ requestId: 'buffered' }));
    const error = await rejectionOf(
      h.executor.switchTo({
        spawn: () => labelledServer(h, 'new', { silentOnHello: true }),
      }),
    );
    expect(error.message).toMatch(/did not prepare/);

    // The old server is still current; the buffered request reaches it.
    await flushTicks();
    expect(requestIds(old)).toContain('buffered');
    old.send(successMessage('buffered', 'old-after-failure'));
    expect(await buffered).toEqual({ ok: true, result: 'old-after-failure' });
    await h.executor.close();
  });

  test('rolls back when the replacement dies before it prepares', async () => {
    const h = switchable({ startTimeoutMs: 500 });
    const old = await started(h);

    const error = await rejectionOf(
      h.executor.switchTo({
        spawn: () => {
          const handle = labelledServer(h, 'new', { silentOnHello: true });
          const spawned = h.servers.at(-1);
          // Crash the replacement after `hello` is sent, before `prepared`.
          setTimeout(() => {
            spawned?.crash(1);
          }, 0);
          return handle;
        },
      }),
    );
    expect(error.message).toMatch(/replacement/);

    // The old process was never asked to stop and still serves.
    expect(messagesTo(old)).toEqual(['hello', 'activate']);
    const outcome = h.executor.execute(request({ requestId: 'after' }));
    old.send(successMessage('after', 'still-old'));
    expect(await outcome).toEqual({ ok: true, result: 'still-old' });
    await h.executor.close();
  });

  test('rolls back to the previous version when the old server dies mid-switch', async () => {
    const h = switchable({ startTimeoutMs: 5000, restartBackoffMs: 5 });
    const old = await started(h);

    const switching = h.executor.switchTo({
      spawn: () => labelledServer(h, 'new', { silentOnHello: true }),
    });
    // The old process dies before the switch retires it. There is no previous
    // Server left, so the switch fails and the crash path restarts.
    old.crash(1);

    const error = await rejectionOf(switching);
    expect(error.message).toMatch(/previous server lost/);
    expect(
      h
        .logs()
        .some((line) => line.msg === SWITCH_LOST && line.level === 'warn'),
    ).toBe(true);

    // The replacement that never prepared is cleaned up.
    expect(serverAt(h, 1).killCount).toBeGreaterThanOrEqual(1);

    await waitFor(() => h.servers.length === 3);
    const restarted = serverAt(h, 2);
    restarted.send(readyMessage());
    await flushTicks();
    const outcome = h.executor.execute(request({ requestId: 'after' }));
    await flushTicks();
    expect(requestIds(restarted)).toContain('after');
    restarted.send(successMessage('after', 'restarted'));
    expect(await outcome).toEqual({ ok: true, result: 'restarted' });
    await h.executor.close();
  });

  test('does not promote a replacement that exited right after ready', async () => {
    // `ready` resolves the barrier, but the process dies before the promotion
    // continuation runs. Promoting it would report success with no live Server
    // behind it; the switch must fail and roll back instead.
    const h = switchable({ startTimeoutMs: 5000, restartBackoffMs: 5 });
    await started(h);

    const switching = h.executor.switchTo({
      spawn: () => labelledServer(h, 'new', { readyOnActivate: false }),
    });
    await flushTicks();
    const replacement = serverAt(h, 1);

    // Same tick: the barrier resolves, then the process is gone. Without the
    // promote guard the switch would report success and leave `ready` with no
    // Server behind it, so the assertion that matters is that it rejects.
    replacement.send(readyMessage());
    replacement.crash(1);

    const error = await rejectionOf(switching);
    expect(error.message).toMatch(/replacement/);

    // No Server was left behind: the rollback restarts the previous version.
    await waitFor(() => h.servers.length === 3);
    const restarted = serverAt(h, 2);
    restarted.send(readyMessage());
    await flushTicks();
    const outcome = h.executor.execute(request({ requestId: 'after' }));
    await flushTicks();
    expect(requestIds(restarted)).toContain('after');
    restarted.send(successMessage('after', 'rolled-back'));
    expect(await outcome).toEqual({ ok: true, result: 'rolled-back' });
    await h.executor.close();
  });

  test('fails the switch and restarts when the first flush kills the promoted server', async () => {
    // Promotion happens before `flush()`. A delivery that fails there means the
    // process is already gone, so the switch must not report success: it has to
    // reject and let the crash path restart, instead of leaving `ready` with no
    // Server and no restart timer behind it.
    const h = switchable({ restartBackoffMs: 5 });
    const old = await started(h);

    // Unacknowledged work is what makes the promotion's `flush()` send: the
    // delivery to the just-promoted process is the failing one.
    const buffered = h.executor.execute(request({ requestId: 'in-flight' }));
    await flushTicks();
    expect(requestIds(old)).toEqual(['in-flight']);

    let spawned = 0;
    const switching = h.executor.switchTo({
      spawn: () => {
        spawned += 1;
        const handle = labelledServer(h, spawned === 1 ? 'new' : 'restarted');
        if (spawned !== 1) {
          return handle;
        }
        // The link dies exactly when the promoted process is first sent work.
        return failFirstDeliveryAfterActivate(handle);
      },
    });

    const error = await rejectionOf(switching);
    expect(error.message).toMatch(/lost/);
    expect(
      h.logs().some((line) => line.msg === 'server switch completed'),
    ).toBe(false);

    // The crash path owns recovery: a new process replaces the dead one and
    // serves requests again, so the executor is never left ready-without-server.
    await waitFor(() => h.servers.length === 3);
    const restarted = serverAt(h, 2);
    restarted.send(readyMessage());
    await flushTicks();
    const outcome = h.executor.execute(request({ requestId: 'after' }));
    await flushTicks();
    // The in-flight request is replayed to the new generation (SPEC 6.5), so it
    // precedes the fresh one.
    expect(requestIds(restarted)).toEqual(['in-flight', 'after']);
    restarted.send(successMessage('in-flight', 'replayed'));
    restarted.send(successMessage('after', 'recovered'));
    expect(await buffered).toEqual({ ok: true, result: 'replayed' });
    expect(await outcome).toEqual({ ok: true, result: 'recovered' });
    await h.executor.close();
  });
});
