import { describe, expect, jest, test } from 'bun:test';

import type { IpcChannel } from '../../ipc/channel.ts';
import { IpcChannelClosedError } from '../../ipc/channel.ts';
import type { SpawnedServer } from '../../ipc/spawn-port.ts';
import { rejectionOf } from '../../testing/common.ts';
import {
  failureOf,
  flushMicrotasks,
  flushTicks,
  harness,
  messagesOf,
  preparedMessage,
  readyMessage,
  request,
  requestIds,
  requestMessages,
  serverAt,
  started,
  successMessage,
  waitFor,
} from '../../testing/ipc-executor.ts';
import type { FakeServerProcess, Harness } from '../../testing/ipc-executor.ts';

const SWITCH_COMPLETED = 'server switch completed';
const SWITCH_FAILED = 'server switch failed; the previous server keeps serving';
const SWITCH_ROLLBACK =
  'server switch failed after the previous server retired; restarting the previous version';
const SWITCH_LOST =
  'server switch failed because the previous server was lost; restarting the previous version';
/** The replacement's pid, distinct from the initial process's 4242. */
const NEW_PID = 5252;

function messagesTo(server: FakeServerProcess): string[] {
  return messagesOf(server).map((message) => message.type);
}

/** One process per generation, with a shared timeline for ordering checks. */
function switchable(
  options: {
    timeoutMs?: number;
    shutdownTimeoutMs?: number;
    startTimeoutMs?: number;
    restartBackoffMs?: number;
    exitOnShutdown?: boolean;
    holdRequests?: boolean;
    readyOnActivate?: boolean;
    killExits?: boolean;
  } = {},
): Harness & { timeline: string[] } {
  const timeline: string[] = [];
  const h = harness({
    timeoutMs: options.timeoutMs ?? 5000,
    shutdownTimeoutMs: options.shutdownTimeoutMs ?? 50,
    startTimeoutMs: options.startTimeoutMs ?? 500,
    restartBackoffMs: options.restartBackoffMs,
    exitOnShutdown: options.exitOnShutdown,
    holdRequests: options.holdRequests,
    readyOnActivate: options.readyOnActivate,
    killExits: options.killExits,
    timeline,
    label: 'old',
  });
  return { ...h, timeline };
}

function labelledServer(
  h: Harness & { timeline: string[] },
  label: string,
  overrides: {
    readyOnActivate?: boolean;
    silentOnHello?: boolean;
    exitOnShutdown?: boolean;
    killExits?: boolean;
    pid?: number;
    crashOnActivate?: boolean;
  } = {},
): SpawnedServer {
  return h.makeServer({
    label,
    // A distinct pid keeps every assertion that names a process pointing at
    // the replacement rather than accidentally matching the old one.
    pid: overrides.pid ?? NEW_PID,
    readyOnActivate: overrides.readyOnActivate ?? true,
    silentOnHello: overrides.silentOnHello,
    exitOnShutdown: overrides.exitOnShutdown ?? true,
    // Absent, not `undefined`: an explicit key would shadow the harness default.
    ...(overrides.killExits === undefined
      ? {}
      : { killExits: overrides.killExits }),
    crashOnActivate: overrides.crashOnActivate,
    timeline: h.timeline,
  });
}

/**
 * Wraps a replacement so that its first delivery after `activate` fails on the
 * wire: the process is promoted and then found dead on the first `flush()`.
 * Only requests fail; the handshake itself stays intact.
 */
function failFirstDeliveryAfterActivate(handle: SpawnedServer): SpawnedServer {
  let activated = false;
  let failed = false;
  const channel: IpcChannel = {
    send(message): void {
      if (message.type === 'activate') {
        activated = true;
        handle.channel.send(message);
        return;
      }
      if (activated && !failed && message.type === 'request') {
        failed = true;
        throw new IpcChannelClosedError('test delivery failed');
      }
      handle.channel.send(message);
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
}

describe('IpcExecutor switchTo', () => {
  test('retires the old process before activating the new one and replays buffered requests to it', async () => {
    // The initial process holds delivered requests until `shutdown`, so the
    // in-flight request settles on the old generation during the drain.
    const h = switchable({ holdRequests: true, readyOnActivate: true });
    const startPromise = h.executor.start();
    serverAt(h, 0).send(readyMessage());
    await startPromise;

    // One request already delivered to the old process, one buffered by the
    // switch: the first must settle on the old process, the second on the new.
    const inFlight = h.executor.execute(request({ requestId: 'in-flight' }));
    expect(requestIds(serverAt(h, 0))).toEqual(['in-flight']);

    let spawnedNew: FakeServerProcess | undefined;
    const switching = h.executor.switchTo({
      spawn: () => {
        const handle = labelledServer(h, 'new');
        spawnedNew = h.servers.at(-1);
        return handle;
      },
    });

    // Requests accepted during the switch stay buffered (undelivered) until the
    // replacement is ready. Checked synchronously, before the switch can finish.
    const buffered = h.executor.execute(request({ requestId: 'buffered' }));
    const newServer = spawnedNew;
    if (newServer === undefined) {
      throw new Error('the replacement was never spawned');
    }
    expect(requestIds(newServer)).toEqual([]);

    // The old process drains its in-flight request before it reports `closed`.
    expect(await inFlight).toEqual({ ok: true, result: 'drained-in-flight' });

    await switching;
    await flushTicks();

    expect(requestIds(newServer)).toEqual(['buffered']);
    newServer.send(successMessage('buffered', 'from-new'));
    expect(await buffered).toEqual({ ok: true, result: 'from-new' });

    // INV-002: `activate` reached the replacement only after the old process
    // had closed and exited, never while it still held storage.
    const oldTypes = messagesOf(serverAt(h, 0)).map((message) => message.type);
    expect(oldTypes).toContain('shutdown');
    const newTypes = messagesTo(newServer);
    expect(newTypes.slice(0, 2)).toEqual(['hello', 'activate']);
    expect(h.timeline.indexOf('old:recv:shutdown')).toBeLessThan(
      h.timeline.indexOf('new:recv:activate'),
    );
    expect(h.timeline.indexOf('old:closed')).toBeLessThan(
      h.timeline.indexOf('new:recv:activate'),
    );

    // Exactly one live Server: the old one is done and the new one is current.
    expect(serverAt(h, 0).killCount).toBe(0);
    expect(newServer.killCount).toBe(0);
    const switched = h.logs().find((line) => line.msg === SWITCH_COMPLETED);
    expect(switched).toMatchObject({ level: 'info', pid: NEW_PID });

    await h.executor.close();
  });

  test('sends activate strictly after the old process reported closed', async () => {
    const h = switchable();
    const old = await started(h);

    const switching = h.executor.switchTo({
      spawn: () => labelledServer(h, 'new'),
    });
    await switching;

    // Message sequence, not final state: the replacement may only be told to
    // open storage once the old one announced it finished closing.
    const oldTypes = messagesOf(old).map((message) => message.type);
    expect(oldTypes).toEqual(['hello', 'activate', 'shutdown']);
    const newServer = serverAt(h, 1);
    expect(messagesTo(newServer)).toEqual(['hello', 'activate']);

    await h.executor.close();
  });

  test('rejects a switch that is not started from ready', async () => {
    const h = harness({ timeoutMs: 5000, autoPrepare: false });
    const startPromise = h.executor.start();
    serverAt(h, 0).send(preparedMessage());
    await flushTicks();

    // `starting`, not `ready`: a switch has nothing to replace yet.
    const error = await rejectionOf(
      h.executor.switchTo({ spawn: () => h.makeServer() }),
    );
    expect(error.message).toMatch(/cannot switch while starting/);
    // No replacement was spawned by the refused call.
    expect(h.servers).toHaveLength(1);
    serverAt(h, 0).send(readyMessage());
    await startPromise;
    await h.executor.close();
  });

  test('rejects a second concurrent switch', async () => {
    const h = switchable();
    await started(h);

    const first = h.executor.switchTo({ spawn: () => labelledServer(h, 'a') });
    const error = await rejectionOf(
      h.executor.switchTo({ spawn: () => labelledServer(h, 'b') }),
    );
    expect(error.message).toMatch(/already in progress/);
    // The first switch is unaffected.
    await first;
    // Only the first replacement was ever spawned.
    expect(h.servers).toHaveLength(2);
    await h.executor.close();
  });

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

  test('waits for the old process to exit before activating the replacement', async () => {
    // The core INV-002 guard: `closed` is not an exit. With a process that
    // acknowledges the shutdown and then lingers, `activate` must not go out
    // until the exit is observed (or the kill lands).
    jest.useFakeTimers();
    try {
      const h = switchable({
        startTimeoutMs: 5000,
        shutdownTimeoutMs: 30,
        exitOnShutdown: false,
      });
      const old = await started(h);

      const switching = h.executor.switchTo({
        spawn: () => labelledServer(h, 'new'),
      });
      await flushMicrotasks();

      // `closed` arrived, but the process is still alive: no `activate` yet.
      expect(h.timeline).toContain('old:closed');
      expect(h.timeline).not.toContain('old:exit');
      expect(h.timeline).not.toContain('new:recv:activate');

      // Past the exit wait the executor kills it, which is what releases the
      // activation.
      jest.advanceTimersByTime(2000 + 100);
      await flushMicrotasks();
      await switching;

      expect(h.timeline).toContain('old:exit');
      expect(h.timeline.indexOf('old:exit')).toBeLessThan(
        h.timeline.indexOf('new:recv:activate'),
      );
      expect(old.killCount).toBe(1);
      await h.executor.close();
    } finally {
      jest.useRealTimers();
    }
  });

  test('aborts a switch in progress when close() is called and still closes cleanly', async () => {
    jest.useFakeTimers();
    try {
      const h = switchable({ startTimeoutMs: 5000, shutdownTimeoutMs: 30 });
      const old = await started(h);

      const switching = h.executor.switchTo({
        spawn: () => labelledServer(h, 'new', { silentOnHello: true }),
      });
      await flushMicrotasks();

      const closing = h.executor.close();
      const rejected = await rejectionOf(switching);
      expect(rejected.message).toMatch(/closing/);

      await flushMicrotasks();
      jest.advanceTimersByTime(30 + 250);
      await flushMicrotasks();
      await closing;

      // The replacement is gone; the old one was closed through the normal path.
      expect(serverAt(h, 1).killCount).toBeGreaterThanOrEqual(1);
      expect(messagesTo(old)).toContain('shutdown');

      // Requests accepted during the switch settle with a stable code instead
      // of hanging until their deadline.
      const late = await h.executor.execute(request({ requestId: 'late' }));
      expect(failureOf(late).code).toBe('FRAMEWORK_SHUTDOWN');
    } finally {
      jest.useRealTimers();
    }
  });

  test('close waits for an activated replacement it abandons', async () => {
    // `close()` during `activating`: the replacement received `activate`, so it
    // may hold storage. `doClose` must not return while it could still write
    // (INV-002); it confirms the exit with the same wait → kill → wait flow it
    // uses for the current Server.
    jest.useFakeTimers();
    try {
      const h = switchable({
        startTimeoutMs: 5000,
        shutdownTimeoutMs: 30,
        killExits: false,
      });
      await started(h);

      const switching = h.executor.switchTo({
        // Prepares but never answers `activate`: the switch is stuck waiting
        // for `ready` when `close()` arrives.
        spawn: () => labelledServer(h, 'new', { readyOnActivate: false }),
      });
      await flushMicrotasks();
      const replacement = serverAt(h, 1);
      expect(h.timeline).toContain('new:recv:activate');

      const closing = h.executor.close();
      const rejected = await rejectionOf(switching);
      expect(rejected.message).toMatch(/closing/);

      // The replacement survives the abort's kill (killExits: false), so close
      // is still waiting on it.
      await flushMicrotasks();
      expect(replacement.killCount).toBeGreaterThanOrEqual(1);
      expect(h.timeline).not.toContain('new:exit');

      // Let the bounded exit wait run out; close kills again and the process
      // is then observed gone. Close returns only after that.
      jest.advanceTimersByTime(2000 + 30 + 250);
      await flushMicrotasks();
      replacement.crash(0);
      await flushMicrotasks();
      await closing;

      expect(h.timeline).toContain('new:exit');
      expect(
        h
          .logs()
          .some(
            (line) =>
              line.msg ===
              'abandoned replacement did not exit; killing it before close returns',
          ),
      ).toBe(true);
    } finally {
      jest.useRealTimers();
    }
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

  test('restarts the switched-to server after it crashes', async () => {
    // After a successful switch the serving process factory is the target one,
    // so a crash must restart that process rather than the one the executor was
    // built with.
    const h = switchable({ restartBackoffMs: 5 });
    await started(h);

    let replacementSpawns = 0;
    await h.executor.switchTo({
      spawn: () => {
        replacementSpawns += 1;
        return labelledServer(h, 'new');
      },
    });
    expect(replacementSpawns).toBe(1);
    const switched = serverAt(h, 1);

    // Kill the switched-to process: the restart must use the switch's spawn.
    switched.crash(1);
    await waitFor(() => h.servers.length === 3);
    const restarted = serverAt(h, 2);
    expect(replacementSpawns).toBe(2);
    restarted.send(readyMessage());
    await flushTicks();

    const outcome = h.executor.execute(request({ requestId: 'after' }));
    await flushTicks();
    expect(requestIds(restarted)).toContain('after');
    restarted.send(successMessage('after', 'restarted'));
    expect(await outcome).toEqual({ ok: true, result: 'restarted' });
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

  test('allows another switch once the previous one settled', async () => {
    const h = switchable();
    await started(h);

    await h.executor.switchTo({ spawn: () => labelledServer(h, 'a') });
    await h.executor.switchTo({ spawn: () => labelledServer(h, 'b') });

    // The last replacement is the current Server and answers.
    const current = serverAt(h, 2);
    const outcome = h.executor.execute(request({ requestId: 'after' }));
    await flushTicks();
    expect(requestIds(current)).toEqual(['after']);
    current.send(successMessage('after', 'second-switch'));
    expect(await outcome).toEqual({ ok: true, result: 'second-switch' });
    await h.executor.close();
  });

  test('never delivers a request to the replacement before it is ready', async () => {
    const h = switchable({ startTimeoutMs: 5000 });
    await started(h);

    const switching = h.executor.switchTo({
      spawn: () => labelledServer(h, 'new', { readyOnActivate: false }),
    });

    // Buffered while the replacement is prepared but not activated.
    const buffered = h.executor.execute(request({ requestId: 'buffered' }));
    await flushTicks();
    const replacement = serverAt(h, 1);
    expect(requestIds(replacement)).toEqual([]);

    // Activate it manually through the real ready message.
    replacement.send(readyMessage());
    await switching;
    await flushTicks();
    expect(requestIds(replacement)).toEqual(['buffered']);
    replacement.send(successMessage('buffered', 'ok'));
    expect(await buffered).toEqual({ ok: true, result: 'ok' });
    await h.executor.close();
  });

  test('replays a request that was in flight on the old generation to the new one', async () => {
    // Delivered to the old process and never answered, so it is still
    // unacknowledged when the switch completes. SPEC 6.5: it must be replayed
    // to the new generation, with a bumped replayCount, rather than dropped.
    const h = switchable();
    const old = await started(h);

    const inFlight = h.executor.execute(request({ requestId: 'in-flight' }));
    await flushTicks();
    expect(requestIds(old)).toEqual(['in-flight']);

    await h.executor.switchTo({ spawn: () => labelledServer(h, 'new') });
    await flushTicks();

    const replacement = serverAt(h, 1);
    const replayed = requestMessages(replacement).find(
      (message) => message.requestId === 'in-flight',
    );
    expect(replayed?.replayCount).toBe(1);

    replacement.send(successMessage('in-flight', 'from-new'));
    expect(await inFlight).toEqual({ ok: true, result: 'from-new' });
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

  test('close waits for the replacement a rollback is already waiting on', async () => {
    // The rollback's exit wait is a floating promise, so a `close()` landing in
    // that window cannot join it. `close()` must confirm the abandoned
    // replacement itself before returning (INV-002), not return while it may
    // still be writing.
    jest.useFakeTimers();
    try {
      const h = switchable({
        startTimeoutMs: 40,
        shutdownTimeoutMs: 30,
        killExits: false,
      });
      await started(h);

      const switching = h.executor.switchTo({
        spawn: () => labelledServer(h, 'new', { readyOnActivate: false }),
      });
      await flushMicrotasks();
      jest.advanceTimersByTime(40 + 1);
      await flushMicrotasks();
      const error = await rejectionOf(switching);
      expect(error.message).toMatch(/did not become ready/);

      // The activated replacement survived its kill: the rollback is waiting.
      const replacement = serverAt(h, 1);
      const killsBeforeClose = replacement.killCount;
      expect(killsBeforeClose).toBeGreaterThanOrEqual(1);
      expect(h.timeline).not.toContain('new:exit');

      const closing = h.executor.close();
      let settled = false;
      void closing.then(() => {
        settled = true;
        return undefined;
      });
      await flushMicrotasks();
      // Still alive: close must not have returned yet.
      expect(settled).toBe(false);
      expect(h.timeline).not.toContain('new:exit');

      // Close runs its own bounded wait → kill → wait for the process the
      // rollback left behind, instead of trusting the rollback's floating wait.
      jest.advanceTimersByTime(2000);
      await flushMicrotasks();
      expect(replacement.killCount).toBeGreaterThan(killsBeforeClose);
      expect(settled).toBe(false);

      // Once the process is observed gone, close can finish.
      replacement.crash(0);
      await flushMicrotasks();
      await closing;

      expect(settled).toBe(true);
      expect(h.timeline).toContain('new:exit');
      expect(h.servers).toHaveLength(2);
    } finally {
      jest.useRealTimers();
    }
  });
});
