import { describe, expect, jest, test } from 'bun:test';

import { rejectionOf } from '../../testing/common.ts';
import {
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
import type { FakeServerProcess } from '../../testing/ipc-executor.ts';
import {
  labelledServer,
  messagesTo,
  NEW_PID,
  SWITCH_COMPLETED,
  switchable,
} from './version-switch.support.ts';

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
});
