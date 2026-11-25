import { describe, expect, jest, test } from 'bun:test';

import { rejectionOf } from '../../testing/common.ts';
import {
  failureOf,
  flushMicrotasks,
  request,
  serverAt,
  started,
} from '../../testing/ipc-executor.ts';
import {
  labelledServer,
  messagesTo,
  switchable,
} from './version-switch.support.ts';

describe('IpcExecutor switchTo', () => {
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
