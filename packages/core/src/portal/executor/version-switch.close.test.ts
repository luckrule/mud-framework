import { describe, expect, jest, test } from 'bun:test';

import { rejectionOf } from '../../testing/common.ts';
import {
  failureOf,
  flushMicrotasks,
  flushUntil,
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

  test.each([
    {
      state: 'activating',
      startTimeoutMs: 5000,
      // Past the exit wait: close gives up on the still-running replacement and
      // kills it again.
      closingTimersMs: 2000 + 30 + 250,
    },
    {
      state: 'rolling back',
      startTimeoutMs: 40,
      closingTimersMs: 2000,
    },
  ] as const)(
    'close waits for a replacement it abandons while $state',
    async ({ state, startTimeoutMs, closingTimersMs }) => {
      // The replacement received `activate`, so it may hold storage. `doClose`
      // must not return while it could still write (INV-002); it confirms the
      // exit with the same wait → kill → wait flow it uses for the current
      // Server.
      jest.useFakeTimers();
      try {
        const h = switchable({
          startTimeoutMs,
          shutdownTimeoutMs: 30,
          killExits: false,
        });
        await started(h);

        const switching = h.executor.switchTo({
          // Prepares but never answers `activate`, so the replacement can only
          // be abandoned: by the in-flight switch, or by the rollback after its
          // start wait expires.
          spawn: () => labelledServer(h, 'new', { readyOnActivate: false }),
        });
        await flushUntil(() => h.timeline.includes('new:recv:activate'));

        if (state === 'activating') {
          // `close()` lands while the switch waits for `ready`.
          expect(h.timeline).toContain('new:recv:activate');
        } else {
          // The start wait expires first: the switch fails, and close then lands
          // while its rollback is already waiting on the replacement's exit.
          jest.advanceTimersByTime(startTimeoutMs + 1);
          await flushMicrotasks();
          const failure = await rejectionOf(switching);
          expect(failure.message).toMatch(/did not become ready/);
        }

        const replacement = serverAt(h, 1);
        const killsBeforeClose = replacement.killCount;
        if (state === 'rolling back') {
          // The rollback killed it and it survived (killExits: false), so the
          // rollback is still waiting.
          expect(killsBeforeClose).toBeGreaterThanOrEqual(1);
        }
        expect(h.timeline).not.toContain('new:exit');

        const closing = h.executor.close();
        let settled = false;
        void closing.then(() => {
          settled = true;
          return undefined;
        });
        if (state === 'activating') {
          const rejected = await rejectionOf(switching);
          expect(rejected.message).toMatch(/closing/);
        }

        // The abort's kill does not free close(): the replacement is still
        // running and may still write.
        await flushMicrotasks();
        expect(replacement.killCount).toBeGreaterThanOrEqual(1);
        expect(settled).toBe(false);
        expect(h.timeline).not.toContain('new:exit');

        // Close runs its own bounded wait → kill for the process the abort or
        // the rollback left behind.
        jest.advanceTimersByTime(closingTimersMs);
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
    },
  );
});
