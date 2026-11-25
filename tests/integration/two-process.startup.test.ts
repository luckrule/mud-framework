import { describe, expect, test } from 'bun:test';

import {
  isProcessAlive,
  sleep,
  tempDatabase,
} from 'mud-framework-test-support';

import { SqliteBackend } from '../../packages/storage-sqlite/src/index.ts';

import {
  call,
  createPortal,
  serverPids,
  settleWithin,
  stopQuietly,
  urlOf,
} from './support/two-process-harness.ts';

describe('two-process startup and stop', () => {
  test('a WebSocket request is executed by the Server process and its write lands in SQLite', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'echo',
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);

      const echoed = await call(url, {
        id: 'r1',
        method: 'echo',
        payload: { hello: 'world' },
      });
      expect(echoed).toEqual({
        id: 'r1',
        ok: true,
        result: { hello: 'world' },
      });

      const stored = await call(url, {
        id: 'r2',
        method: 'store',
        payload: 'note-1',
      });
      expect(stored).toEqual({
        id: 'r2',
        ok: true,
        result: { stored: 'note-1' },
      });

      // `stopQuietly` is `stop()` plus a SIGKILL for every pid that reported
      // ready: `stop()` alone can time out and return while the Server is still
      // alive, and the reader below must not race a writer.
      await stopQuietly(portal);

      // The Portal never opened the file: this handle is a separate reader, so
      // the row proves the Server's commit outlived its own process.
      const reader = SqliteBackend.open({ filename: database.path });
      try {
        const rows = await reader.transaction((tx) =>
          tx.query<{ body: string }>('SELECT body FROM notes'),
        );
        expect(rows).toEqual([{ body: 'note-1' }]);
      } finally {
        await reader.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('stop() returns only after the Server process has exited', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'echo',
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);
      await call(url, { id: 'r1', method: 'echo', payload: 'ping' });

      const pids = serverPids(portal.logs);
      const pid = pids.at(-1);
      if (pid === undefined) {
        throw new Error('the Server never reported a pid');
      }

      await portal.layer.stop();

      // A Server still running here would keep writing to the storage it no
      // longer exclusively owns (INV-002).
      expect(isProcessAlive(pid)).toBe(false);
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('start() fails within a bounded time when the Server cannot boot', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'boot-fail',
      // Shortened from 1500/100 only to cut the fixed wait: with a 25ms base
      // the retries before the 500ms deadline still step through the immediate
      // restart and several backoffs (0, 25, 50, 100, 200, 400), so the same
      // bounded-start path is exercised, not bypassed.
      startTimeoutMs: 500,
      restartBackoffMs: 25,
    });
    const started = portal.layer.start();
    try {
      const outcome = await settleWithin(started, 2000);
      // A readiness barrier that never lifts must fail the start, not leave the
      // caller waiting forever behind a restart loop.
      expect(outcome).toBe('rejected');
      const attemptsAtFailure = portal.spawns();
      expect(attemptsAtFailure).toBeGreaterThan(0);

      // The restart loop must stop with the failure: continuing to spawn on a
      // configuration error is the "unbounded restart" this scenario is for.
      // 300ms is many retry cycles at this backoff base, so any restart that
      // survived the failure would still be seen here.
      await sleep(300);
      expect(portal.spawns()).toBe(attemptsAtFailure);
    } finally {
      // `close()` rejects a start still waiting for `ready`, so the race above
      // must not leave an unhandled rejection behind.
      started.catch(() => undefined);
      await stopQuietly(portal);
      database.remove();
    }
  });
});
