import { describe, expect, test } from 'bun:test';

import { openClient, sleep, tempDatabase } from 'mud-framework-test-support';

import {
  callOnSocket,
  createPortal,
  serverPids,
  settleWithin,
  startupFailure,
  stopQuietly,
  urlOf,
} from './support/two-process-harness.ts';

describe('two-process extensions', () => {
  test('an optional extension skipped for unmet capabilities leaves no handlers behind, and the Server keeps serving', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'extension-trial',
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);

      const socket = await openClient(url);
      try {
        // SQLite declares `concurrency: single`, so the pooled-only extension
        // was never registered and its handler does not exist (SPEC 4.3).
        const pooled = await callOnSocket(socket, {
          id: 'pooled',
          method: 'pooled.ping',
        });
        expect(pooled).toMatchObject({
          id: 'pooled',
          ok: false,
          error: { code: 'UNKNOWN_METHOD' },
        });

        // The skipped extension does not take the process down: the same
        // connection, on the next request, still reaches the satisfied one.
        const created = await callOnSocket(socket, {
          id: 'create',
          method: 'player.create',
          payload: { name: 'bob', sessionId: 'session-bob' },
        });
        expect(created).toEqual({
          id: 'create',
          ok: true,
          result: { created: 'bob', sessionId: 'session-bob' },
        });
      } finally {
        socket.close();
      }

      // A restart would mean the skip was treated as a crash; it is not.
      expect(portal.spawns()).toBe(1);
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('a required extension whose requirements are unmet refuses startup', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'required-extension-fail',
      // Same bounded-start shape as the boot-fail case: short deadline and
      // backoff so the retries before the failure complete quickly.
      startTimeoutMs: 500,
      restartBackoffMs: 25,
    });
    const started = portal.layer.start();
    try {
      const outcome = await settleWithin(started, 2000);
      // SPEC 6.2 step 2 refuses the process before `ready`, so Portal must
      // never see a ready Server and must fail the start.
      expect(outcome).toBe('rejected');
      expect(serverPids(portal.logs)).toEqual([]);
      const attemptsAtFailure = portal.spawns();
      expect(attemptsAtFailure).toBeGreaterThan(0);

      // A configuration error must not become an unbounded restart loop.
      await sleep(300);
      expect(portal.spawns()).toBe(attemptsAtFailure);

      // The refusal is provable, not inferred: the child's stderr goes to the
      // inherited terminal, so the file the fixture writes next to the database
      // is the only evidence of why startup was refused. It must name the
      // required extension and the capability the backend could not provide.
      const failure = await startupFailure(database.path);
      expect(failure).toContain('required extension "pooled-only"');
      expect(failure).toContain('concurrency');
    } finally {
      started.catch(() => undefined);
      await stopQuietly(portal);
      database.remove();
    }
  });
});
