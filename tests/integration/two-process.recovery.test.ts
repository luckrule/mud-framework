import { describe, expect, test } from 'bun:test';

import { openClient, tempDatabase } from 'mud-framework-test-support';

import { SqliteBackend } from '../../packages/storage-sqlite/src/index.ts';

import {
  attemptLog,
  call,
  callOnSocket,
  createPortal,
  replayCounts,
  serverPids,
  stopQuietly,
  urlOf,
} from './support/two-process-harness.ts';

describe('two-process crash recovery', () => {
  test('a Server that dies mid-request is replayed to exhaustion while the Portal keeps serving', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'crash',
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);

      const failed = await call(url, { id: 'boom', method: 'crash' });
      expect(failed.id).toBe('boom');
      expect(failed.ok).toBe(false);
      expect(failed.error).toMatchObject({
        code: 'FRAMEWORK_REPLAY_EXHAUSTED',
      });
      // One replay per restart, counted on the Portal side; the sequence is what
      // makes "which attempt of which input" answerable from the logs.
      expect(replayCounts(portal.logs)).toEqual([1, 2, 3]);
      // One process per attempt plus the one that finds the budget spent. The
      // exact count is an implementation detail; the bound is the contract: a
      // bad input must not keep spawning processes.
      expect(portal.spawns()).toBeGreaterThanOrEqual(4);
      expect(portal.spawns()).toBeLessThanOrEqual(6);
      expect(serverPids(portal.logs).length).toBe(portal.spawns());

      // The Portal itself survived every crash: the same executor, on a new
      // connection, still answers.
      const after = await call(url, {
        id: 'after',
        method: 'echo',
        payload: 'still here',
      });
      expect(after).toEqual({ id: 'after', ok: true, result: 'still here' });
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('a crash on the first execution is replayed and the same request succeeds', async () => {
    const database = tempDatabase({ prefix: 'mud-two-process-' });
    const portal = createPortal({
      databasePath: database.path,
      behavior: 'crash-once',
      timeoutMs: 5000,
    });
    try {
      await portal.layer.start();
      const url = urlOf(portal);

      // One socket for the whole exchange: the connection survives the Server
      // crash, so the reply comes from the replay rather than a reconnect
      // (INV-012).
      const socket = await openClient(url);
      try {
        const frame = await callOnSocket(socket, {
          id: 'once',
          method: 'store-once',
          payload: 'note-once',
        });
        expect(frame).toEqual({
          id: 'once',
          ok: true,
          result: { stored: 'note-once' },
        });

        // The fixture appends the framework `requestId` before crashing, so the
        // two lines are the same Request seen by the first attempt and by the
        // replay: `requestId` is what replay preserves.
        const attempts = attemptLog(database.path);
        expect(attempts).toHaveLength(2);
        expect(attempts[0]).toBe(attempts[1]);
        expect(replayCounts(portal.logs)).toEqual([1]);
        const replayed = portal.logs.lines.filter(
          (line) => line.msg === 'request replayed',
        );
        expect(replayed).toHaveLength(1);
        expect(replayed.at(0)).toMatchObject({
          requestId: attempts[0],
          clientRequestId: 'once',
          replayCount: 1,
        });
        // The first process crashed; the second is the replayer that answered.
        // The exact count is an implementation detail, as in the crash test
        // above: `replayCounts` having one entry is what proves there was
        // exactly one restart, not a retry loop.
        expect(portal.spawns()).toBeGreaterThanOrEqual(2);
        expect(serverPids(portal.logs).length).toBeGreaterThanOrEqual(2);
      } finally {
        socket.close();
      }

      await stopQuietly(portal);

      // Two handler executions, one committed effect. `once_executions` is the
      // evidence: it is not idempotent, so its single row means the interrupted
      // attempt's transaction never reached COMMIT and the replay committed
      // once. The interrupted attempt rolled back, so the replay does not hit
      // `ON CONFLICT DO NOTHING` either; that guard is for a client retry,
      // which keeps `clientRequestId` but arrives with a new `requestId`. A
      // non-transactional side effect can still run twice on replay (INV-011).
      const reader = SqliteBackend.open({ filename: database.path });
      try {
        const stored = await reader.transaction(async (tx) => ({
          notes: await tx.query<{ client_request_id: string; body: string }>(
            'SELECT client_request_id, body FROM once_notes',
          ),
          executions: await tx.query<{ client_request_id: string }>(
            'SELECT client_request_id FROM once_executions',
          ),
        }));
        expect(stored.notes).toEqual([
          { client_request_id: 'once', body: 'note-once' },
        ]);
        expect(stored.executions).toEqual([{ client_request_id: 'once' }]);
      } finally {
        await reader.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });
});
