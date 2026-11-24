import { appendFileSync, readFileSync } from 'node:fs';

import { runServerProcess } from '../../../packages/core/src/index.ts';
import { SqliteBackend } from '../../../packages/storage-sqlite/src/index.ts';

/**
 * Real Server entry for the two-process integration test. It is spawned by
 * `spawnBunServer`, so it must run under Bun: the `ipc` serialization is
 * Bun-only, and a Node child's messages never reach the Portal.
 *
 * argv: [behavior, databasePath]. `boot-fail` exits non-zero before the
 * readiness barrier so Portal's bounded-start behaviour can be observed;
 * `crash` adds a handler that kills the process mid-request so replay can be
 * observed; `crash-once` adds `store-once`, which kills the process on its
 * first execution only, so the replacement can commit the same request.
 *
 * `crash-once` writes the framework `requestId` of every handler invocation to
 * `<databasePath>.attempts`; that file is outside the transaction and therefore
 * survives the crash, unlike the rows the interrupted attempt wrote.
 */
const BEHAVIORS = ['echo', 'crash', 'crash-once', 'boot-fail'] as const;

type Behavior = (typeof BEHAVIORS)[number];

/**
 * Startup rejections are configuration errors, not crashes, and the Portal
 * restarts a Server that exits, so an uncaught throw would print a full stack
 * once per attempt. One line plus a non-zero exit is the whole signal the
 * caller needs. Nothing has been started yet, so there is nothing to clean up
 * and no reason to defer the exit through `process.exitCode`.
 */
function failStartup(message: string): never {
  console.error(`server process failed: ${message}`);
  // oxlint-disable-next-line unicorn/no-process-exit -- the caller observes only the exit code, and returning would let the entry continue past a bad argv
  process.exit(1);
}

/** Pure lookup; the caller owns reporting so no throw can escape to the top. */
function readBehavior(raw: string | undefined): Behavior | undefined {
  return BEHAVIORS.find((candidate) => candidate === (raw ?? 'echo'));
}

/**
 * Appends the framework `requestId` and returns the number of recorded
 * executions. The file, not the transaction, is the evidence: a crash before
 * COMMIT rolls the row back but leaves the line behind.
 */
function recordAttempt(path: string, requestId: string): number {
  appendFileSync(path, `${requestId}\n`);
  return readFileSync(path, 'utf8')
    .split('\n')
    .filter((line) => line.length > 0).length;
}

const rawBehavior = process.argv[2];
const behavior = readBehavior(rawBehavior);
if (behavior === undefined) {
  failStartup(
    `unknown server behaviour ${JSON.stringify(rawBehavior ?? 'echo')}; expected one of ${BEHAVIORS.join(', ')}`,
  );
}

if (behavior === 'boot-fail') {
  failStartup('boot failed');
}

const databasePath = process.argv[3];
if (databasePath === undefined) {
  failStartup('server-entry requires a database path as the second argument');
}

try {
  await runServerProcess({
    createBackend: () => SqliteBackend.open({ filename: databasePath }),
    extensions: [
      {
        name: 'notes',
        register: (dispatcher) => {
          dispatcher.register('echo', (_ctx, payload) =>
            Promise.resolve(payload),
          );
          dispatcher.register('store', async (ctx, payload) => {
            await ctx.storage.execute(
              'CREATE TABLE IF NOT EXISTS notes (body TEXT NOT NULL)',
            );
            await ctx.storage.execute('INSERT INTO notes (body) VALUES (?)', [
              String(payload),
            ]);
            return { stored: payload };
          });
          if (behavior === 'crash') {
            dispatcher.register('crash', () => {
              // oxlint-disable-next-line unicorn/no-process-exit -- the crash must land mid-handler; a graceful stop would let the result out and stop exercising replay
              process.exit(1);
            });
          }
          if (behavior === 'crash-once') {
            dispatcher.register('store-once', async (ctx, payload) => {
              await ctx.storage.execute(
                'CREATE TABLE IF NOT EXISTS once_notes (client_request_id TEXT PRIMARY KEY, body TEXT NOT NULL)',
              );
              // Deliberately not idempotent: its row count shows whether the
              // interrupted attempt's transaction ever committed.
              await ctx.storage.execute(
                'CREATE TABLE IF NOT EXISTS once_executions (id INTEGER PRIMARY KEY AUTOINCREMENT, client_request_id TEXT NOT NULL)',
              );
              await ctx.storage.execute(
                'INSERT INTO once_executions (client_request_id) VALUES (?)',
                [ctx.clientRequestId],
              );
              // `clientRequestId`, not `requestId`, is the business idempotency
              // key: `requestId` is minted per Request and only stable across
              // that Request's own replays, so a client retry would carry a new
              // one, while `clientRequestId` stays the same (SPEC 6.4).
              //
              // This test never reaches the conflict branch: the interrupted
              // attempt's transaction rolled back, so the replay inserts a
              // clean row. `once_executions` having exactly one row is what
              // shows only one execution was ever committed; the guard here
              // only covers the case a real client retry creates.
              await ctx.storage.execute(
                'INSERT INTO once_notes (client_request_id, body) VALUES (?, ?) ON CONFLICT(client_request_id) DO NOTHING',
                [ctx.clientRequestId, String(payload)],
              );
              const attempt = recordAttempt(
                `${databasePath}.attempts`,
                ctx.requestId,
              );
              if (attempt === 1) {
                // oxlint-disable-next-line unicorn/no-process-exit -- the crash must land before COMMIT; returning would commit the row and end the crash path being tested
                process.exit(1);
              }
              return { stored: payload };
            });
          }
        },
      },
    ],
    timeoutMs: 2000,
  });
} catch (error) {
  // stderr is inherited by Portal, so this line stays visible without touching
  // the IPC channel or Portal's stdout.
  console.error(
    'server process failed:',
    error instanceof Error ? error.message : String(error),
  );
  process.exitCode = 1;
}
