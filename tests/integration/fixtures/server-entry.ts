import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';

import { runServerProcess } from '../../../packages/core/src/index.ts';
import type { Extension } from '../../../packages/core/src/index.ts';
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
 * `extension-trial` loads an extension whose requirements SQLite meets and one
 * that needs `concurrency: pooled` (skipped on SQLite);
 * `required-extension-fail` makes that same pooled-only extension required, so
 * the process refuses to start.
 *
 * `crash-once` writes the framework `requestId` of every handler invocation to
 * `<databasePath>.attempts`; that file is outside the transaction and therefore
 * survives the crash, unlike the rows the interrupted attempt wrote.
 *
 * A startup failure is written to `<databasePath>.startup-error` for the same
 * reason: this process's stderr is inherited by Portal, so a test that captures
 * nothing has no other way to see why startup was refused. Like `.attempts`,
 * the file sits in the temporary database directory and goes away with it.
 */
const BEHAVIORS = [
  'echo',
  'crash',
  'crash-once',
  'boot-fail',
  'extension-trial',
  'required-extension-fail',
] as const;

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

/**
 * Every requirement is met by `SqliteBackend` (SPEC 4.3): full transactions,
 * snapshot reads and unique constraints. The handler returns the bound Session
 * in its own payload because the wire result carries only the handler value;
 * `ctx.bindSession` commits the route, and the next request on the connection
 * sees it as `ctx.sessionId`.
 */
function playersExtension(): Extension {
  return {
    name: 'players',
    requires: {
      transactions: 'full',
      snapshotRead: true,
      uniqueConstraint: true,
    },
    register: (registrar) => {
      registrar.register('player.create', async (ctx, payload) => {
        const { name, sessionId } = payload as {
          name: string;
          sessionId: string;
        };
        ctx.bindSession(sessionId);
        await ctx.storage.execute(
          'CREATE TABLE IF NOT EXISTS players (name TEXT NOT NULL)',
        );
        await ctx.storage.execute('INSERT INTO players (name) VALUES (?)', [
          name,
        ]);
        return { created: name, sessionId };
      });
      registrar.register('player.whoami', (ctx) =>
        Promise.resolve({ sessionId: ctx.sessionId ?? null }),
      );
    },
  };
}

/**
 * SQLite declares `concurrency: single`, so this extension is skipped as
 * optional and refuses startup as required (SPEC 4.3, SPEC 6.2 step 2).
 */
function pooledOnlyExtension(required: boolean): Extension {
  return {
    name: 'pooled-only',
    requires: { concurrency: 'pooled' },
    ...(required ? { required: true } : {}),
    register: (registrar) => {
      registrar.register('pooled.ping', () => Promise.resolve({ pong: true }));
    },
  };
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

const extensions: Extension[] = [
  {
    name: 'notes',
    register: (dispatcher) => {
      dispatcher.register('echo', (_ctx, payload) => Promise.resolve(payload));
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
];

if (behavior === 'extension-trial') {
  extensions.push(playersExtension(), pooledOnlyExtension(false));
}
if (behavior === 'required-extension-fail') {
  extensions.push(pooledOnlyExtension(true));
}

try {
  await runServerProcess({
    createBackend: () => SqliteBackend.open({ filename: databasePath }),
    extensions,
    timeoutMs: 2000,
  });
} catch (error) {
  // stderr is inherited by Portal, so this line stays visible without touching
  // the IPC channel or Portal's stdout. The file is what makes the reason
  // observable to a caller that captures no stderr at all.
  const message = error instanceof Error ? error.message : String(error);
  console.error('server process failed:', message);
  writeFileSync(`${databasePath}.startup-error`, `${message}\n`);
  process.exitCode = 1;
}
