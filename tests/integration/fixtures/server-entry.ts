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
 * observed.
 */
const BEHAVIORS = ['echo', 'crash', 'boot-fail'] as const;

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
    },
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
