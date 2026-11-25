import { createJsonLogger, runServerProcess } from 'mud-framework-core';
import type { Extension } from 'mud-framework-core';
import { SqliteBackend } from 'mud-framework-storage-sqlite';

/**
 * Server half of the e2e benchmark, spawned by `fixture-portal.ts` through
 * `spawnBunServer`, so it must run under Bun. Everything is imported by
 * package name, as a consumer would.
 *
 * argv: [databasePath]. Logs are dropped except for a small ring buffer that
 * is flushed to stderr if the process exits abnormally: a per-request sink
 * would add file or pipe I/O to the path under measurement, and stderr is
 * inherited by the Portal, so a failure still reaches the benchmark's
 * terminal.
 *
 * `echo` returns its payload. `store` does one INSERT inside the request's
 * framework transaction; the table is created once in `createBackend`, before
 * the readiness barrier, so the measured path contains no DDL.
 */

const LOG_TAIL = 20;

const logTail: string[] = [];
const logger = createJsonLogger({
  level: 'info',
  sink: (line) => {
    logTail.push(line);
    if (logTail.length > LOG_TAIL) {
      logTail.shift();
    }
  },
});

function seqOf(payload: unknown): number {
  if (
    typeof payload !== 'number' ||
    !Number.isSafeInteger(payload) ||
    payload < 0
  ) {
    throw new Error('store expects a non-negative integer seq payload');
  }
  return payload;
}

const extension: Extension = {
  name: 'bench',
  requires: { transactions: 'full' },
  register: (registrar) => {
    // The benchmark client has no login step and neither handler touches a
    // Session, so both opt out of the Session gate explicitly (SPEC 3.7).
    registrar.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });
    registrar.register(
      'store',
      async (ctx, payload) => {
        const seq = seqOf(payload);
        await ctx.storage.execute(
          'INSERT INTO bench_rows (seq, body) VALUES (?, ?)',
          [seq, `row-${seq}`],
        );
        return { stored: seq };
      },
      { access: 'public' },
    );
  },
};

function reportExit(error: unknown): void {
  const message = error instanceof Error ? error.message : String(error);
  for (const line of logTail) {
    process.stderr.write(`bench server log: ${line}\n`);
  }
  // stderr is inherited by the Portal, so this needs no file to be observable.
  process.stderr.write(`bench server failed: ${message}\n`);
  process.exitCode = 1;
}

async function main(databasePath: string): Promise<void> {
  try {
    await runServerProcess({
      createBackend: async () => {
        const backend = SqliteBackend.open({ filename: databasePath, logger });
        await backend.transaction(async (tx) => {
          await tx.execute(
            'CREATE TABLE IF NOT EXISTS bench_rows (seq INTEGER PRIMARY KEY, body TEXT NOT NULL)',
          );
        });
        return backend;
      },
      extensions: [extension],
      timeoutMs: 10_000,
      logger,
    });
  } catch (error) {
    reportExit(error);
  }
}

const databasePath = process.argv[2];
if (databasePath === undefined) {
  process.stderr.write('bench server: missing database path argument\n');
  process.exitCode = 1;
} else {
  await main(databasePath);
}
