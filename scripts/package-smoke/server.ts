import { appendFileSync } from 'node:fs';

import { createJsonLogger, runServerProcess } from 'mud-framework-core';
import type { Extension } from 'mud-framework-core';
import { SqliteBackend } from 'mud-framework-storage-sqlite';
import { isRecord } from 'mud-framework-test-support';

/**
 * Server half of the package-name smoke. `spawnBunServer` starts this file, so
 * it must run under Bun, and it reaches the framework only through the package
 * names a consumer would use.
 *
 * argv: [databasePath]. Logs go to `<databasePath>.log` for the same reason the
 * integration fixtures write failure files: the child's stderr is inherited by
 * the Portal, so the orchestrator cannot capture it for a failure report.
 */

function valueOf(payload: unknown): string {
  if (!isRecord(payload) || typeof payload.value !== 'string') {
    throw new Error('smoke.store expects { value: string }');
  }
  return payload.value;
}

const extension: Extension = {
  name: 'package-smoke',
  requires: { transactions: 'full', uniqueConstraint: true },
  register: (registrar) => {
    // The smoke has no login step: it proves the package-level assembly, not
    // access control, so the only handler is public.
    registrar.register(
      'smoke.store',
      async (ctx, payload) => {
        const value = valueOf(payload);
        await ctx.storage.execute(
          'CREATE TABLE IF NOT EXISTS smoke_rows (value TEXT NOT NULL)',
        );
        await ctx.storage.execute('INSERT INTO smoke_rows (value) VALUES (?)', [
          value,
        ]);
        return { stored: value };
      },
      { access: 'public' },
    );
  },
};

async function main(databasePath: string): Promise<void> {
  const logPath = `${databasePath}.log`;
  const logger = createJsonLogger({
    level: 'info',
    sink: (line) => {
      appendFileSync(logPath, `${line}\n`);
    },
  });

  try {
    await runServerProcess({
      createBackend: () =>
        SqliteBackend.open({ filename: databasePath, logger }),
      extensions: [extension],
      timeoutMs: 5000,
      logger,
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    appendFileSync(
      logPath,
      `${JSON.stringify({ level: 'error', msg: 'server exited abnormally', error: message })}\n`,
    );
    process.stderr.write(`package-smoke server failed: ${message}\n`);
    process.exitCode = 1;
  }
}

const databasePath = process.argv[2];
if (databasePath === undefined) {
  process.stderr.write(
    'package-smoke server: missing database path argument\n',
  );
  process.exitCode = 1;
} else {
  await main(databasePath);
}
