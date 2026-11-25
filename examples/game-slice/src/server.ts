import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { runServerProcess } from 'mud-framework-core';
import { SqliteBackend } from 'mud-framework-storage-sqlite';

import { gameExtension } from './game.ts';
import { initializeWorld } from './world.ts';

/**
 * The only storage writer for the slice (INV-002). The Portal never imports
 * this file's storage or opens the database.
 *
 * `GAME_SLICE_DATA_DIR` lets the integration test and the demo point the
 * Server at a temporary directory; the default sits beside the example.
 */
const DEFAULT_DATA_DIRECTORY = fileURLToPath(
  new URL('../data/', import.meta.url),
);
const DATABASE_FILENAME = 'game.sqlite';

function dataDirectory(): string {
  const override = process.env.GAME_SLICE_DATA_DIR;
  return override !== undefined && override.length > 0
    ? override
    : DEFAULT_DATA_DIRECTORY;
}

async function openBackend(): Promise<SqliteBackend> {
  const directory = dataDirectory();
  mkdirSync(directory, { recursive: true });
  const backend = SqliteBackend.open({
    filename: join(directory, DATABASE_FILENAME),
  });
  try {
    // Runs after `activate` and before `ready` (ADR-0007), so no business
    // request can arrive before the schema and seeds exist.
    await initializeWorld(backend);
  } catch (error) {
    await backend.close().catch(() => undefined);
    throw error;
  }
  return backend;
}

function messageOf(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

try {
  await runServerProcess({
    createBackend: openBackend,
    extensions: [gameExtension()],
    timeoutMs: 5000,
  });
} catch (error) {
  // A non-zero exit is what process management and the Portal use to tell an
  // abnormal stop from the orderly shutdown path.
  process.stderr.write(`server stopped abnormally: ${messageOf(error)}\n`);
  process.exitCode = 1;
}
