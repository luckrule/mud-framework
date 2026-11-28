import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export interface TempDatabase {
  readonly directory: string;
  readonly path: string;
  remove(): void;
}

/**
 * A fresh SQLite path in its own temp directory. `remove` deletes the whole
 * directory, so a test must not share a directory between two databases.
 */
export function tempDatabase(
  options: {
    readonly prefix?: string;
    readonly filename?: string;
  } = {},
): TempDatabase {
  const directory = mkdtempSync(join(tmpdir(), options.prefix ?? 'mud-test-'));
  return {
    directory,
    path: join(directory, options.filename ?? 'server.sqlite'),
    remove(): void {
      rmSync(directory, { recursive: true, force: true });
    },
  };
}
