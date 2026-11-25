import { randomUUID } from 'node:crypto';
import { mkdirSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { runServerProcess } from 'mud-framework-core';
import type { Extension } from 'mud-framework-core';
import { SqliteBackend } from 'mud-framework-storage-sqlite';

// Resolved from this module, not the working directory: `bun run` from the
// repository root must still put the database under `examples/minimal/data/`.
const DATA_DIRECTORY = fileURLToPath(new URL('../data/', import.meta.url));
const DATABASE_PATH = join(DATA_DIRECTORY, 'world.sqlite');

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function readPlayerName(payload: unknown): string {
  const name = isRecord(payload) ? payload.name : undefined;
  if (typeof name !== 'string' || name.trim().length === 0) {
    throw new Error('player.login requires a non-empty string payload.name');
  }
  return name.trim();
}

/**
 * The whole business example. It demonstrates assembly and the access gate
 * (SPEC 3.7), not authentication: any non-empty name logs in, and nothing
 * checks a password or a role.
 */
function playersExtension(): Extension {
  return {
    name: 'players',
    requires: { transactions: 'full', uniqueConstraint: true },
    register: (registrar) => {
      registrar.register(
        'player.login',
        async (ctx, payload) => {
          const name = readPlayerName(payload);
          // A real application would validate credentials first and choose a
          // retention policy for the Session id; the framework only requires a
          // non-empty id and commits it after this request succeeds.
          const sessionId = `player:${name}:${randomUUID()}`;
          ctx.bindSession(sessionId);
          await ctx.storage.execute(
            `INSERT INTO players (name, session_id, logged_in_at)
             VALUES (?, ?, ?)
             ON CONFLICT(name) DO UPDATE SET
               session_id = excluded.session_id,
               logged_in_at = excluded.logged_in_at`,
            [name, sessionId, new Date().toISOString()],
          );
          return { player: name, sessionId };
        },
        { access: 'public', mayChangeSession: true },
      );

      // Default access is `session`: without a bound Session this is rejected
      // before the handler runs.
      registrar.register('player.whoami', async (ctx) => {
        const sessionId = ctx.sessionId ?? '';
        const rows = await ctx.storage.query<{ name: string }>(
          'SELECT name FROM players WHERE session_id = ?',
          [sessionId],
        );
        return { sessionId, player: rows[0]?.name ?? null };
      });

      registrar.register(
        'player.logout',
        (ctx) => {
          // Removing the route is a Session change, so it needs both a Session
          // to end and the explicit permission below.
          ctx.unbindSession();
          return Promise.resolve({ loggedOut: true });
        },
        { access: 'session', mayChangeSession: true },
      );
    },
  };
}

async function openBackend(): Promise<SqliteBackend> {
  // Runs after `activate` and before `ready` (ADR-0007), so no business
  // request can arrive before the directory and schema exist.
  mkdirSync(DATA_DIRECTORY, { recursive: true });
  const backend = SqliteBackend.open({ filename: DATABASE_PATH });
  await backend.transaction(async (tx) => {
    await tx.execute(
      `CREATE TABLE IF NOT EXISTS players (
         name TEXT NOT NULL PRIMARY KEY,
         session_id TEXT NOT NULL,
         logged_in_at TEXT NOT NULL
       )`,
    );
  });
  return backend;
}

try {
  await runServerProcess({
    createBackend: openBackend,
    extensions: [playersExtension()],
    timeoutMs: 5000,
  });
} catch (error) {
  // A non-zero exit is what process management and the Portal use to tell an
  // abnormal stop from the orderly shutdown path.
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`server stopped abnormally: ${message}\n`);
  process.exitCode = 1;
}
