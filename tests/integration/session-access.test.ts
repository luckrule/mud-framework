import { describe, expect, test } from 'bun:test';
import { join } from 'node:path';

import {
  isRecord,
  nextMessage,
  openClient,
  parseFrame,
  tempDatabase,
  withDeadline,
} from 'mud-framework-test-support';

import { WsTransport } from '../../packages/connector-ws/src/index.ts';
import {
  ConnectionLayer,
  createJsonLogger,
  IpcExecutor,
  spawnBunServer,
} from '../../packages/core/src/index.ts';
import type { Logger } from '../../packages/core/src/index.ts';
import { SqliteBackend } from '../../packages/storage-sqlite/src/index.ts';

/**
 * Access control over the real path: a WebSocket client, a real Portal child
 * and a real Server child running the `extension-trial` fixture. The assertions
 * use only the wire envelope and the business rows the fixture writes, never
 * framework internals (SPEC 3.7).
 */
const FIXTURE = join(import.meta.dir, 'fixtures', 'server-entry.ts');
const REQUEST_TIMEOUT_MS = 5000;

interface Portal {
  readonly layer: ConnectionLayer;
  readonly transport: WsTransport;
  readonly lines: Record<string, unknown>[];
}

function createPortal(databasePath: string): Portal {
  const lines: Record<string, unknown>[] = [];
  const logger: Logger = createJsonLogger({
    level: 'debug',
    sink: (line) => {
      const parsed: unknown = JSON.parse(line);
      if (isRecord(parsed)) {
        lines.push(parsed);
      }
    },
  });
  const executor = new IpcExecutor({
    spawn: () =>
      spawnBunServer({
        command: [
          process.execPath,
          'run',
          FIXTURE,
          'extension-trial',
          databasePath,
        ],
      }),
    timeoutMs: REQUEST_TIMEOUT_MS,
    startTimeoutMs: 10_000,
    logger,
  });
  const transport = new WsTransport({ port: 0, logger });
  const layer = new ConnectionLayer({ transport, executor, logger });
  return { layer, transport, lines };
}

function urlOf(portal: Portal): string {
  const port = portal.transport.port;
  if (port === undefined) {
    throw new Error('expected the transport to bind a port');
  }
  return `ws://127.0.0.1:${port}/`;
}

async function call(
  socket: WebSocket,
  request: { id: string; method: string; payload?: unknown },
): Promise<Record<string, unknown>> {
  const reply = nextMessage(socket);
  socket.send(JSON.stringify(request));
  return parseFrame(
    await withDeadline(reply, REQUEST_TIMEOUT_MS, `reply to ${request.id}`),
  );
}

/** Pids from `ready` lines, so a failed test cannot leave a Server behind. */
function serverPids(lines: readonly Record<string, unknown>[]): number[] {
  const pids: number[] = [];
  for (const line of lines) {
    if (line.msg === 'server ready' && typeof line.pid === 'number') {
      pids.push(line.pid);
    }
  }
  return pids;
}

async function stopQuietly(portal: Portal): Promise<void> {
  try {
    await portal.layer.stop();
  } catch {
    // The test's own failure is the one to report.
  }
  for (const pid of serverPids(portal.lines)) {
    try {
      process.kill(pid, 'SIGKILL');
    } catch {
      // Already gone, which is the expected case.
    }
  }
}

/**
 * Rows written by `player.probe`. The table itself only exists once the handler
 * has run, so zero also covers "the access gate refused before the handler".
 */
async function probeExecutions(databasePath: string): Promise<number> {
  const reader = SqliteBackend.open({ filename: databasePath });
  try {
    const [table] = await reader.transaction((tx) =>
      tx.query<{ total: number }>(
        "SELECT COUNT(*) AS total FROM sqlite_master WHERE type = 'table' AND name = 'probes'",
      ),
    );
    if (table === undefined || table.total === 0) {
      return 0;
    }
    const [rows] = await reader.transaction((tx) =>
      tx.query<{ total: number }>('SELECT COUNT(*) AS total FROM probes'),
    );
    return rows?.total ?? 0;
  } finally {
    await reader.close();
  }
}

describe('session access over a real WebSocket and IPC', () => {
  test('an unbound request to a session method gets AUTH_REQUIRED before the handler runs', async () => {
    const database = tempDatabase({ prefix: 'mud-session-access-' });
    const portal = createPortal(database.path);
    try {
      await portal.layer.start();
      const socket = await openClient(urlOf(portal), {
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      try {
        const whoami = await call(socket, {
          id: 'whoami-unbound',
          method: 'player.whoami',
        });
        expect(whoami).toMatchObject({
          id: 'whoami-unbound',
          ok: false,
          error: { code: 'AUTH_REQUIRED' },
        });

        const probe = await call(socket, {
          id: 'probe-unbound',
          method: 'player.probe',
        });
        expect(probe).toMatchObject({
          id: 'probe-unbound',
          ok: false,
          error: { code: 'AUTH_REQUIRED' },
        });
      } finally {
        socket.close();
      }

      await stopQuietly(portal);

      // `player.probe` writes when it runs. No table means the gate refused
      // both requests before their transactions and handlers existed.
      expect(await probeExecutions(database.path)).toBe(0);
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('a successful public login binds the Session for later requests on the same connection', async () => {
    const database = tempDatabase({ prefix: 'mud-session-access-' });
    const portal = createPortal(database.path);
    try {
      await portal.layer.start();
      const socket = await openClient(urlOf(portal), {
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      try {
        const created = await call(socket, {
          id: 'login',
          method: 'player.create',
          payload: { name: 'alice', sessionId: 'session-alice' },
        });
        expect(created).toEqual({
          id: 'login',
          ok: true,
          result: { created: 'alice', sessionId: 'session-alice' },
        });

        const whoami = await call(socket, {
          id: 'whoami',
          method: 'player.whoami',
        });
        expect(whoami).toEqual({
          id: 'whoami',
          ok: true,
          result: { sessionId: 'session-alice' },
        });

        // The same handler that was refused above now runs, which is what
        // makes its absence in the unbound test meaningful.
        const probe = await call(socket, {
          id: 'probe',
          method: 'player.probe',
        });
        expect(probe).toEqual({
          id: 'probe',
          ok: true,
          result: { sessionId: 'session-alice' },
        });
      } finally {
        socket.close();
      }

      await stopQuietly(portal);
      expect(await probeExecutions(database.path)).toBe(1);
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('a login that throws after bindSession leaves the connection unbound', async () => {
    const database = tempDatabase({ prefix: 'mud-session-access-' });
    const portal = createPortal(database.path);
    try {
      await portal.layer.start();
      const socket = await openClient(urlOf(portal), {
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      try {
        const created = await call(socket, {
          id: 'login-fail',
          method: 'player.create',
          payload: {
            name: 'bob',
            sessionId: 'session-bob',
            fail: true,
          },
        });
        expect(created).toMatchObject({
          id: 'login-fail',
          ok: false,
          error: {
            code: 'INTERNAL_ERROR',
            message: expect.stringContaining(
              'player.create failed after bindSession',
            ),
          },
        });

        // The handler reached bindSession, but the failed request must not
        // commit the route (SPEC 2).
        const whoami = await call(socket, {
          id: 'whoami-after-failure',
          method: 'player.whoami',
        });
        expect(whoami).toMatchObject({
          id: 'whoami-after-failure',
          ok: false,
          error: { code: 'AUTH_REQUIRED' },
        });
      } finally {
        socket.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('logout unbinds the Session so later session methods are refused again', async () => {
    const database = tempDatabase({ prefix: 'mud-session-access-' });
    const portal = createPortal(database.path);
    try {
      await portal.layer.start();
      const socket = await openClient(urlOf(portal), {
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      try {
        const created = await call(socket, {
          id: 'login',
          method: 'player.create',
          payload: { name: 'carol', sessionId: 'session-carol' },
        });
        expect(created).toMatchObject({ id: 'login', ok: true });

        const before = await call(socket, {
          id: 'whoami-before',
          method: 'player.whoami',
        });
        expect(before).toEqual({
          id: 'whoami-before',
          ok: true,
          result: { sessionId: 'session-carol' },
        });

        const logout = await call(socket, {
          id: 'logout',
          method: 'player.logout',
        });
        expect(logout).toEqual({
          id: 'logout',
          ok: true,
          result: { loggedOut: true },
        });

        const after = await call(socket, {
          id: 'whoami-after',
          method: 'player.whoami',
        });
        expect(after).toMatchObject({
          id: 'whoami-after',
          ok: false,
          error: { code: 'AUTH_REQUIRED' },
        });
      } finally {
        socket.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('an access policy refusal returns FORBIDDEN before the session handler runs', async () => {
    const database = tempDatabase({ prefix: 'mud-session-access-' });
    const portal = createPortal(database.path);
    try {
      await portal.layer.start();
      const socket = await openClient(urlOf(portal), {
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      try {
        const created = await call(socket, {
          id: 'login',
          method: 'player.create',
          payload: { name: 'dave', sessionId: 'session-dave' },
        });
        expect(created).toMatchObject({ id: 'login', ok: true });

        const forbidden = await call(socket, {
          id: 'forbidden',
          method: 'player.forbidden',
        });
        expect(forbidden).toMatchObject({
          id: 'forbidden',
          ok: false,
          error: { code: 'FORBIDDEN' },
        });
      } finally {
        socket.close();
      }

      await stopQuietly(portal);
      // The handler writes a probe when it runs. The policy runs before the
      // transaction, so a FORBIDDEN answer leaves no row and no table.
      expect(await probeExecutions(database.path)).toBe(0);
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });
});
