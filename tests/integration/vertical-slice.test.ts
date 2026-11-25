import { describe, expect, test } from 'bun:test';

import {
  isRecord,
  nextMessage,
  openClient,
  parseFrame,
} from 'mud-framework-test-support';

import { WsTransport } from '../../packages/connector-ws/src/index.ts';
import {
  ConnectionLayer,
  Dispatcher,
  LocalExecutor,
} from '../../packages/core/src/index.ts';
import { SqliteBackend } from '../../packages/storage-sqlite/src/index.ts';

/**
 * One request over a fresh real WebSocket, closed afterwards. Each call is a
 * new connection, so the test also exercises connect/disconnect on the server.
 */
async function call(
  url: string,
  request: { id: string; method: string; payload?: unknown },
): Promise<Record<string, unknown>> {
  const socket = await openClient(url);
  try {
    const reply = nextMessage(socket);
    socket.send(JSON.stringify(request));
    return parseFrame(await reply);
  } finally {
    socket.close();
  }
}

interface PlayerRow {
  name: string;
}

describe('vertical slice', () => {
  test('a WebSocket request reaches SQLite and the result comes back', async () => {
    const backend = SqliteBackend.open({ filename: ':memory:' });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 2000 });
    const transport = new WsTransport({ port: 0 });
    const layer = new ConnectionLayer({
      transport,
      executor: new LocalExecutor({ dispatcher }),
    });

    // This slice has no login step, so its handlers declare themselves public;
    // they neither read nor change a Session (SPEC 3.7).
    dispatcher.register(
      'player.create',
      async (ctx, payload) => {
        const name = isRecord(payload) ? payload.name : undefined;
        if (typeof name !== 'string') {
          throw new Error('player.create expects { name: string }');
        }
        await ctx.storage.execute(
          'CREATE TABLE IF NOT EXISTS players (name TEXT PRIMARY KEY)',
        );
        await ctx.storage.execute('INSERT INTO players (name) VALUES (?)', [
          name,
        ]);
        return { created: name, requestId: ctx.requestId };
      },
      { access: 'public' },
    );
    dispatcher.register(
      'player.list',
      async (ctx) => {
        const rows = await ctx.storage.query<PlayerRow>(
          'SELECT name FROM players ORDER BY name',
        );
        return rows.map((row) => row.name);
      },
      { access: 'public' },
    );

    try {
      await layer.start();
      const port = transport.port;
      if (port === undefined) {
        throw new Error('expected the transport to bind a port');
      }
      const url = `ws://127.0.0.1:${port}/`;

      const created = await call(url, {
        id: 'r1',
        method: 'player.create',
        payload: { name: 'conan' },
      });

      expect(created.id).toBe('r1');
      expect(created.ok).toBe(true);
      expect(created.error).toBeUndefined();
      expect(created.result).toMatchObject({ created: 'conan' });

      // Second connection, so the write is visible to a client that never
      // saw the first request's transaction.
      const listed = await call(url, { id: 'r2', method: 'player.list' });

      expect(listed).toEqual({ id: 'r2', ok: true, result: ['conan'] });

      // And the row really is in the database, not just in a handler's memory.
      const stored = await backend.transaction(async (tx) =>
        tx.query<PlayerRow>('SELECT name FROM players'),
      );
      expect(stored).toEqual([{ name: 'conan' }]);
    } finally {
      await layer.stop();
    }
  });

  test('unknown methods and handler failures come back as stable codes', async () => {
    const backend = SqliteBackend.open({ filename: ':memory:' });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 2000 });
    const transport = new WsTransport({ port: 0 });
    const layer = new ConnectionLayer({
      transport,
      executor: new LocalExecutor({ dispatcher }),
    });

    // Public so the test observes the handler failure it is about, not the
    // Session gate that would otherwise answer first.
    dispatcher.register(
      'broken',
      async () => {
        throw new Error('handler exploded');
      },
      { access: 'public' },
    );

    try {
      await layer.start();
      const port = transport.port;
      if (port === undefined) {
        throw new Error('expected the transport to bind a port');
      }
      const url = `ws://127.0.0.1:${port}/`;

      const unknown = await call(url, { id: 'a', method: 'nope' });
      expect(unknown.ok).toBe(false);
      expect(unknown.error).toMatchObject({ code: 'UNKNOWN_METHOD' });

      const failed = await call(url, { id: 'b', method: 'broken' });
      expect(failed.ok).toBe(false);
      expect(failed.error).toMatchObject({
        code: 'INTERNAL_ERROR',
        message: 'handler exploded',
      });
    } finally {
      await layer.stop();
    }
  });

  test('a malformed frame over a real socket gets INVALID_ENVELOPE', async () => {
    const backend = SqliteBackend.open({ filename: ':memory:' });
    const dispatcher = new Dispatcher({ backend, timeoutMs: 2000 });
    const transport = new WsTransport({ port: 0 });
    const layer = new ConnectionLayer({
      transport,
      executor: new LocalExecutor({ dispatcher }),
    });

    try {
      await layer.start();
      const port = transport.port;
      if (port === undefined) {
        throw new Error('expected the transport to bind a port');
      }
      const socket = await openClient(`ws://127.0.0.1:${port}/`);
      try {
        const reply = nextMessage(socket);
        socket.send('{not json');
        expect(parseFrame(await reply)).toMatchObject({
          id: '',
          ok: false,
          error: { code: 'INVALID_ENVELOPE' },
        });
      } finally {
        socket.close();
      }
    } finally {
      await layer.stop();
    }
  });
});
