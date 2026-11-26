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

/**
 * Server-initiated events over the real two-process path (ADR-0008): a WebSocket
 * client, a real Portal and a real Server child running the `extension-trial`
 * fixture. Assertions use the wire frames and Portal log lines only.
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

/**
 * Reads frames from one socket in arrival order. `nextMessage` installs a
 * fresh `once` listener per call, so registering two calls at once makes both
 * resolve on the first frame; this reader keeps a queue instead.
 */
function frameReader(socket: WebSocket): {
  next: () => Promise<Record<string, unknown>>;
} {
  const buffered: (string | Uint8Array)[] = [];
  const waiters: ((frame: string | Uint8Array) => void)[] = [];
  socket.addEventListener('message', (event: MessageEvent) => {
    const data: unknown = event.data;
    if (typeof data !== 'string' && !(data instanceof Uint8Array)) {
      return;
    }
    const waiter = waiters.shift();
    if (waiter === undefined) {
      buffered.push(data);
      return;
    }
    waiter(data);
  });
  return {
    next: async () => {
      const queued = buffered.shift();
      if (queued !== undefined) {
        return parseFrame(queued);
      }
      return parseFrame(
        await new Promise<string | Uint8Array>((resolve) => {
          waiters.push(resolve);
        }),
      );
    },
  };
}

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

describe('Server events over a real WebSocket and IPC', () => {
  test('sends the response first and the event right behind it', async () => {
    const database = tempDatabase({ prefix: 'mud-events-' });
    const portal = createPortal(database.path);
    try {
      await portal.layer.start();
      const socket = await openClient(urlOf(portal), {
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      try {
        const frames = frameReader(socket);
        socket.send(
          JSON.stringify({
            id: 'emit-1',
            method: 'event.emit',
            payload: { text: 'hi' },
          }),
        );

        const replyFrame = await withDeadline(
          frames.next(),
          REQUEST_TIMEOUT_MS,
          'reply to emit-1',
        );
        const eventFrame = await withDeadline(
          frames.next(),
          REQUEST_TIMEOUT_MS,
          'event after emit-1',
        );

        expect(replyFrame).toEqual({
          id: 'emit-1',
          ok: true,
          result: { emitted: true },
        });
        expect(eventFrame).toMatchObject({
          kind: 'event',
          event: 'note',
          payload: { text: 'hi' },
        });
        expect(typeof eventFrame.eventId).toBe('string');
        expect(eventFrame.eventId).toMatch(/:0$/);
      } finally {
        socket.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('delivers an event to the connection that holds the Session', async () => {
    const database = tempDatabase({ prefix: 'mud-events-' });
    const portal = createPortal(database.path);
    try {
      await portal.layer.start();
      const url = urlOf(portal);
      const alice = await openClient(url, { timeoutMs: REQUEST_TIMEOUT_MS });
      const bob = await openClient(url, { timeoutMs: REQUEST_TIMEOUT_MS });
      try {
        const created = await call(alice, {
          id: 'login',
          method: 'player.create',
          payload: { name: 'alice', sessionId: 'session-alice' },
        });
        expect(created.ok).toBe(true);

        // Registered before Bob's request: the event reaches Alice's socket
        // without any request of her own.
        const event = nextMessage(alice);
        const reply = await call(bob, {
          id: 'tell',
          method: 'event.to-session',
          payload: { sessionId: 'session-alice', text: 'hello' },
        });
        expect(reply).toEqual({
          id: 'tell',
          ok: true,
          result: { emitted: true },
        });

        expect(
          parseFrame(
            await withDeadline(event, REQUEST_TIMEOUT_MS, 'session event'),
          ),
        ).toMatchObject({
          kind: 'event',
          event: 'chat.message',
          payload: 'hello',
        });
      } finally {
        alice.close();
        bob.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('a failed request emits no event and the connection keeps working', async () => {
    const database = tempDatabase({ prefix: 'mud-events-' });
    const portal = createPortal(database.path);
    try {
      await portal.layer.start();
      const socket = await openClient(urlOf(portal), {
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      try {
        // The reader is attached before the request, so it sees every frame the
        // failed request produces — including one arriving right behind the
        // reply. `call` installs a one-shot listener, which would miss an event
        // between two calls and let the fault escape the test.
        const frames = frameReader(socket);
        socket.send(
          JSON.stringify({
            id: 'fail',
            method: 'event.fail',
          }),
        );
        const failed = await withDeadline(
          frames.next(),
          REQUEST_TIMEOUT_MS,
          'reply to fail',
        );
        expect(failed).toMatchObject({
          id: 'fail',
          ok: false,
          error: { code: 'INTERNAL_ERROR' },
        });

        // A failed request emits no event. The connection is otherwise idle, so
        // the short silence window after the failure reply must hold no frame
        // at all — any frame here would be the dropped event leaking out.
        let stray: Record<string, unknown> | undefined;
        try {
          stray = await withDeadline(frames.next(), 100, 'stray event frame');
        } catch {
          // No frame within the window: nothing was emitted.
        }
        expect(stray).toBeUndefined();

        // The connection still serves the next request.
        const echo = await call(socket, {
          id: 'after-fail',
          method: 'echo',
          payload: 'still-alive',
        });
        expect(echo).toEqual({
          id: 'after-fail',
          ok: true,
          result: 'still-alive',
        });
      } finally {
        socket.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });

  test('drops an event with no target Session and keeps serving', async () => {
    const database = tempDatabase({ prefix: 'mud-events-' });
    const portal = createPortal(database.path);
    try {
      await portal.layer.start();
      const socket = await openClient(urlOf(portal), {
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      try {
        const reply = await call(socket, {
          id: 'ghost',
          method: 'event.to-session',
          payload: { sessionId: 'session-ghost', text: 'nobody' },
        });
        expect(reply).toEqual({
          id: 'ghost',
          ok: true,
          result: { emitted: true },
        });

        const echo = await call(socket, {
          id: 'after-ghost',
          method: 'echo',
          payload: 'still-alive',
        });
        expect(echo).toEqual({
          id: 'after-ghost',
          ok: true,
          result: 'still-alive',
        });
        const dropped = portal.lines.filter(
          (line) => line.code === 'EVENT_DROPPED',
        );
        expect(dropped.some((line) => line.sessionId === 'session-ghost')).toBe(
          true,
        );
      } finally {
        socket.close();
      }
    } finally {
      await stopQuietly(portal);
      database.remove();
    }
  });
});
