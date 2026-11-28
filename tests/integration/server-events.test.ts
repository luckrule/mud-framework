import { describe, expect, test } from 'bun:test';

import {
  nextMessage,
  openClient,
  parseFrame,
  tempDatabase,
  withDeadline,
} from 'mud-framework-test-support';

import {
  callOnSocket,
  createPortal,
  stopQuietly,
  urlOf,
} from './support/two-process-harness.ts';

/**
 * Server-initiated events over the real two-process path (ADR-0008): a WebSocket
 * client, a real Portal and a real Server child running the `extension-trial`
 * fixture. Assertions use the wire frames and Portal log lines only.
 */
const REQUEST_TIMEOUT_MS = 5000;

function portalFor(databasePath: string) {
  return createPortal({
    databasePath,
    behavior: 'extension-trial',
    timeoutMs: REQUEST_TIMEOUT_MS,
    startTimeoutMs: 10_000,
    logTransport: true,
  });
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

describe('Server events over a real WebSocket and IPC', () => {
  test('sends the response first and the event right behind it', async () => {
    const database = tempDatabase({ prefix: 'mud-events-' });
    const portal = portalFor(database.path);
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
    const portal = portalFor(database.path);
    try {
      await portal.layer.start();
      const url = urlOf(portal);
      const alice = await openClient(url, { timeoutMs: REQUEST_TIMEOUT_MS });
      const bob = await openClient(url, { timeoutMs: REQUEST_TIMEOUT_MS });
      try {
        const created = await callOnSocket(
          alice,
          {
            id: 'login',
            method: 'player.create',
            payload: { name: 'alice', sessionId: 'session-alice' },
          },
          REQUEST_TIMEOUT_MS,
        );
        expect(created.ok).toBe(true);

        // Registered before Bob's request: the event reaches Alice's socket
        // without any request of her own.
        const event = nextMessage(alice);
        const reply = await callOnSocket(
          bob,
          {
            id: 'tell',
            method: 'event.to-session',
            payload: { sessionId: 'session-alice', text: 'hello' },
          },
          REQUEST_TIMEOUT_MS,
        );
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
    const portal = portalFor(database.path);
    try {
      await portal.layer.start();
      const socket = await openClient(urlOf(portal), {
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      try {
        // The reader is attached before the request, so it sees every frame the
        // failed request produces — including one arriving right behind the
        // reply. `callOnSocket` installs a one-shot listener, which would miss
        // an event between two calls and let the fault escape the test.
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
        const echo = await callOnSocket(
          socket,
          {
            id: 'after-fail',
            method: 'echo',
            payload: 'still-alive',
          },
          REQUEST_TIMEOUT_MS,
        );
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
    const portal = portalFor(database.path);
    try {
      await portal.layer.start();
      const socket = await openClient(urlOf(portal), {
        timeoutMs: REQUEST_TIMEOUT_MS,
      });
      try {
        const reply = await callOnSocket(
          socket,
          {
            id: 'ghost',
            method: 'event.to-session',
            payload: { sessionId: 'session-ghost', text: 'nobody' },
          },
          REQUEST_TIMEOUT_MS,
        );
        expect(reply).toEqual({
          id: 'ghost',
          ok: true,
          result: { emitted: true },
        });

        const echo = await callOnSocket(
          socket,
          {
            id: 'after-ghost',
            method: 'echo',
            payload: 'still-alive',
          },
          REQUEST_TIMEOUT_MS,
        );
        expect(echo).toEqual({
          id: 'after-ghost',
          ok: true,
          result: 'still-alive',
        });
        const dropped = portal.logs.lines.filter(
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
