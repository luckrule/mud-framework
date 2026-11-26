import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import { JsonCodec } from '../shared/codec.ts';
import type { ServerEvent } from '../shared/events.ts';
import { parseFrame, sleep, tick } from '../testing/common.ts';
import { sessionHarness } from '../testing/connection-layer.ts';
import type { ConnectionState } from './connection-admission.ts';
import { deliverServerEvent } from './connection-events.ts';
import type { EventDeliveryHost } from './connection-events.ts';
import { SessionRouter } from './session-router.ts';

function event(overrides: Partial<ServerEvent> = {}): ServerEvent {
  return {
    eventId: 'req-1:0',
    target: { kind: 'connection', connectionId: 'c1' },
    event: 'room.description',
    payload: { text: 'a cave' },
    ...overrides,
  };
}

describe('ConnectionLayer server events', () => {
  test('writes an event frame to the target connection', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');

    h.controlled.emit(event());
    await tick();

    expect(h.transport.sentTo('c1').map(parseFrame)).toEqual([
      {
        kind: 'event',
        eventId: 'req-1:0',
        event: 'room.description',
        payload: { text: 'a cave' },
      },
    ]);
  });

  test('routes a session target to the connection that holds it', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind-1","method":"bind"}');

    h.controlled.emit(
      event({ target: { kind: 'session', sessionId: 's1' }, payload: 'hi' }),
    );
    await tick();

    const frames = h.transport.sentTo('c1').map(parseFrame);
    expect(frames.at(-1)).toEqual({
      kind: 'event',
      eventId: 'req-1:0',
      event: 'room.description',
      payload: 'hi',
    });
  });

  test('a queued session event follows a takeover instead of the old holder', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');
    const hanging = h.transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);

    // Both events wait behind c1's hanging request. The Session-addressed one
    // must be re-resolved, not written to the connection that queued it; the
    // connection-addressed one stays pinned to c1.
    h.controlled.emit(
      event({
        eventId: 'req-1:0',
        target: { kind: 'session', sessionId: 's1' },
        payload: 'follow',
      }),
    );
    h.controlled.emit(
      event({
        eventId: 'req-1:1',
        target: { kind: 'connection', connectionId: 'c1' },
        payload: 'pinned',
      }),
    );
    await tick();

    // c2 takes s1 over while c1's chain is still blocked. Its next request
    // waits on the Session barrier, so it occupies c2's chain ahead of the
    // re-addressed event.
    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"3","method":"bind"}');
    const next = h.transport.receive('c2', '{"id":"4","method":"next"}');

    h.controlled.release();
    await Promise.all([hanging, next]);
    await tick();

    expect(h.transport.sentTo('c1').map(parseFrame)).toEqual([
      { id: '1', ok: true, result: 'bind' },
      { id: '2', ok: true, result: 'hang' },
      {
        kind: 'event',
        eventId: 'req-1:1',
        event: 'room.description',
        payload: 'pinned',
      },
    ]);
    expect(h.transport.sentTo('c2').map(parseFrame)).toEqual([
      { id: '3', ok: true, result: 'bind' },
      { id: '4', ok: true, result: 'next' },
      {
        kind: 'event',
        eventId: 'req-1:0',
        event: 'room.description',
        payload: 'follow',
      },
    ]);
    expect(
      h.logs().filter((line) => line.code === 'EVENT_DROPPED'),
    ).toHaveLength(0);
  });

  test('drops a queued session event when its holder leaves before the write', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');
    const hanging = h.transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);

    h.controlled.emit(
      event({
        eventId: 'req-1:0',
        target: { kind: 'session', sessionId: 's1' },
      }),
    );
    await tick();

    await h.transport.disconnect('c1');
    h.controlled.release();
    await hanging;
    await tick();

    expect(h.transport.sentTo('c1').map(parseFrame)).not.toContainEqual(
      expect.objectContaining({ kind: 'event' }),
    );
    const dropped = h.logs().filter((line) => line.code === 'EVENT_DROPPED');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      eventId: 'req-1:0',
      event: 'room.description',
      targetKind: 'session',
      sessionId: 's1',
      reason: 'no connection holds the session',
    });
  });

  test('drops a session target with no current holder', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');

    h.controlled.emit(
      event({ target: { kind: 'session', sessionId: 'nobody' } }),
    );
    await tick();

    expect(h.transport.sentTo('c1')).toEqual([]);
    const dropped = h.logs().filter((line) => line.code === 'EVENT_DROPPED');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      eventId: 'req-1:0',
      targetKind: 'session',
      sessionId: 'nobody',
      reason: 'no connection holds the session',
    });
  });

  test('drops a connection target that is gone', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.disconnect('c1');

    h.controlled.emit(
      event({ target: { kind: 'connection', connectionId: 'c1' } }),
    );
    await tick();

    const dropped = h.logs().filter((line) => line.code === 'EVENT_DROPPED');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      targetKind: 'connection',
      connectionId: 'c1',
      reason: 'target connection is not available',
    });
  });

  test('drops an event that cannot be encoded without throwing', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');

    expect(() => h.controlled.emit(event({ payload: 1n }))).not.toThrow();
    await tick();

    expect(h.transport.sentTo('c1')).toEqual([]);
    const failed = h
      .logs()
      .filter((line) => line.code === 'TRANSPORT_SEND_FAILED');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      eventId: 'req-1:0',
      stage: 'encode',
      reason: expect.stringContaining('BigInt'),
    });
  });

  test('drops an event whose transport write fails without throwing', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');

    h.controlled.emit(event());
    // The frame is queued on the connection's chain; closing the connection
    // before that chain runs makes the write itself fail.
    await h.transport.disconnect('c1');
    await tick();

    const failed = h
      .logs()
      .filter((line) => line.code === 'TRANSPORT_SEND_FAILED');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      connectionId: 'c1',
      eventId: 'req-1:0',
      stage: 'send',
    });
  });

  test('registers with the executor at start and releases at stop', async () => {
    const h = sessionHarness();
    await h.layer.start();
    expect(h.controlled.listenerCount()).toBe(1);

    await h.layer.stop();

    expect(h.controlled.listenerCount()).toBe(0);
  });
});

interface QueuedWrite {
  readonly state: ConnectionState;
  readonly work: () => Promise<void>;
}

/**
 * A host whose `enqueue` records work instead of running it, so a test can move
 * the Session between two queued writes deterministically.
 */
function deferredDeliveryHost() {
  const lines: string[] = [];
  const logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  const router = new SessionRouter({ logger });
  const connections = new Map<string, ConnectionState>();
  const sent = new Map<string, (string | Uint8Array)[]>();
  for (const id of ['c1', 'c2']) {
    const frames: (string | Uint8Array)[] = [];
    sent.set(id, frames);
    connections.set(id, {
      connection: {
        id,
        send: (frame) => {
          frames.push(frame);
          return Promise.resolve();
        },
        close: () => Promise.resolve(),
      },
      chain: Promise.resolve(),
      queued: 0,
      inFlight: new Set(),
      closed: new AbortController(),
    });
  }
  const queue: QueuedWrite[] = [];
  const host: EventDeliveryHost = {
    codec: new JsonCodec(),
    logger,
    connections,
    router,
    enqueue: (state, work) => {
      queue.push({ state, work });
      return Promise.resolve();
    },
  };
  return { host, router, queue, sent, logs: () => lines.map(parseFrame) };
}

describe('deliverServerEvent session re-resolution', () => {
  test('drops after one redelivery instead of following a second takeover', async () => {
    const h = deferredDeliveryHost();
    h.router.bind('c1', 's1', Promise.resolve());

    const delivery = deliverServerEvent(
      h.host,
      event({ target: { kind: 'session', sessionId: 's1' } }),
    );
    expect(h.queue.map((entry) => entry.state.connection.id)).toEqual(['c1']);

    h.router.bind('c2', 's1', Promise.resolve());
    await h.queue[0].work();
    expect(h.sent.get('c1')).toEqual([]);
    expect(h.queue.map((entry) => entry.state.connection.id)).toEqual([
      'c1',
      'c2',
    ]);

    // Back to c1 before the redelivered write runs: chasing again would let two
    // connections hand the event back and forth, so the retry is bounded.
    h.router.bind('c1', 's1', Promise.resolve());
    await h.queue[1].work();

    expect(h.sent.get('c1')).toEqual([]);
    expect(h.sent.get('c2')).toEqual([]);
    expect(h.queue).toHaveLength(2);
    const dropped = h.logs().filter((line) => line.code === 'EVENT_DROPPED');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      eventId: 'req-1:0',
      event: 'room.description',
      targetKind: 'session',
      sessionId: 's1',
      reason: 'the session holder changed again while redelivering',
    });
    await delivery;
  });
});
