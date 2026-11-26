import { describe, expect, test } from 'bun:test';

import type { ServerEvent } from '../shared/events.ts';
import { parseFrame, tick } from '../testing/common.ts';
import { sessionHarness } from '../testing/connection-layer.ts';

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
