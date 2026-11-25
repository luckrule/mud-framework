import { describe, expect, test } from 'bun:test';

import type { ServerEvent } from '../../shared/events.ts';
import { harness, started } from '../../testing/ipc-executor.ts';

function connectionEvent(eventId = 'req-1:0'): {
  type: 'event';
  eventId: string;
  target: { kind: 'connection'; connectionId: string };
  event: string;
  payload: unknown;
} {
  return {
    type: 'event',
    eventId,
    target: { kind: 'connection', connectionId: 'c1' },
    event: 'room.description',
    payload: { text: 'a cave' },
  };
}

describe('IpcExecutor server events', () => {
  test('delivers a parsed server event to every registered listener', async () => {
    const h = harness();
    const server = await started(h);
    const first: ServerEvent[] = [];
    const second: ServerEvent[] = [];

    h.executor.onEvent((event) => first.push(event));
    h.executor.onEvent((event) => second.push(event));
    server.send(connectionEvent());

    const expected: ServerEvent = {
      eventId: 'req-1:0',
      target: { kind: 'connection', connectionId: 'c1' },
      event: 'room.description',
      payload: { text: 'a cave' },
    };
    expect(first).toEqual([expected]);
    expect(second).toEqual([expected]);
  });

  test('drops an event with no listener and logs it', async () => {
    const h = harness();
    const server = await started(h);

    server.send(connectionEvent());

    const dropped = h.logs().filter((line) => line.code === 'EVENT_DROPPED');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      eventId: 'req-1:0',
      event: 'room.description',
      reason: 'no event listener',
    });
  });

  test('stops delivering after the listener unsubscribes', async () => {
    const h = harness();
    const server = await started(h);
    const received: ServerEvent[] = [];
    const unsubscribe = h.executor.onEvent((event) => received.push(event));

    server.send(connectionEvent('req-1:0'));
    unsubscribe();
    server.send(connectionEvent('req-1:1'));

    expect(received.map((event) => event.eventId)).toEqual(['req-1:0']);
    const dropped = h.logs().filter((line) => line.code === 'EVENT_DROPPED');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({ eventId: 'req-1:1' });
  });

  test('a throwing listener does not stop the others and does not escape', async () => {
    const h = harness();
    const server = await started(h);
    const received: ServerEvent[] = [];

    h.executor.onEvent(() => {
      throw new Error('listener exploded');
    });
    h.executor.onEvent((event) => received.push(event));

    expect(() => server.send(connectionEvent())).not.toThrow();
    expect(received).toHaveLength(1);
    const failed = h
      .logs()
      .filter((line) => line.code === 'EVENT_LISTENER_FAILED');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({ reason: 'listener exploded' });
  });

  test('ignores a subscription registered after close', async () => {
    const h = harness();
    await started(h);
    await h.executor.close();

    const unsubscribe = h.executor.onEvent(() => undefined);

    expect(typeof unsubscribe).toBe('function');
    expect(() => unsubscribe()).not.toThrow();
  });
});
