import { describe, expect, test } from 'bun:test';

import { expectRejected } from './protocol.support.ts';
import { parseIpcMessage } from './protocol.ts';

describe('parseIpcMessage event', () => {
  test('accepts a connection target', () => {
    const parsed = parseIpcMessage({
      type: 'event',
      eventId: 'req-1:0',
      target: { kind: 'connection', connectionId: 'conn-1' },
      event: 'room.description',
      payload: { text: 'a cave' },
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'event',
        eventId: 'req-1:0',
        target: { kind: 'connection', connectionId: 'conn-1' },
        event: 'room.description',
        payload: { text: 'a cave' },
      },
    });
  });

  test('accepts a session target', () => {
    const parsed = parseIpcMessage({
      type: 'event',
      eventId: 'req-1:1',
      target: { kind: 'session', sessionId: 'session-2' },
      event: 'chat.message',
      payload: 'hello',
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'event',
        eventId: 'req-1:1',
        target: { kind: 'session', sessionId: 'session-2' },
        event: 'chat.message',
        payload: 'hello',
      },
    });
  });

  test('keeps an undefined payload as absent', () => {
    const parsed = parseIpcMessage({
      type: 'event',
      eventId: 'req-1:0',
      target: { kind: 'connection', connectionId: 'conn-1' },
      event: 'ping',
    });

    expect(parsed.ok).toBe(true);
    if (parsed.ok && parsed.message.type === 'event') {
      expect(parsed.message.payload).toBeUndefined();
    }
  });

  test('rejects an unusable event id', () => {
    for (const eventId of [undefined, '', 7]) {
      expectRejected(
        {
          type: 'event',
          eventId,
          target: { kind: 'connection', connectionId: 'conn-1' },
          event: 'ping',
        },
        'eventId',
      );
    }
  });

  test('rejects an unusable event name', () => {
    for (const event of [undefined, '', 7]) {
      expectRejected(
        {
          type: 'event',
          eventId: 'req-1:0',
          target: { kind: 'connection', connectionId: 'conn-1' },
          event,
        },
        'event',
      );
    }
  });

  test('rejects a target that is not an object', () => {
    expectRejected(
      {
        type: 'event',
        eventId: 'req-1:0',
        target: 'connection:conn-1',
        event: 'ping',
      },
      'target',
    );
  });

  test('rejects an unknown target kind', () => {
    expectRejected(
      {
        type: 'event',
        eventId: 'req-1:0',
        target: { kind: 'broadcast', connectionId: 'conn-1' },
        event: 'ping',
      },
      'kind',
    );
  });

  test('rejects a connection target without a usable connection id', () => {
    for (const connectionId of [undefined, '', 7]) {
      expectRejected(
        {
          type: 'event',
          eventId: 'req-1:0',
          target: { kind: 'connection', connectionId },
          event: 'ping',
        },
        'connectionId',
      );
    }
  });

  test('rejects a session target without a usable session id', () => {
    for (const sessionId of [undefined, '', 7]) {
      expectRejected(
        {
          type: 'event',
          eventId: 'req-1:0',
          target: { kind: 'session', sessionId },
          event: 'ping',
        },
        'sessionId',
      );
    }
  });

  test('drops extra fields from an event', () => {
    const parsed = parseIpcMessage({
      type: 'event',
      eventId: 'req-1:0',
      target: { kind: 'connection', connectionId: 'conn-1', extra: true },
      event: 'ping',
      payload: null,
      futureField: 'ignored',
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'event',
        eventId: 'req-1:0',
        target: { kind: 'connection', connectionId: 'conn-1' },
        event: 'ping',
        payload: null,
      },
    });
  });
});
