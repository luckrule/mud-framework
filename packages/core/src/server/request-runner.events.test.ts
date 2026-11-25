import { describe, expect, test } from 'bun:test';

import type { EventMessage, IpcMessage } from '../ipc/protocol.ts';
import { tick } from '../testing/common.ts';
import { requestMessage } from '../testing/server-process.ts';
import { startRunner } from './request-runner.support.ts';

function eventMessages(sent: readonly IpcMessage[]): EventMessage[] {
  return sent.flatMap((message) => (message.type === 'event' ? [message] : []));
}

describe('RequestRunner server events', () => {
  test('sends the result before every event of that request', async () => {
    const harness = startRunner();
    harness.register(
      'look',
      (ctx) => {
        ctx.emit('room.description', { text: 'a cave' });
        ctx.emitToSession('session-2', 'chat.message', 'hi');
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    harness.runner.accept(
      requestMessage({ requestId: 'req-9', method: 'look' }),
    );
    await tick();

    expect(harness.sent.map((message) => message.type)).toEqual([
      'result',
      'event',
      'event',
    ]);
    expect(harness.sent[0]).toEqual({
      type: 'result',
      requestId: 'req-9',
      ok: true,
      result: 'ok',
    });
    expect(eventMessages(harness.sent)).toEqual([
      {
        type: 'event',
        eventId: 'req-9:0',
        target: { kind: 'connection', connectionId: 'c1' },
        event: 'room.description',
        payload: { text: 'a cave' },
      },
      {
        type: 'event',
        eventId: 'req-9:1',
        target: { kind: 'session', sessionId: 'session-2' },
        event: 'chat.message',
        payload: 'hi',
      },
    ]);
  });

  test('keeps the events out of the result message', async () => {
    const harness = startRunner();
    harness.register(
      'tell',
      (ctx) => {
        ctx.emit('ping', null);
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'tell' }));
    await tick();

    expect(harness.sent[0]).not.toHaveProperty('events');
  });

  test('sends no event for a failed request', async () => {
    const harness = startRunner();
    harness.register(
      'fail',
      (ctx) => {
        ctx.emit('never.sent', null);
        throw new Error('boom');
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'fail' }));
    await tick();

    expect(harness.sent).toHaveLength(1);
    expect(harness.sent[0]).toMatchObject({ type: 'result', ok: false });
    expect(eventMessages(harness.sent)).toEqual([]);
  });

  test('skips every event when the result could not be sent', async () => {
    const harness = startRunner(1000, () => true, {
      send: (_message, what) => what !== 'result',
    });
    harness.register(
      'look',
      (ctx) => {
        ctx.emit('first', 1);
        ctx.emit('second', 2);
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'look' }));
    await tick();

    // The result was attempted and failed; no event was attempted at all.
    expect(harness.sent.map((message) => message.type)).toEqual(['result']);
    const dropped = harness
      .logs()
      .filter((line) => line.code === 'EVENTS_DROPPED');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({ count: 2, requestId: 'req-1' });
  });

  test('logs one failed event and still sends the rest', async () => {
    let eventAttempts = 0;
    const harness = startRunner(1000, () => true, {
      send: (_message, what) => {
        if (what !== 'event') {
          return true;
        }
        eventAttempts += 1;
        return eventAttempts !== 1;
      },
    });
    harness.register(
      'look',
      (ctx) => {
        ctx.emit('first', 1);
        ctx.emit('second', 2);
        return Promise.resolve('ok');
      },
      { access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'look' }));
    await tick();

    expect(harness.sent.map((message) => message.type)).toEqual([
      'result',
      'event',
      'event',
    ]);
    const failed = harness
      .logs()
      .filter((line) => line.code === 'EVENT_SEND_FAILED');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      requestId: 'req-1',
      eventId: 'req-1:0',
      event: 'first',
    });
  });
});
