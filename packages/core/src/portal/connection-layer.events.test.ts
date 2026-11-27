import { describe, expect, test } from 'bun:test';
import { Buffer } from 'node:buffer';

import { withDeadline } from 'mud-framework-test-support';

import type { ServerEvent } from '../shared/events.ts';
import { parseFrame, sleep, tick } from '../testing/common.ts';
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

function sessionEvent(
  eventId: string,
  payload: unknown,
  sessionId = 's1',
): ServerEvent {
  return event({ eventId, target: { kind: 'session', sessionId }, payload });
}

/** Anything a test can read written frames from. */
interface FrameSource {
  sentTo(connectionId: string): readonly (string | Uint8Array)[];
}

/** Event frames written to `connectionId`, in write order. */
function eventFrames(
  source: FrameSource,
  connectionId: string,
): Record<string, unknown>[] {
  return source
    .sentTo(connectionId)
    .map(parseFrame)
    .filter((frame) => frame.kind === 'event');
}

function dropLines(
  lines: Record<string, unknown>[],
): Record<string, unknown>[] {
  return lines.filter((line) => line.code === 'EVENT_DROPPED');
}

function sendFailures(
  lines: Record<string, unknown>[],
): Record<string, unknown>[] {
  return lines.filter((line) => line.code === 'TRANSPORT_SEND_FAILED');
}

/**
 * Reads the layer's private Session queue table. The map is an implementation
 * detail (it exists so ordering and bound state is per layer), so the test
 * borrows the live object rather than exposing it on the public surface.
 */
function sessionQueuesOf(layer: unknown): Map<string, unknown> {
  return (layer as { sessionQueues: Map<string, unknown> }).sessionQueues;
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

    // c2 takes s1 over while c1's request queue is still blocked. Its next request
    // waits on the Session barrier, so it occupies c2's request queue ahead of the
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

  test('a takeover does not reorder two events addressed to the same session', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');
    const hanging = h.transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);

    // The first event waits behind c1's hung request queue. The second arrives after
    // c2 took the Session over; it must still wait for the first one, or c2
    // would see the events in reverse emission order.
    h.controlled.emit(
      event({
        eventId: 'req-1:0',
        target: { kind: 'session', sessionId: 's1' },
        payload: 'first',
      }),
    );
    await tick();

    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"3","method":"bind"}');
    h.controlled.emit(
      event({
        eventId: 'req-2:0',
        target: { kind: 'session', sessionId: 's1' },
        payload: 'second',
      }),
    );
    await tick();

    // c2 is idle, but writing the second event now would overtake the first.
    expect(eventFrames(h.transport, 'c2')).toEqual([]);

    h.controlled.release();
    await hanging;
    await tick();

    expect(eventFrames(h.transport, 'c1')).toEqual([]);
    expect(
      eventFrames(h.transport, 'c2').map((frame) => frame.payload),
    ).toEqual(['first', 'second']);
    expect(h.logs().filter((line) => line.code === 'EVENT_DROPPED')).toEqual(
      [],
    );
  });

  test('drops an event waiting on a disconnected holder and delivers the next one', async () => {
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
        payload: 'lost',
      }),
    );
    await tick();

    // c1's request queue never reaches the queued event: dropping it on abort is what
    // releases the Session queue so a later event can still be delivered.
    await h.transport.disconnect('c1');
    await tick();
    const dropped = h.logs().filter((line) => line.code === 'EVENT_DROPPED');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      eventId: 'req-1:0',
      event: 'room.description',
      targetKind: 'session',
      sessionId: 's1',
      connectionId: 'c1',
      reason: 'the target connection closed before the event was written',
    });

    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"3","method":"bind"}');
    h.controlled.emit(
      event({
        eventId: 'req-2:0',
        target: { kind: 'session', sessionId: 's1' },
        payload: 'next',
      }),
    );
    await tick();

    expect(
      eventFrames(h.transport, 'c2').map((frame) => frame.payload),
    ).toEqual(['next']);

    h.controlled.release();
    await hanging;
    await tick();
    expect(eventFrames(h.transport, 'c1')).toEqual([]);
  });

  test('a blocked session queue does not hold up other sessions or connection targets', async () => {
    const h = sessionHarness({
      sessionOf: (method) =>
        method === 'bind1' ? 's1' : method === 'bind2' ? 's2' : undefined,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind1"}');
    const hanging = h.transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);

    h.controlled.emit(
      event({
        eventId: 'req-1:0',
        target: { kind: 'session', sessionId: 's1' },
        payload: 'blocked',
      }),
    );
    await tick();

    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"3","method":"bind2"}');
    h.controlled.emit(
      event({
        eventId: 'req-2:0',
        target: { kind: 'session', sessionId: 's2' },
        payload: 'other session',
      }),
    );
    h.controlled.emit(
      event({
        eventId: 'req-3:0',
        target: { kind: 'connection', connectionId: 'c2' },
        payload: 'pinned',
      }),
    );
    await tick();

    expect(
      eventFrames(h.transport, 'c2').map((frame) => frame.payload),
    ).toEqual(['other session', 'pinned']);

    h.controlled.release();
    await hanging;
    await tick();
    expect(
      eventFrames(h.transport, 'c1').map((frame) => frame.payload),
    ).toEqual(['blocked']);
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
      reason: 'the target connection closed before the event was written',
    });
  });

  test('a write in flight keeps the serial slot across a takeover and is not dropped', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    // E1 starts writing on c1 and its send stays pending: the frame is on the
    // wire, but the transport has not settled it.
    const write = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('req-1:0', 'first'));
    await write.entered;

    // c1 drops while E1 is writing, then c2 takes the Session over.
    await h.transport.disconnect('c1');
    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"3","method":"bind"}');

    // E2 must not start: E1 has not settled, so its serial slot is still held.
    h.controlled.emit(sessionEvent('req-2:0', 'second'));
    await tick();
    expect(eventFrames(h.transport, 'c2')).toEqual([]);

    write.release();
    await tick();
    await tick();

    // E1 was written, so it is not a drop; E2 follows it on the new holder.
    expect(dropLines(h.logs())).toEqual([]);
    expect(
      eventFrames(h.transport, 'c2').map((frame) => frame.payload),
    ).toEqual(['second']);
    expect(
      eventFrames(h.transport, 'c1').map((frame) => frame.payload),
    ).toEqual(['first']);
  });

  test('a failed write is a transport failure, not a drop, and releases the slot', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    const write = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('req-1:0', 'first'));
    await write.entered;
    await h.transport.disconnect('c1');

    // The socket breaks while the write is in flight. The disconnect must not
    // have released the slot, so the failure is what ends E1.
    write.fail(new Error('socket closed mid-write'));
    await tick();
    await tick();

    const failures = sendFailures(h.logs());
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      eventId: 'req-1:0',
      connectionId: 'c1',
      stage: 'send',
      reason: 'socket closed mid-write',
    });
    expect(dropLines(h.logs())).toEqual([]);

    // The slot is free again, so a later event reaches the new holder.
    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"3","method":"bind"}');
    h.controlled.emit(sessionEvent('req-2:0', 'second'));
    await tick();

    expect(
      eventFrames(h.transport, 'c2').map((frame) => frame.payload),
    ).toEqual(['second']);
  });

  test('a write that never settles is bounded by the send timeout', async () => {
    const h = sessionHarness({ eventSendTimeoutMs: 10 });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    const write = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('req-1:0', 'stuck'));
    await write.entered;
    await sleep(30);

    const failures = sendFailures(h.logs());
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      eventId: 'req-1:0',
      connectionId: 'c1',
      stage: 'send',
      reason: expect.stringContaining('did not settle'),
    });

    // The bound released the slot even though the send never settled.
    h.controlled.emit(sessionEvent('req-2:0', 'next'));
    await tick();
    expect(
      eventFrames(h.transport, 'c1').map((frame) => frame.payload),
    ).toEqual(['stuck', 'next']);
    write.release();
  });

  test('uses the transport declared send bound when the layer has no option', async () => {
    // 30ms is well under the default bound, so only adopting the declaration
    // can release the slot this quickly.
    const h = sessionHarness({ transportSendTimeoutMs: 30 });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    const write = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('req-1:0', 'stuck'));
    await write.entered;
    await sleep(60);

    const failures = sendFailures(h.logs());
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      eventId: 'req-1:0',
      stage: 'send',
      reason: 'the transport did not settle the write within 30ms',
    });
    write.release();
  });

  test('an explicit layer option outranks the transport declared bound', async () => {
    // Transport declares 50ms, layer asks for 30ms: the slot must free at the
    // layer's bound, which is the shorter one and proves it was the one used.
    const h = sessionHarness({
      transportSendTimeoutMs: 50,
      eventSendTimeoutMs: 30,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    const write = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('req-1:0', 'stuck'));
    await write.entered;
    await sleep(40);

    const failures = sendFailures(h.logs());
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      eventId: 'req-1:0',
      stage: 'send',
      reason: 'the transport did not settle the write within 30ms',
    });
    write.release();
  });

  test('a send that throws synchronously is recorded, not thrown at the caller', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    // The interface forbids it, but nothing awaits this delivery, so an
    // escaping throw would become an unhandled rejection instead of a log line.
    h.transport.throwOnSend('c1', new Error('send exploded'));

    h.controlled.emit(sessionEvent('req-1:0', 'boom'));
    await tick();

    const failures = sendFailures(h.logs());
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      eventId: 'req-1:0',
      stage: 'send',
      reason: 'send exploded',
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

  test('a disconnect never leaves a route pointing at an untracked connection', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    // A Session-addressed event landing in the same turn as the disconnect must
    // see a released route, not a route whose connection is already gone: the
    // "holder is not tracked" branch has no reachable path and this pins it.
    await h.transport.disconnect('c1');
    h.controlled.emit(sessionEvent('req-1:0', 'after disconnect'));
    await tick();

    const dropped = dropLines(h.logs());
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      eventId: 'req-1:0',
      targetKind: 'session',
      sessionId: 's1',
      reason: 'no connection holds the session',
    });
    // No line ever reports the stale-route reason.
    expect(
      h
        .logs()
        .filter((line) => line.reason === 'the session holder is not tracked'),
    ).toEqual([]);
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

    // The frame must already be handed to the transport for this to be a send
    // failure: a frame still queued when its connection closes is a drop
    // (SPEC 3.8), not a transport failure.
    const write = h.transport.holdSends('c1');
    h.controlled.emit(event());
    await write.entered;
    write.fail(new Error('socket closed mid-write'));
    await tick();

    const failed = h
      .logs()
      .filter((line) => line.code === 'TRANSPORT_SEND_FAILED');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      connectionId: 'c1',
      eventId: 'req-1:0',
      stage: 'send',
      reason: 'socket closed mid-write',
    });
    expect(h.logs().filter((line) => line.code === 'EVENT_DROPPED')).toEqual(
      [],
    );
  });

  test('registers with the executor at start and releases at stop', async () => {
    const h = sessionHarness();
    await h.layer.start();
    expect(h.controlled.listenerCount()).toBe(1);

    await h.layer.stop();

    expect(h.controlled.listenerCount()).toBe(0);
  });

  test('clears the session queues on stop, so a restarted layer holds no stale entry', async () => {
    const h = sessionHarness({ shutdownTimeoutMs: 20 });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    // Hold the write: the delivery cannot finish, so its Session queue stays in
    // the map until the layer drops it. Without an explicit clear the entry
    // would outlive the layer.
    const write = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('req-1:0', 'stuck'));
    await write.entered;

    const queues = sessionQueuesOf(h.layer);
    expect(queues.size).toBe(1);

    await h.layer.stop();

    expect(queues.size).toBe(0);
    write.release();
  });
});

/** Correlates the frames written to one connection: reply id or event id. */
function frameLabels(source: FrameSource, connectionId: string): string[] {
  return source
    .sentTo(connectionId)
    .map(parseFrame)
    .map((frame) => {
      if (typeof frame.id === 'string') {
        return frame.id;
      }
      return String(frame.eventId);
    });
}

describe('ConnectionLayer event write scheduling', () => {
  test('a request result overtakes an event still waiting and the event follows', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // The first event is handed to the transport and held there; the second
    // stays in the connection's event queue.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(event({ eventId: 'e:0' }));
    h.controlled.emit(event({ eventId: 'e:1' }));
    await held.entered;

    const request = h.transport.receive('c1', '{"id":"r1","method":"work"}');
    await tick();
    // A frame already handed to the transport cannot be preempted, and the
    // result must not wait for the event that is still queued.
    expect(frameLabels(h.transport, 'c1')).toEqual(['bind', 'e:0']);

    held.release();
    await request;
    await tick();

    expect(frameLabels(h.transport, 'c1')).toEqual([
      'bind',
      'e:0',
      'r1',
      'e:1',
    ]);
  });

  test('gives the event queue one turn between request completions', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    const held = h.transport.holdSends('c1');
    h.controlled.emit(event({ eventId: 'e:0' }));
    h.controlled.emit(event({ eventId: 'e:1' }));
    await held.entered;

    const first = h.transport.receive('c1', '{"id":"r1","method":"work"}');
    const second = h.transport.receive('c1', '{"id":"r2","method":"work"}');
    await tick();

    held.release();
    await Promise.all([first, second]);
    await tick();

    // Request work wins whenever both are ready, and each completion hands the
    // queue head one turn. SPEC 3.8 only promises bounded event waiting, so this
    // exact interleaving pins the current one-turn-per-request policy rather
    // than a contract; retuning that policy changes this expectation.
    expect(frameLabels(h.transport, 'c1')).toEqual([
      'bind',
      'e:0',
      'r1',
      'e:1',
      'r2',
    ]);
  });

  test('stop discards a queued event instead of waiting for the transport', async () => {
    // A budget far beyond the test deadline: only an implementation that
    // drained events at shutdown could take this long.
    const h = sessionHarness({ shutdownTimeoutMs: 60_000 });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    const held = h.transport.holdSends('c1');
    h.controlled.emit(event({ eventId: 'e:0' }));
    h.controlled.emit(event({ eventId: 'e:1' }));
    await held.entered;
    // The transport cannot be read once stopped, so the frames written so far
    // are captured here: e:0 is on the wire, e:1 is still in the queue.
    expect(frameLabels(h.transport, 'c1')).toEqual(['bind', 'e:0']);

    await withDeadline(
      h.layer.stop(),
      1000,
      'stop with an event write in flight',
    );

    held.release();
    await tick();
    await tick();

    // e:1 was still queued and must not be attempted after the layer stopped.
    // Attempting it would fail inside the stopped transport and leave a
    // send-failure line behind.
    expect(
      h.logs().filter((line) => line.code === 'TRANSPORT_SEND_FAILED'),
    ).toEqual([]);
  });

  test('stop during a takeover hand-off counts the in-hand frame in the shutdown summary', async () => {
    const payload = 'first';
    const frameBytes = Buffer.byteLength(
      JSON.stringify({
        kind: 'event',
        eventId: 'e:0',
        event: 'room.description',
        payload,
      }),
      'utf8',
    );
    const h = sessionHarness({ shutdownTimeoutMs: 30 });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');
    const hanging = h.transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);

    // E1 is queued for s1 on c1, behind the hanging request.
    h.controlled.emit(sessionEvent('e:0', payload));
    await tick();

    // c2 takes s1 over while E1 waits, so E1's write will observe a moved
    // holder and earn exactly one redelivery.
    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"3","method":"bind"}');
    await sleep(0);

    h.controlled.release();
    // Wait until the moved frame has left every buffer: it was released from
    // c1's queue and the redelivery has not queued it again yet. Polling a
    // public health count ties the test to observable state rather than to the
    // number of promise hops the implementation happens to use.
    let hops = 0;
    while (h.layer.health().eventBacklog.frames !== 0 && hops < 50) {
      await Promise.resolve();
      hops += 1;
    }
    expect(h.layer.health().eventBacklog.frames).toBe(0);
    const c2Frames = h.transport.sentTo('c2').map(parseFrame);

    const stopping = h.layer.stop();
    await Promise.all([hanging, stopping]);
    await tick();
    await tick();

    // The redelivery must not join c2's queue after shutdown started: the only
    // guard is the accepting-events check at the head of the redelivery loop.
    // Without it the frame is queued, immediately aborted by the shutdown and
    // reported as an EVENT_DROPPED, which the shutdown contract forbids.
    expect(c2Frames).toEqual([{ id: '3', ok: true, result: 'bind' }]);
    expect(h.logs().filter((line) => line.code === 'EVENT_DROPPED')).toEqual(
      [],
    );
    expect(
      h.logs().filter((line) => line.code === 'TRANSPORT_SEND_FAILED'),
    ).toEqual([]);
    // The frame was between buffers when shutdown ran: out of the Session
    // queue, not yet in c2's connection queue. SPEC 3.8 names in-hand frames
    // explicitly, so this one must appear in the single summary line. It is not
    // a connection queue's frame, so `connections` stays 0 (SPEC 3.8 excludes
    // Session queues and in-hand frames from that count).
    const summary = h
      .logs()
      .filter((line) => line.code === 'EVENT_BACKLOG_DISCARDED');
    expect(summary).toHaveLength(1);
    expect(summary[0]).toMatchObject({
      reason: 'shutdown',
      count: 1,
      connections: 0,
      frames: 1,
      bytes: frameBytes,
    });
    // Nothing is left counted once the frame is accounted for.
    expect(h.layer.health().eventBacklog).toEqual({
      connections: 0,
      frames: 0,
      bytes: 0,
      dropped: 0,
    });
  });
});

/**
 * A layer whose successful `unbind` request removes the route. `hang` is a
 * request still running when the event arrives: it declares the unbind, so the
 * route is gone by the time the queued event's write turn comes, while the
 * connection itself stays open (a disconnect would abort the frame instead and
 * take the `'aborted'` path).
 */
function unbindingHarness() {
  return sessionHarness({
    changeOf: (method) =>
      method === 'bind'
        ? { type: 'bind', sessionId: 's1' }
        : method === 'hang'
          ? { type: 'unbind' }
          : undefined,
  });
}

describe('deliverServerEvent session re-resolution', () => {
  test('drops after one redelivery instead of following a second takeover', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');

    // c1's writer is held by the bind reply, so the Session event that follows
    // is accepted into c1's connection queue without running. Holding the reply
    // is how the test catches the "queued, not started" window a real writer
    // closes on its next microtask.
    const heldC1 = h.transport.holdSends('c1');
    const bindingC1 = h.transport.receive('c1', '{"id":"1","method":"bind"}');
    await heldC1.entered;
    h.controlled.emit(sessionEvent('req-1:0', 'chase'));
    await tick();
    expect(h.layer.health().eventBacklog).toMatchObject({
      connections: 1,
      frames: 1,
    });

    // c2 takes the Session over, and its writer is held the same way: the
    // redelivered frame will stop in c2's queue instead of running.
    h.transport.connect('c2');
    const heldC2 = h.transport.holdSends('c2');
    const bindingC2 = h.transport.receive('c2', '{"id":"2","method":"bind"}');
    await heldC2.entered;

    // Releasing c1 lets the queued frame's turn arrive: it re-resolves the
    // holder, sees c2, and is redelivered rather than written to c1.
    heldC1.release();
    await bindingC1;
    await tick();
    expect(eventFrames(h.transport, 'c1')).toEqual([]);
    expect(dropLines(h.logs())).toEqual([]);
    // Exactly one frame is outstanding and only c2's buffer can hold it: c1's
    // write already ran and re-resolved. That is the single redelivery, seen
    // through public health rather than a private queue.
    expect(h.layer.health().eventBacklog).toMatchObject({
      connections: 1,
      frames: 1,
    });

    // Back to c1 before the redelivered write runs: chasing again would let two
    // connections hand the event back and forth, so the retry is bounded. c1's
    // writer is idle now, so a third attempt would write the frame there and be
    // caught by the empty frame list below.
    await h.transport.receive('c1', '{"id":"3","method":"bind"}');
    heldC2.release();
    await bindingC2;
    // Wait for the bounded delivery to finish by its observable outcome instead
    // of counting promise hops: the drop is logged, the Session queue is gone and
    // no buffer still holds the frame.
    const settled = (): boolean =>
      dropLines(h.logs()).length > 0 &&
      sessionQueuesOf(h.layer).size === 0 &&
      h.layer.health().eventBacklog.frames === 0;
    let hops = 0;
    while (!settled() && hops < 50) {
      await Promise.resolve();
      hops += 1;
    }

    expect(eventFrames(h.transport, 'c1')).toEqual([]);
    expect(eventFrames(h.transport, 'c2')).toEqual([]);
    const dropped = dropLines(h.logs());
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      eventId: 'req-1:0',
      event: 'room.description',
      targetKind: 'session',
      sessionId: 's1',
      reason: 'the session holder changed again while redelivering',
    });
    // The drop released the Session's serial slot, leaving no queue behind.
    expect(sessionQueuesOf(h.layer).size).toBe(0);
    expect(h.layer.health().eventBacklog).toEqual({
      connections: 0,
      frames: 0,
      bytes: 0,
      dropped: 0,
    });
  });

  test('writes one session event at a time and clears the queue when idle', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    // c1's writer is busy with the first frame: its send stays pending, so the
    // second event cannot start writing.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('req-1:0', 'first'));
    await held.entered;
    h.controlled.emit(sessionEvent('req-2:0', 'second'));
    await tick();

    // Only the first frame reached the transport; the second still waits in the
    // Session's own queue, which is why that queue has an entry. Two accepted
    // frames are outstanding — one in the connection queue, one in the Session
    // queue — so the second was not dropped.
    expect(
      eventFrames(h.transport, 'c1').map((frame) => frame.payload),
    ).toEqual(['first']);
    expect(sessionQueuesOf(h.layer).size).toBe(1);
    expect(h.layer.health().eventBacklog.frames).toBe(2);

    held.release();
    await tick();
    await tick();

    // Serialized in emission order, and the idle Session leaves no entry.
    expect(
      eventFrames(h.transport, 'c1').map((frame) => frame.payload),
    ).toEqual(['first', 'second']);
    expect(sessionQueuesOf(h.layer).size).toBe(0);
  });

  test('drops a queued event whose holder left before the write ran', async () => {
    const h = unbindingHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    // The running request holds c1's writer, so the event cannot write yet: its
    // frame waits in c1's connection queue and the Session's serial slot stays
    // held.
    const unbind = h.transport.receive('c1', '{"id":"2","method":"hang"}');
    await tick();
    h.controlled.emit(sessionEvent('req-1:0', 'lost'));
    await tick();
    expect(sessionQueuesOf(h.layer).size).toBe(1);

    // The request succeeds and declares the unbind, removing the route while
    // the event waits. The write-time resolution must see the missing holder
    // rather than write to the connection that queued it.
    h.controlled.release();
    await unbind;
    await tick();
    await tick();

    expect(eventFrames(h.transport, 'c1')).toEqual([]);
    const dropped = dropLines(h.logs());
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      eventId: 'req-1:0',
      targetKind: 'session',
      sessionId: 's1',
      reason: 'no connection holds the session',
    });
  });

  test('clears the queue after a failed write', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    // The frame reaches the socket and the socket breaks: the write is a
    // transport failure rather than a drop, and it must still free the Session's
    // serial slot.
    const write = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('req-1:0', 'first'));
    await write.entered;
    write.fail(new Error('socket closed'));
    await tick();
    await tick();

    expect(sendFailures(h.logs())).toHaveLength(1);
    expect(sessionQueuesOf(h.layer).size).toBe(0);
  });

  test('clears the queue after a dropped event', async () => {
    const h = unbindingHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    // Same construction as the drop above: the event waits while the running
    // request removes the route, so its write turn finds no holder.
    const unbind = h.transport.receive('c1', '{"id":"2","method":"hang"}');
    await tick();
    h.controlled.emit(sessionEvent('req-1:0', 'lost'));
    await tick();
    expect(sessionQueuesOf(h.layer).size).toBe(1);

    h.controlled.release();
    await unbind;
    await tick();
    await tick();

    expect(dropLines(h.logs())).toHaveLength(1);
    // The drop must release the Session's serial slot, or every later event
    // for that Session would wait behind it forever.
    expect(sessionQueuesOf(h.layer).size).toBe(0);
  });

  test('one session queue does not hold up another session', async () => {
    const h = sessionHarness({
      sessionOf: (method) =>
        method === 'bind1' ? 's1' : method === 'bind2' ? 's2' : undefined,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind1"}');
    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"2","method":"bind2"}');

    // s1's frame is handed to c1 and held there, so s1's serial slot stays
    // taken for the whole test.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('req-1:0', 'blocked', 's1'));
    await held.entered;

    // s2 shares no queue with s1, so its frame must be written while s1's is
    // still in flight.
    h.controlled.emit(sessionEvent('req-2:0', 'other session', 's2'));
    await tick();

    expect(
      eventFrames(h.transport, 'c2').map((frame) => frame.payload),
    ).toEqual(['other session']);
    // Only s1 is still queued; s2's queue was released as soon as it settled.
    expect(sessionQueuesOf(h.layer).size).toBe(1);

    held.release();
    await tick();
    await tick();

    expect(
      eventFrames(h.transport, 'c1').map((frame) => frame.payload),
    ).toEqual(['blocked']);
    expect(sessionQueuesOf(h.layer).size).toBe(0);
  });
});
