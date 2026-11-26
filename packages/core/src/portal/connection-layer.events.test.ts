import { describe, expect, test } from 'bun:test';

import { withDeadline } from 'mud-framework-test-support';

import { createJsonLogger } from '../logging/json-logger.ts';
import { JsonCodec } from '../shared/codec.ts';
import type { ServerEvent } from '../shared/events.ts';
import { parseFrame, sleep, tick } from '../testing/common.ts';
import { sessionHarness } from '../testing/connection-layer.ts';
import type { ConnectionState } from './connection-admission.ts';
import { EventWrite, deliverServerEvent } from './connection-events.ts';
import type { EventDeliveryHost } from './connection-events.ts';
import { ConnectionWriter } from './connection-writer.ts';
import { EventBacklog } from './event-backlog.ts';
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

  test('a takeover does not reorder two events addressed to the same session', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');
    const hanging = h.transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);

    // The first event waits behind c1's hung chain. The second arrives after
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

    // c1's chain never reaches the queued event: dropping it on abort is what
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
    // exact interleaving pins the current EVENTS_PER_REQUEST_TURN value rather
    // than a contract; retuning that constant changes this expectation.
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
});

interface QueuedWrite {
  readonly state: ConnectionState;
  readonly write: EventWrite;
}

/**
 * A host whose event queue records frames instead of running them, so a test
 * can move the Session between two queued writes deterministically.
 */
function deferredDeliveryHost(
  options: {
    /** Makes every write on these connections reject, as a broken socket would. */
    failSends?: ReadonlySet<string>;
  } = {},
) {
  const lines: string[] = [];
  const logger = createJsonLogger({
    sink: (line) => {
      lines.push(line);
    },
  });
  const router = new SessionRouter({ logger });
  const backlog = new EventBacklog();
  const connections = new Map<string, ConnectionState>();
  const sent = new Map<string, (string | Uint8Array)[]>();
  for (const id of ['c1', 'c2']) {
    const frames: (string | Uint8Array)[] = [];
    sent.set(id, frames);
    connections.set(id, {
      connection: {
        id,
        send: (frame) => {
          if (options.failSends?.has(id) === true) {
            return Promise.reject(new Error('socket closed'));
          }
          frames.push(frame);
          return Promise.resolve();
        },
        close: () => Promise.resolve(),
      },
      writer: new ConnectionWriter({
        maxQueuedEvents: 1024,
        maxQueuedEventBytes: 4 * 1024 * 1024,
        backlog,
        logger,
      }),
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
    sessionQueues: new Map(),
    sendTimeoutMs: 1000,
    acceptingEvents: () => true,
    maxQueuedEvents: 1024,
    maxQueuedEventBytes: 4 * 1024 * 1024,
    backlog,
    queueEvent: (state, write) => {
      queue.push({ state, write });
      return { accepted: true };
    },
    cancelEvent: (_state, write) => {
      if (write.started || write.settled) {
        return false;
      }
      write.settle('aborted');
      return true;
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
    await tick();
    expect(h.queue.map((entry) => entry.state.connection.id)).toEqual(['c1']);

    h.router.bind('c2', 's1', Promise.resolve());
    await h.queue[0].write.run();
    await tick();
    expect(h.sent.get('c1')).toEqual([]);
    expect(h.queue.map((entry) => entry.state.connection.id)).toEqual([
      'c1',
      'c2',
    ]);

    // Back to c1 before the redelivered write runs: chasing again would let two
    // connections hand the event back and forth, so the retry is bounded.
    h.router.bind('c1', 's1', Promise.resolve());
    await h.queue[1].write.run();
    await tick();

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

  test('writes one session event at a time and clears the queue when idle', async () => {
    const h = deferredDeliveryHost();
    h.router.bind('c1', 's1', Promise.resolve());

    const first = deliverServerEvent(
      h.host,
      event({
        eventId: 'req-1:0',
        target: { kind: 'session', sessionId: 's1' },
        payload: 'first',
      }),
    );
    const second = deliverServerEvent(
      h.host,
      event({
        eventId: 'req-2:0',
        target: { kind: 'session', sessionId: 's1' },
        payload: 'second',
      }),
    );
    await tick();

    // c1 is free, but the second event must wait for the first to finish.
    expect(h.queue).toHaveLength(1);

    await h.queue[0].write.run();
    await tick();
    expect(h.queue).toHaveLength(2);
    expect(h.sent.get('c1')).toHaveLength(1);

    await h.queue[1].write.run();
    await Promise.all([first, second]);
    await tick();

    expect(
      (h.sent.get('c1') ?? []).map(parseFrame).map((frame) => frame.payload),
    ).toEqual(['first', 'second']);
    expect(h.host.sessionQueues.size).toBe(0);
  });

  test('drops a queued event whose holder left before the write ran', async () => {
    const h = deferredDeliveryHost();
    h.router.bind('c1', 's1', Promise.resolve());
    const delivery = deliverServerEvent(
      h.host,
      event({ target: { kind: 'session', sessionId: 's1' } }),
    );
    await tick();
    expect(h.queue).toHaveLength(1);

    // An unbind removes the route while the event waits; the write-time
    // resolution must see the missing holder rather than write to c1.
    h.router.release('c1');
    await h.queue[0].write.run();
    await delivery;

    expect(h.sent.get('c1')).toEqual([]);
    const dropped = h.logs().filter((line) => line.code === 'EVENT_DROPPED');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      eventId: 'req-1:0',
      targetKind: 'session',
      sessionId: 's1',
      reason: 'no connection holds the session',
    });
  });

  test('clears the queue after a failed write', async () => {
    const h = deferredDeliveryHost({ failSends: new Set(['c1']) });
    h.router.bind('c1', 's1', Promise.resolve());

    const delivery = deliverServerEvent(
      h.host,
      event({ target: { kind: 'session', sessionId: 's1' } }),
    );
    await tick();
    await h.queue[0].write.run();
    await delivery;
    await tick();

    expect(
      h.logs().filter((line) => line.code === 'TRANSPORT_SEND_FAILED'),
    ).toHaveLength(1);
    expect(h.host.sessionQueues.size).toBe(0);
  });

  test('clears the queue after a dropped event', async () => {
    const h = deferredDeliveryHost();
    h.router.bind('c1', 's1', Promise.resolve());

    const delivery = deliverServerEvent(
      h.host,
      event({ target: { kind: 'session', sessionId: 's1' } }),
    );
    await tick();
    h.router.release('c1');
    await h.queue[0].write.run();
    await delivery;
    await tick();

    expect(
      h.logs().filter((line) => line.code === 'EVENT_DROPPED'),
    ).toHaveLength(1);
    expect(h.host.sessionQueues.size).toBe(0);
  });

  test('one session queue does not hold up another session', async () => {
    const h = deferredDeliveryHost();
    h.router.bind('c1', 's1', Promise.resolve());
    h.router.bind('c2', 's2', Promise.resolve());

    const blocked = deliverServerEvent(
      h.host,
      event({
        eventId: 'req-1:0',
        target: { kind: 'session', sessionId: 's1' },
      }),
    );
    const other = deliverServerEvent(
      h.host,
      event({
        eventId: 'req-2:0',
        target: { kind: 'session', sessionId: 's2' },
      }),
    );
    await tick();

    // s1's event is parked, so s2's write must already be queued and settle.
    expect(h.queue.map((entry) => entry.state.connection.id)).toEqual([
      'c1',
      'c2',
    ]);
    await h.queue[1].write.run();
    await other;
    await tick();

    expect(h.sent.get('c2')).toHaveLength(1);
    expect(h.host.sessionQueues.size).toBe(1);

    await h.queue[0].write.run();
    await blocked;
    await tick();

    expect(h.sent.get('c1')).toHaveLength(1);
    expect(h.host.sessionQueues.size).toBe(0);
  });
});
