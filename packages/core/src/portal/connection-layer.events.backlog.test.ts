import { describe, expect, test } from 'bun:test';
import { Buffer } from 'node:buffer';

import { EVENT_BACKLOG_CLOSE_CODE } from '../shared/close-codes.ts';
import type { ServerEvent } from '../shared/events.ts';
import { parseFrame, sleep, tick } from '../testing/common.ts';
import { sessionHarness } from '../testing/connection-layer.ts';
import { BACKLOG_CLOSED_REASON } from './connection-admission.ts';

function connectionEvent(eventId: string, payload: unknown = 'x'): ServerEvent {
  return {
    eventId,
    target: { kind: 'connection', connectionId: 'c1' },
    event: 'note',
    payload,
  };
}

function sessionEvent(eventId: string, payload: unknown): ServerEvent {
  return {
    eventId,
    target: { kind: 'session', sessionId: 's1' },
    event: 'note',
    payload,
  };
}

function dropLines(
  lines: Record<string, unknown>[],
): Record<string, unknown>[] {
  return lines.filter((line) => line.code === 'EVENT_DROPPED');
}

function highLines(
  lines: Record<string, unknown>[],
): Record<string, unknown>[] {
  return lines.filter((line) => line.code === 'EVENT_BACKLOG_HIGH');
}

/** Wire size of the event frame a test emits with payload `'x'`. */
function eventBytes(eventId: string): number {
  return Buffer.byteLength(
    JSON.stringify({
      kind: 'event',
      eventId,
      event: 'note',
      payload: 'x',
    }),
    'utf8',
  );
}

function frameLabels(frames: readonly (string | Uint8Array)[]): unknown[] {
  return frames.map(parseFrame).map((frame) => frame.id ?? frame.eventId);
}

function disconnectLines(
  lines: Record<string, unknown>[],
): Record<string, unknown>[] {
  return lines.filter((line) => line.code === 'EVENT_BACKLOG_DISCONNECTED');
}

describe('ConnectionLayer event backlog bounds', () => {
  test('closes the connection at the frame bound instead of dropping the event', async () => {
    const h = sessionHarness({
      maxQueuedEvents: 2,
      maxQueuedEventBytes: 1_000_000,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // The first event is on the wire but not settled (counted until it is);
    // the second fills the queue, so the third is the one that overflows.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(connectionEvent('e:0'));
    h.controlled.emit(connectionEvent('e:1'));
    await held.entered;
    h.controlled.emit(connectionEvent('e:2'));
    await tick();

    // The frame is not discarded: the connection that could not accept it is
    // closed with 1013 (Try Again Later) so the client learns and reconnects.
    expect(h.transport.closedWith('c1')).toEqual({
      code: EVENT_BACKLOG_CLOSE_CODE,
      reason: 'server event backlog is full',
    });
    const disconnects = disconnectLines(h.logs());
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      level: 'warn',
      connectionId: 'c1',
      buffer: 'connection',
      eventId: 'e:2',
      event: 'note',
      queuedEvents: 2,
      maxQueuedEvents: 2,
      maxQueuedEventBytes: 1_000_000,
      closeCode: EVENT_BACKLOG_CLOSE_CODE,
    });
    expect(typeof disconnects[0]?.queuedEventBytes).toBe('number');
    expect(h.layer.health().eventBacklog.dropped).toBe(1);

    // The frame already handed to the transport is left to its own outcome; the
    // queued one is never written, because the connection is gone.
    held.release();
    await tick();
    expect(frameLabels(h.transport.sentTo('c1'))).toEqual(['bind', 'e:0']);
  });

  test('closes the connection at the byte bound and reports the frame wire size', async () => {
    // Non-ASCII payload: its UTF-8 wire size is larger than its code-unit
    // length, so the charged size must follow the bytes the transport sends.
    const payload = '洞穴';
    const frameBytes = Buffer.byteLength(
      JSON.stringify({
        kind: 'event',
        eventId: 'e:0',
        event: 'note',
        payload,
      }),
      'utf8',
    );
    const h = sessionHarness({
      maxQueuedEvents: 100,
      maxQueuedEventBytes: frameBytes * 2,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    const held = h.transport.holdSends('c1');
    h.controlled.emit(connectionEvent('e:0', payload));
    h.controlled.emit(connectionEvent('e:1', payload));
    await held.entered;
    h.controlled.emit(connectionEvent('e:2', payload));
    await tick();

    expect(h.transport.closedWith('c1')).toMatchObject({
      code: EVENT_BACKLOG_CLOSE_CODE,
    });
    const disconnects = disconnectLines(h.logs());
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      buffer: 'connection',
      eventId: 'e:2',
      queuedEvents: 2,
      queuedEventBytes: frameBytes * 2,
      maxQueuedEventBytes: frameBytes * 2,
    });

    held.release();
    await tick();
    expect(frameLabels(h.transport.sentTo('c1'))).toEqual(['bind', 'e:0']);
  });

  test('closes the Session holder when the Session pending queue overflows', async () => {
    const payload = { text: 'held' };
    const frameBytes = Buffer.byteLength(
      JSON.stringify({
        kind: 'event',
        eventId: 'e:1',
        event: 'note',
        payload,
      }),
      'utf8',
    );
    const h = sessionHarness({
      maxQueuedEvents: 2,
      maxQueuedEventBytes: 1_000_000,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // e:0 is handed to c1's connection queue and held there; e:1 and e:2 fill
    // the Session's own queue, so e:3 is the one the Session bound refuses.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('e:0', payload));
    await held.entered;
    h.controlled.emit(sessionEvent('e:1', payload));
    h.controlled.emit(sessionEvent('e:2', payload));
    h.controlled.emit(sessionEvent('e:3', payload));
    await tick();

    // The Session queue has no connection of its own, so the holder is the
    // connection to close.
    expect(h.transport.closedWith('c1')).toMatchObject({
      code: EVENT_BACKLOG_CLOSE_CODE,
    });
    const disconnects = disconnectLines(h.logs());
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      buffer: 'session',
      connectionId: 'c1',
      sessionId: 's1',
      eventId: 'e:3',
      queuedEvents: 2,
      queuedEventBytes: frameBytes * 2,
      maxQueuedEvents: 2,
      maxQueuedEventBytes: 1_000_000,
    });
    expect(h.layer.health().eventBacklog.dropped).toBe(1);

    held.release();
    await tick();
    await tick();
    // Nothing more is written to the connection that overflowed.
    expect(frameLabels(h.transport.sentTo('c1'))).toEqual(['bind', 'e:0']);
  });

  test('closes the Session holder at the Session byte bound', async () => {
    // CJK payload: the UTF-8 frame is larger than its code-unit length, so only
    // a byte-based bound can be the one that trips here.
    const payload = { text: '洞窟洞窟' };
    const frameBytes = Buffer.byteLength(
      JSON.stringify({
        kind: 'event',
        eventId: 'e:1',
        event: 'note',
        payload,
      }),
      'utf8',
    );
    const h = sessionHarness({
      maxQueuedEvents: 100,
      maxQueuedEventBytes: frameBytes * 2,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // e:0 is held on c1's connection queue; e:1 and e:2 fill the Session queue
    // to its byte bound, so e:3 is refused by bytes, not by frame count.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('e:0', payload));
    await held.entered;
    h.controlled.emit(sessionEvent('e:1', payload));
    h.controlled.emit(sessionEvent('e:2', payload));
    h.controlled.emit(sessionEvent('e:3', payload));
    await tick();

    expect(h.transport.closedWith('c1')).toMatchObject({
      code: EVENT_BACKLOG_CLOSE_CODE,
    });
    const disconnects = disconnectLines(h.logs());
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      buffer: 'session',
      connectionId: 'c1',
      sessionId: 's1',
      eventId: 'e:3',
      queuedEvents: 2,
      queuedEventBytes: frameBytes * 2,
      maxQueuedEvents: 100,
      maxQueuedEventBytes: frameBytes * 2,
    });
    expect(h.layer.health().eventBacklog.dropped).toBe(1);

    held.release();
    await tick();
    await tick();
    expect(frameLabels(h.transport.sentTo('c1'))).toEqual(['bind', 'e:0']);
  });

  test('drops a Session event with no holder instead of closing anything', async () => {
    const h = sessionHarness({ maxQueuedEvents: 1 });
    await h.layer.start();
    h.transport.connect('c1');

    // No holder exists, so there is no connection to close: the overflow has no
    // target to act on and the event is the ordinary target-missing drop.
    h.controlled.emit(sessionEvent('e:0', 'x'));
    await tick();
    h.controlled.emit(sessionEvent('e:1', 'x'));
    await tick();

    expect(h.transport.closedWith('c1')).toBeUndefined();
    expect(disconnectLines(h.logs())).toEqual([]);
    expect(dropLines(h.logs()).map((line) => line.eventId)).toContain('e:1');
    expect(h.layer.health().eventBacklog.dropped).toBe(0);
  });

  test('lets a request already executing settle instead of cancelling it', async () => {
    const h = sessionHarness({
      maxQueuedEvents: 1,
      maxQueuedEventBytes: 1_000_000,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // A hanging request occupies c1's writer and stays in flight while the
    // event that overflows the queue arrives.
    const hanging = h.transport.receive('c1', '{"id":"work","method":"hang"}');
    await tick();
    h.controlled.emit(connectionEvent('e:0'));
    h.controlled.emit(connectionEvent('e:1'));
    await tick();

    expect(h.transport.closedWith('c1')).toMatchObject({
      code: EVENT_BACKLOG_CLOSE_CODE,
    });
    // The backlog close is not a client disconnect: the request keeps running.
    expect(h.controlled.started).toContain('hang');
    expect(h.controlled.finished).not.toContain('hang');

    h.controlled.release();
    await hanging;
    await tick();
    await tick();

    // It ran to completion; the result simply had nowhere to go.
    expect(h.controlled.finished).toContain('hang');
    expect(
      h.logs().filter((line) => line.code === 'TRANSPORT_SEND_FAILED'),
    ).toHaveLength(1);
  });

  test('does not start a request that was still queued at the overflow', async () => {
    const h = sessionHarness({
      maxQueuedEvents: 1,
      maxQueuedEventBytes: 1_000_000,
    });
    await h.layer.start();
    h.transport.connect('c1');

    // The report of the close is withheld, which is the ordering a socket
    // transport has: `close()` returns at once, its `close` event lands in a
    // later macrotask. The writer's next drain turn is a microtask, so without a
    // fence at the decision the queued request would start in this window.
    h.transport.deferCloseReport('c1');
    const hanging = h.transport.receive('c1', '{"id":"a","method":"hang"}');
    await tick();
    const queued = h.transport.receive('c1', '{"id":"b","method":"work"}');
    await tick();
    expect(h.controlled.started).toEqual(['hang']);

    // One frame fills the queue and the next overflows it: the connection is
    // closed for backlog while the request still waits behind `hang`.
    h.controlled.emit(connectionEvent('e:0'));
    h.controlled.emit(connectionEvent('e:1'));
    await tick();
    expect(h.transport.closedWith('c1')).toMatchObject({
      code: EVENT_BACKLOG_CLOSE_CODE,
    });

    h.controlled.release();
    await hanging;
    await queued;
    await tick();

    // Only the request that had already started ran; the queued one never
    // reached the executor, and it was not answered either: a shutdown refusal
    // would misreport why the layer stopped serving it.
    expect(h.controlled.started).toEqual(['hang']);
    expect(h.controlled.finished).toEqual(['hang']);
    const dropped = h
      .logs()
      .filter((line) => line.msg === 'request dropped before start');
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      requestId: expect.any(String),
      connectionId: 'c1',
      clientRequestId: 'b',
      method: 'work',
      code: 'TRANSPORT_CLOSED',
      reason: BACKLOG_CLOSED_REASON,
    });
    expect(
      h.logs().filter((line) => line.code === 'FRAMEWORK_SHUTDOWN'),
    ).toEqual([]);

    await h.transport.reportClose('c1');
    await tick();
  });

  test('drops the frames still queued behind the overflow with a backlog reason', async () => {
    const h = sessionHarness({
      maxQueuedEvents: 2,
      maxQueuedEventBytes: 1_000_000,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // e:0 is held on the wire, e:1 fills the queue, e:2 overflows it. The close
    // then aborts the queue, so e:1 is dropped unwritten.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(connectionEvent('e:0'));
    h.controlled.emit(connectionEvent('e:1'));
    await held.entered;
    h.controlled.emit(connectionEvent('e:2'));
    await tick();

    const drops = dropLines(h.logs());
    expect(drops).toHaveLength(1);
    expect(drops[0]).toMatchObject({
      eventId: 'e:1',
      targetKind: 'connection',
      connectionId: 'c1',
      // Distinguishable from a client that simply left: this drop is the
      // layer's own decision, and the two need different follow-up.
      reason: BACKLOG_CLOSED_REASON,
    });
    expect(drops[0]?.droppedTotal).toBe(1);

    held.release();
    await tick();
    await tick();
  });

  test('serves a reconnected client after a backlog close', async () => {
    const h = sessionHarness({
      maxQueuedEvents: 1,
      maxQueuedEventBytes: 1_000_000,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    const held = h.transport.holdSends('c1');
    h.controlled.emit(connectionEvent('e:0'));
    h.controlled.emit(connectionEvent('e:1'));
    await tick();
    expect(h.transport.closedWith('c1')).toMatchObject({
      code: EVENT_BACKLOG_CLOSE_CODE,
    });
    held.release();
    await tick();

    // The same Session comes back on a fresh connection (ids are never reused,
    // SPEC 5.2) and the layer serves it normally.
    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"login","method":"bind"}');
    await h.transport.receive('c2', '{"id":"after","method":"work"}');
    expect(frameLabels(h.transport.sentTo('c2'))).toEqual(['login', 'after']);
    expect(h.transport.closedWith('c2')).toBeUndefined();
  });

  test('reports every event buffer and counts only backlog disconnects as dropped', async () => {
    const payload = { text: 'held' };
    const frameBytes = Buffer.byteLength(
      JSON.stringify({
        kind: 'event',
        eventId: 'e:0',
        event: 'note',
        payload,
      }),
      'utf8',
    );
    const h = sessionHarness({
      maxQueuedEvents: 2,
      maxQueuedEventBytes: 1_000_000,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    expect(h.layer.health().eventBacklog).toEqual({
      connections: 0,
      frames: 0,
      bytes: 0,
      dropped: 0,
    });

    const held = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('e:0', payload));
    await held.entered;
    h.controlled.emit(sessionEvent('e:1', payload));
    h.controlled.emit(sessionEvent('e:2', payload));

    // e:0 is in c1's connection queue (one writing frame), e:1 and e:2 are
    // still in the Session's own queue: the Session contributes frames and
    // bytes but cannot make a connection's queue look non-empty.
    expect(h.layer.health().eventBacklog).toEqual({
      connections: 1,
      frames: 3,
      bytes: frameBytes * 3,
      dropped: 0,
    });

    // The Session queue is full, so its holder is closed: `dropped` counts
    // connections closed for backlog, not events discarded.
    h.controlled.emit(sessionEvent('e:3', payload));
    await tick();
    expect(h.layer.health().eventBacklog.dropped).toBe(1);

    // A target that is simply gone is not backpressure: it must not move the
    // counter a probe watches.
    h.controlled.emit({
      eventId: 'e:lost',
      target: { kind: 'connection', connectionId: 'nobody' },
      event: 'note',
      payload,
    });
    expect(h.layer.health().eventBacklog.dropped).toBe(1);

    held.release();
    await tick();
    await tick();
    await tick();
    expect(h.layer.health().eventBacklog).toEqual({
      connections: 0,
      frames: 0,
      bytes: 0,
      dropped: 1,
    });
  });

  test('keeps counting a queue whose connection was already removed', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    const held = h.transport.holdSends('c1');
    h.controlled.emit(connectionEvent('e:0'));
    await held.entered;
    await h.transport.disconnect('c1');

    // The connection is gone from the admission table, but a frame it accepted
    // is still in flight: dropping it from the total would under-report the
    // memory the layer still holds.
    expect(h.layer.health().eventBacklog).toMatchObject({
      connections: 1,
      frames: 1,
      dropped: 0,
    });

    held.release();
    await tick();
    await tick();
    expect(h.layer.health().eventBacklog).toEqual({
      connections: 0,
      frames: 0,
      bytes: 0,
      dropped: 0,
    });
  });

  test('reports one discard summary at shutdown and keeps it out of dropped', async () => {
    const payload = { text: 'held' };
    const frameBytes = Buffer.byteLength(
      JSON.stringify({ kind: 'event', eventId: 'e:1', event: 'note', payload }),
      'utf8',
    );
    const h = sessionHarness({ maxQueuedEvents: 100 });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // e:0 is being written, e:1 waits in the Session queue and e:2 waits in
    // c1's connection queue: two of the three buffers hold unwritten frames.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('e:0', payload));
    h.controlled.emit(sessionEvent('e:1', payload));
    h.controlled.emit(connectionEvent('e:2', payload));
    await held.entered;

    await h.layer.stop();

    const summary = h
      .logs()
      .filter((line) => line.code === 'EVENT_BACKLOG_DISCARDED');
    expect(summary).toHaveLength(1);
    expect(summary[0]).toMatchObject({
      reason: 'shutdown',
      count: 2,
      connections: 1,
      frames: 2,
      bytes: frameBytes * 2,
    });
    expect(dropLines(h.logs())).toEqual([]);
    // A rolling restart must not look like runtime backpressure to a probe.
    expect(h.layer.health().eventBacklog.dropped).toBe(0);

    held.release();
    await tick();
    await tick();
    expect(h.layer.health().eventBacklog).toEqual({
      connections: 0,
      frames: 0,
      bytes: 0,
      dropped: 0,
    });
  });

  test('a frame dropped on disconnect does not reach the shutdown summary', async () => {
    const h = sessionHarness({ maxQueuedEvents: 100 });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // e:0 is on the wire; e:1 is still queued when the client leaves. The
    // queued frame is a disconnect drop, so by the time the layer stops there
    // is nothing left for the shutdown discard to report: one frame is
    // accounted for exactly once.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(connectionEvent('e:0'));
    h.controlled.emit(connectionEvent('e:1'));
    await held.entered;
    await h.transport.disconnect('c1');
    await tick();

    expect(dropLines(h.logs()).map((line) => line.eventId)).toEqual(['e:1']);

    await h.layer.stop();

    expect(
      h.logs().filter((line) => line.code === 'EVENT_BACKLOG_DISCARDED'),
    ).toEqual([]);
    // A disconnect drop is not backpressure either.
    expect(h.layer.health().eventBacklog.dropped).toBe(0);

    held.release();
    await tick();
    await tick();
  });

  test('drops a queued connection event when its connection closes before the write', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // e:0 is handed to the transport; e:1 waits behind it in the queue.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(connectionEvent('e:0'));
    h.controlled.emit(connectionEvent('e:1'));
    await held.entered;

    await h.transport.disconnect('c1');
    held.release();
    await tick();
    await tick();

    // The queued frame targets a closed connection, so it is a drop, exactly
    // as for a Session target; only the frame already on the wire can fail as
    // a transport failure.
    expect(
      h.logs().filter((line) => line.code === 'TRANSPORT_SEND_FAILED'),
    ).toEqual([]);
    const drops = dropLines(h.logs());
    expect(drops).toHaveLength(1);
    expect(drops[0]).toMatchObject({
      eventId: 'e:1',
      event: 'note',
      targetKind: 'connection',
      connectionId: 'c1',
      reason: 'the target connection closed before the event was written',
    });
  });

  test('ignores executor dispatch that arrives after stop has unsubscribed', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');
    const before = h.layer.health().eventBacklog;

    const stopping = h.layer.stop();
    // `doStop` unsubscribes before its first await, so a dispatch that lands
    // afterwards never reaches the layer. This case covers that unsubscribe
    // path only; the `acceptingEvents` guard for a delivery already in progress
    // is not observed here.
    expect(() => h.controlled.emit(connectionEvent('e:late'))).not.toThrow();
    expect(() => h.controlled.emit(sessionEvent('e:late', 'x'))).not.toThrow();
    await stopping;
    await tick();
    await tick();

    // The transport cannot be read once stopped, so the frame count and the
    // absence of diagnostics are the observable evidence: nothing was written
    // and nothing was reported.
    expect(h.layer.health().eventBacklog).toEqual(before);
    expect(h.logs().filter((line) => line.code === 'EVENT_DROPPED')).toEqual(
      [],
    );
    expect(
      h.logs().filter((line) => line.code === 'TRANSPORT_SEND_FAILED'),
    ).toEqual([]);
  });

  test('applies the documented defaults when no bound is passed', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // Observed through the overflow log, not a private field: the first frame
    // is held on the wire and the next 1023 fill the queue to 1024, so the
    // 1025th event is the one refused.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(connectionEvent('e:0'));
    for (let i = 1; i <= 1023; i += 1) {
      h.controlled.emit(connectionEvent(`e:${i}`));
    }
    await held.entered;
    expect(disconnectLines(h.logs())).toEqual([]);

    h.controlled.emit(connectionEvent('e:1024'));
    const disconnects = disconnectLines(h.logs());
    expect(disconnects.map((line) => line.eventId)).toEqual(['e:1024']);
    expect(disconnects[0]).toMatchObject({
      buffer: 'connection',
      queuedEvents: 1024,
      maxQueuedEvents: 1024,
      maxQueuedEventBytes: 4 * 1024 * 1024,
    });

    held.release();
    await tick();
  });

  test('closes the connection once for a flood and leaves the rest of the layer serving', async () => {
    const h = sessionHarness({
      maxQueuedEvents: 2,
      maxQueuedEventBytes: 1_000_000,
    });
    await h.layer.start();
    h.transport.connect('c1');
    h.transport.connect('c2');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');
    await h.transport.receive('c2', '{"id":"bind2","method":"bind2"}');

    // A slow reader: the first frame is held on the wire, the next fills the
    // queue, and the first event after that closes the connection. Later events
    // for the same connection find it gone and are ordinary target-missing
    // drops, not a second close and not a per-event backlog line.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(connectionEvent('e:0'));
    h.controlled.emit(connectionEvent('e:1'));
    await held.entered;
    for (let i = 2; i < 200; i += 1) {
      h.controlled.emit(connectionEvent(`e:${i}`));
    }
    await tick();

    expect(h.transport.closedWith('c1')).toMatchObject({
      code: EVENT_BACKLOG_CLOSE_CODE,
    });
    // One line for the close, not one per refused event: the shape that used to
    // write a warn per event.
    const disconnects = disconnectLines(h.logs());
    expect(disconnects).toHaveLength(1);
    expect(disconnects[0]).toMatchObject({
      buffer: 'connection',
      eventId: 'e:2',
      connectionId: 'c1',
    });
    // One connection closed, so the probe sees one, not the event count.
    expect(h.layer.health().eventBacklog.dropped).toBe(1);

    // c2 never overflowed: it is untouched and still serves requests.
    expect(h.transport.closedWith('c2')).toBeUndefined();
    await h.transport.receive('c2', '{"id":"after","method":"work"}');
    expect(frameLabels(h.transport.sentTo('c2'))).toContain('after');

    held.release();
    await tick();
  });

  test('counts a redelivered Session event once and clears it after settlement', async () => {
    const h = sessionHarness();
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"1","method":"bind"}');

    // A hanging request occupies c1's writer, so the Session event that follows
    // stays queued and has not started writing.
    const hanging = h.transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);
    h.controlled.emit(sessionEvent('e:0', 'first'));
    await tick();

    const accepted = h.layer.health().eventBacklog;
    expect(accepted.frames).toBe(1);
    expect(accepted.connections).toBe(1);
    // Nothing was written: the frame is queued, not in flight.
    expect(
      h.transport
        .sentTo('c1')
        .map(parseFrame)
        .filter((frame) => frame.kind === 'event'),
    ).toEqual([]);

    // c2 takes the Session over while e:0 still waits in c1's queue. The frame
    // moves between buffers, so the totals must not grow.
    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"3","method":"bind"}');
    const afterTakeover = h.layer.health().eventBacklog;
    expect(afterTakeover.frames).toBe(accepted.frames);
    expect(afterTakeover.bytes).toBe(accepted.bytes);

    // Releasing the request lets the queued frame's turn arrive: it re-resolves
    // the holder, sees c2, and is redelivered there rather than written to c1.
    h.controlled.release();
    await hanging;
    await tick();
    await tick();
    await tick();

    expect(
      h.transport
        .sentTo('c2')
        .map(parseFrame)
        .filter((frame) => frame.kind === 'event')
        .map((frame) => frame.payload),
    ).toEqual(['first']);
    expect(
      h.transport
        .sentTo('c1')
        .map(parseFrame)
        .filter((frame) => frame.kind === 'event'),
    ).toEqual([]);
    expect(dropLines(h.logs())).toEqual([]);
    expect(h.layer.health().eventBacklog).toEqual({
      connections: 0,
      frames: 0,
      bytes: 0,
      dropped: 0,
    });
  });
});

describe('ConnectionLayer aggregate backlog warning', () => {
  test('warns once when the aggregate crosses the threshold and stays crossed', async () => {
    const bytes = eventBytes('e:0');
    const h = sessionHarness({
      maxQueuedEvents: 100,
      maxQueuedEventBytes: 1_000_000,
      eventBacklogWarnBytes: bytes * 2,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // e:0 is on the wire and held; e:1 and e:2 pile up behind it. The second
    // accepted frame is what reaches the threshold, and the third must not
    // report again.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('e:0', 'x'));
    h.controlled.emit(sessionEvent('e:1', 'x'));
    h.controlled.emit(sessionEvent('e:2', 'x'));
    await held.entered;
    await tick();

    const warnings = highLines(h.logs());
    expect(warnings).toHaveLength(1);
    expect(warnings[0]).toMatchObject({
      bytes: bytes * 2,
      frames: 2,
      activeConnections: 1,
      warnBytes: bytes * 2,
    });

    // The warning is an observation: every accepted event is still written.
    held.release();
    await tick();
    await tick();
    await tick();
    expect(
      h.transport
        .sentTo('c1')
        .map(parseFrame)
        .filter((frame) => frame.kind === 'event')
        .map((frame) => frame.eventId),
    ).toEqual(['e:0', 'e:1', 'e:2']);
    expect(dropLines(h.logs())).toEqual([]);
  });

  test('rearms only after the aggregate drains to empty', async () => {
    const bytes = eventBytes('e:0');
    const h = sessionHarness({
      maxQueuedEvents: 100,
      maxQueuedEventBytes: 1_000_000,
      eventBacklogWarnBytes: bytes * 2,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    const held = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('e:0', 'x'));
    h.controlled.emit(sessionEvent('e:1', 'x'));
    await held.entered;
    await tick();
    expect(highLines(h.logs())).toHaveLength(1);

    // Draining everything ends the episode; nothing is logged for the recovery
    // itself, only for the next crossing.
    held.release();
    await tick();
    await tick();
    await tick();
    expect(h.layer.health().eventBacklog.frames).toBe(0);
    expect(highLines(h.logs())).toHaveLength(1);

    const again = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('e:2', 'x'));
    h.controlled.emit(sessionEvent('e:3', 'x'));
    await again.entered;
    await tick();
    expect(highLines(h.logs())).toHaveLength(2);

    again.release();
    await tick();
    await tick();
    await tick();
  });

  test('a backlog that stays above the threshold logs once across many frames', async () => {
    const bytes = eventBytes('e:0');
    const h = sessionHarness({
      maxQueuedEvents: 100,
      maxQueuedEventBytes: 1_000_000,
      eventBacklogWarnBytes: bytes * 3,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // Every Session frame is charged to the Session queue and then to the
    // connection queue, so the total dips by one between the two calls. With a
    // rearm rule of "below the threshold" that dip would re-arm and the next add
    // would warn again, once per frame. Only draining to empty may re-arm.
    const held = h.transport.holdSends('c1');
    for (let i = 0; i < 20; i += 1) {
      h.controlled.emit(sessionEvent(`e:${i}`, 'x'));
    }
    await held.entered;
    await tick();
    expect(highLines(h.logs())).toHaveLength(1);

    held.release();
    await tick();
    await tick();
    await tick();
    await tick();
    // The frames were all written: the warning never blocked a delivery.
    expect(h.layer.health().eventBacklog.frames).toBe(0);
    expect(highLines(h.logs())).toHaveLength(1);
    expect(dropLines(h.logs())).toEqual([]);
  });

  test('a handoff between buffers does not rearm, so one frame logs once', async () => {
    const bytes = eventBytes('e:0');
    const h = sessionHarness({
      maxQueuedEvents: 100,
      maxQueuedEventBytes: 1_000_000,
      eventBacklogWarnBytes: bytes,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    // One Session frame is the whole backlog. The pump shifts it out of the
    // Session queue (total drops to zero) and queues it on the connection
    // (total back to one) in two calls; rearming on the dip would fire again
    // for every single frame.
    const held = h.transport.holdSends('c1');
    h.controlled.emit(sessionEvent('e:0', 'x'));
    await held.entered;
    await tick();
    expect(highLines(h.logs())).toHaveLength(1);

    held.release();
    await tick();
    await tick();
    await tick();
    expect(highLines(h.logs())).toHaveLength(1);
  });

  test('does not warn while the aggregate stays below the threshold', async () => {
    const bytes = eventBytes('e:0');
    const h = sessionHarness({
      maxQueuedEvents: 100,
      maxQueuedEventBytes: 1_000_000,
      eventBacklogWarnBytes: bytes * 10,
    });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"bind","method":"bind"}');

    const held = h.transport.holdSends('c1');
    for (let i = 0; i < 5; i += 1) {
      h.controlled.emit(sessionEvent(`e:${i}`, 'x'));
    }
    await held.entered;
    await tick();

    expect(highLines(h.logs())).toEqual([]);
    held.release();
    await tick();
    await tick();
    await tick();
    expect(highLines(h.logs())).toEqual([]);
  });
});
