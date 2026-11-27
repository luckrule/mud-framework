import { describe, expect, test } from 'bun:test';

import { withDeadline } from 'mud-framework-test-support';

import { parseFrame, tick } from '../testing/common.ts';
import { sessionHarness } from '../testing/connection-layer.ts';

/**
 * The response write shares the event write's bound (SPEC 3.8). Without it a
 * frame the client never reads would hold the connection's single writer — and,
 * because the reply is part of the request work, the Session's terminal barrier
 * — forever, which no `sendTimeoutMs: Infinity` transport may turn into a
 * process-wide stall.
 */
describe('ConnectionLayer response write bound (SPEC 3.8, 5.4)', () => {
  test('a result write that never settles is bounded and recorded', async () => {
    const h = sessionHarness({ eventSendTimeoutMs: 10 });
    await h.layer.start();
    h.transport.connect('c1');

    const held = h.transport.holdSends('c1');
    const request = h.transport.receive('c1', '{"id":"r1","method":"work"}');
    await held.entered;
    // The send never settles; only the layer's own bound can end the wait.
    await withDeadline(request, 1000, 'the bounded reply should return');

    const failures = h
      .logs()
      .filter((line) => line.code === 'TRANSPORT_SEND_FAILED');
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      // Degraded channel, not a failed request (SPEC 7.1): the request's
      // success is unchanged, so this is a warning like the event path.
      level: 'warn',
      connectionId: 'c1',
      stage: 'send',
      reason: 'the transport did not settle the write within 10ms',
    });

    held.release();
    await h.layer.stop();
  });

  test('a bounded result write frees the connection writer for the next request', async () => {
    const h = sessionHarness({ eventSendTimeoutMs: 10 });
    await h.layer.start();
    h.transport.connect('c1');

    const held = h.transport.holdSends('c1');
    const first = h.transport.receive('c1', '{"id":"r1","method":"first"}');
    await held.entered;
    await withDeadline(first, 1000, 'the bounded reply should return');

    // One writer per connection: if the first reply still held it, this request
    // would never be written.
    const second = h.transport.receive('c1', '{"id":"r2","method":"second"}');
    await withDeadline(second, 1000, 'the writer should continue');

    expect(h.transport.sentTo('c1').map(parseFrame)).toEqual([
      { id: 'r1', ok: true, result: 'first' },
      { id: 'r2', ok: true, result: 'second' },
    ]);
    held.release();
    await h.layer.stop();
  });

  test('a bounded result write releases the Session terminal barrier', async () => {
    const h = sessionHarness({ eventSendTimeoutMs: 20 });
    await h.layer.start();
    h.transport.connect('c1');
    await h.transport.receive('c1', '{"id":"b1","method":"bind"}');

    // c1 holds s1 and starts a request whose reply never settles.
    const held = h.transport.holdSends('c1');
    const stuck = h.transport.receive('c1', '{"id":"r1","method":"work"}');
    await held.entered;

    // c2 takes s1 over while c1's request is still unfinished, then queues a
    // request behind the barrier that request holds (INV-004). Only the bounded
    // reply can release it.
    h.transport.connect('c2');
    await h.transport.receive('c2', '{"id":"b2","method":"bind"}');
    const next = h.transport.receive('c2', '{"id":"r2","method":"work"}');
    await withDeadline(next, 1000, 'the Session barrier should be released');

    expect(h.transport.sentTo('c2').map(parseFrame)).toContainEqual({
      id: 'r2',
      ok: true,
      result: 'work',
    });
    held.release();
    await stuck;
    await h.layer.stop();
  });

  test('a queue-full refusal frame is bounded even though it skips the scheduler', async () => {
    const h = sessionHarness({
      eventSendTimeoutMs: 10,
      maxQueuedPerConnection: 1,
    });
    await h.layer.start();
    h.transport.connect('c1');

    // One request executes (and hangs), one waits for its turn: the next input
    // exceeds the queue bound and is refused on the admission path, which
    // writes its frame directly instead of through the connection's writer.
    const running = h.transport.receive('c1', '{"id":"a","method":"hang"}');
    await tick();
    const queued = h.transport.receive('c1', '{"id":"b","method":"work"}');
    await tick();

    const held = h.transport.holdSends('c1');
    const refused = h.transport.receive('c1', '{"id":"c","method":"work"}');
    await held.entered;
    await withDeadline(refused, 1000, 'the refusal should be bounded');

    const failures = h
      .logs()
      .filter((line) => line.code === 'TRANSPORT_SEND_FAILED');
    expect(failures).toHaveLength(1);
    // The refused frame never entered the scheduler, so this line is the only
    // one that can name the connection and the request it was answering.
    expect(failures[0]).toMatchObject({
      level: 'warn',
      stage: 'send',
      connectionId: 'c1',
      requestId: expect.any(String),
    });
    expect(
      h.logs().filter((line) => line.code === 'FRAMEWORK_QUEUE_FULL'),
    ).toHaveLength(1);

    held.release();
    h.controlled.release();
    await Promise.all([running, queued]);
    await h.layer.stop();
  });

  test('a shutdown refusal frame is bounded too', async () => {
    const h = sessionHarness({ eventSendTimeoutMs: 10 });
    await h.layer.start();
    const connection = h.transport.connect('c1');

    const held = h.transport.holdSends('c1');
    const stopping = h.layer.stop();
    // Shutdown started synchronously, so this input is refused without ever
    // reaching the request scheduler.
    const refused = h.layer.onMessage(
      connection,
      '{"id":"late","method":"work"}',
    );
    await held.entered;
    await withDeadline(refused, 1000, 'the shutdown refusal should be bounded');
    await withDeadline(stopping, 2000, 'shutdown should finish');

    const failures = h
      .logs()
      .filter((line) => line.code === 'TRANSPORT_SEND_FAILED');
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      level: 'warn',
      stage: 'send',
      connectionId: 'c1',
    });
    held.release();
  });

  test('a write that settles normally is unaffected by the bound', async () => {
    const h = sessionHarness({ eventSendTimeoutMs: 10 });
    await h.layer.start();
    h.transport.connect('c1');

    await h.transport.receive('c1', '{"id":"r1","method":"work"}');

    expect(h.transport.sentTo('c1').map(parseFrame)).toEqual([
      { id: 'r1', ok: true, result: 'work' },
    ]);
    expect(
      h.logs().filter((line) => line.code === 'TRANSPORT_SEND_FAILED'),
    ).toEqual([]);
    await h.layer.stop();
  });
});
