import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import type { Logger } from '../logging/types.ts';
import type { ExecutorRequest, RequestExecutor } from '../shared/executor.ts';
import { parseFrame, sleep } from '../testing/common.ts';
import {
  fakeConnection,
  harness,
  sessionHarness,
} from '../testing/connection-layer.ts';
import { FakeTransport } from '../testing/fake-transport.ts';
import { ConnectionLayer } from './connection-layer.ts';

describe('ConnectionLayer disconnect', () => {
  test('records a send failure when a successful result reaches a closed connection', async () => {
    const transport = new FakeTransport();
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });
    const gate = Promise.withResolvers<void>();
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      // An executor that ignores the cancel and still reports success: the
      // layer must not throw, and the failed write must still be recorded.
      execute: async () => {
        await gate.promise;
        return { ok: true, result: 'done' };
      },
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
      health: () => ({ state: 'ready', ready: true }),
    };
    const layer = new ConnectionLayer({ transport, executor, logger });
    await layer.start();
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"pending"}');
    await sleep(0);
    await transport.disconnect('c1', 'client left');
    gate.resolve();
    await inFlight;

    expect(transport.sentTo('c1')).toHaveLength(0);
    const failures = lines
      .map(parseFrame)
      .filter((entry) => entry.code === 'TRANSPORT_SEND_FAILED');
    expect(failures).toHaveLength(1);
    expect(failures[0]).toMatchObject({
      level: 'warn',
      stage: 'send',
      connectionId: 'c1',
    });
    await layer.stop();
  });

  test('ends a request quietly when its connection closed before the outcome', async () => {
    const { transport, dispatcher, logs } = await harness({ level: 'debug' });
    const entered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    dispatcher.register(
      'pending',
      async () => {
        entered.resolve();
        await gate.promise;
        return 'done';
      },
      { access: 'public' },
    );
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"pending"}');
    await entered.promise;
    // The disconnect cancels the request, so its outcome is TRANSPORT_CLOSED.
    await transport.disconnect('c1', 'client left');
    gate.resolve();
    await inFlight;
    await sleep(5);

    const entries = logs();
    expect(entries.filter((entry) => entry.level === 'error')).toHaveLength(0);
    expect(
      entries.filter((entry) => entry.code === 'TRANSPORT_SEND_FAILED'),
    ).toHaveLength(0);
    expect(transport.sentTo('c1')).toHaveLength(0);
    const ended = entries.filter(
      (entry) => entry.msg === 'request ended after the connection closed',
    );
    expect(ended).toHaveLength(1);
    expect(ended[0]).toMatchObject({
      level: 'debug',
      method: 'pending',
      code: 'TRANSPORT_CLOSED',
      connectionId: 'c1',
    });
    expect(ended[0]?.durationMs).toBeGreaterThanOrEqual(0);
    expect(Number.isFinite(ended[0]?.durationMs)).toBe(true);
  });

  test('logs a send failure at warn while the connection is tracked', async () => {
    const transport = new FakeTransport();
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      execute: () =>
        Promise.resolve({
          ok: false,
          error: { code: 'INTERNAL_ERROR', message: 'backend is gone' },
        }),
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
      health: () => ({ state: 'ready', ready: true }),
    };
    const layer = new ConnectionLayer({ transport, executor, logger });
    await layer.start();
    const connection = transport.connect('c1');
    // The connection is still tracked, but the write itself fails. The request
    // already failed on its own; an unwritable reply is a degraded channel, not
    // a second failure, so it is a warning (SPEC 3.8, 7.1).
    connection.send = () => Promise.reject(new Error('socket is broken'));

    await transport.receive('c1', '{"id":"r1","method":"echo"}');

    const entries = lines.map(parseFrame);
    expect(
      entries.filter((entry) => entry.msg === 'request failed'),
    ).toHaveLength(1);
    const failedSend = entries.filter(
      (entry) => entry.code === 'TRANSPORT_SEND_FAILED',
    );
    expect(failedSend).toHaveLength(1);
    expect(failedSend[0]).toMatchObject({
      level: 'warn',
      stage: 'send',
      connectionId: 'c1',
      reason: 'socket is broken',
    });
    await layer.stop();
  });

  test('a successful late result cannot route a session to the closed connection', async () => {
    const transport = new FakeTransport();
    const lines: string[] = [];
    const logger: Logger = createJsonLogger({
      sink: (line) => {
        lines.push(line);
      },
    });
    const started = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    const requests: ExecutorRequest[] = [];
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      // Deliberately ignores cancel and still reports a binding success after
      // the connection is gone: the late result must not create a route.
      execute: async (request) => {
        requests.push(request);
        if (request.method === 'login') {
          started.resolve();
          await gate.promise;
          return {
            ok: true,
            result: 'welcome',
            sessionChange: { type: 'bind', sessionId: 's1' },
          };
        }
        return { ok: true, result: 'next' };
      },
      cancel: () => Promise.resolve(),
      close: () => Promise.resolve(),
      health: () => ({ state: 'ready', ready: true }),
    };
    const layer = new ConnectionLayer({ transport, executor, logger });
    await layer.start();
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"login"}');
    await started.promise;
    await transport.disconnect('c1', 'client left');
    gate.resolve();
    await inFlight;
    await sleep(5);

    // A new transport connection reuses the id. If the late success had been
    // applied, this request would enter with the Session bound to that id.
    const sent: string[] = [];
    const replacement = {
      id: 'c1',
      send: (payload: string | Uint8Array) => {
        sent.push(typeof payload === 'string' ? payload : '<binary>');
        return Promise.resolve();
      },
      close: () => Promise.resolve(),
    };
    await layer.onConnect(replacement);
    await layer.onMessage(replacement, '{"id":"r2","method":"next"}');

    expect(requests).toHaveLength(2);
    expect(requests[1]?.sessionId).toBeUndefined();
    expect(sent.map(parseFrame)).toMatchObject([
      { id: 'r2', ok: true, result: 'next' },
    ]);
    expect(
      lines.map(parseFrame).filter((entry) => entry.msg === 'session bound'),
    ).toHaveLength(0);
    await layer.stop();
  });

  test('drops a request whose connection closed before it started', async () => {
    const { transport, dispatcher, logs } = await harness();
    let calls = 0;
    dispatcher.register(
      'echo',
      (_ctx, payload) => {
        calls += 1;
        return Promise.resolve(payload);
      },
      { access: 'public' },
    );
    transport.connect('c1');

    // Queued behind the current request work, so the disconnect wins the race
    // to start it.
    const inFlight = transport.receive('c1', '{"id":"r1","method":"echo"}');
    await transport.disconnect('c1', 'client left');
    await inFlight;

    // Executing it would commit writes for a player who is already gone.
    expect(calls).toBe(0);
    expect(transport.sentTo('c1')).toHaveLength(0);
    const dropped = logs().filter(
      (entry) => entry.msg === 'request dropped before start',
    );
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      level: 'warn',
      code: 'TRANSPORT_CLOSED',
      connectionId: 'c1',
    });
  });

  test('cancels an in-flight request when its connection closes', async () => {
    const { transport, dispatcher } = await harness();
    const entered = Promise.withResolvers<void>();
    const gate = Promise.withResolvers<void>();
    let writeError = '';
    dispatcher.register(
      'pending',
      async (ctx) => {
        entered.resolve();
        await gate.promise;
        try {
          await ctx.storage.execute('INSERT 1');
          writeError = 'write was accepted';
        } catch (error) {
          writeError = error instanceof Error ? error.message : String(error);
        }
        return 'done';
      },
      { access: 'public' },
    );
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"pending"}');
    await entered.promise;
    // SPEC 3.6: a connection closing mid-request cancels it, so the handler's
    // later writes are rejected and its transaction rolls back.
    await transport.disconnect('c1', 'client left');
    gate.resolve();
    await inFlight;
    await sleep(5);

    expect(writeError).toMatch(/expired/);
    expect(transport.sentTo('c1')).toHaveLength(0);
  });

  test('aborts ctx.signal when a disconnect cancels the request', async () => {
    // The full path, not just the executor call: onDisconnect → executor.cancel
    // → dispatcher.cancel → expiry.controller.abort (SPEC 3.6). A handler that
    // only listens to the signal must be woken, not left waiting.
    const { transport, dispatcher } = await harness();
    const entered = Promise.withResolvers<void>();
    const woke = Promise.withResolvers<string>();
    dispatcher.register(
      'waiting',
      async (ctx) => {
        entered.resolve();
        const reason = await new Promise<unknown>((resolve) => {
          ctx.signal.addEventListener('abort', () => {
            resolve(ctx.signal.reason);
          });
        });
        woke.resolve(String(reason));
        return 'done';
      },
      { access: 'public' },
    );
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"waiting"}');
    await entered.promise;
    await transport.disconnect('c1', 'client left');

    expect(await woke.promise).toBe('TRANSPORT_CLOSED');
    await inFlight;
  });

  test('calls the executor cancel with TRANSPORT_CLOSED on disconnect', async () => {
    const transport = new FakeTransport();
    const cancelled: { requestId: string; code: string }[] = [];
    const gate = Promise.withResolvers<void>();
    const executor: RequestExecutor = {
      start: () => Promise.resolve(),
      onEvent: () => () => undefined,
      execute: async () => {
        await gate.promise;
        return { ok: true, result: null };
      },
      cancel: (requestId, code) => {
        cancelled.push({ requestId, code });
        return Promise.resolve();
      },
      close: () => Promise.resolve(),
      health: () => ({ state: 'ready', ready: true }),
    };
    const layer = new ConnectionLayer({ transport, executor });
    await layer.start();
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"echo"}');
    await sleep(0);
    await transport.disconnect('c1', 'client left');
    gate.resolve();
    await inFlight;

    expect(cancelled).toHaveLength(1);
    expect(cancelled[0]?.code).toBe('TRANSPORT_CLOSED');
    expect(cancelled[0]?.requestId.length).toBeGreaterThan(0);
    await layer.stop();
  });

  test('a late result after the disconnect does not throw', async () => {
    const { transport, dispatcher } = await harness();
    const gate = Promise.withResolvers<void>();
    dispatcher.register(
      'pending',
      async () => {
        await gate.promise;
        return 'done';
      },
      { access: 'public' },
    );
    transport.connect('c1');

    const inFlight = transport.receive('c1', '{"id":"r1","method":"pending"}');
    await transport.disconnect('c1', 'client left');
    // The reply path must survive a connection that is already gone.
    gate.resolve();
    await inFlight;

    expect(transport.sentTo('c1')).toHaveLength(0);
  });

  test('does not run a request queued on a closed connection against a same-id replacement', async () => {
    const { transport, layer, controlled, logs } = sessionHarness();
    await layer.start();
    transport.connect('c1');

    const first = transport.receive('c1', '{"id":"r1","method":"hang"}');
    await sleep(0);
    const queued = transport.receive('c1', '{"id":"r2","method":"bind"}');
    await sleep(0);
    expect(controlled.started).toEqual(['hang']);

    await transport.disconnect('c1', 'client left');
    // The transport mints a new connection that reuses the id. The queued
    // request must keep acting for the ConnectionState it entered with, not
    // for whatever the id resolves to now.
    const replacement = fakeConnection('c1');
    await layer.onConnect(replacement.connection);

    controlled.release();
    await Promise.all([first, queued]);
    await sleep(5);

    // The old login never reached a handler and left no route behind.
    expect(controlled.started).not.toContain('bind');
    expect(
      logs().filter((entry) => entry.msg === 'session bound'),
    ).toHaveLength(0);
    expect(
      logs().filter((entry) => entry.msg === 'request dropped before start'),
    ).toHaveLength(1);

    // The replacement is still tracked and its request runs unbound.
    await layer.onMessage(
      replacement.connection,
      '{"id":"r3","method":"probe"}',
    );
    expect(controlled.started).toContain('probe');
    expect(
      controlled.requests.find((request) => request.method === 'probe')
        ?.sessionId,
    ).toBeUndefined();
    expect(replacement.sent.map(parseFrame)).toMatchObject([
      { id: 'r3', ok: true, result: 'probe' },
    ]);
    await layer.stop();
  });

  test('a disconnect for a previous connection does not remove a same-id replacement', async () => {
    const { layer, controlled } = sessionHarness();
    await layer.start();
    const previous = fakeConnection('c1');
    const replacement = fakeConnection('c1');

    await layer.onConnect(previous.connection);
    await layer.onConnect(replacement.connection);
    // Stale disconnect: the id is the same, but this event belongs to the
    // connection the replacement already displaced.
    await layer.onDisconnect(previous.connection);

    expect(layer.health().connections).toBe(1);
    await layer.onMessage(
      replacement.connection,
      '{"id":"r1","method":"probe"}',
    );
    expect(controlled.started).toEqual(['probe']);
    expect(replacement.sent.map(parseFrame)).toMatchObject([
      { id: 'r1', ok: true, result: 'probe' },
    ]);
    await layer.stop();
  });

  test('ignores messages for a connection it does not track', async () => {
    const { transport, dispatcher, layer } = await harness();
    dispatcher.register('echo', async (_ctx, payload) => payload, {
      access: 'public',
    });
    const connection = transport.connect('c1');
    await transport.disconnect('c1');

    await layer.onMessage(connection, '{"id":"r1","method":"echo"}');

    expect(transport.sentTo('c1')).toHaveLength(0);
  });
});
