import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import { DispatcherExecutor } from '../server/dispatcher-executor.ts';
import { Dispatcher } from '../server/dispatcher.ts';
import { parseFrame, sleep } from '../testing/common.ts';
import {
  fakeConnection,
  harness,
  lastFrame,
  scriptedTransport,
  sessionHarness,
} from '../testing/connection-layer.ts';
import { MemoryStorageBackend } from '../testing/memory-backend.ts';
import { ConnectionLayer } from './connection-layer.ts';

describe('ConnectionLayer session routing (SPEC 3.2, INV-004)', () => {
  test('passes the bound session to the executor on the next request', async () => {
    const { transport, layer, controlled } = sessionHarness();
    await layer.start();
    transport.connect('c1');

    // The binding request itself runs unbound: the binding it declares is
    // applied only after it succeeds.
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    await transport.receive('c1', '{"id":"2","method":"next"}');

    expect(controlled.requests).toHaveLength(2);
    expect(controlled.requests[0]?.sessionId).toBeUndefined();
    // SPEC 7.1: the Session is the context's only source, so it must reach
    // the executor rather than stay in this layer's routing table.
    expect(controlled.requests[1]?.sessionId).toBe('s1');
    await layer.stop();
  });

  test('passes no session on the first request of an unbound connection', async () => {
    const { transport, layer, controlled } = sessionHarness();
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"next"}');

    expect(controlled.requests).toHaveLength(1);
    expect(controlled.requests[0]?.sessionId).toBeUndefined();
    await layer.stop();
  });

  test('carries the session on the lines this layer does emit', async () => {
    const { transport, layer, logs } = sessionHarness();
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"bind"}');

    // SPEC 7.1: request lines name the Session. Successful requests are not
    // logged here, so the observable line is the binding itself — it belongs
    // to the request that declared it and carries the id it just bound.
    const bound = logs().filter((line) => line.msg === 'session bound');
    expect(bound).toHaveLength(1);
    expect(bound[0]?.sessionId).toBe('s1');
    await layer.stop();
  });

  test('passes the newest session after a rebind', async () => {
    let bound = 's1';
    const { transport, layer, controlled } = sessionHarness({
      sessionOf: (method) => (method === 'bind' ? bound : undefined),
    });
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"bind"}');
    bound = 's2';
    await transport.receive('c1', '{"id":"2","method":"bind"}');
    await transport.receive('c1', '{"id":"3","method":"next"}');

    expect(controlled.requests.map((request) => request.sessionId)).toEqual([
      undefined,
      's1',
      's2',
    ]);
    await layer.stop();
  });

  test('passes the reconnected session to the new connection', async () => {
    const { transport, layer, controlled } = sessionHarness();
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');

    transport.connect('c2');
    // A new connection starts unbound; the bind it sends is a handoff.
    await transport.receive('c2', '{"id":"2","method":"next"}');
    await transport.receive('c2', '{"id":"3","method":"bind"}');
    await transport.receive('c2', '{"id":"4","method":"next"}');

    expect(controlled.requests.map((request) => request.sessionId)).toEqual([
      undefined,
      undefined,
      // The rebinding request on the new connection runs before its own
      // binding is applied, exactly like a first bind.
      undefined,
      's1',
    ]);
    await layer.stop();
  });

  test('shutdown refuses a session waiter instead of bypassing unfinished work', async () => {
    const { transport, dispatcher, layer, logs } = await harness({
      timeoutMs: 5000,
    });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let nextStarted = false;
    dispatcher.register(
      'bind',
      async (ctx) => {
        ctx.bindSession('s1');
        return null;
      },
      { access: 'public', mayChangeSession: true },
    );
    dispatcher.register(
      'hold',
      async () => {
        entered.resolve();
        await release.promise;
        return null;
      },
      { access: 'public' },
    );
    dispatcher.register(
      'next',
      async () => {
        nextStarted = true;
        return null;
      },
      { access: 'public' },
    );
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    const holding = transport.receive('c1', '{"id":"2","method":"hold"}');
    await entered.promise;
    transport.connect('c2');
    await transport.receive('c2', '{"id":"3","method":"bind"}');
    const next = transport.receive('c2', '{"id":"4","method":"next"}');
    await sleep(0);
    const stopping = layer.stop();

    try {
      await next;
      expect(nextStarted).toBe(false);
      expect(lastFrame(transport, 'c2')).toMatchObject({
        id: '4',
        ok: false,
        error: { code: 'FRAMEWORK_SHUTDOWN' },
      });
      // The request had taken its queue seat, so its refusal is correlatable
      // with the entry id minted for it (SPEC 7.3).
      const refused = logs().filter(
        (entry) =>
          entry.msg === 'request refused' &&
          entry.code === 'FRAMEWORK_SHUTDOWN',
      );
      expect(refused).toHaveLength(1);
      expect(refused[0]?.clientRequestId).toBe('4');
      expect(refused[0]?.requestId).toBeTruthy();
    } finally {
      release.resolve();
      await Promise.all([holding, stopping]);
    }
  });

  test.each(['shutdown', 'disconnect'] as const)(
    '%s cancels a session waiter when its barrier settles before it resumes',
    async (closing) => {
      const logger = createJsonLogger({ sink: () => undefined });
      const dispatcher = new Dispatcher({
        backend: new MemoryStorageBackend(),
        timeoutMs: 5000,
        logger,
      });
      const local = new DispatcherExecutor({ dispatcher });
      const entered = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const events: string[] = [];
      dispatcher.register(
        'bind',
        async (ctx) => {
          ctx.bindSession('s1');
          return null;
        },
        { access: 'public', mayChangeSession: true },
      );
      dispatcher.register(
        'hold',
        async () => {
          entered.resolve();
          await release.promise;
          return 'done';
        },
        { access: 'public' },
      );
      dispatcher.register('next', async () => 'next', { access: 'public' });
      const layer = new ConnectionLayer({
        transport: scriptedTransport([]).transport,
        executor: {
          start: () => local.start(),
          execute: (request) => {
            events.push(`execute ${request.method}`);
            return local.execute(request);
          },
          cancel: (requestId, code) => local.cancel(requestId, code),
          close: () => local.close(),
          health: () => local.health(),
        },
        logger,
      });
      await layer.start();
      const old = fakeConnection('c1');
      const first = {
        ...old.connection,
        send: (frame: string | Uint8Array) => {
          if (parseFrame(frame).id === 'old') {
            events.push('old reply');
          }
          return old.connection.send(frame);
        },
      };
      const successor = fakeConnection('c2');
      await layer.onConnect(first);
      await layer.onConnect(successor.connection);
      await layer.onMessage(first, '{"id":"bind-old","method":"bind"}');
      const holding = layer.onMessage(first, '{"id":"old","method":"hold"}');
      await entered.promise;
      await layer.onMessage(
        successor.connection,
        '{"id":"bind-new","method":"bind"}',
      );
      const next = layer.onMessage(
        successor.connection,
        '{"id":"next","method":"next"}',
      );
      await Promise.resolve();
      // Public completion runs after the barrier settles, before its waiter resumes.
      const closed = holding.then(() => {
        events.push(closing);
        return closing === 'shutdown'
          ? layer.stop()
          : layer.onDisconnect(successor.connection);
      });

      try {
        release.resolve();
        await Promise.all([holding, next, closed]);
        expect(events).toEqual([
          'execute bind',
          'execute hold',
          'execute bind',
          'old reply',
          closing,
        ]);
        expect(old.sent.map(parseFrame)).toMatchObject([
          { id: 'bind-old', ok: true },
          { id: 'old', ok: true, result: 'done' },
        ]);
        expect(successor.sent.map(parseFrame)).toMatchObject(
          closing === 'shutdown'
            ? [
                { id: 'bind-new', ok: true },
                {
                  id: 'next',
                  ok: false,
                  error: { code: 'FRAMEWORK_SHUTDOWN' },
                },
              ]
            : [{ id: 'bind-new', ok: true }],
        );
      } finally {
        release.resolve();
        await Promise.all([holding, next, closed]);
        await layer.stop();
      }
    },
  );
});
