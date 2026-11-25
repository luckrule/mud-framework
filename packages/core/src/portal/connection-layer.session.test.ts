import { describe, expect, test } from 'bun:test';

import { createJsonLogger } from '../logging/json-logger.ts';
import { Dispatcher } from '../server/dispatcher.ts';
import { LocalExecutor } from '../server/local-executor.ts';
import { MemoryStorageBackend } from '../storage/memory.ts';
import { parseFrame, sleep } from '../testing/common.ts';
import {
  fakeConnection,
  harness,
  lastFrame,
  scriptedTransport,
  sessionHarness,
} from '../testing/connection-layer.ts';
import { ConnectionLayer } from './connection-layer.ts';

describe('ConnectionLayer session routing (SPEC 3.2, INV-004)', () => {
  test('a displaced connection sends new requests without the session held by its successor', async () => {
    const { transport, dispatcher, layer } = await harness({ timeoutMs: 5000 });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    dispatcher.register('bind', async (ctx) => {
      ctx.bindSession('s1');
      return null;
    });
    dispatcher.register('hold', async (ctx) => {
      entered.resolve();
      await release.promise;
      return ctx.sessionId;
    });
    dispatcher.register('identity', async (ctx) => ctx.sessionId ?? null);
    transport.connect('c1');
    transport.connect('c2');
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    await transport.receive('c2', '{"id":"2","method":"bind"}');
    const holding = transport.receive('c2', '{"id":"3","method":"hold"}');
    await entered.promise;

    try {
      await transport.receive('c1', '{"id":"4","method":"identity"}');
      expect(lastFrame(transport, 'c1')).toEqual({
        id: '4',
        ok: true,
        result: null,
      });
    } finally {
      release.resolve();
      await holding;
      await layer.stop();
    }
  });

  test('successive takeovers preserve the first holder terminal barrier before any successor request starts', async () => {
    const { transport, dispatcher, layer } = await harness({ timeoutMs: 5000 });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const started: string[] = [];
    dispatcher.register('bind', async (ctx) => {
      ctx.bindSession('s1');
      return null;
    });
    dispatcher.register('hold', async () => {
      entered.resolve();
      await release.promise;
      return null;
    });
    dispatcher.register('identity', async (ctx) => {
      started.push(ctx.connectionId);
      return ctx.sessionId;
    });
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    const holding = transport.receive('c1', '{"id":"2","method":"hold"}');
    await entered.promise;
    transport.connect('c2');
    await transport.receive('c2', '{"id":"3","method":"bind"}');
    transport.connect('c3');
    await transport.receive('c3', '{"id":"4","method":"bind"}');
    const next = transport.receive('c3', '{"id":"5","method":"identity"}');

    try {
      await sleep(10);
      expect(started).toEqual([]);
    } finally {
      release.resolve();
      await Promise.all([holding, next]);
      await layer.stop();
    }
  });

  test('a session handoff never blocks work for a different session', async () => {
    const { transport, dispatcher, layer } = await harness({ timeoutMs: 5000 });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    const started: string[] = [];
    dispatcher.register('bind', async (ctx, payload) => {
      ctx.bindSession(String(payload));
      return null;
    });
    dispatcher.register('hold', async () => {
      entered.resolve();
      await release.promise;
      return null;
    });
    dispatcher.register('identity', async (ctx) => {
      started.push(ctx.connectionId);
      return ctx.sessionId;
    });
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind","payload":"s1"}');
    const holding = transport.receive('c1', '{"id":"2","method":"hold"}');
    await entered.promise;
    transport.connect('c2');
    await transport.receive('c2', '{"id":"3","method":"bind","payload":"s1"}');
    const waiting = transport.receive('c2', '{"id":"4","method":"identity"}');
    transport.connect('c3');

    try {
      await transport.receive(
        'c3',
        '{"id":"5","method":"bind","payload":"s2"}',
      );
      await transport.receive('c3', '{"id":"6","method":"identity"}');
      expect(lastFrame(transport, 'c3')).toEqual({
        id: '6',
        ok: true,
        result: 's2',
      });
      expect(started).toEqual(['c3']);
    } finally {
      release.resolve();
      await Promise.all([holding, waiting]);
      await layer.stop();
    }
  });

  test('a bound session makes a later connection rebind it as a handoff', async () => {
    const { transport, layer, logs } = sessionHarness();
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');

    transport.connect('c2');
    await transport.receive('c2', '{"id":"2","method":"bind"}');

    // The route exists because the second bind was recognised as a reconnect
    // rather than a first binding.
    const rebound = logs().filter(
      (entry) => entry.msg === 'session rebound to a new connection',
    );
    expect(rebound).toHaveLength(1);
    expect(rebound[0]).toMatchObject({
      level: 'warn',
      sessionId: 's1',
      previousConnectionId: 'c1',
      connectionId: 'c2',
    });
    expect(
      logs().filter((entry) => entry.msg === 'session bound'),
    ).toHaveLength(2);
    await layer.stop();
  });

  test('rebinding the same session on one connection is idempotent', async () => {
    const { transport, layer, logs } = sessionHarness();
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"bind"}');
    await transport.receive('c1', '{"id":"2","method":"bind"}');
    await transport.receive('c1', '{"id":"3","method":"bind"}');

    // A long-lived connection binds once; repeating it must not log per
    // request or turn into a self-handoff.
    expect(
      logs().filter((entry) => entry.msg === 'session bound'),
    ).toHaveLength(1);
    expect(
      logs().filter(
        (entry) => entry.msg === 'session rebound to a new connection',
      ),
    ).toHaveLength(0);
    await layer.stop();
  });

  test('binding a new session on one connection drops the old route', async () => {
    let bound = 's1';
    const { transport, layer, logs } = sessionHarness({
      sessionOf: (method) => (method === 'bind' ? bound : undefined),
    });
    await layer.start();
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"bind"}');
    bound = 's2';
    await transport.receive('c1', '{"id":"2","method":"bind"}');
    // c1 no longer holds s1, so this is a first binding, not a handoff.
    transport.connect('c2');
    bound = 's1';
    await transport.receive('c2', '{"id":"3","method":"bind"}');

    expect(
      logs().filter(
        (entry) => entry.msg === 'session rebound to a new connection',
      ),
    ).toHaveLength(0);
    await layer.stop();
  });

  test('a reconnecting connection waits for the previous connection to settle', async () => {
    const { transport, layer, controlled } = sessionHarness();
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    const hanging = transport.receive('c1', '{"id":"2","method":"hang"}');
    await sleep(0);
    expect(controlled.started).toEqual(['bind', 'hang']);

    // The binding request itself is not delayed: it already executed, and its
    // result belongs to the client that sent it.
    transport.connect('c2');
    await transport.receive('c2', '{"id":"3","method":"bind"}');
    expect(lastFrame(transport, 'c2')).toEqual({
      id: '3',
      ok: true,
      result: 'bind',
    });

    const next = transport.receive('c2', '{"id":"4","method":"next"}');
    await sleep(10);
    // SPEC 3.2: the new connection's next request must not start while the
    // old connection still has work without a terminal state.
    expect(controlled.started).toEqual(['bind', 'hang', 'bind']);

    controlled.release();
    await Promise.all([hanging, next]);
    expect(controlled.started).toEqual(['bind', 'hang', 'bind', 'next']);
    // The handoff ordering is what matters: the old connection's work has a
    // terminal state before the new connection's next request starts.
    expect(controlled.finished.indexOf('hang')).toBeLessThan(
      controlled.started.lastIndexOf('next'),
    );
    await layer.stop();
  });

  test('a connection that lost its session and takes it back is gated again', async () => {
    const { transport, layer, controlled, logs } = sessionHarness();
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');

    // c2 takes the Session over, so c1 is no longer the holder.
    transport.connect('c2');
    await transport.receive('c2', '{"id":"2","method":"bind"}');
    const hanging = transport.receive('c2', '{"id":"3","method":"hang"}');
    await sleep(0);

    // c1 binding s1 again is a reconnect from c1's side, not an idempotent
    // rebind of its own route: it must be gated on c2, which still has work.
    await transport.receive('c1', '{"id":"4","method":"bind"}');
    expect(
      logs().filter(
        (entry) => entry.msg === 'session rebound to a new connection',
      ),
    ).toHaveLength(2);

    const next = transport.receive('c1', '{"id":"5","method":"next"}');
    await sleep(10);
    expect(controlled.started).not.toContain('next');

    controlled.release();
    await Promise.all([hanging, next]);
    expect(controlled.started).toContain('next');
    await layer.stop();
  });

  test('waits for framework timeout rather than an uncooperative handler return', async () => {
    const { transport, dispatcher, layer } = await harness({ timeoutMs: 60 });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let handlerReturned = false;
    dispatcher.register('bind', async (ctx) => {
      ctx.bindSession('s1');
      return null;
    });
    dispatcher.register('hang', async () => {
      entered.resolve();
      await release.promise;
      handlerReturned = true;
      return null;
    });
    dispatcher.register('next', async (ctx) => ctx.sessionId);
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    const hanging = transport.receive('c1', '{"id":"2","method":"hang"}');
    await entered.promise;
    transport.connect('c2');
    await transport.receive('c2', '{"id":"3","method":"bind"}');

    try {
      await transport.receive('c2', '{"id":"4","method":"next"}');
      await hanging;
      expect(lastFrame(transport, 'c1').error).toMatchObject({
        code: 'FRAMEWORK_TIMEOUT',
      });
      expect(lastFrame(transport, 'c2')).toEqual({
        id: '4',
        ok: true,
        result: 's1',
      });
      expect(handlerReturned).toBe(false);
    } finally {
      release.resolve();
      await layer.stop();
    }
  });

  test('disconnecting an intermediate holder cannot erase an earlier unfinished session request', async () => {
    const { transport, dispatcher, layer } = await harness({ timeoutMs: 5000 });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let nextStarted = false;
    dispatcher.register('bind', async (ctx) => {
      ctx.bindSession('s1');
      return null;
    });
    dispatcher.register('hold', async () => {
      entered.resolve();
      await release.promise;
      return null;
    });
    dispatcher.register('next', async (ctx) => {
      nextStarted = true;
      return ctx.sessionId;
    });
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');
    const holding = transport.receive('c1', '{"id":"2","method":"hold"}');
    await entered.promise;
    transport.connect('c2');
    await transport.receive('c2', '{"id":"3","method":"bind"}');
    await transport.disconnect('c2');
    transport.connect('c3');
    await transport.receive('c3', '{"id":"4","method":"bind"}');
    const next = transport.receive('c3', '{"id":"5","method":"next"}');

    try {
      await sleep(10);
      expect(nextStarted).toBe(false);
      release.resolve();
      await Promise.all([holding, next]);
      expect(lastFrame(transport, 'c3')).toEqual({
        id: '5',
        ok: true,
        result: 's1',
      });
    } finally {
      release.resolve();
      await layer.stop();
    }
  });

  test('refuses a binding once the session table is full but still answers the request', async () => {
    let bound = 's1';
    const { transport, layer, logs } = sessionHarness({
      maxSessions: 1,
      sessionOf: (method) => (method === 'bind' ? bound : undefined),
    });
    await layer.start();
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"bind"}');

    bound = 's2';
    await transport.receive('c1', '{"id":"2","method":"bind"}');

    // The request succeeded: a routing limit is not a business failure.
    expect(lastFrame(transport, 'c1')).toEqual({
      id: '2',
      ok: true,
      result: 'bind',
    });
    const refused = logs().filter(
      (entry) => entry.msg === 'session binding refused: session table is full',
    );
    expect(refused).toHaveLength(1);
    expect(refused[0]).toMatchObject({
      level: 'warn',
      sessionId: 's2',
      connectionId: 'c1',
      limit: 1,
    });
    // The refused id left no route behind.
    expect(
      logs().filter((entry) => entry.msg === 'session bound'),
    ).toHaveLength(1);
    await layer.stop();
  });

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

  test.each(['shutdown', 'disconnect'] as const)(
    '%s cancels a session waiter when its barrier settles before it resumes',
    async (closing) => {
      const logger = createJsonLogger({ sink: () => undefined });
      const dispatcher = new Dispatcher({
        backend: new MemoryStorageBackend(),
        timeoutMs: 5000,
        logger,
      });
      const local = new LocalExecutor({ dispatcher });
      const entered = Promise.withResolvers<void>();
      const release = Promise.withResolvers<void>();
      const events: string[] = [];
      dispatcher.register('bind', async (ctx) => {
        ctx.bindSession('s1');
        return null;
      });
      dispatcher.register('hold', async () => {
        entered.resolve();
        await release.promise;
        return 'done';
      });
      dispatcher.register('next', async () => 'next');
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

  test('shutdown refuses a session waiter instead of bypassing unfinished work', async () => {
    const { transport, dispatcher, layer } = await harness({ timeoutMs: 5000 });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
    let nextStarted = false;
    dispatcher.register('bind', async (ctx) => {
      ctx.bindSession('s1');
      return null;
    });
    dispatcher.register('hold', async () => {
      entered.resolve();
      await release.promise;
      return null;
    });
    dispatcher.register('next', async () => {
      nextStarted = true;
      return null;
    });
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
    } finally {
      release.resolve();
      await Promise.all([holding, stopping]);
    }
  });
});
