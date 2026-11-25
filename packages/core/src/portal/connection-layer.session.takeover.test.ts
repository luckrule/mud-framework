import { describe, expect, test } from 'bun:test';

import { sleep } from '../testing/common.ts';
import {
  harness,
  lastFrame,
  sessionHarness,
} from '../testing/connection-layer.ts';

describe('ConnectionLayer session routing (SPEC 3.2, INV-004)', () => {
  test('a displaced connection sends new requests without the session held by its successor', async () => {
    const { transport, dispatcher, layer } = await harness({ timeoutMs: 5000 });
    const entered = Promise.withResolvers<void>();
    const release = Promise.withResolvers<void>();
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
      async (ctx) => {
        entered.resolve();
        await release.promise;
        return ctx.sessionId;
      },
      { access: 'public' },
    );
    dispatcher.register('identity', async (ctx) => ctx.sessionId ?? null, {
      access: 'public',
    });
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
      'identity',
      async (ctx) => {
        started.push(ctx.connectionId);
        return ctx.sessionId;
      },
      { access: 'public' },
    );
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
    dispatcher.register(
      'bind',
      async (ctx, payload) => {
        ctx.bindSession(String(payload));
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
      'identity',
      async (ctx) => {
        started.push(ctx.connectionId);
        return ctx.sessionId;
      },
      { access: 'public' },
    );
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
    dispatcher.register(
      'bind',
      async (ctx) => {
        ctx.bindSession('s1');
        return null;
      },
      { access: 'public', mayChangeSession: true },
    );
    dispatcher.register(
      'hang',
      async () => {
        entered.resolve();
        await release.promise;
        handlerReturned = true;
        return null;
      },
      { access: 'public' },
    );
    dispatcher.register('next', async (ctx) => ctx.sessionId, {
      access: 'public',
    });
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

  test('a late unbind from a displaced holder does not release the new holder', async () => {
    const { transport, dispatcher, layer, logs } = await harness({
      timeoutMs: 5000,
    });
    const logoutEntered = Promise.withResolvers<void>();
    const releaseLogout = Promise.withResolvers<void>();
    dispatcher.register(
      'login',
      async (ctx) => {
        ctx.bindSession('s1');
        return null;
      },
      { access: 'public', mayChangeSession: true },
    );
    dispatcher.register(
      'logout',
      async (ctx) => {
        logoutEntered.resolve();
        await releaseLogout.promise;
        ctx.unbindSession();
        return 'bye';
      },
      { access: 'public', mayChangeSession: true },
    );
    dispatcher.register('protected', async (ctx) => ctx.sessionId ?? null);
    transport.connect('c1');
    await transport.receive('c1', '{"id":"1","method":"login"}');
    const logout = transport.receive('c1', '{"id":"2","method":"logout"}');
    await logoutEntered.promise;

    // c2 takes the Session over while c1's logout handler is still running, so
    // c1 no longer holds it when the success is applied (SPEC 3.2).
    transport.connect('c2');
    await transport.receive('c2', '{"id":"3","method":"login"}');
    releaseLogout.resolve();
    await logout;

    // The client still sees its own success; the drop is routing-only.
    expect(lastFrame(transport, 'c1')).toEqual({
      id: '2',
      ok: true,
      result: 'bye',
    });
    await transport.receive('c2', '{"id":"4","method":"protected"}');
    expect(lastFrame(transport, 'c2')).toEqual({
      id: '4',
      ok: true,
      result: 's1',
    });
    await transport.receive('c1', '{"id":"5","method":"protected"}');
    expect(lastFrame(transport, 'c1')).toMatchObject({
      id: '5',
      ok: false,
      error: { code: 'AUTH_REQUIRED' },
    });
    const dropped = logs().filter(
      (entry) => entry.code === 'SESSION_CHANGE_DROPPED',
    );
    expect(dropped).toHaveLength(1);
    expect(dropped[0]).toMatchObject({
      level: 'warn',
      connectionId: 'c1',
      sessionId: 's1',
      reason: expect.any(String),
    });
    await layer.stop();
  });
});
