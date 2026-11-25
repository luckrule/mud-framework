import { describe, expect, test } from 'bun:test';

import { sleep } from '../testing/common.ts';
import { harness, lastFrame } from '../testing/connection-layer.ts';

describe('ConnectionLayer session routing (SPEC 3.2, INV-004)', () => {
  test('a successful unbind removes the route before the next request', async () => {
    const { transport, dispatcher, layer } = await harness();
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
        ctx.unbindSession();
        return 'bye';
      },
      { access: 'public', mayChangeSession: true },
    );
    // A default-access handler: it proves the route rather than the handler
    // decides whether a later request carries a Session.
    dispatcher.register('protected', async (ctx) => ctx.sessionId ?? null);
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"login"}');
    await transport.receive('c1', '{"id":"2","method":"protected"}');
    expect(lastFrame(transport, 'c1')).toEqual({
      id: '2',
      ok: true,
      result: 's1',
    });

    await transport.receive('c1', '{"id":"3","method":"logout"}');
    expect(lastFrame(transport, 'c1')).toEqual({
      id: '3',
      ok: true,
      result: 'bye',
    });

    await transport.receive('c1', '{"id":"4","method":"protected"}');
    expect(lastFrame(transport, 'c1')).toMatchObject({
      id: '4',
      ok: false,
      error: { code: 'AUTH_REQUIRED' },
    });
    await layer.stop();
  });

  test.each(['failed', 'timed-out'] as const)(
    'a %s unbind keeps the route',
    async (mode) => {
      const { transport, dispatcher, layer } =
        mode === 'timed-out'
          ? await harness({ timeoutMs: 20 })
          : await harness();
      const handlerDone = Promise.withResolvers<void>();
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
          ctx.unbindSession();
          if (mode === 'failed') {
            throw new Error('logout refused');
          }
          await sleep(50);
          handlerDone.resolve();
          return 'bye';
        },
        { access: 'public', mayChangeSession: true },
      );
      dispatcher.register('protected', async (ctx) => ctx.sessionId ?? null);
      transport.connect('c1');

      await sleep(1);
      await transport.receive('c1', '{"id":"1","method":"login"}');
      await sleep(1);
      await transport.receive('c1', '{"id":"2","method":"logout"}');
      expect(lastFrame(transport, 'c1')).toMatchObject({
        id: '2',
        ok: false,
        error: {
          code: mode === 'failed' ? 'INTERNAL_ERROR' : 'FRAMEWORK_TIMEOUT',
        },
      });

      if (mode === 'timed-out') {
        // The abandoned handler returns after the deadline; it must not apply
        // its change then either (SPEC 2: only a successful request commits).
        await handlerDone.promise;
      }

      await transport.receive('c1', '{"id":"3","method":"protected"}');
      expect(lastFrame(transport, 'c1')).toEqual({
        id: '3',
        ok: true,
        result: 's1',
      });
      await layer.stop();
    },
  );

  test.each(['failed', 'timed-out'] as const)(
    'a %s bind leaves the connection unbound',
    async (mode) => {
      const { transport, dispatcher, layer } =
        mode === 'timed-out'
          ? await harness({ timeoutMs: 20 })
          : await harness();
      const handlerDone = Promise.withResolvers<void>();
      dispatcher.register(
        'login',
        async (ctx) => {
          ctx.bindSession(mode === 'failed' ? 'half-made' : 'too-late');
          if (mode === 'failed') {
            throw new Error('login refused');
          }
          await sleep(50);
          handlerDone.resolve();
          return 'late';
        },
        { access: 'public', mayChangeSession: true },
      );
      dispatcher.register('protected', async (ctx) => ctx.sessionId ?? null);
      transport.connect('c1');

      await transport.receive('c1', '{"id":"1","method":"login"}');
      expect(lastFrame(transport, 'c1')).toMatchObject({
        id: '1',
        ok: false,
        error: {
          code: mode === 'failed' ? 'INTERNAL_ERROR' : 'FRAMEWORK_TIMEOUT',
        },
      });
      if (mode === 'timed-out') {
        await handlerDone.promise;
      }

      await transport.receive('c1', '{"id":"2","method":"protected"}');
      // The failure is reported, and no route outlived it: the later request
      // entered without a Session and the gate refused it.
      expect(lastFrame(transport, 'c1')).toMatchObject({
        id: '2',
        ok: false,
        error: { code: 'AUTH_REQUIRED' },
      });
      await layer.stop();
    },
  );

  test('a handler that swallows the mayChangeSession error succeeds without changing the route', async () => {
    const { transport, dispatcher, layer, logs } = await harness();
    const swallowed: string[] = [];
    const capture = (error: unknown): void => {
      swallowed.push(error instanceof Error ? error.message : String(error));
    };
    dispatcher.register(
      'login',
      async (ctx) => {
        ctx.bindSession('s1');
        return null;
      },
      { access: 'public', mayChangeSession: true },
    );
    dispatcher.register(
      'sneaky-bind',
      async (ctx) => {
        try {
          ctx.bindSession('s2');
        } catch (error) {
          capture(error);
        }
        return 'swallowed';
      },
      { access: 'public' },
    );
    dispatcher.register(
      'sneaky-unbind',
      async (ctx) => {
        try {
          ctx.unbindSession();
        } catch (error) {
          capture(error);
        }
        return 'swallowed';
      },
      { access: 'public' },
    );
    dispatcher.register('protected', async (ctx) => ctx.sessionId ?? null);
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"login"}');
    await transport.receive('c1', '{"id":"2","method":"sneaky-bind"}');
    await transport.receive('c1', '{"id":"3","method":"protected"}');
    expect(lastFrame(transport, 'c1')).toEqual({
      id: '3',
      ok: true,
      result: 's1',
    });

    await transport.receive('c1', '{"id":"4","method":"sneaky-unbind"}');
    await transport.receive('c1', '{"id":"5","method":"protected"}');
    expect(lastFrame(transport, 'c1')).toEqual({
      id: '5',
      ok: true,
      result: 's1',
    });

    expect(swallowed).toHaveLength(2);
    for (const message of swallowed) {
      expect(message).toMatch(/mayChangeSession/);
    }
    expect(
      logs().filter((entry) => entry.msg === 'session unbound'),
    ).toHaveLength(0);
    await layer.stop();
  });

  test('an unbind from a request without an entry session leaves the routing table untouched', async () => {
    const { transport, dispatcher, layer, logs } = await harness();
    dispatcher.register(
      'logout',
      async (ctx) => {
        ctx.unbindSession();
        return 'bye';
      },
      { access: 'public', mayChangeSession: true },
    );
    dispatcher.register('protected', async (ctx) => ctx.sessionId ?? null);
    transport.connect('c1');

    await transport.receive('c1', '{"id":"1","method":"logout"}');
    expect(lastFrame(transport, 'c1')).toEqual({
      id: '1',
      ok: true,
      result: 'bye',
    });
    // A repeat is still a successful no-op: the router cannot tell "never
    // bound" from "already unbound" (SPEC 3.2).
    await transport.receive('c1', '{"id":"2","method":"logout"}');
    expect(lastFrame(transport, 'c1')).toEqual({
      id: '2',
      ok: true,
      result: 'bye',
    });
    await transport.receive('c1', '{"id":"3","method":"protected"}');
    expect(lastFrame(transport, 'c1')).toMatchObject({
      id: '3',
      ok: false,
      error: { code: 'AUTH_REQUIRED' },
    });
    expect(
      logs().filter((entry) => entry.msg === 'session unbound'),
    ).toHaveLength(0);
    await layer.stop();
  });
});
