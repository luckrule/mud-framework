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
});
