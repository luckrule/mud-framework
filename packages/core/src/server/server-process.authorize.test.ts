import { describe, expect, test } from 'bun:test';

import { tick } from '../testing/common.ts';
import { CountingBackend } from '../testing/dispatcher.ts';
import {
  requestMessage,
  results,
  startServer,
} from '../testing/server-process.ts';

describe('runServerProcess access policy', () => {
  test('forwards authorize to the dispatcher and refuses the request', async () => {
    const backend = new CountingBackend();
    let calls = 0;
    const harness = startServer({
      backend,
      authorize: () => false,
      extensions: [
        {
          name: 'game',
          register: (dispatcher) => {
            dispatcher.register(
              'look',
              async () => {
                calls += 1;
                return 'ok';
              },
              { access: 'public' },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'look' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: false,
        error: {
          code: 'FORBIDDEN',
          message: 'access to "look" was denied',
        },
      },
    ]);
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
    await harness.stop();
  });

  test('maps a throwing policy to INTERNAL_ERROR through the same path', async () => {
    const backend = new CountingBackend();
    let calls = 0;
    const harness = startServer({
      backend,
      authorize: () => {
        throw new Error('policy exploded');
      },
      extensions: [
        {
          name: 'game',
          register: (dispatcher) => {
            dispatcher.register(
              'look',
              async () => {
                calls += 1;
                return 'ok';
              },
              { access: 'public' },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'look' }));
    await tick();

    const [outcome] = results(harness.messages);
    expect(outcome).toMatchObject({
      ok: false,
      error: { code: 'INTERNAL_ERROR' },
    });
    if (outcome === undefined || outcome.ok) {
      throw new Error('expected a failure outcome');
    }
    expect(outcome.error.message).toContain('policy exploded');
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
    await harness.stop();
  });

  test('forwards mayChangeSession from ExtensionRegistrar to the dispatcher', async () => {
    const harness = startServer({
      extensions: [
        {
          name: 'auth',
          register: (registrar) => {
            registrar.register(
              'bind',
              async (ctx) => {
                ctx.bindSession('s1');
                return 'ok';
              },
              { access: 'public', mayChangeSession: true },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'bind' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: 'ok',
        sessionChange: { type: 'bind', sessionId: 's1' },
      },
    ]);
    await harness.stop();
  });

  test('refuses a session change the Extension did not declare', async () => {
    const harness = startServer({
      extensions: [
        {
          name: 'auth',
          register: (registrar) => {
            registrar.register(
              'bind',
              async (ctx) => {
                ctx.bindSession('s1');
                return 'ok';
              },
              { access: 'public' },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'bind' }));
    await tick();

    const [outcome] = results(harness.messages);
    expect(outcome).toMatchObject({
      ok: false,
      error: {
        code: 'INTERNAL_ERROR',
        message: expect.stringContaining('mayChangeSession'),
      },
    });
    expect(outcome).not.toHaveProperty('sessionChange');
    await harness.stop();
  });
});
