import { describe, expect, test } from 'bun:test';

import {
  AuthenticationRequiredError,
  AUTH_REQUIRED,
  UnknownHandlerError,
} from '../shared/wire-codes.ts';
import { rejectionOf } from '../testing/common.ts';
import {
  CountingBackend,
  parseLines,
  quietDispatcher,
} from '../testing/dispatcher.ts';

function countingDispatcher(): {
  backend: CountingBackend;
  dispatcher: ReturnType<typeof quietDispatcher>['dispatcher'];
  lines: string[];
} {
  const backend = new CountingBackend();
  const { dispatcher, lines } = quietDispatcher({ backend, timeoutMs: 1000 });
  return { backend, dispatcher, lines };
}

describe('Dispatcher access gate (SPEC 3.7)', () => {
  test('a default handler requires a session instead of entering a transaction', async () => {
    const { backend, dispatcher } = countingDispatcher();
    let calls = 0;
    dispatcher.register('inspect', async () => {
      calls += 1;
      return null;
    });

    const error = await rejectionOf(dispatcher.dispatch('inspect', null));

    expect(error).toBeInstanceOf(AuthenticationRequiredError);
    if (error instanceof AuthenticationRequiredError) {
      expect(error.code).toBe(AUTH_REQUIRED);
      expect(error.method).toBe('inspect');
    }
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
  });

  test('a default handler runs when the request carries a session', async () => {
    const { backend, dispatcher } = countingDispatcher();
    dispatcher.register('inspect', async (ctx) => ctx.sessionId ?? null);

    const outcome = await dispatcher.dispatch('inspect', null, {
      sessionId: 's1',
    });

    expect(outcome).toEqual({ result: 's1' });
    expect(backend.transactions).toBe(1);
  });

  test('an empty session id counts as missing', async () => {
    const { backend, dispatcher } = countingDispatcher();
    let calls = 0;
    dispatcher.register('inspect', async () => {
      calls += 1;
      return null;
    });

    const error = await rejectionOf(
      dispatcher.dispatch('inspect', null, { sessionId: '' }),
    );

    expect(error).toBeInstanceOf(AuthenticationRequiredError);
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
  });

  test('a public handler runs without a session', async () => {
    const { backend, dispatcher } = countingDispatcher();
    dispatcher.register('look', async () => 'a rock', { access: 'public' });

    const outcome = await dispatcher.dispatch('look', null);

    expect(outcome).toEqual({ result: 'a rock' });
    expect(backend.transactions).toBe(1);
  });

  test('an unknown method stays UNKNOWN_METHOD without a session', async () => {
    const { backend, dispatcher } = countingDispatcher();

    const error = await rejectionOf(dispatcher.dispatch('missing', null));

    // The gate order is contract: adding a Session must not turn a routing
    // question into an authentication answer.
    expect(error).toBeInstanceOf(UnknownHandlerError);
    expect(backend.transactions).toBe(0);
  });

  test('logs the refusal at warn with the code, method and request id', async () => {
    const { dispatcher, lines } = countingDispatcher();
    dispatcher.register('inspect', async () => null);

    await rejectionOf(
      dispatcher.dispatch('inspect', null, { requestId: 'r1' }),
    );

    const refusals = parseLines(lines).filter(
      (entry) => entry.code === AUTH_REQUIRED,
    );
    expect(refusals).toHaveLength(1);
    expect(refusals[0]).toMatchObject({
      level: 'warn',
      requestId: 'r1',
      method: 'inspect',
      code: AUTH_REQUIRED,
    });
  });
});
