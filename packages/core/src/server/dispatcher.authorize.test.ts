import { describe, expect, test } from 'bun:test';

import {
  AUTH_REQUIRED,
  AuthenticationRequiredError,
  BusinessError,
  FORBIDDEN,
  ForbiddenError,
  FrameworkTimeoutError,
  RequestCancelledError,
  UnknownHandlerError,
} from '../shared/wire-codes.ts';
import { rejectionOf, sleep } from '../testing/common.ts';
import {
  CountingBackend,
  parseLines,
  quietDispatcher,
} from '../testing/dispatcher.ts';
import { DispatcherExecutor } from './dispatcher-executor.ts';
import type { AccessContext } from './dispatcher.ts';

function harness(
  authorize: (
    ctx: AccessContext,
    method: string,
    payload: unknown,
  ) => boolean | Promise<boolean>,
  timeoutMs = 1000,
): {
  backend: CountingBackend;
  dispatcher: ReturnType<typeof quietDispatcher>['dispatcher'];
  lines: string[];
} {
  const backend = new CountingBackend();
  const { dispatcher, lines } = quietDispatcher({
    backend,
    timeoutMs,
    authorize,
  });
  return { backend, dispatcher, lines };
}

describe('Dispatcher access policy (SPEC 3.7)', () => {
  test('a false decision refuses the request before the transaction', async () => {
    const { backend, dispatcher, lines } = harness(() => false);
    let calls = 0;
    dispatcher.register(
      'look',
      async () => {
        calls += 1;
        return null;
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('look', null, { requestId: 'r1' }),
    );

    expect(error).toBeInstanceOf(ForbiddenError);
    if (error instanceof ForbiddenError) {
      expect(error.code).toBe(FORBIDDEN);
      expect(error.method).toBe('look');
    }
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
    const refusals = parseLines(lines).filter(
      (entry) => entry.code === FORBIDDEN,
    );
    expect(refusals).toHaveLength(1);
    expect(refusals[0]).toMatchObject({
      level: 'warn',
      requestId: 'r1',
      method: 'look',
      code: FORBIDDEN,
    });
  });

  test('a true decision lets the request run', async () => {
    const { backend, dispatcher } = harness(() => true);
    dispatcher.register('look', async () => 'a rock', { access: 'public' });

    const outcome = await dispatcher.dispatch('look', null, {
      requestId: 'r1',
    });

    expect(outcome).toEqual({ result: 'a rock' });
    expect(backend.transactions).toBe(1);
  });

  test('the policy sees the request identity, extension and signal only', async () => {
    let seen: AccessContext | undefined;
    const { dispatcher } = harness((ctx) => {
      seen = ctx;
      return true;
    });
    dispatcher.register('look', async () => null, {
      access: 'public',
      extension: 'game',
    });

    await dispatcher.dispatch('look', null, {
      requestId: 'r1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
    });

    expect(seen).toMatchObject({
      requestId: 'r1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      sessionId: 's1',
      extension: 'game',
    });
    expect(seen?.signal).toBeInstanceOf(AbortSignal);
    expect(seen?.logger).toBeDefined();
    // Authorisation runs before any transaction exists: exposing storage or a
    // Session change here would make the gate able to write.
    for (const forbidden of ['storage', 'bindSession', 'unbindSession']) {
      expect(seen).not.toHaveProperty(forbidden);
    }
  });

  test('omits an absent client request id from the access context', async () => {
    let seen: AccessContext | undefined;
    const { dispatcher } = harness((ctx) => {
      seen = ctx;
      return true;
    });
    dispatcher.register('look', async () => null, { access: 'public' });

    await dispatcher.dispatch('look', null, { requestId: 'r1' });

    const context = seen;
    if (context === undefined) {
      throw new Error('authorize did not receive a context');
    }
    expect(context.clientRequestId).toBeUndefined();
    expect(Object.hasOwn(context, 'clientRequestId')).toBe(false);
  });

  test('the policy receives the caller payload unchanged', async () => {
    const payload = { target: 'rock', nested: [1, 2] };
    let seen: unknown;
    const { dispatcher } = harness((_ctx, _method, value) => {
      seen = value;
      return true;
    });
    dispatcher.register('look', async () => null, { access: 'public' });

    await dispatcher.dispatch('look', payload);

    expect(seen).toBe(payload);
  });

  test('a session handler refused by the policy opens no transaction', async () => {
    const { backend, dispatcher } = harness(() => false);
    let calls = 0;
    dispatcher.register('inspect', async () => {
      calls += 1;
      return null;
    });

    const error = await rejectionOf(
      dispatcher.dispatch('inspect', null, {
        requestId: 'r1',
        sessionId: 's1',
      }),
    );

    expect(error).toBeInstanceOf(ForbiddenError);
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
  });

  test('public handlers also pass through the policy', async () => {
    const { dispatcher } = harness(() => false);
    let calls = 0;
    dispatcher.register(
      'look',
      async () => {
        calls += 1;
        return null;
      },
      { access: 'public' },
    );

    const error = await rejectionOf(dispatcher.dispatch('look', null));

    expect(error).toBeInstanceOf(ForbiddenError);
    expect(calls).toBe(0);
  });

  test('an unknown method is answered before the policy runs', async () => {
    let calls = 0;
    const { backend, dispatcher } = harness(() => {
      calls += 1;
      return true;
    });

    const error = await rejectionOf(dispatcher.dispatch('missing', null));

    expect(error).toBeInstanceOf(UnknownHandlerError);
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
  });

  test('the Session gate is answered before the policy runs', async () => {
    let calls = 0;
    const { backend, dispatcher, lines } = harness(() => {
      calls += 1;
      return true;
    });
    dispatcher.register('inspect', async () => null);

    const error = await rejectionOf(dispatcher.dispatch('inspect', null));

    expect(error).toBeInstanceOf(AuthenticationRequiredError);
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
    expect(
      parseLines(lines).filter((entry) => entry.code === AUTH_REQUIRED),
    ).toHaveLength(1);
  });

  test('a throwing policy becomes INTERNAL_ERROR, not a wire code of its own', async () => {
    const { backend, dispatcher } = harness(() => {
      throw new Error('policy exploded');
    });
    dispatcher.register('look', async () => null, { access: 'public' });
    const executor = new DispatcherExecutor({ dispatcher });

    const outcome = await executor.execute({
      requestId: 'r1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      method: 'look',
      payload: null,
    });

    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure outcome');
    }
    expect(outcome.error.code).toBe('INTERNAL_ERROR');
    expect(outcome.error.message).toContain('policy exploded');
    expect(backend.transactions).toBe(0);
    await executor.close();
  });

  test('a truthy non-boolean decision is an invalid policy result', async () => {
    const { backend, dispatcher } = harness(
      // A consumer policy is outside code: the declared return type does not
      // constrain what it actually returns, so the cast models the wire-level
      // shape this check has to refuse.
      () => 1 as unknown as boolean,
    );
    let calls = 0;
    dispatcher.register(
      'look',
      async () => {
        calls += 1;
        return 'ok';
      },
      { access: 'public' },
    );
    const executor = new DispatcherExecutor({ dispatcher });

    const outcome = await executor.execute({
      requestId: 'r1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      method: 'look',
      payload: null,
    });

    expect(outcome.ok).toBe(false);
    if (outcome.ok) {
      throw new Error('expected a failure outcome');
    }
    expect(outcome.error.code).toBe('INTERNAL_ERROR');
    expect(outcome.error.message).toContain('boolean');
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
    await executor.close();
  });

  test('a policy throwing a BusinessError still becomes INTERNAL_ERROR', async () => {
    const { dispatcher } = harness(() => {
      throw new BusinessError('POLICY_SAYS_NO', 'not a business refusal');
    });
    dispatcher.register('look', async () => null, { access: 'public' });
    const executor = new DispatcherExecutor({ dispatcher });

    const outcome = await executor.execute({
      requestId: 'r1',
      connectionId: 'c1',
      clientRequestId: 'client-1',
      method: 'look',
      payload: null,
    });

    expect(outcome).toEqual({
      ok: false,
      error: {
        code: 'INTERNAL_ERROR',
        message: expect.stringContaining('not a business refusal'),
      },
    });
    await executor.close();
  });

  test('a slow policy is bounded by the request deadline', async () => {
    const { backend, dispatcher } = harness(
      () => new Promise<boolean>(() => undefined),
      10,
    );
    let calls = 0;
    dispatcher.register(
      'look',
      async () => {
        calls += 1;
        return null;
      },
      { access: 'public' },
    );

    const error = await rejectionOf(
      dispatcher.dispatch('look', null, { requestId: 'r1' }),
    );

    expect(error).toBeInstanceOf(FrameworkTimeoutError);
    expect(calls).toBe(0);
    expect(backend.transactions).toBe(0);
  });

  test('a cancel ends a pending policy decision', async () => {
    const entered = Promise.withResolvers<void>();
    const { backend, dispatcher } = harness(() => {
      entered.resolve();
      return new Promise<boolean>(() => undefined);
    });
    dispatcher.register('look', async () => null, { access: 'public' });

    const pending = dispatcher.dispatch('look', null, { requestId: 'r1' });
    await entered.promise;
    expect(dispatcher.cancel('r1', 'TRANSPORT_CLOSED')).toBe(true);
    const error = await rejectionOf(pending);

    expect(error).toBeInstanceOf(RequestCancelledError);
    expect(backend.transactions).toBe(0);
  });

  test('a late policy rejection after the deadline is not unhandled', async () => {
    const unhandled: unknown[] = [];
    const onUnhandled = (reason: unknown): void => {
      unhandled.push(reason);
    };
    process.on('unhandledRejection', onUnhandled);
    try {
      const { backend, dispatcher } = harness(async () => {
        await sleep(30);
        throw new Error('late policy failure');
      }, 5);
      dispatcher.register('look', async () => null, { access: 'public' });

      const error = await rejectionOf(
        dispatcher.dispatch('look', null, { requestId: 'r1' }),
      );
      expect(error).toBeInstanceOf(FrameworkTimeoutError);
      await sleep(60);

      expect(unhandled).toEqual([]);
      expect(backend.transactions).toBe(0);
    } finally {
      process.off('unhandledRejection', onUnhandled);
    }
  });
});
