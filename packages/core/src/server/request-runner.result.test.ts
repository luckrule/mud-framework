import { describe, expect, test } from 'bun:test';

import { tick } from '../testing/common.ts';
import { requestMessage } from '../testing/server-process.ts';
import { startRunner } from './request-runner.support.ts';

describe('RequestRunner', () => {
  test('accept runs the handler and answers with its result', async () => {
    const harness = startRunner();
    harness.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });

    harness.runner.accept(requestMessage({ payload: 'ping' }));
    await tick();

    expect(harness.sent).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'ping' },
    ]);
    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    expect(completed[0]).toMatchObject({ requestId: 'req-1' });
  });

  test('echoes the entry session id on a success result', async () => {
    const harness = startRunner();
    harness.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });

    harness.runner.accept(requestMessage({ sessionId: 'entry-1' }));
    await tick();

    expect(harness.sent).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: true,
        result: 'hi',
      },
    ]);
  });

  test('echoes the entry session id on a failure result', async () => {
    const harness = startRunner();
    harness.register('fail', () => Promise.reject(new Error('boom')), {
      access: 'public',
    });

    harness.runner.accept(
      requestMessage({ method: 'fail', sessionId: 'entry-1' }),
    );
    await tick();

    expect(harness.sent).toHaveLength(1);
    expect(harness.sent[0]).toMatchObject({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 'entry-1',
      ok: false,
      error: { code: 'INTERNAL_ERROR' },
    });
  });

  test('logs and relays an unbind change without inventing a session id', async () => {
    const harness = startRunner();
    harness.register(
      'logout',
      async (ctx) => {
        ctx.unbindSession();
        return 'bye';
      },
      { access: 'public', mayChangeSession: true },
    );

    harness.runner.accept(
      requestMessage({ method: 'logout', sessionId: 'entry-1' }),
    );
    await tick();

    expect(harness.sent).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: true,
        result: 'bye',
        sessionChange: { type: 'unbind' },
      },
    ]);
    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    expect(completed[0]).toMatchObject({
      requestId: 'req-1',
      sessionId: 'entry-1',
      sessionChange: { type: 'unbind' },
    });
  });

  test('keeps a newly bound session distinct from the entry session', async () => {
    const harness = startRunner();
    harness.register(
      'login',
      async (ctx) => {
        ctx.bindSession('bound-1');
        return 'welcome';
      },
      { access: 'public', mayChangeSession: true },
    );

    harness.runner.accept(
      requestMessage({ method: 'login', sessionId: 'entry-1' }),
    );
    await tick();

    expect(harness.sent).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: true,
        result: 'welcome',
        sessionChange: { type: 'bind', sessionId: 'bound-1' },
      },
    ]);
    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    // `sessionId` stays the entry Session; the new binding is the change.
    expect(completed[0]).toMatchObject({
      requestId: 'req-1',
      sessionId: 'entry-1',
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
    });
  });

  test('logs no sessionId for a bind when the request entered without one', async () => {
    const harness = startRunner();
    harness.register(
      'login',
      async (ctx) => {
        ctx.bindSession('bound-1');
        return 'welcome';
      },
      { access: 'public', mayChangeSession: true },
    );

    harness.runner.accept(requestMessage({ method: 'login' }));
    await tick();

    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    expect(completed[0]).not.toHaveProperty('sessionId');
    expect(completed[0]).toMatchObject({
      requestId: 'req-1',
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
    });
  });

  test('logs the entry session without a sessionChange when nothing changed', async () => {
    const harness = startRunner();
    harness.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });

    harness.runner.accept(requestMessage({ sessionId: 'entry-1' }));
    await tick();

    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    expect(completed[0]).toMatchObject({
      requestId: 'req-1',
      sessionId: 'entry-1',
    });
    expect(completed[0]).not.toHaveProperty('sessionChange');
  });

  test('omits requestSessionId when the request entered without a session', async () => {
    const harness = startRunner();
    harness.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });
    harness.register('fail', () => Promise.reject(new Error('boom')), {
      access: 'public',
    });

    harness.runner.accept(requestMessage());
    harness.runner.accept(
      requestMessage({ requestId: 'req-2', method: 'fail' }),
    );
    await tick();

    expect(harness.sent).toHaveLength(2);
    for (const message of harness.sent) {
      expect(message).not.toHaveProperty('requestSessionId');
    }
  });

  test('echoes the entry session id when refusing during shutdown', () => {
    const harness = startRunner(1000, () => false);

    harness.runner.accept(requestMessage({ sessionId: 'entry-1' }));

    expect(harness.sent).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: false,
        error: {
          code: 'FRAMEWORK_SHUTDOWN',
          message: 'server is shutting down',
        },
      },
    ]);
  });

  test('omits requestSessionId when refusing a request without a session', () => {
    const harness = startRunner(1000, () => false);

    harness.runner.accept(requestMessage());

    expect(harness.sent).toHaveLength(1);
    expect(harness.sent[0]).toMatchObject({
      ok: false,
      error: { code: 'FRAMEWORK_SHUTDOWN' },
    });
    expect(harness.sent[0]).not.toHaveProperty('requestSessionId');
  });

  test('request completed names the Extension that registered the handler', async () => {
    const harness = startRunner();
    harness.register('deposit', (_ctx, payload) => Promise.resolve(payload), {
      extension: 'banking',
      access: 'public',
    });

    harness.runner.accept(requestMessage({ method: 'deposit', payload: 'ok' }));
    await tick();

    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    expect(completed[0]).toMatchObject({
      requestId: 'req-1',
      extension: 'banking',
    });
  });

  test('request failed names the Extension that registered the handler', async () => {
    const harness = startRunner();
    harness.register(
      'withdraw',
      () => Promise.reject(new Error('insufficient funds')),
      { extension: 'banking', access: 'public' },
    );

    harness.runner.accept(requestMessage({ method: 'withdraw' }));
    await tick();

    const failed = harness
      .logs()
      .filter((line) => line.msg === 'request failed');
    expect(failed).toHaveLength(1);
    expect(failed[0]).toMatchObject({
      code: 'INTERNAL_ERROR',
      extension: 'banking',
    });
  });

  test('a handler no Extension registered logs no extension key', async () => {
    const harness = startRunner();
    harness.register('echo', (_ctx, payload) => Promise.resolve(payload), {
      access: 'public',
    });

    harness.runner.accept(requestMessage({ payload: 'ping' }));
    await tick();

    const completed = harness
      .logs()
      .filter((line) => line.msg === 'request completed');
    expect(completed).toHaveLength(1);
    // An absent field, not `extension: undefined` (SPEC 7.1).
    expect(completed[0]).not.toHaveProperty('extension');
  });
});
