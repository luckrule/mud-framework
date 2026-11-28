import { describe, expect, test } from 'bun:test';

import { CLIENT_REQUEST_ID_EMPTY_MESSAGE } from '../../shared/validation.ts';
import {
  FakeServerProcess,
  failureOf,
  flushTicks,
  harness,
  readyMessage,
  request,
  requestMessages,
  started,
  successMessage,
} from '../../testing/ipc-executor.ts';
import { IpcExecutor } from './ipc-executor.ts';

describe('IpcExecutor result', () => {
  test('rejects an explicitly empty clientRequestId without delivering it', async () => {
    const h = harness();
    const server = await started(h);

    const outcome = await h.executor.execute(request({ clientRequestId: '' }));

    expect(failureOf(outcome)).toEqual({
      code: 'INTERNAL_ERROR',
      message: CLIENT_REQUEST_ID_EMPTY_MESSAGE,
    });
    expect(requestMessages(server)).toHaveLength(0);
    const rejected = h.logs().find((line) => line.msg === 'request rejected');
    expect(rejected).toMatchObject({ code: 'INTERNAL_ERROR' });
    // The invalid value never becomes a correlation key in the log.
    expect(rejected).not.toHaveProperty('clientRequestId');
    await h.executor.close();
  });

  test('returns the server result as a success outcome', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(
      request({
        requestId: 'req-1',
        connectionId: 'c9',
        clientRequestId: 'client-9',
        sessionId: 's1',
        method: 'look',
        payload: { at: 'rock' },
      }),
    );

    const [message] = requestMessages(server);
    expect(message).toMatchObject({
      type: 'request',
      requestId: 'req-1',
      connectionId: 'c9',
      clientRequestId: 'client-9',
      sessionId: 's1',
      method: 'look',
      payload: { at: 'rock' },
      replayCount: 0,
    });
    // The Portal sends the remaining budget, which may already be a millisecond
    // below the ceiling by the time the message is built.
    expect(message?.timeoutMs).toBeGreaterThan(0);
    expect(message?.timeoutMs).toBeLessThanOrEqual(1000);
    server.send(successMessage('req-1', { hp: 3 }));
    expect(await outcomePromise).toEqual({ ok: true, result: { hp: 3 } });
    await h.executor.close();
  });

  test('passes a business failure through unchanged', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    server.send({
      type: 'result',
      requestId: 'req-1',
      ok: false,
      error: { code: 'NOT_ENOUGH_GOLD', message: 'you cannot afford that' },
    });

    const outcome = await outcomePromise;
    expect(outcome.ok).toBe(false);
    expect(failureOf(outcome)).toEqual({
      code: 'NOT_ENOUGH_GOLD',
      message: 'you cannot afford that',
    });
    expect(outcome).not.toHaveProperty('type');
    expect(outcome).not.toHaveProperty('requestId');
    await h.executor.close();
  });

  test('carries the session change a result reports into the outcome', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    server.send(
      successMessage('req-1', 'welcome', {
        type: 'bind',
        sessionId: 'session-1',
      }),
    );

    const outcome = await outcomePromise;
    expect(outcome).toEqual({
      ok: true,
      result: 'welcome',
      sessionChange: { type: 'bind', sessionId: 'session-1' },
    });
    // The outcome is the message minus its envelope: a leaked `type` or
    // `requestId` would mean the split is wrong, and the connection layer would
    // be looking at wire fields it does not own.
    expect(outcome).not.toHaveProperty('type');
    expect(outcome).not.toHaveProperty('requestId');
    const received = h.logs().find((line) => line.msg === 'result received');
    // No entry Session: the log names no `sessionId`, and the binding is
    // carried by `sessionChange`.
    expect(received).not.toHaveProperty('sessionId');
    expect(received).toMatchObject({
      sessionChange: { type: 'bind', sessionId: 'session-1' },
    });
    await h.executor.close();
  });

  test('carries an unbind change a result reports into the outcome', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(
      request({ requestId: 'req-1', sessionId: 'entry-1' }),
    );
    server.send(successMessage('req-1', 'bye', { type: 'unbind' }));

    const outcome = await outcomePromise;
    expect(outcome).toEqual({
      ok: true,
      result: 'bye',
      sessionChange: { type: 'unbind' },
    });
    expect(outcome).not.toHaveProperty('type');
    expect(outcome).not.toHaveProperty('requestId');
    const received = h.logs().find((line) => line.msg === 'result received');
    expect(received).toMatchObject({
      sessionId: 'entry-1',
      sessionChange: { type: 'unbind' },
    });
    await h.executor.close();
  });

  test('logs the session a request already holds when the result reports none', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(
      request({ requestId: 'req-1', sessionId: 's-existing' }),
    );
    server.send({
      type: 'result',
      requestId: 'req-1',
      ok: false,
      error: { code: 'NOT_ENOUGH_GOLD', message: 'you cannot afford that' },
    });

    await outcomePromise;
    const received = h.logs().find((line) => line.msg === 'result received');
    expect(received).toMatchObject({ sessionId: 's-existing' });
    expect(received).not.toHaveProperty('sessionChange');
    await h.executor.close();
  });

  test('omits the session id when the result reports none', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    server.send(successMessage('req-1', 'ok'));

    expect(await outcomePromise).toEqual({ ok: true, result: 'ok' });
    const received = h.logs().find((line) => line.msg === 'result received');
    expect(received).not.toHaveProperty('sessionId');
    expect(received).not.toHaveProperty('sessionChange');
    await h.executor.close();
  });

  test('logs the request session of a late failure result', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    await h.executor.cancel('req-1', 'TRANSPORT_CLOSED');
    await outcomePromise;

    server.send({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 'entry-1',
      ok: false,
      error: { code: 'FRAMEWORK_TIMEOUT', message: 'too late' },
    });
    await flushTicks();

    const late = h.logs().find((line) => line.msg === 'late result discarded');
    expect(late).toMatchObject({ requestId: 'req-1', sessionId: 'entry-1' });
    await h.executor.close();
  });

  test('prefers the request echo over the binding of a late success', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h);

    const outcomePromise = h.executor.execute(request({ requestId: 'req-1' }));
    await h.executor.cancel('req-1', 'TRANSPORT_CLOSED');
    await outcomePromise;

    server.send({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 'entry-1',
      ok: true,
      result: 'late',
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
    });
    await flushTicks();

    // The echo names the entry session, so it wins over a binding the discarded
    // result still happens to carry.
    const late = h.logs().find((line) => line.msg === 'late result discarded');
    expect(late).toMatchObject({ requestId: 'req-1', sessionId: 'entry-1' });
    await h.executor.close();
  });

  test('logs the echoed entry session of a late success that bound nothing', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h);

    const outcomePromise = h.executor.execute(
      request({ requestId: 'req-1', sessionId: 'entry-1' }),
    );
    await h.executor.cancel('req-1', 'TRANSPORT_CLOSED');
    const outcome = await outcomePromise;

    server.send({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 'entry-1',
      ok: true,
      result: 'late',
    });
    await flushTicks();

    // The entry session now exists only in the echo; the pending entry that
    // held it was removed when the Portal settled the request.
    const late = h.logs().find((line) => line.msg === 'late result discarded');
    expect(late).toMatchObject({ requestId: 'req-1', sessionId: 'entry-1' });
    expect(outcome).toEqual({
      ok: false,
      error: {
        code: 'TRANSPORT_CLOSED',
        message: 'request req-1 was cancelled',
      },
    });
    expect(outcome).not.toHaveProperty('requestSessionId');
    await h.executor.close();
  });

  test('leaves a late result without a session when the result carries no echo', async () => {
    const h = harness({ timeoutMs: 5000 });
    const server = await started(h);

    const outcomePromise = h.executor.execute(
      request({ requestId: 'req-1', sessionId: 'entry-1' }),
    );
    await h.executor.cancel('req-1', 'TRANSPORT_CLOSED');
    await outcomePromise;

    server.send(successMessage('req-1', 'late'));
    await flushTicks();

    // The result carries no echo, so it names no Session; the pending entry is
    // gone, so correlation falls back to the stable requestId (SPEC 7.3).
    const late = h.logs().find((line) => line.msg === 'late result discarded');
    expect(late).toBeDefined();
    expect(late).not.toHaveProperty('sessionId');
    await h.executor.close();
  });

  test('keeps the entry session and reports the binding as a session change on result received', async () => {
    const h = harness();
    const server = await started(h);

    const outcomePromise = h.executor.execute(
      request({ requestId: 'req-1', sessionId: 'entry-1' }),
    );
    server.send({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 'entry-1',
      ok: true,
      result: 'welcome',
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
    });

    await outcomePromise;
    const received = h.logs().find((line) => line.msg === 'result received');
    // The entry Session stays the request's identity; the new binding is the
    // change, not a replacement for `sessionId`.
    expect(received).toMatchObject({
      sessionId: 'entry-1',
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
    });
    await h.executor.close();
  });

  test('keeps the request session echo out of the settled outcome', async () => {
    const h = harness();
    const server = await started(h);

    const first = h.executor.execute(request({ requestId: 'req-1' }));
    server.send({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 'entry-1',
      ok: true,
      result: 'welcome',
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
    });
    const firstOutcome = await first;
    expect(firstOutcome).toEqual({
      ok: true,
      result: 'welcome',
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
    });
    expect(firstOutcome).not.toHaveProperty('requestSessionId');

    const second = h.executor.execute(request({ requestId: 'req-2' }));
    server.send({
      type: 'result',
      requestId: 'req-2',
      requestSessionId: 'entry-2',
      ok: false,
      error: { code: 'NOT_ENOUGH_GOLD', message: 'you cannot afford that' },
    });
    const secondOutcome = await second;
    expect(secondOutcome).toEqual({
      ok: false,
      error: { code: 'NOT_ENOUGH_GOLD', message: 'you cannot afford that' },
    });
    expect(secondOutcome).not.toHaveProperty('requestSessionId');
    await h.executor.close();
  });

  test('works without a logger option, using the default stdout logger', async () => {
    // The default must be observable (SPEC §7.1, INV-013): an executor that is
    // silent unless the caller injects a logger leaves crash, replay and
    // shutdown events invisible in the documented minimal setup. This asserts
    // the default is usable; the stdout side effect is the intended behaviour.
    const server = new FakeServerProcess({ autoCloseOnShutdown: true });
    const executor = new IpcExecutor({
      spawn: () => server.handle,
      timeoutMs: 1000,
    });

    const startPromise = executor.start();
    server.send(readyMessage());
    await startPromise;

    const outcomePromise = executor.execute(request({ requestId: 'req-1' }));
    server.send(successMessage('req-1', 'ok'));

    expect(await outcomePromise).toEqual({ ok: true, result: 'ok' });
    await executor.close();
  });
});
