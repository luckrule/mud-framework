import { describe, expect, test } from 'bun:test';

import { BusinessError } from '../shared/wire-codes.ts';
import { isRecord, sleep, tick } from '../testing/common.ts';
import {
  echoRegistration,
  received,
  requestMessage,
  results,
  startServer,
} from '../testing/server-process.ts';

describe('runServerProcess requests', () => {
  test('executes a request and answers with a result', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    // `startServer` already drove the handshake, so the Server is ready here.
    await tick();

    harness.portal.send(requestMessage({ payload: 'ping' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      { type: 'result', requestId: 'req-1', ok: true, result: 'ping' },
    ]);
    await harness.stop();
  });

  test('resends ready with the code version for a new portal generation', async () => {
    // The generation sequence is the point here, so this test drives the
    // handshake itself: the automatic generation 1 would make an explicit one
    // stale.
    const harness = startServer({
      autoHandshake: false,
      codeVersion: 'build-1',
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();
    harness.portal.send({ type: 'hello', portalGeneration: 1 });
    harness.portal.send({ type: 'activate' });
    await tick();
    harness.portal.send({ type: 'hello', portalGeneration: 2 });
    await tick();

    const ready = received(harness.messages).filter(
      (message) => message.type === 'ready',
    );
    expect(ready).toHaveLength(2);
    for (const message of ready) {
      expect(message).toMatchObject({
        type: 'ready',
        codeVersion: 'build-1',
      });
    }
    await harness.stop();
  });

  test('maps a thrown handler error to a failure result with its code', async () => {
    const harness = startServer({
      extensions: [
        {
          name: 'failures',
          register: (dispatcher) => {
            dispatcher.register(
              'explode',
              () => Promise.reject(new Error('business failure')),
              { access: 'public' },
            );
            dispatcher.register(
              'denied',
              () =>
                Promise.reject(
                  new BusinessError('BUSINESS_DENIED', 'not allowed'),
                ),
              { access: 'public' },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'explode' }));
    harness.portal.send(
      requestMessage({ requestId: 'req-2', method: 'denied' }),
    );
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: false,
        error: { code: 'INTERNAL_ERROR', message: 'business failure' },
      },
      {
        type: 'result',
        requestId: 'req-2',
        ok: false,
        error: { code: 'BUSINESS_DENIED', message: 'not allowed' },
      },
    ]);
    await harness.stop();
  });

  test('maps an unknown method to UNKNOWN_METHOD', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'missing' }));
    await tick();

    const [outcome] = results(harness.messages);
    expect(outcome).toMatchObject({
      ok: false,
      error: { code: 'UNKNOWN_METHOD' },
    });
    await harness.stop();
  });

  test('carries the session change a handler bound into the result message', async () => {
    const harness = startServer({
      extensions: [
        {
          name: 'auth',
          register: (dispatcher) => {
            dispatcher.register(
              'login',
              async (ctx) => {
                ctx.bindSession('session-1');
                return 'welcome';
              },
              { access: 'public', mayChangeSession: true },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'login' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: 'welcome',
        sessionChange: { type: 'bind', sessionId: 'session-1' },
      },
    ]);
    await harness.stop();
  });

  test('omits the session change from the result when the handler changes nothing', async () => {
    const harness = startServer({
      extensions: [{ name: 'echo', register: echoRegistration }],
    });
    await tick();

    harness.portal.send(requestMessage());
    await tick();

    expect(results(harness.messages)).toEqual([
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: 'hi',
      },
    ]);
    // `LocalExecutor` omits the key entirely for an unbound success, and both
    // JSON and structured clone keep that shape, so "absent" is literally what
    // "unbound" looks like on the wire (SPEC 2). `toBeUndefined` would also
    // pass on a present-but-undefined key, i.e. on the shape this asserts
    // against, so the assertion is on the key's presence.
    const raw = harness.messages.find(
      (message) => isRecord(message) && message.type === 'result',
    );
    if (!isRecord(raw)) {
      throw new Error('expected a raw result message');
    }
    expect(raw).not.toHaveProperty('sessionId');
    await harness.stop();
  });

  test('maps a request timeout to FRAMEWORK_TIMEOUT', async () => {
    const harness = startServer({
      timeoutMs: 10,
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register(
              'slow',
              async () => {
                await sleep(40);
                return 'late';
              },
              { access: 'public' },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow' }));
    await sleep(20);

    const [outcome] = results(harness.messages);
    expect(outcome).toMatchObject({
      ok: false,
      error: { code: 'FRAMEWORK_TIMEOUT' },
    });
    // Let the abandoned handler settle before closing the backend.
    await sleep(40);
    await harness.stop();
  });

  test('ignores a request that arrives before hello', async () => {
    // ADR-0007: nothing can run before `ready`, and a Portal that delivers
    // early is out of protocol (SPEC 6.2). The fault is logged instead of
    // silently tolerated.
    const harness = startServer({
      autoHandshake: false,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });

    harness.portal.send(requestMessage());
    await tick();

    expect(results(harness.messages)).toEqual([]);
    const warnings = harness
      .logs()
      .filter((line) => line.code === 'REQUEST_BEFORE_READY');
    expect(warnings).toHaveLength(1);
    await harness.stop();
  });

  test('logs REQUEST_BEFORE_READY once, not once per request', async () => {
    // The timing fault stays diagnosable without letting the log volume track
    // the request rate (SPEC 7.1).
    const harness = startServer({
      autoHandshake: false,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });

    for (let index = 0; index < 5; index += 1) {
      harness.portal.send(requestMessage({ requestId: `req-${index}` }));
    }
    await tick();

    expect(results(harness.messages)).toEqual([]);
    const warnings = harness
      .logs()
      .filter((line) => line.code === 'REQUEST_BEFORE_READY');
    expect(warnings).toHaveLength(1);
    await harness.stop();
  });

  test('starts executing requests once ready, without a second warning', async () => {
    const harness = startServer({
      autoHandshake: false,
      extensions: [{ name: 'echo', register: echoRegistration }],
    });

    harness.portal.send(requestMessage({ requestId: 'req-1' }));
    await tick();
    harness.portal.send({ type: 'hello', portalGeneration: 1 });
    harness.portal.send({ type: 'activate' });
    await tick();
    harness.portal.send(requestMessage({ requestId: 'req-2' }));
    await tick();

    expect(results(harness.messages)).toEqual([
      { type: 'result', requestId: 'req-2', ok: true, result: 'hi' },
    ]);
    const warnings = harness
      .logs()
      .filter((line) => line.code === 'REQUEST_BEFORE_READY');
    expect(warnings).toHaveLength(1);
    await harness.stop();
  });

  test('runs a duplicated request id only once', async () => {
    let calls = 0;
    const gate = Promise.withResolvers<void>();
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register(
              'slow',
              async () => {
                calls += 1;
                await gate.promise;
                return 'done';
              },
              { access: 'public' },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow', replayCount: 0 }));
    harness.portal.send(requestMessage({ method: 'slow', replayCount: 1 }));
    gate.resolve();
    await tick();

    expect(calls).toBe(1);
    expect(results(harness.messages)).toHaveLength(1);
    const duplicates = harness
      .logs()
      .filter((line) => line.code === 'DUPLICATE_REQUEST');
    expect(duplicates).toHaveLength(1);
    expect(duplicates[0]?.replayCount).toBe(1);
    await harness.stop();
  });

  test('logs DUPLICATE_REQUEST once per request id, not once per replay', async () => {
    const gate = Promise.withResolvers<void>();
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register(
              'slow',
              async () => {
                await gate.promise;
                return 'done';
              },
              { access: 'public' },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow', replayCount: 0 }));
    for (let replayCount = 1; replayCount <= 5; replayCount += 1) {
      harness.portal.send(requestMessage({ method: 'slow', replayCount }));
    }
    gate.resolve();
    await tick();

    // One line for the id, however many replays of it arrived (SPEC 7.1).
    const duplicates = harness
      .logs()
      .filter((line) => line.code === 'DUPLICATE_REQUEST');
    expect(duplicates).toHaveLength(1);
    await harness.stop();
  });

  test('logs DUPLICATE_REQUEST again after the id went terminal', async () => {
    // The suppression set is pruned when a request reaches a terminal state, so
    // a later burst of the same id is reported again instead of being silenced
    // for the rest of the process lifetime.
    let gate = Promise.withResolvers<void>();
    const harness = startServer({
      extensions: [
        {
          name: 'slow',
          register: (dispatcher) => {
            dispatcher.register(
              'slow',
              async () => {
                await gate.promise;
                return 'done';
              },
              { access: 'public' },
            );
          },
        },
      ],
    });
    await tick();

    harness.portal.send(requestMessage({ method: 'slow', replayCount: 0 }));
    harness.portal.send(requestMessage({ method: 'slow', replayCount: 1 }));
    gate.resolve();
    await tick();

    gate = Promise.withResolvers<void>();
    harness.portal.send(requestMessage({ method: 'slow', replayCount: 2 }));
    harness.portal.send(requestMessage({ method: 'slow', replayCount: 3 }));
    gate.resolve();
    await tick();

    const duplicates = harness
      .logs()
      .filter((line) => line.code === 'DUPLICATE_REQUEST');
    expect(duplicates).toHaveLength(2);
    await harness.stop();
  });
});
