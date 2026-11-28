import { describe, expect, test } from 'bun:test';

import { expectRejected } from './protocol.support.ts';
import { parseIpcMessage } from './protocol.ts';

describe('parseIpcMessage', () => {
  test('accepts a request without sessionId or payload', () => {
    const parsed = parseIpcMessage({
      type: 'request',
      requestId: 'req-1',
      connectionId: 'conn-1',
      clientRequestId: 'client-1',
      method: 'ping',
      timeoutMs: 250,
      replayCount: 2,
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'request',
        requestId: 'req-1',
        connectionId: 'conn-1',
        clientRequestId: 'client-1',
        sessionId: undefined,
        method: 'ping',
        payload: undefined,
        timeoutMs: 250,
        replayCount: 2,
      },
    });
  });

  test('accepts a request without a clientRequestId', () => {
    const parsed = parseIpcMessage({
      type: 'request',
      requestId: 'req-1',
      connectionId: 'conn-1',
      method: 'ping',
      timeoutMs: 250,
      replayCount: 0,
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'request',
        requestId: 'req-1',
        connectionId: 'conn-1',
        clientRequestId: undefined,
        sessionId: undefined,
        method: 'ping',
        payload: undefined,
        timeoutMs: 250,
        replayCount: 0,
      },
    });
  });

  test('rejects an empty clientRequestId', () => {
    expectRejected(
      {
        type: 'request',
        requestId: 'req-1',
        connectionId: 'conn-1',
        clientRequestId: '',
        method: 'ping',
        timeoutMs: 250,
        replayCount: 0,
      },
      'clientRequestId',
    );
  });

  test('accepts activate and drops extra fields', () => {
    const parsed = parseIpcMessage({ type: 'activate', futureField: true });

    expect(parsed).toStrictEqual({
      ok: true,
      message: { type: 'activate' },
    });
  });

  test('rejects a field of the wrong type', () => {
    expectRejected(
      { type: 'cancel', requestId: 42, code: 'FRAMEWORK_TIMEOUT' },
      'requestId',
    );
  });

  test('rejects an empty string identifier', () => {
    expectRejected(
      { type: 'cancel', requestId: '', code: 'FRAMEWORK_TIMEOUT' },
      'non-empty string',
    );
  });

  test('rejects a non-finite timeoutMs', () => {
    expectRejected(
      {
        type: 'request',
        requestId: 'req-1',
        connectionId: 'conn-1',
        clientRequestId: 'client-1',
        method: 'look',
        timeoutMs: Number.POSITIVE_INFINITY,
        replayCount: 0,
      },
      'timeoutMs',
    );
  });

  test('rejects a shutdown without a budget', () => {
    // A Server must know how long the Portal will wait; without the field it
    // would fall back to a budget derived from its own request ceiling, which is
    // exactly the mismatch that let a correctly closing Server be killed.
    expectRejected({ type: 'shutdown' }, 'timeoutMs');
  });

  test('rejects a shutdown budget that is not a positive number', () => {
    for (const timeoutMs of [0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expectRejected({ type: 'shutdown', timeoutMs }, 'timeoutMs');
    }
  });

  test('rejects a shutdown budget that is not a number', () => {
    expectRejected({ type: 'shutdown', timeoutMs: '5000' }, 'timeoutMs');
  });

  test('rejects a negative replayCount', () => {
    expectRejected(
      {
        type: 'request',
        requestId: 'req-1',
        connectionId: 'conn-1',
        clientRequestId: 'client-1',
        method: 'look',
        timeoutMs: 100,
        replayCount: -1,
      },
      'replayCount',
    );
  });
});
