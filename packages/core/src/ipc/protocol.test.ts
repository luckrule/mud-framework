import { describe, expect, test } from 'bun:test';

import type { IpcMessage } from './protocol.ts';
import { parseIpcMessage } from './protocol.ts';

const capabilities = {
  transactions: 'full',
  concurrency: 'pooled',
  snapshotRead: true,
  uniqueConstraint: true,
  maxTransactionDurationMs: 5000,
} as const;

const validMessages: IpcMessage[] = [
  { type: 'hello', portalGeneration: 3 },
  {
    type: 'request',
    requestId: 'req-1',
    connectionId: 'conn-1',
    clientRequestId: 'client-1',
    sessionId: 'session-1',
    method: 'look',
    payload: { target: 'rock' },
    timeoutMs: 1000,
    replayCount: 0,
  },
  { type: 'cancel', requestId: 'req-1', code: 'FRAMEWORK_TIMEOUT' },
  { type: 'shutdown', timeoutMs: 5000 },
  { type: 'ready', pid: 4242, capabilities },
  { type: 'result', requestId: 'req-1', ok: true, result: { hp: 3 } },
  {
    type: 'result',
    requestId: 'req-1',
    ok: false,
    error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
  },
  { type: 'closed', reason: 'shutdown complete' },
];

function rejectionReason(raw: unknown): string {
  const parsed = parseIpcMessage(raw);
  if (parsed.ok) {
    throw new Error(
      `expected rejection, got ${JSON.stringify(parsed.message)}`,
    );
  }
  return parsed.reason;
}

function expectRejected(raw: unknown, fragment: string): void {
  expect(rejectionReason(raw)).toContain(fragment);
}

describe('parseIpcMessage', () => {
  test('accepts every message in the protocol', () => {
    for (const message of validMessages) {
      const parsed = parseIpcMessage(message);
      expect(parsed.ok).toBe(true);
      if (parsed.ok) {
        expect(parsed.message.type).toBe(message.type);
      }
    }
  });

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

  test('keeps a success result that carries no value', () => {
    const parsed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      ok: true,
    });

    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.message).toEqual({
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: undefined,
        sessionId: undefined,
      });
    }
  });

  test('accepts a success result carrying a session id', () => {
    const parsed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      ok: true,
      result: null,
      sessionId: 'session-1',
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionId: 'session-1',
      },
    });
  });

  test('accepts a success result carrying the request session id', () => {
    const parsed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 'entry-1',
      ok: true,
      result: null,
      sessionId: 'bound-1',
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: true,
        result: null,
        sessionId: 'bound-1',
      },
    });
  });

  test('accepts a failed result carrying the request session id', () => {
    const parsed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 'entry-1',
      ok: false,
      error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: false,
        error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
      },
    });
  });

  test('rejects an empty request session id', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: '',
        ok: true,
        result: null,
      },
      'non-empty string',
    );
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: '',
        ok: false,
        error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
      },
      'non-empty string',
    );
  });

  test('rejects a non-string request session id', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 7,
        ok: true,
        result: null,
      },
      'requestSessionId',
    );
  });

  test('keeps the result shape when requestSessionId is absent', () => {
    // `toEqual` ignores an own `undefined` property, so a parser that always
    // attached `requestSessionId: undefined` would pass. `toStrictEqual` plus
    // the explicit property checks pin "absent" against that.
    const success = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      ok: true,
    });
    expect(success).toStrictEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: undefined,
        sessionId: undefined,
      },
    });
    expect(success.ok).toBe(true);
    if (success.ok) {
      expect(success.message).not.toHaveProperty('requestSessionId');
      expect('requestSessionId' in success.message).toBe(false);
    }

    const failed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      ok: false,
      error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
    });
    expect(failed).toStrictEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        ok: false,
        error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
      },
    });
    expect(failed.ok).toBe(true);
    if (failed.ok) {
      expect(failed.message).not.toHaveProperty('requestSessionId');
      expect('requestSessionId' in failed.message).toBe(false);
    }
  });

  test('rejects an empty session id on a success result', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionId: '',
      },
      'non-empty string',
    );
  });

  test('rejects a non-string session id on a success result', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionId: 7,
      },
      'sessionId',
    );
  });

  test('tolerates unknown extra fields from a newer peer', () => {
    const parsed = parseIpcMessage({
      type: 'hello',
      portalGeneration: 1,
      futureField: 'ignored',
    });

    expect(parsed).toEqual({
      ok: true,
      message: { type: 'hello', portalGeneration: 1 },
    });
  });

  test('drops unknown fields from a result', () => {
    // SPEC/ADR promise an old Portal tolerates a newer peer's additions; the
    // parsed shape must stay the documented one rather than pass them through.
    const parsed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      ok: true,
      result: { hp: 3 },
      sessionId: 'bound-1',
      futureField: 'ignored',
    });

    expect(parsed).toStrictEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: { hp: 3 },
        sessionId: 'bound-1',
      },
    });
    expect(parsed.ok).toBe(true);
    if (parsed.ok) {
      expect(parsed.message).not.toHaveProperty('futureField');
    }
  });

  test('rejects an unknown type and names it', () => {
    const reason = rejectionReason({ type: 'teleport' });

    expect(reason).toContain('teleport');
    expect(reason).toContain('unknown ipc message type');
  });

  test('rejects a missing type', () => {
    expectRejected({}, 'must be a string');
  });

  test('rejects a non-string type', () => {
    expectRejected({ type: 7 }, 'must be a string');
  });

  test('rejects a missing field', () => {
    expectRejected(
      {
        type: 'request',
        connectionId: 'conn-1',
        clientRequestId: 'client-1',
        method: 'look',
        timeoutMs: 100,
        replayCount: 0,
      },
      'requestId',
    );
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

  test('rejects a result without ok', () => {
    expectRejected({ type: 'result', requestId: 'req-1' }, 'ok');
  });

  test('rejects a result whose ok is not a boolean', () => {
    expectRejected(
      { type: 'result', requestId: 'req-1', ok: 'yes' },
      'must be a boolean',
    );
  });

  test('rejects a failed result without an error object', () => {
    expectRejected({ type: 'result', requestId: 'req-1', ok: false }, 'error');
  });

  test('rejects a failed result whose error lacks a code', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        ok: false,
        error: { message: 'boom' },
      },
      'code',
    );
  });

  test('rejects a ready message without capabilities', () => {
    expectRejected({ type: 'ready', pid: 1 }, 'capabilities');
  });

  test('rejects a capability value outside its enum', () => {
    expectRejected(
      {
        type: 'ready',
        pid: 1,
        capabilities: { ...capabilities, transactions: 'partial' },
      },
      'transactions',
    );
  });

  test('rejects a capability that is not a boolean', () => {
    expectRejected(
      {
        type: 'ready',
        pid: 1,
        capabilities: { ...capabilities, snapshotRead: 'yes' },
      },
      'snapshotRead',
    );
  });

  test('reads a null transaction ceiling as unbounded', () => {
    const parsed = parseIpcMessage({
      type: 'ready',
      pid: 1,
      capabilities: { ...capabilities, maxTransactionDurationMs: null },
    });

    // JSON turns Infinity into null, so both must land on the same value. The
    // whole message is asserted so a rejection cannot pass as a success.
    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'ready',
        pid: 1,
        capabilities: {
          ...capabilities,
          maxTransactionDurationMs: Number.POSITIVE_INFINITY,
        },
      },
    });
  });

  test('keeps a positive finite transaction ceiling as is', () => {
    const parsed = parseIpcMessage({
      type: 'ready',
      pid: 1,
      capabilities: { ...capabilities, maxTransactionDurationMs: 250 },
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'ready',
        pid: 1,
        capabilities: { ...capabilities, maxTransactionDurationMs: 250 },
      },
    });
  });

  test('rejects a transaction ceiling that would clamp the timeout', () => {
    for (const value of [0, -1, Number.NaN, Number.NEGATIVE_INFINITY]) {
      expectRejected(
        {
          type: 'ready',
          pid: 1,
          capabilities: { ...capabilities, maxTransactionDurationMs: value },
        },
        'a positive number or null',
      );
    }
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

  test('never throws on hostile input', () => {
    const hostile: unknown[] = [
      null,
      undefined,
      'hello',
      0,
      42,
      Number.NaN,
      true,
      [],
      [{ type: 'hello', portalGeneration: 1 }],
      new Map(),
      Symbol('x'),
      () => undefined,
    ];

    for (const raw of hostile) {
      const parsed = parseIpcMessage(raw);
      expect(parsed.ok).toBe(false);
      if (!parsed.ok) {
        expect(parsed.reason.length).toBeGreaterThan(0);
      }
    }
  });

  test('describes what it received so the reason is diagnosable', () => {
    expect(rejectionReason(null)).toContain('null');
    expect(rejectionReason('hello')).toContain('string "hello"');
    expect(rejectionReason(7)).toContain('number 7');
    expect(rejectionReason([])).toContain('array');
  });
});
