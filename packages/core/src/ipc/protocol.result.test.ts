import { describe, expect, test } from 'bun:test';

import { capabilities, expectRejected } from './protocol.support.ts';
import { parseIpcMessage } from './protocol.ts';

describe('parseIpcMessage', () => {
  test('accepts a prepared message', () => {
    const parsed = parseIpcMessage({ type: 'prepared', pid: 4242 });

    expect(parsed).toEqual({
      ok: true,
      message: { type: 'prepared', pid: 4242 },
    });
  });

  test('rejects a prepared without a usable pid', () => {
    for (const pid of [undefined, 0, -1, 1.5, '4242']) {
      expectRejected({ type: 'prepared', pid }, 'pid');
    }
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
        sessionChange: undefined,
      });
    }
  });

  test('accepts a success result carrying a bind change', () => {
    const parsed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      ok: true,
      result: null,
      sessionChange: { type: 'bind', sessionId: 'session-1' },
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionChange: { type: 'bind', sessionId: 'session-1' },
      },
    });
  });

  test('accepts a success result carrying an unbind change', () => {
    const parsed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      ok: true,
      result: null,
      sessionChange: { type: 'unbind' },
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionChange: { type: 'unbind' },
      },
    });
  });

  test('rejects an unknown session change type', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionChange: { type: 'rebind' },
      },
      'bind',
    );
  });

  test('rejects a session change that is not an object', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionChange: 'bind',
      },
      'sessionChange',
    );
  });

  test('rejects a bind change without a usable session id', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionChange: { type: 'bind' },
      },
      'sessionId',
    );
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionChange: { type: 'bind', sessionId: '' },
      },
      'non-empty string',
    );
  });

  test('rejects an unbind change that carries a session id', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionChange: { type: 'unbind', sessionId: 'session-1' },
      },
      'absent for an unbind',
    );
  });

  test('accepts a success result carrying the request session id', () => {
    const parsed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      requestSessionId: 'entry-1',
      ok: true,
      result: null,
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: 'entry-1',
        ok: true,
        result: null,
        sessionChange: { type: 'bind', sessionId: 'bound-1' },
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

  test('reports no request session id when the peer sent none', () => {
    const success = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      ok: true,
    });
    // Value only: `toEqual` ignores undefined own keys, so the presence of the
    // key itself is not part of the contract.
    expect(success).toEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: undefined,
        ok: true,
        result: undefined,
        sessionChange: undefined,
      },
    });
    expect(success.ok).toBe(true);
    if (success.ok && success.message.type === 'result') {
      expect(success.message.requestSessionId).toBeUndefined();
    }

    const failed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      ok: false,
      error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
    });
    expect(failed).toEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: undefined,
        ok: false,
        error: { code: 'FRAMEWORK_TIMEOUT', message: 'timed out' },
      },
    });
    expect(failed.ok).toBe(true);
    if (failed.ok && failed.message.type === 'result') {
      expect(failed.message.requestSessionId).toBeUndefined();
    }
  });

  test('rejects an empty session id on a success bind change', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionChange: { type: 'bind', sessionId: '' },
      },
      'non-empty string',
    );
  });

  test('rejects a non-string session id on a success bind change', () => {
    expectRejected(
      {
        type: 'result',
        requestId: 'req-1',
        ok: true,
        result: null,
        sessionChange: { type: 'bind', sessionId: 7 },
      },
      'sessionId',
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

  test('rejects a null transaction ceiling', () => {
    expectRejected(
      {
        type: 'ready',
        pid: 1,
        capabilities: { ...capabilities, maxTransactionDurationMs: null },
      },
      'maxTransactionDurationMs',
    );
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
        'a positive finite number or Infinity',
      );
    }
  });
});
