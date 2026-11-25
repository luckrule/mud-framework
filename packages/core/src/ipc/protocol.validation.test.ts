import { describe, expect, test } from 'bun:test';

import {
  expectRejected,
  rejectionReason,
  validMessages,
} from './protocol.support.ts';
import { parseIpcMessage } from './protocol.ts';

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

  test('keeps extra fields out of the parsed message', () => {
    const parsed = parseIpcMessage({
      type: 'hello',
      futureField: 'ignored',
    });

    expect(parsed).toEqual({
      ok: true,
      message: { type: 'hello' },
    });
  });

  test('drops extra fields from a result', () => {
    // The parsed message is the documented shape, not a copy of the frame: an
    // unknown field must not reach the caller.
    const parsed = parseIpcMessage({
      type: 'result',
      requestId: 'req-1',
      ok: true,
      result: { hp: 3 },
      sessionChange: { type: 'bind', sessionId: 'bound-1' },
      futureField: 'ignored',
    });

    expect(parsed).toEqual({
      ok: true,
      message: {
        type: 'result',
        requestId: 'req-1',
        requestSessionId: undefined,
        ok: true,
        result: { hp: 3 },
        sessionChange: { type: 'bind', sessionId: 'bound-1' },
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
      [{ type: 'hello' }],
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
