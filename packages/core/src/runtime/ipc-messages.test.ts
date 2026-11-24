import { describe, expect, test } from 'bun:test';

import type { ExecutorRequest } from './executor.ts';
import {
  failure,
  nextPortalGeneration,
  requestMessage,
} from './ipc-messages.ts';

const request: ExecutorRequest = {
  requestId: 'req-1',
  connectionId: 'conn-1',
  clientRequestId: 'client-1',
  method: 'math.add',
  payload: { a: 1, b: 2 },
  timeoutMs: 1000,
};

describe('requestMessage', () => {
  test('carries the identity and body of the request', () => {
    const message = requestMessage(request, 900, 0);

    expect(message).toMatchObject({
      type: 'request',
      requestId: 'req-1',
      connectionId: 'conn-1',
      clientRequestId: 'client-1',
      method: 'math.add',
      payload: { a: 1, b: 2 },
    });
  });

  test('uses the remaining budget rather than the original ceiling', () => {
    // The Portal's deadline is authoritative: a replay that arrives late gets
    // what is left, not the budget it started with (INV-007).
    const message = requestMessage(request, 120, 2);

    expect(message.timeoutMs).toBe(120);
    expect(message.replayCount).toBe(2);
  });

  test('the replay count is the one passed in, including 0', () => {
    expect(requestMessage(request, 900, 0).replayCount).toBe(0);
    expect(requestMessage(request, 900, 7).replayCount).toBe(7);
  });

  test('a remaining budget below 1 is clamped to 1', () => {
    // The protocol requires a positive `timeoutMs`; an exhausted budget still
    // has to produce a deliverable message.
    expect(requestMessage(request, 0, 0).timeoutMs).toBe(1);
    expect(requestMessage(request, -250, 1).timeoutMs).toBe(1);
  });

  test('the session id is only present when the request was bound', () => {
    const bound = requestMessage({ ...request, sessionId: 'sess-1' }, 900, 0);
    expect(bound.sessionId).toBe('sess-1');
    expect('sessionId' in requestMessage(request, 900, 0)).toBe(false);
  });
});

describe('failure', () => {
  test('produces the error half of an outcome', () => {
    expect(failure('FRAMEWORK_TIMEOUT', 'deadline passed')).toEqual({
      ok: false,
      error: { code: 'FRAMEWORK_TIMEOUT', message: 'deadline passed' },
    });
  });

  test('does not carry a result or a session id', () => {
    const outcome = failure('INTERNAL_ERROR', 'boom');
    expect(outcome).not.toHaveProperty('result');
    expect(outcome).not.toHaveProperty('sessionId');
  });
});

describe('nextPortalGeneration', () => {
  test('strictly increases', () => {
    const first = nextPortalGeneration();
    const second = nextPortalGeneration();
    const third = nextPortalGeneration();

    expect(second).toBeGreaterThan(first);
    expect(third).toBeGreaterThan(second);
  });

  test('hands out a fresh value on every call', () => {
    const seen = new Set<number>();
    for (let i = 0; i < 50; i += 1) {
      seen.add(nextPortalGeneration());
    }
    expect(seen.size).toBe(50);
  });
});
