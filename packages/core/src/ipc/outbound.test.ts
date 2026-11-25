import { describe, expect, test } from 'bun:test';

import type { ExecutorRequest } from '../shared/executor.ts';
import { requestMessage } from './outbound.ts';

const request: ExecutorRequest = {
  requestId: 'req-1',
  connectionId: 'conn-1',
  clientRequestId: 'client-1',
  method: 'math.add',
  payload: { a: 1, b: 2 },
  timeoutMs: 1000,
};

describe('requestMessage', () => {
  test('uses the remaining budget rather than the original ceiling', () => {
    // The Portal's deadline is authoritative: a replay that arrives late gets
    // what is left, not the budget it started with (INV-007).
    const message = requestMessage(request, 120, 2);

    expect(message.timeoutMs).toBe(120);
    expect(message.replayCount).toBe(2);
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
